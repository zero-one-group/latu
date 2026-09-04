defmodule Latu.MergeTest do
  use ExUnit.Case, async: true

  import Latu.Column, only: [col: 1, expr: 1, lit: 1]
  import Latu.Wire

  alias Latu.MergeInto
  alias Latu.Session

  # Golden plans come from PySpark: python dev/pyspark_oracle.py --generate
  #
  # The fixtures go through PySpark's own `mergeInto(...).whenMatched(...)` builder, minus the
  # terminal `merge()` that executes — `merge_cmd` in the oracle. Everything before that call
  # is client-side in both clients, so the whole plan is pinned with no server.
  #
  # The source is aliased in every case, because the merge condition names both sides: the
  # target by its table name, the source by whatever `as/2` called it.

  setup do
    session = Session.from_url!("sc://h")
    widened = session |> Latu.range(5) |> Latu.select_expr(["id", "id * 2 as n"]) |> Latu.as("s")

    %{session: session, plain: Latu.as(Latu.range(session, 5), "s"), widened: widened}
  end

  describe "against PySpark" do
    test "a matched update and an unmatched insert-all", %{widened: source} do
      source
      |> Latu.merge_into("people", expr("people.id = s.id"))
      |> Latu.when_matched(:update, set: [n: col("s.n")])
      |> Latu.when_not_matched(:insert_all)
      |> MergeInto.command()
      |> assert_wire_command("merge_update_insert")
    end

    test "all three clauses, in their star forms", %{plain: source} do
      source
      |> Latu.merge_into("people", expr("people.id = s.id"))
      |> Latu.when_matched(:update_all)
      |> Latu.when_not_matched(:insert_all)
      |> Latu.when_not_matched_by_source(:delete)
      |> MergeInto.command()
      |> assert_wire_command("merge_all_three")
    end

    test "clause conditions, two matched clauses, schema evolution", %{widened: source} do
      source
      |> Latu.merge_into("people", expr("people.id = s.id"), schema_evolution: true)
      |> Latu.when_matched(:delete, on: expr("s.n > 4"))
      |> Latu.when_matched(:update, set: [n: col("s.n")])
      |> MergeInto.command()
      |> assert_wire_command("merge_conditional")
    end

    test "an explicit insert, and a bare Elixir value as a literal", %{plain: source} do
      # PySpark writes `F.lit(True)`; Latu's usual coercion turns a bare `true` into the same
      # literal, and the fixture is what says so.
      source
      |> Latu.merge_into("people", expr("people.id = s.id"))
      |> Latu.when_not_matched(:insert, set: [id: col("s.id"), live: true])
      |> MergeInto.command()
      |> assert_wire_command("merge_insert_assignments")
    end

    test "lit/1 spelled out is the same plan", %{plain: source} do
      source
      |> Latu.merge_into("people", expr("people.id = s.id"))
      |> Latu.when_not_matched(:insert, set: [id: col("s.id"), live: lit(true)])
      |> MergeInto.command()
      |> assert_wire_command("merge_insert_assignments")
    end
  end

  describe "what each clause may do" do
    setup %{plain: source} do
      %{merge: Latu.merge_into(source, "people", expr("people.id = s.id"))}
    end

    test "an unmatched source row cannot be updated or deleted", %{merge: merge} do
      for action <- [:update, :update_all, :delete] do
        error = assert_raise ArgumentError, fn -> Latu.when_not_matched(merge, action) end

        assert error.message =~ "not_matched clause cannot #{inspect(action)}"
        assert error.message =~ "[:insert, :insert_all]"
      end
    end

    test "a matched row cannot be inserted", %{merge: merge} do
      for action <- [:insert, :insert_all] do
        assert_raise ArgumentError, ~r/matched clause cannot/, fn ->
          Latu.when_matched(merge, action)
        end

        assert_raise ArgumentError, ~r/not_matched_by_source clause cannot/, fn ->
          Latu.when_not_matched_by_source(merge, action)
        end
      end
    end

    test "an action Spark has no name for", %{merge: merge} do
      error = assert_raise ArgumentError, fn -> Latu.when_matched(merge, :upsert) end

      assert error.message =~ "cannot :upsert"
    end
  end

  describe "the assignments" do
    setup %{plain: source} do
      %{merge: Latu.merge_into(source, "people", expr("people.id = s.id"))}
    end

    test ":update and :insert need one", %{merge: merge} do
      assert_raise ArgumentError, ~r/update clause needs :set/, fn ->
        Latu.when_matched(merge, :update)
      end

      assert_raise ArgumentError, ~r/insert clause needs :set/, fn ->
        Latu.when_not_matched(merge, :insert)
      end
    end

    test "the star forms and delete refuse one", %{merge: merge} do
      for {clause, action} <- [
            {:when_matched, :update_all},
            {:when_matched, :delete},
            {:when_not_matched, :insert_all}
          ] do
        error =
          assert_raise ArgumentError, fn ->
            apply(Latu, clause, [merge, action, [set: [n: col("s.n")]]])
          end

        assert error.message =~ "assigns nothing"
      end
    end

    test "an empty :set assigns nothing, which is a mistake and not a no-op", %{merge: merge} do
      assert_raise ArgumentError, ~r/:set is empty/, fn ->
        Latu.when_matched(merge, :update, set: [])
      end
    end

    test "a key is a target column name", %{merge: merge} do
      # Not an expression: the key sits on the left of an `=`, and PySpark puts it through
      # `expr(k)` for exactly that reason. A built expression there would encode and then fail
      # at analysis, which is a worse error than this one.
      assert_raise ArgumentError, ~r/merge assignment key/, fn ->
        Latu.when_matched(merge, :update, set: [{col("n"), col("s.n")}])
      end
    end

    test "an unknown option is refused", %{merge: merge} do
      assert_raise ArgumentError, fn -> Latu.when_matched(merge, :update_all, where: nil) end
    end
  end

  describe "the merge itself" do
    test "needs at least one clause", %{plain: source} do
      merge = Latu.merge_into(source, "people", expr("people.id = s.id"))

      error = assert_raise ArgumentError, fn -> MergeInto.command(merge) end

      assert error.message =~ "at least one clause"
      assert error.message =~ "NO_MERGE_ACTION_SPECIFIED"
    end

    test "clauses accumulate in the order they were written", %{plain: source} do
      merge =
        source
        |> Latu.merge_into("people", expr("people.id = s.id"))
        |> Latu.when_matched(:delete, on: expr("s.id = 1"))
        |> Latu.when_matched(:update_all)
        |> Latu.when_not_matched(:insert_all)

      assert length(merge.matched) == 2
      assert length(merge.not_matched) == 1
      assert merge.not_matched_by_source == []

      assert Enum.map(merge.matched, &action_type/1) ==
               [:ACTION_TYPE_DELETE, :ACTION_TYPE_UPDATE_STAR]
    end

    test "is inert until merge/1", %{plain: source} do
      # No session is reachable from this test, so anything that executed would fail at the
      # transport. Building the command is the proof that nothing does.
      merge =
        source
        |> Latu.merge_into("people", expr("people.id = s.id"))
        |> Latu.when_not_matched(:insert_all)

      assert %Latu.Protocol.Spark.Connect.Command{} = MergeInto.command(merge)
    end

    test "a subquery in the condition has nowhere to hoist", %{plain: source, session: session} do
      # Refused when the merge is *started*, not when it is sent: `MergeInto.new/4` coerces
      # the condition through `Plan.merge_condition/1` for that reason. A command has no
      # `WithRelations` to hoist the referenced frame into, which write_v2/3 found first.
      inner = Latu.range(session, 3)

      assert_raise ArgumentError, ~r/nowhere to hoist/, fn ->
        Latu.merge_into(source, "people", Latu.Column.isin(:id, inner))
      end
    end
  end

  defp action_type(expression) do
    {:merge_action, action} = expression.expr_type
    action.action_type
  end
end
