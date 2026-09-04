defmodule Latu.ObserveTest do
  use ExUnit.Case, async: true

  import Latu.Wire

  alias Latu.Column
  alias Latu.DataFrame
  alias Latu.Functions, as: F
  alias Latu.Plan
  alias Latu.Protocol.Spark.Connect, as: Proto
  alias Latu.Session

  # Golden plans come from PySpark: python dev/pyspark_oracle.py --generate

  setup do
    %{session: Session.from_url!("sc://h")}
  end

  describe "observe/3 against PySpark" do
    test "one named metric", %{session: session} do
      session
      |> Latu.range(5)
      |> Latu.observe("checks", total: F.count(:id))
      |> assert_wire("observe_one")
    end

    test "an atom names the observation, as everywhere else", %{session: session} do
      session
      |> Latu.range(5)
      |> Latu.observe(:checks, total: F.count(:id))
      |> assert_wire("observe_one")
    end

    test "two metrics keep their order", %{session: session} do
      session
      |> Latu.range(5)
      |> Latu.observe("checks", total: F.count(:id), lowest: F.min(:id))
      |> assert_wire("observe_two")
    end

    test "a verb after the observe sits above it", %{session: session} do
      session
      |> Latu.range(5)
      |> Latu.observe("checks", total: F.count(:id))
      |> Latu.filter(Column.greater(:id, 1))
      |> assert_wire("observe_under_verb")
    end

    test "an observed write carries the node in the command's input", %{session: session} do
      session
      |> Latu.range(5)
      |> Latu.observe("q", rows: F.count(:id))
      |> DataFrame.write_command(format: "parquet", path: "/tmp/latu/out")
      |> assert_wire_command("observe_write")
    end

    test "refuses an empty metric list rather than a node with none", %{session: session} do
      df = Latu.range(session, 5)

      assert_raise ArgumentError, ~r/at least one metric/, fn ->
        Latu.observe(df, "checks", [])
      end
    end
  end

  describe "Plan.observed_names/1" do
    test "a plain plan observes nothing", %{session: session} do
      assert Plan.observed_names(Latu.range(session, 5).plan) == []
    end

    test "finds a node under a verb", %{session: session} do
      df =
        session
        |> Latu.range(5)
        |> Latu.observe("checks", total: F.count(:id))
        |> Latu.filter(Column.greater(:id, 1))

      assert Plan.observed_names(df.plan) == ["checks"]
    end

    test "nested observes come back innermost first", %{session: session} do
      df =
        session
        |> Latu.range(5)
        |> Latu.observe("inner", total: F.count(:id))
        |> Latu.filter(Column.greater(:id, 1))
        |> Latu.observe("outer", kept: F.count(:id))

      assert Plan.observed_names(df.plan) == ["inner", "outer"]
    end

    test "walks a command as well as a relation", %{session: session} do
      command =
        session
        |> Latu.range(5)
        |> Latu.observe("q", rows: F.count(:id))
        |> DataFrame.write_command(format: "parquet", path: "/tmp/latu/out")

      assert Plan.observed_names(command) == ["q"]
    end

    test "finds one hoisted into WithRelations.references", %{session: session} do
      # A subquery carrying an observe: the reference is hoisted beside the root, not under it,
      # which is the shape a naive input-only walk would miss.
      inner = session |> Latu.range(5) |> Latu.observe("inner", total: F.count(:id))

      df = Latu.filter(Latu.range(session, 5), Column.isin(:id, inner))

      assert Plan.observed_names(df.plan) == ["inner"]
    end
  end

  describe "the refusal that gives the write twins their scope" do
    setup %{session: session} do
      %{df: Latu.observe(Latu.range(session, 5), "checks", total: F.count(:id))}
    end

    # A write consumes the frame, so a plain write would produce the metrics and drop them. The
    # list is the scope: a new writer added without deciding which side it falls on shows up
    # here as a miss rather than as a silently dropped metric six months later.
    test "every plain write raises before it reaches the server", %{df: df} do
      writes = [
        {"write/2", fn -> Latu.write(df, path: "/tmp/latu/out") end},
        {"save_as_table/3", fn -> Latu.save_as_table(df, "t") end},
        {"insert_into/3", fn -> Latu.insert_into(df, "t") end},
        {"write_v2/3", fn -> Latu.write_v2(df, "t", mode: :append) end},
        {"merge/2",
         fn ->
           df
           |> Latu.merge_into("t", Column.expr("t.id = s.id"))
           |> Latu.when_not_matched(:insert_all)
           |> Latu.merge()
         end}
      ]

      assert writes |> Enum.reject(&refused?/1) |> Enum.map(&elem(&1, 0)) == []
    end

    test "the message names the observation and the twins", %{df: df} do
      error = assert_raise ArgumentError, fn -> Latu.write(df, path: "/tmp/latu/out") end

      assert error.message =~ ~s("checks")
      assert error.message =~ "write_with_metrics/2"
      assert error.message =~ "merge_with_metrics/2"
    end

    # Reads and previews run and simply do not report, as PySpark's do when nobody reads the
    # Observation. Not connected, so each fails at the transport — proof the guard let it through.
    test "every read runs, and an observed frame composes like any other", %{df: df} do
      reads = [
        {"collect/2", fn -> Latu.collect(df) end},
        {"count/2", fn -> Latu.count(df) end},
        {"first/2", fn -> Latu.first(df) end},
        {"show/2", fn -> Latu.show(df) end},
        {"to_html/2", fn -> Latu.to_html(df) end},
        {"glimpse/2", fn -> Latu.glimpse(df) end},
        {"to_explorer/2", fn -> Latu.to_explorer(df) end},
        {"to_arrow/2", fn -> Latu.to_arrow(df) end},
        {"create_temp_view/3", fn -> Latu.create_temp_view(df, "v") end},
        {"checkpoint/2", fn -> Latu.checkpoint(df) end},
        {"cov/4", fn -> Latu.cov(df, :id, :id) end},
        {"join then count", fn -> df |> Latu.join(df, on: :id) |> Latu.count() end}
      ]

      for {name, call} <- reads do
        assert {:error, %Latu.Error{kind: :connect}} = call.(), "#{name} was refused"
      end

      assert_raise Latu.Error, ~r/not connected/, fn -> Latu.stream(df) end
    end
  end

  # Named rather than `assert_raise` in a loop, so a miss reports *which* action let the plan
  # through instead of one anonymous line number.
  defp refused?({_name, call}) do
    call.()
    false
  rescue
    error in ArgumentError -> error.message =~ "would discard the metrics"
  end

  describe "Plan.plan_id/0" do
    test "hands out distinct, increasing ids from the allocator relations use" do
      ids = for _ <- 1..3, do: Plan.plan_id()

      assert ids == Enum.sort(ids)
      assert length(Enum.uniq(ids)) == 3
    end

    test "a relation's own id comes from the same sequence", %{session: session} do
      before = Plan.plan_id()
      %Proto.Relation{common: %{plan_id: id}} = Latu.range(session, 1).plan

      assert id > before
    end
  end
end
