defmodule Latu.Integration.MergeTest do
  use ExUnit.Case, async: true

  # **A stock Spark cannot run a merge.** `RewriteMergeIntoTable` only rewrites a target that
  # is a v2 `SupportsRowLevelOperations` table; for anything else it falls through
  # (`case _ => m`) and the unrewritten node is refused further down. Iceberg and Delta
  # provide such tables; Spark's own built-in sources do not, and the test server has neither
  # connector.
  #
  # So what is testable here is that the plan is *correct enough to be refused for the right
  # reason*, and the way to show that without pinning the server's wording is a contrast: a
  # merge into a table that exists fails differently from a merge into one that does not. If
  # the first failed at name resolution too, the plan would not be reaching the analyzer at
  # all — which is the failure mode a golden fixture cannot catch.
  #
  # Needs a Spark Connect server on :15002.
  import Latu.Column, only: [col: 1, expr: 1]

  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  @run System.system_time(:millisecond)

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session) end)

    %{session: session}
  end

  defp target(session) do
    table = "merge_target_#{@run}_#{System.unique_integer([:positive])}"
    :ok = Latu.save_as_table(Latu.range(session, 3), table, mode: :overwrite)
    table
  end

  defp source(session) do
    session
    |> Latu.range(5)
    |> Latu.select_expr(["id", "id * 2 as n"])
    |> Latu.as("s")
  end

  describe "merge/1" do
    test "the plan reaches the analyzer, which refuses it on capability", %{session: session} do
      table = target(session)

      assert {:error, %Latu.Error{} = error} =
               session
               |> source()
               |> Latu.merge_into(table, expr("#{table}.id = s.id"))
               |> Latu.when_matched(:update, set: [id: col("s.id")])
               |> Latu.when_not_matched(:insert_all)
               |> Latu.merge()

      message = Exception.message(error)

      refute message =~ "TABLE_OR_VIEW_NOT_FOUND"
      refute message =~ "UNRESOLVED_COLUMN"
    end

    test "a target that does not exist is named as missing", %{session: session} do
      # The control for the case above: same plan shape, a table nobody created, and the
      # server's complaint is about the *name*. Two different refusals is the evidence that
      # the first one got past resolution.
      missing = "merge_absent_#{@run}_#{System.unique_integer([:positive])}"

      assert {:error, %Latu.Error{} = error} =
               session
               |> source()
               |> Latu.merge_into(missing, expr("#{missing}.id = s.id"))
               |> Latu.when_not_matched(:insert_all)
               |> Latu.merge()

      assert Exception.message(error) =~ missing
    end

    test "an empty merge never leaves the client", %{session: session} do
      # `NO_MERGE_ACTION_SPECIFIED` is thrown in `MergeIntoWriter.mergeCommand`, so the answer
      # is fixed and Latu gives it without the round trip.
      table = target(session)

      merge = Latu.merge_into(source(session), table, expr("#{table}.id = s.id"))

      assert_raise ArgumentError, ~r/at least one clause/, fn -> Latu.merge(merge) end
    end
  end
end
