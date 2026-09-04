defmodule Latu.CheckpointTest do
  use ExUnit.Case, async: true

  import Latu.Wire

  alias Latu.DataFrame
  alias Latu.Plan
  alias Latu.Session

  # Golden plans come from PySpark: python dev/pyspark_oracle.py --generate
  #
  # The four fixtures build `cat.Checkpoint(...)` and `cat.RemoveRemoteCachedRelation(...)` by
  # hand instead of calling `df.checkpoint()`, because PySpark's verbs here *execute*: they
  # need a live server and they hand back a DataFrame, not a plan. The relation id is the one
  # value neither side can mint offline, so `"abc"` stands in for the server's UUID in both.
  #
  # These assert through `Latu.Plan` rather than `Latu.DataFrame` for the same reason — every
  # DataFrame entry point in this family is an action. The lifecycle those actions implement is
  # `test/integration/checkpoint_test.exs`.

  setup do
    %{session: Session.from_url!("sc://h")}
  end

  describe "Plan.checkpoint/2 against PySpark" do
    test "reliable and eager, which are the defaults", %{session: session} do
      session
      |> Latu.range(5)
      |> plan()
      |> Plan.checkpoint()
      |> assert_wire_command("checkpoint")
    end

    test "local and lazy, with a storage level", %{session: session} do
      session
      |> Latu.range(5)
      |> plan()
      |> Plan.checkpoint(local: true, eager: false, storage_level: :memory_and_disk)
      |> assert_wire_command("checkpoint_local")
    end

    test "refuses a storage level on a reliable checkpoint", %{session: session} do
      # The server reads `storage_level` inside `handleCheckpointCommand`'s `if (getLocal)`
      # branch and calls `checkpoint(eager)` with nothing else in the other, so sending one
      # here would be ignored in silence. PySpark cannot express it: `storageLevel` is
      # `localCheckpoint`'s parameter, not `checkpoint`'s.
      relation = plan(Latu.range(session, 5))

      assert_raise ArgumentError, ~r/local checkpoint only/, fn ->
        Plan.checkpoint(relation, storage_level: :memory_and_disk)
      end
    end

    test "an unknown storage level names the whole set", %{session: session} do
      relation = plan(Latu.range(session, 5))

      error =
        assert_raise ArgumentError, fn ->
          Plan.checkpoint(relation, local: true, storage_level: :ram)
        end

      assert error.message =~ "memory_and_disk_deser"
      assert error.message =~ "off_heap"
    end

    test "an unknown option is refused, not ignored", %{session: session} do
      relation = plan(Latu.range(session, 5))

      assert_raise ArgumentError, fn -> Plan.checkpoint(relation, lazy: true) end
    end
  end

  describe "the relation a checkpoint hands back" do
    test "cached_remote_relation/1 against PySpark" do
      "abc" |> Plan.cached_remote_relation() |> assert_wire("cached_remote_relation")
    end

    test "remove_cached_relation/1 against PySpark" do
      "abc" |> Plan.remove_cached_relation() |> assert_wire_command("remove_cached_relation")
    end

    test "cached_relation_id/1 reads the server's id back" do
      assert Plan.cached_relation_id(Plan.cached_remote_relation("abc")) == {:ok, "abc"}
    end

    test "cached_relation_id/1 says :error for anything else", %{session: session} do
      assert Plan.cached_relation_id(plan(Latu.range(session, 5))) == :error
    end
  end

  describe "release/1 and with_checkpoint/3, offline" do
    test "release/1 refuses a frame that was not checkpointed", %{session: session} do
      # Raises rather than returning an error tuple: passing an ordinary frame is a mistake in
      # the caller's code, not a failure of the release. Checked before any RPC, which is why
      # this one is testable with no server.
      df = Latu.range(session, 5)

      assert_raise ArgumentError, ~r/not checkpointed/, fn -> Latu.release(df) end
    end

    test "with_checkpoint/3 takes a function of one argument", %{session: session} do
      # `apply/3` because Elixir 1.20's type checker excludes an always-raising call from the
      # inferred domain, and a direct call with a literal `fn -> :x end` fails the build.
      df = Latu.range(session, 5)

      assert_raise FunctionClauseError, fn ->
        apply(Latu, :with_checkpoint, [df, [], fn -> :nope end])
      end
    end
  end

  defp plan(%DataFrame{} = df), do: df.plan
end
