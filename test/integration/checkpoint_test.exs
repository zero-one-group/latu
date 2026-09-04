defmodule Latu.Integration.CheckpointTest do
  use ExUnit.Case, async: true

  # Every other integration file asserts what a plan means; this one asserts a *lifecycle*.
  # What only the server can answer: whether the plan is actually cut (a `CachedRemoteRelation`
  # is `getDataFrameOrThrow(id).logicalPlan`, so an explain should show a scan over an existing
  # RDD and no `Range`); what a released id does (a double release is a silent no-op, a *query*
  # after a release fails — the opposite pair from a guess); and whether a reliable checkpoint
  # works here at all, which needs `spark.checkpoint.dir` from `docker-compose.yml`. `local:
  # true` needs no directory, which is why the local cases carry the storage levels.
  #
  # Needs a Spark Connect server on :15002.
  alias Latu.Plan

  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session) end)

    %{session: session}
  end

  describe "checkpoint/2" do
    test "hands back a frame rooted at the server's own relation", %{session: session} do
      base = Latu.filter(Latu.range(session, 10), Latu.Column.greater(:id, 5))
      cached = Latu.checkpoint!(base)

      assert {:ok, id} = Plan.cached_relation_id(cached.plan)
      assert id =~ ~r/^[0-9a-f]{8}-[0-9a-f]{4}-/
      assert Latu.collect!(cached) == [%{id: 6}, %{id: 7}, %{id: 8}, %{id: 9}]

      :ok = Latu.release(cached)
    end

    test "the frame outlives the execution that made it", %{session: session} do
      # The whole reason `checkpoint/2` exists beside `with_checkpoint/3`: two separate actions,
      # one materialisation, and the frame is still usable in between.
      cached = Latu.checkpoint!(Latu.range(session, 4))

      assert Latu.count!(cached) == 4
      assert Latu.collect!(Latu.select(cached, :id)) == [%{id: 0}, %{id: 1}, %{id: 2}, %{id: 3}]
      assert Latu.count!(cached) == 4

      :ok = Latu.release(cached)
    end

    test "the plan above the checkpoint is gone", %{session: session} do
      base = Latu.select_expr(Latu.range(session, 10), ["id * 3 as tripled"])

      assert Latu.explain_string!(base) =~ "Range"

      cached = Latu.checkpoint!(base)
      explained = Latu.explain_string!(cached)

      refute explained =~ "Range"
      assert explained =~ "RDD"

      :ok = Latu.release(cached)
    end

    test "a local checkpoint takes a storage level", %{session: session} do
      cached = Latu.checkpoint!(Latu.range(session, 5), local: true, storage_level: :disk_only)

      assert Latu.count!(cached) == 5

      :ok = Latu.release(cached)
    end

    test "a lazy local checkpoint still names its relation", %{session: session} do
      # `eager: false` defers the materialisation to the next action, so the id comes back
      # before anything has been computed. There is nothing client-side that can tell the
      # difference; what this pins is that the command result arrives either way.
      cached = Latu.checkpoint!(Latu.range(session, 5), local: true, eager: false)

      assert {:ok, _id} = Plan.cached_relation_id(cached.plan)
      assert Latu.count!(cached) == 5

      :ok = Latu.release(cached)
    end
  end

  describe "release/1" do
    test "a released id is refused by the server, not silently empty", %{session: session} do
      cached = Latu.checkpoint!(Latu.range(session, 5), local: true)
      assert {:ok, id} = Plan.cached_relation_id(cached.plan)

      assert :ok = Latu.release(cached)
      assert {:error, %Latu.Error{kind: :rpc} = error} = Latu.collect(cached)
      assert Exception.message(error) =~ id
    end

    test "releasing twice is a no-op, because the server invalidates a cache", %{
      session: session
    } do
      cached = Latu.checkpoint!(Latu.range(session, 5), local: true)

      assert :ok = Latu.release(cached)
      assert :ok = Latu.release(cached)
    end
  end

  describe "with_checkpoint/3" do
    test "returns the function's value and releases on the way out", %{session: session} do
      base = Latu.range(session, 6)

      assert {:ok, {count, escaped}} =
               Latu.with_checkpoint(base, [local: true], fn cached ->
                 {Latu.count!(cached), cached}
               end)

      assert count == 6
      assert {:error, %Latu.Error{kind: :rpc}} = Latu.collect(escaped)
    end

    test "a raise inside the function still releases", %{session: session} do
      # The frame is captured by side effect precisely because the bracket will not hand it
      # back on this path, and the release is the thing under test.
      parent = self()

      assert_raise RuntimeError, "boom", fn ->
        Latu.with_checkpoint!(Latu.range(session, 6), [local: true], fn cached ->
          send(parent, {:cached, cached})
          raise "boom"
        end)
      end

      assert_received {:cached, cached}
      assert {:error, %Latu.Error{kind: :rpc}} = Latu.collect(cached)
    end
  end
end
