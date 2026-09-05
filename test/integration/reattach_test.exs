defmodule Latu.Integration.ReattachTest do
  use ExUnit.Case, async: true

  alias Latu.Client
  alias Latu.Plan

  # Needs both servers — docker compose up -d. See dev/README.md.
  #
  # :15003 caps a response stream at 1 MB and hands the client a clean EOF with no
  # ResultComplete, expecting a ReattachExecute. A result past that cap is where a client
  # either reattaches or silently returns a short answer, so these are the tests that matter.
  @moduletag :integration
  @moduletag :capture_log

  @normal "sc://localhost:15002"
  @capped "sc://localhost:15003"

  # ~4 MB of Arrow, so the capped server cuts the stream several times over.
  @over_cap 500_000

  test "a small result completes on both servers" do
    for url <- [@normal, @capped] do
      assert {:ok, batches, _executed} = run(url, 5), "failed against #{url}"
      assert rows(batches) == 5
    end
  end

  test "a result past the cap is identical on both servers" do
    assert {:ok, uncapped, _executed} = run(@normal, @over_cap)
    assert {:ok, reattached, _executed} = run(@capped, @over_cap)

    # Offsets are contiguous by construction — Execution refuses a batch that starts anywhere
    # but where the last one ended — so matching totals means no batch was dropped or replayed.
    # The batch *shapes* are deliberately not compared: :15002 runs local[1] and :15003 runs
    # local[*], so its batch boundaries follow the machine's core count (10 partitions on a
    # laptop, 4 on a CI runner) and matched only where the partition size divided evenly.
    assert rows(uncapped) == @over_cap
    assert rows(reattached) == @over_cap
  end

  test "the reattached result decodes to the same rows" do
    assert {:ok, batches, _executed} = run(@capped, 200_000)
    assert {:ok, frame} = Latu.Result.decode(batches)

    assert Explorer.DataFrame.n_rows(frame) == 200_000
    assert Explorer.Series.at(Explorer.DataFrame.pull(frame, "id"), 199_999) == 199_999
  end

  test "stream/2 gives the same rows lazily, across the cuts" do
    session = Latu.connect!(@capped)

    try do
      frames = session |> Latu.range(@over_cap) |> Latu.stream() |> Enum.to_list()

      assert Enum.sum(Enum.map(frames, &Explorer.DataFrame.n_rows/1)) == @over_cap
    after
      Latu.disconnect(session, release: true)
    end
  end

  test "stream/2 can be abandoned part way" do
    session = Latu.connect!(@capped)

    try do
      assert [frame] = session |> Latu.range(@over_cap) |> Latu.stream() |> Enum.take(1)
      assert Explorer.DataFrame.n_rows(frame) > 0

      # The session is still usable, which is the point: the abandoned execution was released
      # and the channel left alone.
      small = Plan.new(Latu.range(session, 5).plan)
      assert {:ok, [_ | _], _executed} = Client.execute(session, small)
    after
      Latu.disconnect(session, release: true)
    end
  end

  defp run(url, count) do
    session = Latu.connect!(url)

    try do
      Client.execute(session, Plan.new(Latu.range(session, count).plan))
    after
      Latu.disconnect(session, release: true)
    end
  end

  defp rows(batches), do: Enum.sum(Enum.map(batches, & &1.row_count))
end
