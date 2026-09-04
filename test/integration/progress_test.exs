defmodule Latu.Integration.ProgressTest do
  use ExUnit.Case, async: false

  alias Latu.Progress

  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  #
  # **This file needs `spark.connect.progress.reportInterval=100ms`**, from `docker-compose.yml`.
  # The server's own default is 2 seconds, so without it a query has to run longer than that
  # before it reports anything. A conf change needs
  # `docker compose up -d --force-recreate spark-connect`.
  #
  # async: false, like control_test: `@slow_enough` deliberately occupies the server, and
  # `--master local[1]` is a single task slot.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  # Long enough to cross the report interval several times, and bounded — nothing here waits
  # for it to be cancelled, it simply runs to completion.
  @slow_enough 500_000_000

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session) end)

    %{session: session}
  end

  defp collector do
    owner = self()

    {owner, fn progress -> send(owner, {:progress, progress}) end}
  end

  defp reports do
    receive do
      {:progress, progress} -> [progress | reports()]
    after
      0 -> []
    end
  end

  test "the server reports progress for a query that takes a moment", %{session: session} do
    {_owner, handler} = collector()

    assert {:ok, @slow_enough} =
             session |> Latu.range(@slow_enough) |> Latu.count(progress: handler)

    # The measurement this file exists for. If it comes back empty, the interval conf is not on
    # the server (recreate the container) or Spark reports differently than read.
    assert [_ | _] = seen = reports()

    for progress <- seen do
      assert %Progress{} = progress
      assert is_integer(progress.inflight)
      assert Enum.all?(progress.stages, &is_integer(&1.num_tasks))
      assert Progress.percent(progress) in 0..100
    end
  end

  test "a handler does not change the answer", %{session: session} do
    {_owner, handler} = collector()

    rows = session |> Latu.range(5) |> Latu.collect!(progress: handler)

    assert length(rows) == 5
  end

  test "a fast query may report nothing, and that is not an error", %{session: session} do
    {_owner, handler} = collector()

    # No assertion on the reports on purpose: whether one arrives is the server's timer, not
    # Latu's business. The claim is that the action behaves identically either way.
    assert {:ok, 5} = session |> Latu.range(5) |> Latu.count(progress: handler)
  end

  test "a handler that raises fails the query rather than being swallowed", %{session: session} do
    # It runs in the caller's own process, which is exactly why it cannot be swallowed. Latu
    # holds no process to isolate it in, and `Latu.Progress` says so.
    assert_raise RuntimeError, "handler exploded", fn ->
      session
      |> Latu.range(@slow_enough)
      |> Latu.count!(progress: fn _ -> raise "handler exploded" end)
    end
  end

  test "the option is refused where it is not a function", %{session: session} do
    assert_raise ArgumentError, ~r/:progress is a one-argument function/, fn ->
      session |> Latu.range(5) |> Latu.collect(progress: :yes_please)
    end
  end

  # That a writer accepts `:progress` and does not pass it on is a *plan* property, so the other
  # three writers are checked offline in `test/latu/progress_test.exs` rather than by writing
  # three more tables to a shared warehouse. This one runs end to end because a write is the
  # long-running action progress exists for.
  test "a write takes it, and it never reaches Spark as a writer option", %{session: session} do
    {_owner, handler} = collector()
    path = "/tmp/latu_test/progress_#{System.unique_integer([:positive])}"

    assert :ok =
             session
             |> Latu.range(100)
             |> Latu.write(format: "parquet", path: path, progress: handler)

    assert session |> Latu.read(format: "parquet", path: path) |> Latu.count!() == 100
  end
end
