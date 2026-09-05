defmodule Latu.Integration.WindowTest do
  use ExUnit.Case, async: true

  import Latu.Column

  alias Latu.Client
  alias Latu.Error
  alias Latu.Functions, as: F
  alias Latu.Plan
  alias Latu.Result
  alias Latu.Window, as: W

  # Needs a Spark Connect server on :15003 — docker compose up -d spark-reattach.
  # Run with: mix test --include integration
  #
  # A golden test proves Latu's plan matches PySpark's bytes. It cannot prove Spark *accepts*
  # the plan: a frame can encode perfectly and still be refused at analysis, and a frame type
  # can reach the server and mean nothing. Only a live server says.
  @moduletag :integration
  @moduletag :capture_log

  setup do
    session = Latu.connect!("sc://localhost:15003")
    on_exit(fn -> Latu.disconnect(session, release: true) end)
    %{session: session}
  end

  # One execution per DataFrame, then read as many columns as the test needs.
  defp collect(df) do
    {:ok, batches, _executed} = Client.execute(df.session, Plan.new(df.plan))
    {:ok, frame} = Result.decode(batches)

    frame
  end

  defp column(frame, name) do
    frame |> Explorer.DataFrame.pull(name) |> Explorer.Series.to_list()
  end

  test "ranking and a running total, computed by Spark", %{session: session} do
    window = W.partition_by([:bucket]) |> W.order_by([:id])
    running = W.rows_between(window, :unbounded_preceding, :current_row)

    df =
      session
      |> Latu.range(6)
      |> Latu.with_columns(bucket: remainder(:id, 3))
      |> Latu.with_columns(rn: over(F.row_number(), window), total: over(F.sum(:id), running))
      |> Latu.sort([:bucket, :id])

    # Batches arrive in offset order — Latu.Client.Execution refuses one that starts anywhere
    # else — so a globally sorted result decodes in sorted order.
    frame = collect(df)

    assert column(frame, "bucket") == [0, 0, 1, 1, 2, 2]
    assert column(frame, "id") == [0, 3, 1, 4, 2, 5]
    assert column(frame, "rn") == [1, 2, 1, 2, 1, 2]
    assert column(frame, "total") == [0, 3, 1, 5, 2, 7]
  end

  test "a row frame counts rows and a range frame counts values", %{session: session} do
    # ids 0, 3, 6, 9 — gapped on purpose. With consecutive ids the two frames agree and the
    # test would pass even if the frame type never reached the server.
    base =
      session
      |> Latu.range(0, 10, 3)
      |> Latu.with_columns(g: 1)

    window = W.partition_by([:g]) |> W.order_by([:id])

    by_rows = Latu.with_columns(base, s: over(F.sum(:id), W.rows_between(window, -1, 1)))
    by_range = Latu.with_columns(base, s: over(F.sum(:id), W.range_between(window, -1, 1)))

    assert by_rows |> collect() |> column("s") == [3, 9, 18, 15]
    assert by_range |> collect() |> column("s") == [0, 3, 6, 9]
  end

  test "Spark refuses a frame row_number cannot use", %{session: session} do
    # row_number requires exactly ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW. The plan
    # encodes fine, so nothing offline can catch this.
    window = W.partition_by([:id]) |> W.order_by([:id]) |> W.rows_between(-1, 1)

    df = Latu.with_columns(Latu.range(session, 3), rn: over(F.row_number(), window))

    assert {:error, %Error{kind: :rpc}} = Client.execute(session, Plan.new(df.plan))
  end

  test "an unpartitioned window still computes", %{session: session} do
    df =
      session
      |> Latu.range(4)
      |> Latu.with_columns(rn: over(F.row_number(), W.order_by([:id])))
      |> Latu.sort([:id])

    assert df |> collect() |> column("rn") == [1, 2, 3, 4]
  end
end
