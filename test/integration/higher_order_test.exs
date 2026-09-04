defmodule Latu.Integration.HigherOrderTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO
  import Latu.Column

  alias Latu.Client
  alias Latu.Functions, as: F
  alias Latu.Plan
  alias Latu.Result

  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  # Run with: mix test --include integration
  #
  # A golden test proves the lambda encodes the way PySpark encodes it. Only a server proves
  # Spark can *resolve* the variables — a name that failed to reach both the declaration and
  # every use would still be a well-formed plan.
  @moduletag :integration
  @moduletag :capture_log

  setup do
    session = Latu.connect!("sc://localhost:15002")
    on_exit(fn -> Latu.disconnect(session) end)
    %{session: session}
  end

  # Array results are rendered by Spark rather than decoded through Arrow: an Arrow dtype Polars
  # cannot handle surfaces as a NIF panic with no diagnostic, and a list column is exactly the
  # kind of thing to find that out with. Scalars go through Result.
  defp shown(df), do: capture_io(fn -> Latu.show(df) end)

  defp scalar(df, column) do
    {:ok, batches, _executed} = Client.execute(df.session, Plan.new(df.plan))
    {:ok, frame} = Result.decode(batches)

    frame |> Explorer.DataFrame.pull(column) |> Explorer.Series.at(0)
  end

  test "transform and filter run, and the variables resolve", %{session: session} do
    df =
      session
      |> Latu.range(1)
      |> Latu.select(
        t: F.transform(F.array([1, 2, 3]), fn x -> multiply(x, 10) end),
        f: F.filter(F.array([1, 2, 3]), fn x -> greater(x, 1) end)
      )

    output = shown(df)

    assert output =~ "[10, 20, 30]"
    assert output =~ "[2, 3]"
  end

  test "a two-parameter lambda gets the element and its index", %{session: session} do
    df =
      session
      |> Latu.range(1)
      |> Latu.select(t: F.transform(F.array([10, 20, 30]), fn x, i -> add(x, i) end))

    assert shown(df) =~ "[10, 21, 32]"
  end

  test "aggregate folds, and the finish lambda runs", %{session: session} do
    plain =
      session
      |> Latu.range(1)
      |> Latu.select(a: F.aggregate(F.array([1, 2, 3]), 0, fn acc, x -> add(acc, x) end))

    finished =
      session
      |> Latu.range(1)
      |> Latu.select(
        a:
          F.aggregate(F.array([1, 2, 3]), 0, fn acc, x -> add(acc, x) end, fn acc ->
            multiply(acc, 2)
          end)
      )

    assert scalar(plain, "a") == 6
    assert scalar(finished, "a") == 12
  end

  test "a nested lambda resolves both scopes", %{session: session} do
    # The inner lambda's first parameter has the same position — and so the same letter — as the
    # outer one. If the suffix were dropped, Spark would resolve the inner `x` to the outer.
    nested =
      F.transform(F.array([1, 2, 3]), fn x ->
        F.aggregate(F.array([x, x]), 0, fn a, b -> add(a, b) end)
      end)

    df = session |> Latu.range(1) |> Latu.select(n: nested)

    assert shown(df) =~ "[2, 4, 6]"
  end

  test "zip_with pairs two arrays", %{session: session} do
    zipped = F.zip_with(F.array([1, 2]), F.array([10, 20]), fn x, y -> add(x, y) end)

    assert shown(session |> Latu.range(1) |> Latu.select(z: zipped)) =~ "[11, 22]"
  end

  test "array_sort with and without a comparator", %{session: session} do
    plain = Latu.select(Latu.range(session, 1), s: F.array_sort(F.array([3, 1, 2])))

    reversed =
      Latu.select(Latu.range(session, 1),
        s: F.array_sort(F.array([3, 1, 2]), fn x, y -> subtract(y, x) end)
      )

    assert shown(plain) =~ "[1, 2, 3]"
    assert shown(reversed) =~ "[3, 2, 1]"
  end
end
