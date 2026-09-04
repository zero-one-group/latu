defmodule Latu.Integration.GlimpseTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO
  import Latu.Column

  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  #
  # The rendering is asserted with no server in test/latu/glimpse_test.exs; what only a server
  # can settle is here — that the two round
  # trips agree on the column order, and that `Rows:` is exact exactly when the sample proved
  # it and a lower bound otherwise.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session) end)
    %{session: session}
  end

  defp glimpsed(df, opts \\ []) do
    capture_io(fn -> assert :ok = Latu.glimpse(df, opts) end)
  end

  test "one line per column, with Spark's own name for each type", %{session: session} do
    df = Latu.select(Latu.range(session, 5), id: :id, name: lit("Ada"), score: lit(1.5))

    out = glimpsed(df)

    assert out =~ "Columns: 3\n"
    assert out =~ ~r/\$ id\s+<bigint> 0, 1, 2, 3, 4\n/
    assert out =~ ~r/\$ name\s+<string> "Ada"/
    assert out =~ ~r/\$ score\s+<double> 1\.5/
  end

  test "Rows: is exact when the sample came back short of what it asked for", %{
    session: session
  } do
    assert glimpsed(Latu.range(session, 3)) =~ "Rows: 3\n"
  end

  test "and a lower bound when the sample filled", %{session: session} do
    assert glimpsed(Latu.range(session, 100), num_rows: 2) =~ "Rows: at least 2\n"
  end

  test "count: true pays a full scan for the exact number", %{session: session} do
    assert glimpsed(Latu.range(session, 100), num_rows: 2, count: true) =~ "Rows: 100\n"
  end

  test "an empty frame is the columns and nothing else", %{session: session} do
    out = glimpsed(Latu.filter(Latu.range(session, 5), greater(:id, 99)))

    assert out =~ "Rows: 0\n"
    assert out =~ "Columns: 1\n"
    assert out =~ ~r/\$ id <bigint>\n/
  end

  test "the $ lines follow the schema's order, not a row map's", %{session: session} do
    df = Latu.select(Latu.range(session, 1), z: :id, a: :id, m: :id)

    order =
      df
      |> glimpsed()
      |> String.split("\n")
      |> Enum.drop(2)
      |> Enum.reject(&(&1 == ""))
      |> Enum.map(&(&1 |> String.split() |> Enum.at(1)))

    assert order == ["z", "a", "m"]
  end
end
