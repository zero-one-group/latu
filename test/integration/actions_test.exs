defmodule Latu.Integration.ActionsTest do
  use ExUnit.Case, async: true

  import Latu.Column

  alias Latu.Functions, as: F

  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session) end)
    %{session: session}
  end

  describe "collect/2" do
    test "rows as maps, atom keys, in order", %{session: session} do
      df = session |> Latu.range(3) |> Latu.with_columns(doubled: multiply(:id, 2))

      assert Latu.collect(df) ==
               {:ok, [%{id: 0, doubled: 0}, %{id: 1, doubled: 2}, %{id: 2, doubled: 4}]}
    end

    test "string keys on request", %{session: session} do
      df = session |> Latu.range(1)

      assert Latu.collect(df, keys: :strings) == {:ok, [%{"id" => 0}]}
    end

    test "row order survives a multi-batch result", %{session: session} do
      # range arrives as one batch per partition; 1000 rows is enough for several.
      assert {:ok, rows} = session |> Latu.range(1_000) |> Latu.collect()

      assert length(rows) == 1_000
      assert rows == Enum.sort_by(rows, & &1.id)
      assert List.last(rows) == %{id: 999}
    end

    test "an empty result is an empty list, not an error", %{session: session} do
      assert session |> Latu.range(0) |> Latu.collect() == {:ok, []}
      assert session |> Latu.range(5) |> Latu.filter("id < 0") |> Latu.collect() == {:ok, []}
    end
  end

  describe "count and friends" do
    test "count runs the aggregate server-side", %{session: session} do
      assert session |> Latu.range(10) |> Latu.count() == {:ok, 10}
      assert session |> Latu.range(0) |> Latu.count() == {:ok, 0}
      assert session |> Latu.range(10) |> Latu.count!() == 10
    end

    test "take is limit then collect", %{session: session} do
      assert session |> Latu.range(10) |> Latu.take(3) == {:ok, [%{id: 0}, %{id: 1}, %{id: 2}]}
      assert session |> Latu.range(10) |> Latu.take(0) == {:ok, []}
    end

    test "first and head give one row, or nil from an empty frame", %{session: session} do
      assert session |> Latu.range(10) |> Latu.first() == {:ok, %{id: 0}}
      assert session |> Latu.range(0) |> Latu.first() == {:ok, nil}
      assert session |> Latu.range(10) |> Latu.head() == {:ok, %{id: 0}}
      assert session |> Latu.range(10) |> Latu.head(2) == {:ok, [%{id: 0}, %{id: 1}]}
      assert session |> Latu.range(0) |> Latu.head(2) == {:ok, []}
    end
  end

  describe "the schema guard" do
    test "an interval column is refused with its name, not a NIF panic", %{session: session} do
      # A YEAR TO MONTH interval panics inside the NIF (dev/probe_dtypes.exs). A day-time
      # interval decodes as a duration and passes — dtypes_test pins it.
      df =
        session
        |> Latu.range(1)
        |> Latu.select(gap: expr("INTERVAL '1-2' YEAR TO MONTH"))

      assert {:error, %Latu.Error{kind: :decode, message: message}} = Latu.collect(df)
      assert message =~ "column gap is a year-month interval"
      assert message =~ "cast it to another type first"
    end

    test "and the cast it suggests actually works", %{session: session} do
      df =
        session
        |> Latu.range(1)
        |> Latu.select(gap: cast(expr("INTERVAL '1-2' YEAR TO MONTH"), "string"))

      assert {:ok, [%{gap: gap}]} = Latu.collect(df)
      assert gap =~ "1"
    end

    test "aggregates decode: sum, avg, distinct count", %{session: session} do
      df =
        session
        |> Latu.range(10)
        |> Latu.agg(total: F.sum(:id), mean: F.avg(:id), n: F.count_distinct(:id))

      assert Latu.collect(df) == {:ok, [%{total: 45, mean: 4.5, n: 10}]}
    end
  end
end
