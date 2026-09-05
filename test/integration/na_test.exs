defmodule Latu.Integration.NaTest do
  use ExUnit.Case, async: true

  # The four drop arities, and the one that surprises people — filling a String column with a
  # number does nothing at all, quietly. Reads fixtures/measurements.parquet, whose row shape is
  # documented in dev/make_data_fixtures.py: non-null counts of 4, 4, 4, 3, 3, 2, 3, 0.
  #
  # Needs a Spark Connect server on :15003 and the fixtures generated — `mix fixtures`.
  import Latu.Column

  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15003"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session, release: true) end)

    df = Latu.read(session, format: "parquet", path: "/fixtures/measurements.parquet")

    # The row that is null in every column, picked out by a filter rather than by position —
    # a fill would make it unfindable afterwards, since `id` would no longer be null.
    %{df: df, blank: Latu.filter(df, is_null(:id))}
  end

  describe "drop_na/2, in all four of its arities" do
    test "any null anywhere", %{df: df} do
      assert df |> Latu.drop_na() |> Latu.count!() == 3
    end

    test "only a row that is null all the way across", %{df: df} do
      assert df |> Latu.drop_na(how: :all) |> Latu.count!() == 7
    end

    test "a minimum count of non-nulls", %{df: df} do
      assert df |> Latu.drop_na(min_non_nulls: 3) |> Latu.count!() == 6
    end

    test "and judged on a subset", %{df: df} do
      assert df |> Latu.drop_na(subset: [:score]) |> Latu.count!() == 5
    end

    test "min_non_nulls overrides how, so this is not seven", %{df: df} do
      assert df |> Latu.drop_na(how: :all, min_non_nulls: 4) |> Latu.count!() == 3
    end
  end

  describe "fill_na/3" do
    test "fills the columns whose type fits the value", %{blank: blank} do
      row = one(Latu.fill_na(blank, -1))

      assert row.score == -1.0
      assert row.weight == -1.0
      assert row.id == -1
    end

    test "and silently does nothing to the ones it does not — Spark's rule, not Latu's", %{
      blank: blank
    } do
      assert %{team: nil} = one(Latu.fill_na(blank, -1))
      assert %{team: "unknown", score: nil} = one(Latu.fill_na(blank, "unknown"))
    end

    test "a subset narrows it further", %{blank: blank} do
      row = one(Latu.fill_na(blank, 0, subset: [:score]))

      assert row.score == 0.0
      assert row.weight == nil
    end

    test "pairs give a column each its own value", %{blank: blank} do
      row = one(Latu.fill_na(blank, score: 0, team: "none"))

      assert row.score == 0.0
      assert row.team == "none"
      assert row.weight == nil
    end
  end

  describe "replace/3" do
    test "swaps one value for another, in the columns named", %{df: df} do
      teams =
        df
        |> Latu.replace([{"red", "crimson"}], subset: [:team])
        |> Latu.collect!()
        |> Enum.map(& &1.team)
        |> Enum.uniq()
        |> Enum.sort()

      assert teams == [nil, "blue", "crimson"]
    end

    test "a pair whose type cannot occur in a column simply never matches", %{df: df} do
      assert Latu.collect!(Latu.replace(df, [{"red", "crimson"}], subset: [:score])) ==
               Latu.collect!(df)
    end
  end

  defp one(df) do
    [row] = Latu.collect!(df)

    row
  end
end
