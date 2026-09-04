defmodule Latu.Integration.StatTest do
  use ExUnit.Case, async: true

  # The stat family against a real server. Reads fixtures/measurements.parquet, whose row shape
  # is documented in dev/make_data_fixtures.py.
  #
  # The cov and corr numbers below come from ALL EIGHT rows with every null read as 0.0, not
  # from the four rows where both columns are non-null. That is Spark's own rule —
  # StatFunctions.calculateCovImpl and calculateCorrImpl both wrap each column in
  # `when(isnull(col(c)), lit(0.0))` before aggregating. The contrast test below pins it against
  # `F.covar_samp`, which drops those rows and gives a different answer on the same data.
  #
  # Needs a Spark Connect server on :15002 and the fixtures generated — `mix fixtures`.
  import Latu.Column

  alias Latu.Functions, as: F

  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session) end)

    df = Latu.read(session, format: "parquet", path: "/fixtures/measurements.parquet")

    %{df: df, blank: Latu.filter(df, is_null(:id))}
  end

  describe "summary/2 and describe/2" do
    test "summary's default eight, in Spark's order", %{df: df} do
      rows = df |> Latu.summary() |> Latu.collect!()

      assert Enum.map(rows, & &1.summary) ==
               ["count", "mean", "stddev", "min", "25%", "50%", "75%", "max"]
    end

    test "only the statistics named", %{df: df} do
      rows = df |> Latu.summary(["count", "max"]) |> Latu.collect!()

      assert Enum.map(rows, & &1.summary) == ["count", "max"]
      # Everything comes back as a string: one column holds counts and maxima at once.
      assert [%{score: "5"}, %{score: "70.0"}] = rows
    end

    test "describe is the fixed five", %{df: df} do
      rows = df |> Latu.describe() |> Latu.collect!()

      assert Enum.map(rows, & &1.summary) == ["count", "mean", "stddev", "min", "max"]
    end

    test "and can be narrowed to some columns", %{df: df} do
      [row | _] = df |> Latu.describe([:score]) |> Latu.collect!()

      assert Enum.sort(Map.keys(row)) == [:score, :summary]
    end
  end

  describe "crosstab/3" do
    test "counts the pairs, with Spark's own column naming", %{df: df} do
      rows = df |> Latu.crosstab(:team, :id) |> Latu.collect!(keys: :strings)

      by_team = Map.new(rows, &{&1["team_id"], &1})

      assert Map.keys(by_team) |> Enum.sort() == ["blue", "null", "red"]
      # red is ids 1, 2 and 5; a null lands under the *string* "null", in the row and the column.
      assert by_team["red"]["1"] == 1
      assert by_team["red"]["3"] == 0
      assert by_team["null"]["7"] == 1
    end
  end

  describe "freq_items/3" do
    test "finds every distinct value when support is low enough", %{df: df} do
      [row] = df |> Latu.freq_items([:score]) |> Latu.collect!(keys: :strings)

      items = Enum.sort(Enum.reject(row["score_freqItems"], &is_nil/1))

      assert items == [10.0, 30.0, 50.0, 70.0]
    end
  end

  describe "sample_by/4" do
    test "a fraction per stratum, and 1.0/0.0 make it exact", %{df: df} do
      teams =
        df
        |> Latu.sample_by(:team, [{"red", 1.0}, {"blue", 0.0}], seed: 42)
        |> Latu.collect!()
        |> Enum.map(& &1.team)

      assert teams == ["red", "red", "red"]
    end

    test "a stratum the fractions do not mention contributes nothing", %{df: df} do
      assert df
             |> Latu.sample_by(:team, [{"red", 1.0}], seed: 42)
             |> Latu.count!() == 3
    end
  end

  describe "the three that collect a scalar" do
    test "cov reads a null as zero rather than dropping the row", %{df: df} do
      # 248.75 / 7 over all eight rows, nulls as 0.0 — not 220 / 3 over the four full ones.
      assert_in_delta Latu.cov!(df, :score, :weight), 248.75 / 7, 0.0001
    end

    test "and so does corr", %{df: df} do
      assert_in_delta Latu.corr!(df, :score, :weight), 0.5433975, 0.0001
    end

    test "which is a different answer from the aggregate of the same name", %{df: df} do
      # `F.covar_samp` drops a row where either value is null; `cov/3` zeroes it. Same data,
      # same question, two numbers — Spark's, not Latu's. 220 / 3 is the four rows where score
      # and weight are both non-null.
      %{dropped: dropped} = df |> Latu.agg(dropped: F.covar_samp(:score, :weight)) |> one()

      assert_in_delta dropped, 220 / 3, 0.0001
      refute_in_delta dropped, Latu.cov!(df, :score, :weight), 0.0001
    end

    test "corr takes a method only because PySpark's signature does", %{df: df} do
      assert Latu.corr!(df, :score, :weight, method: :pearson) ==
               Latu.corr!(df, :score, :weight)
    end

    test "approx_quantile: one name gives one flat list", %{df: df} do
      assert Latu.approx_quantile!(df, :score, [0.0, 0.5, 1.0], 0.0) == [10.0, 30.0, 70.0]
    end

    test "a list of names gives a list per column", %{df: df} do
      assert Latu.approx_quantile!(df, [:score, :weight], [0.5], 0.0) == [[30.0], [3.0]]
    end

    test "and a column with no non-null values gives an empty one", %{blank: blank} do
      assert Latu.approx_quantile!(blank, :score, [0.5], 0.0) == []
    end
  end

  defp one(df) do
    [row] = Latu.collect!(df)

    row
  end
end
