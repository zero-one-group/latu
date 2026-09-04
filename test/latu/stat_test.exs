defmodule Latu.StatTest do
  use ExUnit.Case, async: true

  import Latu.Column, only: [cast: 2]
  import Latu.Wire

  alias Latu.Plan
  alias Latu.Session

  # Golden plans come from PySpark: python dev/pyspark_oracle.py --generate

  setup do
    %{df: Latu.range(Session.from_url!("sc://h"), 5)}
  end

  describe "the lazy relations against PySpark" do
    test "summary sends no statistics and lets the server pick", %{df: df} do
      df |> Latu.summary() |> assert_wire("stat_summary")
    end

    test "or the ones named", %{df: df} do
      df |> Latu.summary(["count", "min", "max"]) |> assert_wire("stat_summary_named")
    end

    test "describe is its own relation, not an option on summary", %{df: df} do
      df |> Latu.describe() |> assert_wire("stat_describe")
    end

    test "describe over the columns named", %{df: df} do
      df |> Latu.describe(:id) |> assert_wire("stat_describe_cols")
    end

    test "crosstab takes two names", %{df: df} do
      df |> Latu.crosstab(:id, :id) |> assert_wire("stat_crosstab")
    end

    test "freq_items sends the default support rather than leaving it off", %{df: df} do
      df |> Latu.freq_items([:id]) |> assert_wire("stat_freq_items")
    end

    test "and a given one", %{df: df} do
      df |> Latu.freq_items(:id, support: 0.4) |> assert_wire("stat_freq_items_support")
    end

    test "sample_by strata are bare literals under lit/1's own rule", %{df: df} do
      df
      |> Latu.sample_by(:id, [{1, 0.5}, {2, 1.0}], seed: 42)
      |> assert_wire("stat_sample_by")
    end

    test "and its column may be an expression", %{df: df} do
      df
      |> Latu.sample_by(cast(:id, "string"), [{"a", 0.5}], seed: 42)
      |> assert_wire("stat_sample_by_strings")
    end
  end

  describe "the action relations against PySpark" do
    # Their verbs collect, so the plan is built here rather than piped: PySpark has no public
    # method that hands one back either, which is why the fixtures use its LogicalPlan classes.
    test "cov", %{df: df} do
      assert_wire(Plan.cov(df.plan, :id, :id), "stat_cov")
    end

    test "corr sends the method even when nobody asked for one", %{df: df} do
      assert_wire(Plan.corr(df.plan, :id, :id), "stat_corr")
    end

    test "approx_quantile carries the probabilities and the error", %{df: df} do
      assert_wire(
        Plan.approx_quantile(df.plan, [:id], [0.0, 0.5, 1.0], 0.01),
        "stat_approx_quantile"
      )
    end
  end

  describe "validation" do
    test "a summary statistic is a name, and says so when it is not", %{df: df} do
      assert_raise ArgumentError, ~r/a summary statistic is a name/, fn ->
        apply(Latu, :summary, [df, [1]])
      end
    end

    test "corr has one method and refuses any other", %{df: df} do
      assert_raise ArgumentError, ~r/:pearson is the only method/, fn ->
        apply(Latu, :corr, [df, :id, :id, [method: :spearman]])
      end
    end

    test "a probability outside 0..1 is refused before the wire", %{df: df} do
      for bad <- [-0.1, 1.5, "0.5"] do
        assert_raise ArgumentError, ~r/a probability is between 0 and 1/, fn ->
          apply(Plan, :approx_quantile, [df.plan, [:id], [bad], 0.01])
        end
      end
    end

    test "a negative relative error is refused", %{df: df} do
      assert_raise ArgumentError, ~r/relative error is not negative/, fn ->
        apply(Plan, :approx_quantile, [df.plan, [:id], [0.5], -1.0])
      end
    end

    test "a stratum is a value, so an atom — a column name everywhere else — is refused", %{
      df: df
    } do
      assert_raise ArgumentError, ~r/a stratum is a string or a number/, fn ->
        apply(Latu, :sample_by, [df, :id, [{:red, 0.5}], [seed: 1]])
      end
    end

    test "fractions come as pairs, not a flat list", %{df: df} do
      assert_raise ArgumentError, ~r/\{stratum, fraction\} pairs/, fn ->
        apply(Latu, :sample_by, [df, :id, ["red", 0.5], [seed: 1]])
      end
    end

    test "a map of fractions is accepted too", %{df: df} do
      from_map = Latu.sample_by(df, :id, %{1 => 0.5}, seed: 42)
      from_list = Latu.sample_by(df, :id, [{1, 0.5}], seed: 42)

      assert Plan.normalize_ids(from_map.plan) == Plan.normalize_ids(from_list.plan)
    end

    test "a seedless sample_by draws one, so the plan differs between builds", %{df: df} do
      %{rel_type: {:sample_by, first}} = Latu.sample_by(df, :id, [{1, 0.5}]).plan
      %{rel_type: {:sample_by, second}} = Latu.sample_by(df, :id, [{1, 0.5}]).plan

      assert is_integer(first.seed)
      assert first.seed != second.seed
    end

    test "an integer stratum keeps lit/1's magnitude rule, unlike a fill value", %{df: df} do
      %{rel_type: {:sample_by, sample}} = Latu.sample_by(df, :id, [{1, 0.5}], seed: 1).plan

      assert [%{stratum: %{literal_type: {:integer, 1}}}] = sample.fractions
    end
  end
end
