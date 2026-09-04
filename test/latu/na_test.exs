defmodule Latu.NaTest do
  use ExUnit.Case, async: true

  import Latu.Wire

  alias Latu.Plan
  alias Latu.Session

  # Golden plans come from PySpark: python dev/pyspark_oracle.py --generate

  setup do
    %{df: Latu.range(Session.from_url!("sc://h"), 5)}
  end

  describe "fill_na/3 against PySpark" do
    test "a value alone names no columns", %{df: df} do
      df |> Latu.fill_na(0) |> assert_wire("na_fill_scalar")
    end

    test "a subset names them", %{df: df} do
      df |> Latu.fill_na(0, subset: [:id]) |> assert_wire("na_fill_subset")
    end

    test "pairs name a value each", %{df: df} do
      df |> Latu.fill_na(id: 0) |> assert_wire("na_fill_per_column")
    end
  end

  describe "drop_na/2 against PySpark" do
    test "the default sends no min_non_nulls at all — absent already means every column", %{
      df: df
    } do
      df |> Latu.drop_na() |> assert_wire("na_drop_any")
    end

    test "how: :all is the number one", %{df: df} do
      df |> Latu.drop_na(how: :all) |> assert_wire("na_drop_all")
    end

    test "min_non_nulls is PySpark's thresh", %{df: df} do
      df |> Latu.drop_na(min_non_nulls: 2) |> assert_wire("na_drop_thresh")
    end

    test "a subset judges on those columns only", %{df: df} do
      df |> Latu.drop_na(subset: [:id]) |> assert_wire("na_drop_subset")
    end
  end

  describe "replace/3 against PySpark" do
    test "pairs of bare literals", %{df: df} do
      df |> Latu.replace([{1, 10}], subset: [:id]) |> assert_wire("na_replace")
    end
  end

  describe "validation" do
    test "min_non_nulls overrides how, as thresh does", %{df: df} do
      # Normalised, because every build draws a fresh plan_id.
      overridden = Plan.normalize_ids(Latu.drop_na(df, how: :all, min_non_nulls: 3).plan)

      assert overridden == Plan.normalize_ids(Latu.drop_na(df, min_non_nulls: 3).plan)
    end

    test "an unknown how says what it accepts", %{df: df} do
      assert_raise ArgumentError, ~r/how: :any or :all/, fn ->
        apply(Latu, :drop_na, [df, [how: :some]])
      end
    end

    test "a min_non_nulls that is not a positive count is refused", %{df: df} do
      for bad <- [0, -1, "2"] do
        assert_raise ArgumentError, fn -> apply(Latu, :drop_na, [df, [min_non_nulls: bad]]) end
      end
    end

    test "pairs and a subset together are refused rather than one being ignored", %{df: df} do
      assert_raise ArgumentError, ~r/would be ignored/, fn ->
        apply(Latu, :fill_na, [df, [id: 0], [subset: [:id]]])
      end
    end

    test "a list that is not pairs says so", %{df: df} do
      assert_raise ArgumentError, ~r/a value, or \{column, value\} pairs/, fn ->
        apply(Latu, :fill_na, [df, [1, 2]])
      end
    end

    # Two layers refuse two different things: the verb refuses a *shape* (the test above), and
    # the plan layer refuses a *value type*. A list never reaches the second, being a shape.
    test "a fill value Spark has no literal for is refused before the wire", %{df: df} do
      for bad <- [nil, ~D[2026-01-02], {1, 2}] do
        assert_raise ArgumentError, ~r/Spark fills with nothing else/, fn ->
          apply(Latu, :fill_na, [df, bad])
        end
      end
    end

    test "an integer fill goes as a long, because Spark refuses an int32 outright", %{df: df} do
      %{rel_type: {:fill_na, fill}} = Latu.fill_na(df, 1).plan

      assert [%{literal_type: {:long, 1}}] = fill.values
    end

    test "and an integer replacement goes as a double, as PySpark converts it", %{df: df} do
      %{rel_type: {:replace, replace}} = Latu.replace(df, [{1, 10}]).plan

      assert [%{old_value: %{literal_type: {:double, 1.0}}}] = replace.replacements
    end

    test "replace takes pairs, not a flat list", %{df: df} do
      assert_raise ArgumentError, ~r/\{old, new\} pairs/, fn ->
        apply(Latu, :replace, [df, [1, 10]])
      end
    end
  end
end
