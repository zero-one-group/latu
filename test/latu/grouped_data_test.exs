defmodule Latu.GroupedDataTest do
  use ExUnit.Case, async: true

  import Latu.Wire

  alias Latu.Column
  alias Latu.GroupedData
  alias Latu.Session

  # Golden plans come from PySpark: python dev/pyspark_oracle.py --generate

  setup do
    %{session: Session.from_url!("sc://h")}
  end

  describe "group_by/2 and count/1" do
    test "count is count(1) under that alias", %{session: session} do
      df = session |> Latu.range(10) |> Latu.group_by(:id) |> Latu.count()

      assert_wire(df, "group_count")
    end

    test "grouping takes names as strings or atoms", %{session: session} do
      grouped = session |> Latu.range(10) |> Latu.group_by("id")

      assert_wire(Latu.count(grouped), "group_count")
    end

    test "nothing is built until agg", %{session: session} do
      grouped = session |> Latu.range(10) |> Latu.group_by(:id)

      assert %GroupedData{type: :group_by, groupings: [_], pivot: nil} = grouped
    end
  end

  describe "agg/2" do
    test "several aggregates keep their order", %{session: session} do
      df =
        session
        |> Latu.range(10)
        |> Latu.group_by(:id)
        |> Latu.agg(total: Column.fun("sum", [:id]), n: Column.fun("count", [:id]))

      assert_wire(df, "group_agg_multi")
    end

    test "a DataFrame aggregates whole, with no grouping", %{session: session} do
      df = Latu.agg(Latu.range(session, 10), total: Column.fun("sum", [:id]))

      assert_wire(df, "agg_no_group")
    end

    test "group_by([]) is the same relation", %{session: session} do
      grouped = session |> Latu.range(10) |> Latu.group_by([])

      assert_wire(Latu.agg(grouped, total: Column.fun("sum", [:id])), "agg_no_group")
    end
  end

  describe "rollup/2 and cube/2" do
    test "rollup", %{session: session} do
      df = session |> Latu.range(10) |> Latu.rollup(:id) |> Latu.count()

      assert_wire(df, "group_rollup")
    end

    test "cube", %{session: session} do
      df = session |> Latu.range(10) |> Latu.cube(:id) |> Latu.count()

      assert_wire(df, "group_cube")
    end
  end

  describe "pivot/3" do
    test "the pivot column carries the input relation's plan_id", %{session: session} do
      df = session |> Latu.range(10) |> Latu.group_by(:id) |> Latu.pivot(:id) |> Latu.count()

      assert_wire(df, "group_pivot")
    end

    test "values are bare literals", %{session: session} do
      df =
        session
        |> Latu.range(10)
        |> Latu.group_by(:id)
        |> Latu.pivot(:id, [1, 2])
        |> Latu.count()

      assert_wire(df, "group_pivot_values")
    end

    test "only a grouped frame can be pivoted", %{session: session} do
      rolled = session |> Latu.range(10) |> Latu.rollup(:id)

      assert_raise ArgumentError, ~r/only a grouped frame/, fn -> Latu.pivot(rolled, :id) end
    end

    test "and not twice", %{session: session} do
      pivoted = session |> Latu.range(10) |> Latu.group_by(:id) |> Latu.pivot(:id)

      assert_raise ArgumentError, ~r/only a grouped frame/, fn -> Latu.pivot(pivoted, :id) end
    end
  end
end
