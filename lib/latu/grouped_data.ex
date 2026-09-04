defmodule Latu.GroupedData do
  @moduledoc """
  A DataFrame with grouping attached, waiting for `agg/2`.

  Spark has no `group_by` relation: `Aggregate` is a single node holding the group type, the
  grouping expressions and the aggregates, so `groupBy(...).agg(...)` is a client-side fiction
  in every Spark client. This struct is where Latu keeps that half-built state, and it is inert
  data like everything else.
  """

  alias Latu.DataFrame
  alias Latu.Plan

  @enforce_keys [:input, :type]
  defstruct [:input, :type, groupings: [], pivot: nil, pivot_values: [], grouping_sets: []]

  @type t :: %__MODULE__{
          input: DataFrame.t(),
          type: :group_by | :rollup | :cube | :pivot | :grouping_sets,
          groupings: [Plan.expression()],
          pivot: String.t() | atom() | nil,
          pivot_values: [term()],
          grouping_sets: [[term()]]
        }

  @doc false
  def new(%DataFrame{} = input, type, groupings, opts \\ []) do
    %__MODULE__{
      input: input,
      type: type,
      groupings: Plan.to_projections(groupings),
      grouping_sets: Keyword.get(opts, :grouping_sets, [])
    }
  end

  @doc """
  Pivot the grouped frame on a column, turning its values into columns.

      df |> Latu.group_by(:suburb) |> Latu.pivot(:year) |> Latu.agg(total: F.sum(:price))
      df |> Latu.group_by(:suburb) |> Latu.pivot(:year, [2025, 2026]) |> Latu.count()

  Without `values` Spark runs a separate query first to find the distinct ones, so pass them
  when you know them. Only a grouped frame can be pivoted, as in PySpark.
  """
  @spec pivot(t(), String.t() | atom(), [term()]) :: t()
  def pivot(grouped, column, values \\ [])

  def pivot(%__MODULE__{type: :group_by} = grouped, column, values) do
    %{grouped | type: :pivot, pivot: column, pivot_values: values}
  end

  def pivot(%__MODULE__{type: type}, _column, _values) do
    raise ArgumentError, "only a grouped frame can be pivoted, not #{inspect(type)}"
  end

  @doc """
  Apply aggregates, giving a DataFrame back.

      df |> Latu.group_by(:suburb) |> Latu.agg(total: F.sum(:price), n: F.count(:id))

  Takes the mixed list `Latu.select/2` takes: trailing keywords name their expressions.
  """
  @spec agg(t(), term()) :: DataFrame.t()
  def agg(%__MODULE__{input: input} = grouped, aggregates) do
    plan =
      Plan.aggregate(input.plan,
        type: grouped.type,
        groupings: grouped.groupings,
        aggregates: Plan.to_projections(aggregates),
        pivot: grouped.pivot,
        pivot_values: grouped.pivot_values,
        grouping_sets: grouped.grouping_sets
      )

    %{input | plan: plan}
  end

  @doc """
  Rows per group, in a column called `count`.

  PySpark's `GroupedData.count()`, which is `count(1)` under that alias — reproduced, so the
  output column matches.
  """
  @spec count(t()) :: DataFrame.t()
  def count(%__MODULE__{} = grouped) do
    agg(grouped, count: Plan.fun("count", [Plan.lit(1)]))
  end
end

defimpl Inspect, for: Latu.GroupedData do
  import Inspect.Algebra

  def inspect(%{type: type, groupings: groupings}, _opts) do
    names = Enum.map_join(groupings, ", ", &Latu.Plan.Inspect.describe/1)

    concat(["#Latu.GroupedData<", to_string(type), ": ", names, ">"])
  end
end
