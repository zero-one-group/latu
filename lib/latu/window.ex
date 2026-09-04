defmodule Latu.Window do
  @moduledoc """
  A window specification: how to partition, how to order, and which rows the frame covers.

      alias Latu.Window, as: W

      W.partition_by([:suburb])
      |> W.order_by([desc(:price)])
      |> W.rows_between(:unbounded_preceding, :current_row)

  Alias it, as PySpark does. `order_by/2` would collide with `Latu.order_by/2` on import.

  There is no window relation and no window message of its own: `Latu.Column.over/2` folds
  this into an `Expression.Window` that rides inside an ordinary `Project` or `WithColumns`.
  So this struct is client-side state, like `Latu.GroupedData` and `Latu.CaseWhen`, and
  reaches the wire only through `Latu.Column.over/2`.

  Not to be confused with **`Latu.Functions.window/2`**, Spark's tumbling *time* window, which is
  an ordinary function over a timestamp column and has nothing to do with this.

  ## Frame boundaries

  `:current_row`, `:unbounded_preceding` (lower only), `:unbounded_following` (upper only), or an
  integer offset. Zero is `:current_row` — Spark has no other reading of it, and PySpark encodes
  it that way, so `rows_between(0, 2)` and `rows_between(:current_row, 2)` are the same plan.

  `rows_between/3` counts rows and `range_between/3` counts *values of the ordering column*, so
  the latter needs exactly one `order_by` key to mean anything.
  """

  alias Latu.Plan

  # `partitions: nil` is a specification nobody partitioned; `[]` is one deliberately left
  # global with `partition_by([])`. `Latu.Column.over/2` warns on the first only.
  defstruct partitions: nil, orders: [], frame: nil

  @typedoc "The two unbounded atoms are positional; see `rows_between/3`."
  @type boundary :: :current_row | :unbounded_preceding | :unbounded_following | integer()

  @type t :: %__MODULE__{
          partitions: [Plan.expression()] | nil,
          orders: [Plan.sort_order()],
          frame: nil | {:rows | :range, boundary(), boundary()}
        }

  @doc """
  Partition the rows. Starts a specification, or replaces the partitioning of one.

  A window nobody partitioned moves every row to a single partition; `Latu.Column.over/2` says
  so out loud, as PySpark does. `partition_by([])` is the explicit global window, and quiet.

  A single column needs no list, as everywhere else.
  """
  @spec partition_by([term()] | term()) :: t()
  def partition_by(columns), do: partition_by(%__MODULE__{}, columns)

  @doc "Replace the partitioning of an existing specification."
  @spec partition_by(t(), [term()] | term()) :: t()
  def partition_by(%__MODULE__{} = window, columns) do
    %{window | partitions: columns |> List.wrap() |> Enum.map(&Plan.to_expr/1)}
  end

  @doc """
  Order within each partition. Takes column names or sort keys.

  A bare name sorts ascending with nulls first, which is `Latu.Column.asc/1`'s rule and SQL's.
  """
  @spec order_by([term()] | term()) :: t()
  def order_by(columns), do: order_by(%__MODULE__{}, columns)

  @doc "Replace the ordering of an existing specification."
  @spec order_by(t(), [term()] | term()) :: t()
  def order_by(%__MODULE__{} = window, columns) do
    %{window | orders: columns |> List.wrap() |> Enum.map(&Plan.to_sort_order/1)}
  end

  @doc """
  A frame counted in rows, relative to the current one.

      W.partition_by([:suburb]) |> W.rows_between(-1, 1)

  Offsets are 32-bit here and 64-bit in `range_between/3`, which is Spark's asymmetry, not a
  choice — see `docs/decisions.md`.
  """
  @spec rows_between(t(), boundary(), boundary()) :: t()
  def rows_between(%__MODULE__{} = window, lower, upper) do
    %{window | frame: {:rows, check(lower, :lower), check(upper, :upper)}}
  end

  @doc "A frame counted in values of the ordering column. `rows_between/3` counts rows instead."
  @spec range_between(t(), boundary(), boundary()) :: t()
  def range_between(%__MODULE__{} = window, lower, upper) do
    %{window | frame: {:range, check(lower, :lower), check(upper, :upper)}}
  end

  # The wire has one `unbounded` flag and takes the direction from which side it sits on, so a
  # boundary naming the wrong direction would encode as a valid plan meaning something else.
  defp check(:unbounded_following, :lower) do
    raise ArgumentError, ":unbounded_following is an upper bound; the lower one is preceding"
  end

  defp check(:unbounded_preceding, :upper) do
    raise ArgumentError, ":unbounded_preceding is a lower bound; the upper one is following"
  end

  defp check(boundary, _side)
       when boundary in [:current_row, :unbounded_preceding, :unbounded_following] do
    boundary
  end

  defp check(offset, _side) when is_integer(offset), do: offset

  defp check(other, side) do
    raise ArgumentError,
          "#{side} bound #{inspect(other)} is not an offset, :current_row, " <>
            ":unbounded_preceding or :unbounded_following"
  end
end
