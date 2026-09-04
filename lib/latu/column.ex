defmodule Latu.Column do
  @moduledoc """
  Expressions: column references, literals, operators.

      import Latu.Column

      Latu.filter(df, all([greater(:price, 100), not_equal(:suburb, "Reservoir")]))

  Names match `Explorer.Series` wherever the operation is the same one. Latu already depends on
  Explorer, so there is no reason to make you carry two vocabularies.

  Expressions are values. Extract one into a function, put a list of them through `Enum`, hand
  them around — nothing here is a macro.

  `asc/1` and friends build a *sort key*, not an expression: Spark's `SortOrder` is its own
  message. `asc` puts nulls first and `desc` puts them last, which is SQL's asymmetry and
  PySpark's.

  This module is PySpark's `Column` *methods* — operators, predicates, casts, sort keys and
  `over/2`. Spark's free functions are `Latu.Functions`, which is aliased rather than imported.
  Where Spark offers both spellings of one idea (`Column.isNull` and the SQL function `isnull`),
  **this module wins and `Latu.Functions` does not wrap the twin** — nine of them; see
  `docs/deviations.md`.

  There is no `as` here: an expression is named where it is projected — `select(df, total:
  F.sum(:price))`, `Latu.agg/2`, `Latu.with_columns/2`, `Latu.observe/3` — so nothing in this
  module shares a name with `Latu`, and importing both is clean. `Latu.Plan.as/2` is the bare
  alias if a plan needs one.

      def expensive, do: greater(:price, 100)

      Latu.filter(df, all([expensive() | extra]))
  """

  require Logger

  alias Latu.DataFrame
  alias Latu.Plan
  alias Latu.Window

  # Spark's own spellings, which are not uniform: comparison and arithmetic are symbols,
  # boolean is words. Measured from the fixtures, not guessed.
  @binary [
    equal: "==",
    greater: ">",
    greater_equal: ">=",
    less: "<",
    less_equal: "<=",
    equal_null_safe: "<=>",
    add: "+",
    subtract: "-",
    multiply: "*",
    divide: "/",
    remainder: "%",
    pow: "power"
  ]

  # =============================================
  # Building blocks
  # =============================================

  @doc "A column reference. See `Latu.Plan.col/1`."
  @spec col(String.t() | atom()) :: Plan.expression()
  defdelegate col(name), to: Plan

  @doc "A typed literal. See `Latu.Plan.lit/1`."
  @spec lit(term()) :: Plan.expression()
  defdelegate lit(value), to: Plan

  @doc "Raw SQL, parsed by the server. See `Latu.Plan.expr/1`."
  @spec expr(String.t()) :: Plan.expression()
  defdelegate expr(sql), to: Plan

  @doc "Every column."
  @spec star() :: Plan.expression()
  defdelegate star(), to: Plan

  @doc "Cast to a Spark type, spelled as SQL spells it. See `Latu.Plan.cast/2`."
  @spec cast(term(), String.t()) :: Plan.expression()
  defdelegate cast(column, type), to: Plan

  @doc "`cast/2`, but null where a cast would fail. See `Latu.Plan.try_cast/2`."
  @spec try_cast(term(), String.t()) :: Plan.expression()
  defdelegate try_cast(column, type), to: Plan

  @doc """
  Any Spark function, by name.

      fun("upper", [:suburb])

  The escape hatch for a function `Latu.Functions` has no wrapper for. See `Latu.Plan.fun/3`;
  `distinct: true` is the one option.
  """
  @spec fun(String.t(), [term()], keyword()) :: Plan.expression()
  defdelegate fun(name, arguments, opts \\ []), to: Plan

  # Spark's names here are camelCase, unlike its SQL function names. Measured, not guessed.
  @unary [is_null: "isNull", is_not_null: "isNotNull", is_nan: "isNaN"]

  @predicates [
    contains: "contains",
    starts_with: "startsWith",
    ends_with: "endsWith",
    like: "like",
    rlike: "rlike",
    ilike: "ilike"
  ]

  # PySpark's six spellings. `asc` means nulls first and `desc` means nulls last — SQL's rule,
  # and asymmetric, so both are passed rather than left to a default.
  @orders [
    asc: [direction: :asc, nulls: :first],
    asc_nulls_first: [direction: :asc, nulls: :first],
    asc_nulls_last: [direction: :asc, nulls: :last],
    desc: [direction: :desc, nulls: :last],
    desc_nulls_first: [direction: :desc, nulls: :first],
    desc_nulls_last: [direction: :desc, nulls: :last]
  ]

  # =============================================
  # Operators
  # =============================================

  for {name, spark} <- @binary do
    @doc "Spark's `#{spark}`."
    @spec unquote(name)(term(), term()) :: Plan.expression()
    def unquote(name)(left, right), do: Plan.fun(unquote(spark), [left, right])
  end

  @doc """
  Not equal.

  Spark has no `!=` function: PySpark negates `==`, and the `op_compare` fixture is why this is
  not a row in the table above.
  """
  @spec not_equal(term(), term()) :: Plan.expression()
  def not_equal(left, right), do: not_(equal(left, right))

  @doc """
  Every predicate holds.

  Left-folded, so the tree matches PySpark's `a & b & c`. One predicate is itself, and none is
  `lit(true)` — the identity, so a filtered list of predicates composes without a branch.
  """
  @spec all([term()]) :: Plan.expression()
  def all(predicates) when is_list(predicates), do: combine("and", predicates, true)

  @doc "Any predicate holds. `all/1`, with `or`; none is `lit(false)`."
  @spec any([term()]) :: Plan.expression()
  def any(predicates) when is_list(predicates), do: combine("or", predicates, false)

  @doc "Negate a predicate. `not` is an operator — `def not(x)` is a syntax error."
  @spec not_(term()) :: Plan.expression()
  def not_(predicate), do: Plan.fun("not", [predicate])

  # =============================================
  # Predicates
  # =============================================

  for {name, spark} <- @unary do
    @doc "Spark's `#{spark}`."
    @spec unquote(name)(term()) :: Plan.expression()
    def unquote(name)(column), do: Plan.fun(unquote(spark), [column])
  end

  for {name, spark} <- @predicates do
    @doc "Spark's `#{spark}`."
    @spec unquote(name)(term(), term()) :: Plan.expression()
    def unquote(name)(column, value), do: Plan.fun(unquote(spark), [column, value])
  end

  @doc """
  Between two bounds, inclusive.

  Not a Spark function: PySpark composes it as `(c >= lower) and (c <= upper)`, and so does
  this.
  """
  @spec between(term(), term(), term()) :: Plan.expression()
  def between(column, lower, upper) do
    all([greater_equal(column, lower), less_equal(column, upper)])
  end

  @doc """
  One of these values, or one of a DataFrame's rows.

      isin(:suburb, ["Reservoir", "Northcote"])
      isin(:id, Latu.select(recent, :id))

  Spark calls the function `in`, which Elixir cannot, so this keeps PySpark's name.

  A DataFrame builds an IN subquery rather than a function call, as `Column.isin` does in
  PySpark: the frame is hoisted into the plan that uses it and has to have one column — or one
  per field, when the left side is a `F.struct/1`. No same-session check, as in
  `Latu.DataFrame.scalar/1`.
  """
  @spec isin(term(), term()) :: Plan.expression()
  def isin(column, %DataFrame{} = df), do: Plan.subquery(df.plan, :in, values: [column])
  def isin(column, values), do: Plan.fun("in", [column | List.wrap(values)])

  @doc """
  A SQL `LIKE` pattern with an explicit escape character.

  `like/2` sends two arguments and this sends three — Spark distinguishes them, so this is a
  second clause rather than a default. `Latu.Functions` deliberately does not also wrap `like`:
  the Column spelling owns it. See `docs/deviations.md`.
  """
  @spec like(term(), term(), term()) :: Plan.expression()
  def like(column, pattern, escape), do: Plan.fun("like", [column, pattern, escape])

  @doc "Case-insensitive `like/3`."
  @spec ilike(term(), term(), term()) :: Plan.expression()
  def ilike(column, pattern, escape), do: Plan.fun("ilike", [column, pattern, escape])

  # =============================================
  # Windows
  # =============================================

  @doc """
  Evaluate an expression over a window.

      alias Latu.Window, as: W

      window = W.partition_by([:suburb]) |> W.order_by([desc(:price)])

      Latu.with_columns(df, rank: over(F.rank(), window))

  A window with no `partition_by` moves every row into one partition. Spark allows it and it is
  occasionally what you want, so this warns rather than refusing — as PySpark does.
  `W.partition_by([])` says the global window is meant, and is not warned about.
  """
  @spec over(Plan.expression(), Window.t()) :: Plan.expression()
  def over(expression, %Window{partitions: nil} = window) do
    Logger.warning(
      "this window has no partition_by, so every row moves to a single partition — " <>
        "fine for a small frame, a performance cliff otherwise. W.partition_by([]) says it is " <>
        "meant."
    )

    Plan.over(expression, window)
  end

  def over(expression, %Window{} = window), do: Plan.over(expression, window)

  # =============================================
  # Sort keys
  # =============================================

  for {name, opts} <- @orders do
    @doc "A sort key, as PySpark's `Column.#{name}`."
    @spec unquote(name)(term()) :: Plan.sort_order()
    def unquote(name)(column), do: Plan.sort_order(column, unquote(opts))
  end

  defp combine(_op, [], identity), do: Plan.lit(identity)
  defp combine(_op, [only], _identity), do: Plan.to_expr(only)

  defp combine(op, [first | rest], _identity) do
    Enum.reduce(rest, Plan.to_expr(first), fn right, left -> Plan.fun(op, [left, right]) end)
  end
end
