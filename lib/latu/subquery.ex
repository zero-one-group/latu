defmodule Latu.Subquery do
  @moduledoc """
  An expression that references other DataFrames, and the relations it references.

  `SubqueryExpression` carries only the referenced relation's `plan_id`; the relation itself
  has no field to travel in, so it travels here instead — beside the expression — until the
  verb consuming it hoists it into a `WithRelations` wrapper. PySpark keeps the same state on
  its expression objects and collects it in `LogicalPlan._collect_references`.

  Inert data, like `Latu.CaseWhen`: `Latu.Plan`'s builders take one wherever an expression
  belongs and pass the references up, so `F.abs(scalar(other))` carries as readily as
  `scalar(other)` does. Build one with `Latu.Plan.subquery/3`.

  A subquery is the *only* cross-DataFrame reference Spark resolves. A bare `Latu.col/2`
  pointing outside its own tree is refused whether or not the relation is hoisted — measured,
  see `docs/decisions.md` (M9.1).
  """

  @enforce_keys [:expr]
  defstruct [:expr, refs: []]

  @typedoc """
  `expr` is the built proto — an `Expression`, or a `SortOrder` when the reference sits inside
  a sort key. `refs` are the relations to hoist, in the order Spark's client collects them.
  """
  @type t :: %__MODULE__{expr: struct(), refs: [struct()]}

  @doc false
  # No references means nothing to carry, so the bare proto travels on as it always did. That
  # is what keeps every plan without a subquery in it unchanged.
  def wrap(expr, []), do: expr
  def wrap(%__MODULE__{} = subquery, refs), do: %{subquery | refs: subquery.refs ++ refs}
  def wrap(expr, refs) when is_list(refs), do: %__MODULE__{expr: expr, refs: refs}
end

defimpl Inspect, for: Latu.Subquery do
  import Inspect.Algebra

  def inspect(%{expr: expr, refs: refs}, _opts) do
    concat([
      "#Latu.Subquery<",
      Latu.Plan.Inspect.describe(expr),
      ", refs: ",
      to_string(length(refs)),
      ">"
    ])
  end
end
