defmodule Latu.CaseWhen do
  @moduledoc """
  A `CASE WHEN` chain, waiting for `otherwise/2`.

  Spark has no case-when node. The wire carries a single `when` function call whose arguments
  are the branches in order — condition, value, condition, value — followed by the else value if
  there is one; the `case_when` fixture is the proof. So the chain is a client-side fiction, the
  same way `group_by` is (`Latu.GroupedData`), and this struct is where Latu keeps it. Inert
  data, coerced by `Latu.Plan.to_expr/1` wherever an expression is accepted.

  Build it through `Latu.Functions.when_/2` rather than here.

  With no `otherwise/2` the else branch is `NULL`, as in SQL and PySpark. `otherwise(nil)` is
  different: it sends an explicit NULL literal.
  """

  @enforce_keys [:branches]
  defstruct branches: [], otherwise: :none

  @typedoc "`otherwise` is `:none` until set, so an absent else differs from an explicit NULL."
  @type t :: %__MODULE__{
          branches: [{term(), term()}],
          otherwise: :none | {:value, term()}
        }

  @doc false
  def new(condition, value), do: %__MODULE__{branches: [{condition, value}]}

  @doc false
  def when_(%__MODULE__{otherwise: {:value, _}}, _condition, _value) do
    raise ArgumentError, "otherwise/2 ends the chain; add every when_/3 before it"
  end

  def when_(%__MODULE__{} = chain, condition, value) do
    %{chain | branches: chain.branches ++ [{condition, value}]}
  end

  @doc false
  def otherwise(%__MODULE__{otherwise: {:value, _}}, _value) do
    raise ArgumentError, "otherwise/2 is already set"
  end

  def otherwise(%__MODULE__{} = chain, value), do: %{chain | otherwise: {:value, value}}

  @doc """
  The arguments of the one `when` call this chain becomes, in wire order.

  `Latu.Plan.to_expr/1` coerces each one; nothing here touches a proto.
  """
  @spec arguments(t()) :: [term()]
  def arguments(%__MODULE__{branches: branches, otherwise: otherwise}) do
    args = Enum.flat_map(branches, fn {condition, value} -> [condition, value] end)

    case otherwise do
      :none -> args
      {:value, value} -> args ++ [value]
    end
  end
end
