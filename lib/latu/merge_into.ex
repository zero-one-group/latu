defmodule Latu.MergeInto do
  @moduledoc """
  A merge being assembled: a source frame, a target table, and the clauses to apply.

  `MergeIntoTableCommand` carries three lists of actions, so a merge is built up before it is
  sent — the same client-side fiction as `Latu.GroupedData`, and inert data in the same way.
  PySpark spells it as a two-level fluent builder (`whenMatched(cond).update({...})`), which
  needs an object per clause; here each clause is one call that says both what it matches and
  what it does.

      source
      |> Latu.as("s")
      |> Latu.merge_into("people", expr("people.id = s.id"))
      |> Latu.when_matched(:update, set: [name: col("s.name")])
      |> Latu.when_not_matched(:insert_all)
      |> Latu.merge()

  Nothing reaches the server until `Latu.merge/2`, so a half-built merge can be passed around,
  stored, or extended in a pipeline like any other value.
  """

  alias Latu.DataFrame
  alias Latu.Plan

  @enforce_keys [:source, :table, :condition]
  defstruct [
    :source,
    :table,
    :condition,
    matched: [],
    not_matched: [],
    not_matched_by_source: [],
    schema_evolution: false
  ]

  @type t :: %__MODULE__{
          source: DataFrame.t(),
          table: String.t() | atom(),
          condition: Plan.expression(),
          matched: [Plan.expression()],
          not_matched: [Plan.expression()],
          not_matched_by_source: [Plan.expression()],
          schema_evolution: boolean()
        }

  @doc false
  def new(%DataFrame{} = source, table, condition, opts \\ []) do
    opts = Keyword.validate!(opts, schema_evolution: false)

    %__MODULE__{
      source: source,
      table: table,
      condition: Plan.merge_condition(condition),
      schema_evolution: opts[:schema_evolution]
    }
  end

  @doc "See `Latu.when_matched/3`."
  @spec when_matched(t(), atom(), keyword()) :: t()
  def when_matched(%__MODULE__{} = merge, action, opts \\ []) do
    add(merge, :matched, action, opts)
  end

  @doc "See `Latu.when_not_matched/3`."
  @spec when_not_matched(t(), atom(), keyword()) :: t()
  def when_not_matched(%__MODULE__{} = merge, action, opts \\ []) do
    add(merge, :not_matched, action, opts)
  end

  @doc "See `Latu.when_not_matched_by_source/3`."
  @spec when_not_matched_by_source(t(), atom(), keyword()) :: t()
  def when_not_matched_by_source(%__MODULE__{} = merge, action, opts \\ []) do
    add(merge, :not_matched_by_source, action, opts)
  end

  # The action is built here rather than at `merge/2`, so a clause that cannot do what it was
  # asked raises at the call site that asked for it. Appended rather than prepended: the wire
  # order is the order the clauses were written, which is the order Spark applies them in.
  defp add(merge, clause, action, opts) do
    built = Plan.merge_action(clause, action, opts)

    Map.update!(merge, clause, &(&1 ++ [built]))
  end

  @doc false
  def command(%__MODULE__{} = merge) do
    Plan.merge_into(merge.source.plan, merge.table, merge.condition,
      matched: merge.matched,
      not_matched: merge.not_matched,
      not_matched_by_source: merge.not_matched_by_source,
      schema_evolution: merge.schema_evolution
    )
  end
end
