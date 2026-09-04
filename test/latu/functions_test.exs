defmodule Latu.FunctionsTest do
  use ExUnit.Case, async: true

  import Latu.Wire

  alias Latu.CaseWhen
  alias Latu.Functions, as: F
  alias Latu.Functions.Docs
  alias Latu.Functions.Groups
  alias Latu.Plan
  alias Latu.Protocol.Spark.Connect, as: Proto
  alias Latu.Session

  setup do
    {:ok, session: Session.from_url!("sc://localhost:15002")}
  end

  # =============================================
  # Golden
  # =============================================

  describe "against PySpark" do
    test "variadic", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(a: F.array([1, 2, 3]))
      |> assert_wire("array_fn")
    end

    test "a column and a literal", %{session: session} do
      session
      |> Latu.range(10)
      |> Latu.select(c: F.coalesce([:id, 0]))
      |> assert_wire("coalesce_fn")
    end

    test "one argument", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(d: F.to_date("2026-01-02"))
      |> assert_wire("to_date_fn")
    end

    test "two shapes in one projection", %{session: session} do
      session
      |> Latu.range(10)
      |> Latu.select(u: F.upper("abc"), j: F.concat_ws("-", ["a", "b"]))
      |> assert_wire("string_fns")
    end

    test "distinct is a flag, not a name", %{session: session} do
      session
      |> Latu.range(10)
      |> Latu.agg(d: F.count_distinct(:id))
      |> assert_wire("count_distinct")
    end

    test "when/otherwise is one call, not a nest", %{session: session} do
      size = F.when_(Latu.Column.greater(:id, 5), "big") |> F.otherwise("small")

      session
      |> Latu.range(10)
      |> Latu.select(size: size)
      |> assert_wire("case_when")
    end

    test "an optional trailing argument, present", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(r: F.round(2.5, 1))
      |> assert_wire("round_scale")
    end

    test "distinct over more than one argument", %{session: session} do
      session
      |> Latu.range(10)
      |> Latu.agg(d: F.count_distinct(:id, [1]))
      |> assert_wire("count_distinct_multi")
    end

    test "no arguments at all", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(today: F.current_date())
      |> assert_wire("zero_arg_fn")
    end
  end

  # =============================================
  # The registry
  # =============================================

  describe "every registered function" do
    test "is exported at the arity the registry claims" do
      exported =
        F.__info__(:functions)
        |> Enum.reject(fn {name, _arity} -> name in [:registered, :wire_name, :handwritten] end)
        |> Enum.sort()

      # Everything is either generated from a registry row or declared hand-written. A new
      # function that is neither fails here, which is the point.
      assert Enum.sort(F.registered() ++ F.handwritten()) == exported
    end

    test "builds an UnresolvedFunction carrying its registered wire name" do
      for {name, arity} <- F.registered() do
        expr = call(name, arity)

        assert %Proto.Expression{expr_type: {:unresolved_function, call}} = expr,
               "#{name}/#{arity} did not build an UnresolvedFunction"

        assert call.function_name == F.wire_name(name),
               "#{name}/#{arity} sent #{inspect(call.function_name)}, " <>
                 "registry says #{inspect(F.wire_name(name))}"

        assert Enum.all?(call.arguments, &match?(%Proto.Expression{}, &1)),
               "#{name}/#{arity} left an argument uncoerced"
      end
    end

    test "an always-sent default is not the same shape as an omitted one" do
      # round/1 sends one argument; split/2 sends three. Two different answers to "the caller
      # left it out", and the registry has to know which is which.
      assert %Proto.Expression{expr_type: {:unresolved_function, omitted}} = F.round(:x)
      assert %Proto.Expression{expr_type: {:unresolved_function, filled}} = F.split(:x, ",")

      assert length(omitted.arguments) == 1
      assert length(filled.arguments) == 3
    end

    test "sends no arguments Spark did not ask for" do
      # An optional trailing argument must be absent, not defaulted: `round(x)` and
      # `round(x, 0)` are different plans, and Spark's own default is not always 0.
      assert %Proto.Expression{expr_type: {:unresolved_function, one}} = F.round(:x)
      assert %Proto.Expression{expr_type: {:unresolved_function, two}} = F.round(:x, 2)
      assert length(one.arguments) == 1
      assert length(two.arguments) == 2
    end

    test "an argument Spark always sends is filled with its default", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(s: F.split("a,b", ","))
      |> assert_wire("default_split")
    end

    test "the default comes from PySpark's body, not its signature", %{session: session} do
      # All four of mask's parameters default to None and send "X", "x", "n" and NULL. This is
      # the fixture that would catch a reading of the signature instead.
      session
      |> Latu.range(1)
      |> Latu.select(m: F.mask("AbC1"))
      |> assert_wire("default_mask")
    end

    test "supplying some of them fills only the rest", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(m: F.mask("AbC1", "*"))
      |> assert_wire("default_mask_partial")
    end

    test "sets is_distinct only where the registry says so" do
      assert %Proto.Expression{expr_type: {:unresolved_function, plain}} = F.count(:id)
      assert %Proto.Expression{expr_type: {:unresolved_function, dist}} = F.count_distinct(:id)

      refute plain.is_distinct
      assert dist.is_distinct
      assert plain.function_name == dist.function_name
    end
  end

  describe "when_/otherwise" do
    test "is a struct until something needs an expression" do
      chain = F.when_(:a, 1)

      assert %CaseWhen{} = chain
      assert CaseWhen.arguments(chain) == [:a, 1]
    end

    test "accumulates branches into one call, in order" do
      expr =
        F.when_(:a, 1)
        |> F.when_(:b, 2)
        |> F.otherwise(3)
        |> Plan.to_expr()

      assert %Proto.Expression{expr_type: {:unresolved_function, call}} = expr
      assert call.function_name == "when"
      assert length(call.arguments) == 5
    end

    test "an absent else is not an explicit NULL" do
      assert [_, _] = F.when_(:a, 1) |> CaseWhen.arguments()
      assert [_, _, nil] = F.when_(:a, 1) |> F.otherwise(nil) |> CaseWhen.arguments()
    end

    test "coerces wherever an expression is accepted" do
      session = Session.from_url!("sc://localhost:15002")
      chain = F.when_(Latu.Column.greater(:id, 5), true) |> F.otherwise(false)

      assert %Latu.DataFrame{} = session |> Latu.range(3) |> Latu.filter(chain)
      assert %Latu.DataFrame{} = session |> Latu.range(3) |> Latu.with_columns(big: chain)
    end

    test "refuses a branch added after the else" do
      chain = F.when_(:a, 1) |> F.otherwise(2)

      assert_raise ArgumentError, fn -> F.when_(chain, :b, 3) end
      assert_raise ArgumentError, fn -> F.otherwise(chain, 3) end
    end
  end

  # ExDoc groups this module's page from `priv/function_groups.exs`, through `mix.exs`'s
  # `:default_group_for_doc`. Only the compiler knows what the module exports — some names are
  # built from a module attribute and are invisible to `dev/harvest_function_groups.py` — so the
  # join is proved here rather than there. When this goes red, re-run the harvest; if a name is
  # genuinely absent from Spark's own reference, its `EXTRA` entry is where the reason goes.
  test "every exported function carries one of Spark's own categories" do
    ungrouped =
      F.__info__(:functions)
      |> Enum.map(&Kernel.elem(&1, 0))
      |> Enum.uniq()
      |> Enum.reject(&(&1 in [:registered, :wire_name, :handwritten]))
      |> Enum.reject(&Groups.of/1)
      |> Enum.sort()

    assert ungrouped == []
  end

  # Both harvests are checked in and compiling refuses to go on without them — which catches an
  # absent file, not a truncated one. Floors sit under the counts at Spark 4.2.0 (435 wire names
  # documented, 529 names grouped); a Spark bump moves them up, never down.
  test "neither harvest is truncated" do
    assert Docs.harvested() >= 400
    assert Groups.harvested() >= 500
  end

  # The registry records a name and an arity, not how the arguments are shaped, so this tries
  # the two shapes it generates: bare arguments, and a list for the variadic rows.
  defp call(name, arity) do
    bare = List.duplicate(:c, arity)

    try do
      apply(F, name, bare)
    rescue
      FunctionClauseError -> apply(F, name, List.replace_at(bare, arity - 1, [:c]))
    end
  end
end
