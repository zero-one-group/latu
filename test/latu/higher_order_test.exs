defmodule Latu.HigherOrderTest do
  use ExUnit.Case, async: true

  import Latu.Column
  import Latu.Wire

  alias Latu.Functions, as: F
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
    test "one lambda parameter", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(t: F.transform(F.array([1, 2]), fn x -> add(x, 1) end))
      |> assert_wire("higher_order_transform")
    end

    test "a predicate lambda", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(f: F.filter(F.array([1, 2, 3]), fn x -> greater(x, 1) end))
      |> assert_wire("higher_order_filter")
    end

    test "two lambda parameters", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(t: F.transform(F.array([10, 20]), fn x, i -> add(x, i) end))
      |> assert_wire("higher_order_transform_index")
    end

    test "columns before lambdas, in one argument list", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(a: F.aggregate(F.array([1, 2]), 0, fn acc, x -> add(acc, x) end))
      |> assert_wire("higher_order_aggregate")
    end

    test "two lambdas in one call", %{session: session} do
      aggregate =
        F.aggregate(F.array([1, 2]), 0, fn acc, x -> add(acc, x) end, fn acc ->
          multiply(acc, 2)
        end)

      session
      |> Latu.range(1)
      |> Latu.select(a: aggregate)
      |> assert_wire("higher_order_aggregate_finish")
    end

    test "a lambda nested inside a lambda", %{session: session} do
      nested =
        F.transform(F.array([1, 2]), fn x ->
          F.aggregate(F.array([x, x]), 0, fn a, b -> add(a, b) end)
        end)

      session
      |> Latu.range(1)
      |> Latu.select(n: nested)
      |> assert_wire("higher_order_nested")
    end

    test "two columns and a lambda", %{session: session} do
      zipped = F.zip_with(F.array([1]), F.array([2]), fn x, y -> add(x, y) end)

      session
      |> Latu.range(1)
      |> Latu.select(z: zipped)
      |> assert_wire("higher_order_zip_with")
    end

    test "an optional lambda, present", %{session: session} do
      sorted = F.array_sort(F.array([2, 1]), fn x, y -> subtract(x, y) end)

      session
      |> Latu.range(1)
      |> Latu.select(s: sorted)
      |> assert_wire("array_sort_comparator")
    end
  end

  # =============================================
  # Lambda variables
  # =============================================

  describe "lambda/1" do
    test "names parameters by position, as Spark does" do
      assert ["x_" <> _] = arguments(Plan.lambda(fn x -> x end))
      assert ["x_" <> _, "y_" <> _] = arguments(Plan.lambda(fn x, _y -> x end))
      assert ["x_" <> _, "y_" <> _, "z_" <> _] = arguments(Plan.lambda(fn x, _y, _z -> x end))
    end

    test "refuses an arity Spark has no name for" do
      assert_raise ArgumentError, ~r/one to three/, fn -> Plan.lambda(fn -> 1 end) end
      assert_raise ArgumentError, ~r/one to three/, fn -> Plan.lambda(fn a, _, _, _ -> a end) end
    end

    test "a nested lambda does not shadow the one outside it" do
      outer = Plan.lambda(fn x -> Plan.fun("f", [x, Plan.lambda(fn y -> y end)]) end)

      # Both are the first parameter of their own lambda, so both are `x`. The suffix is the
      # only thing keeping them apart, which is the whole reason it exists — Spark resolves a
      # lambda variable by name, so without it the inner one would capture the outer.
      assert [first, second] = outer |> lambda_names() |> Enum.uniq()
      assert String.starts_with?(first, "x_")
      assert String.starts_with?(second, "x_")
      refute first == second
    end
  end

  describe "normalize_ids/1 and lambda names" do
    test "the same plan built twice differs, and normalises the same", %{session: session} do
      build = fn ->
        session
        |> Latu.range(1)
        |> Latu.select(t: F.transform(F.array([1]), fn x -> add(x, 1) end))
      end

      first = build.()
      second = build.()

      refute first.plan == second.plan
      assert Plan.normalize_ids(first.plan) == Plan.normalize_ids(second.plan)
    end

    test "normalising twice changes nothing", %{session: session} do
      plan =
        session
        |> Latu.range(1)
        |> Latu.select(t: F.transform(F.array([1]), fn x -> add(x, 1) end))
        |> Map.fetch!(:plan)
        |> Plan.normalize_ids()

      assert Plan.normalize_ids(plan) == plan
    end

    test "renumbers from zero, keeping the position letter", %{session: session} do
      plan =
        session
        |> Latu.range(1)
        |> Latu.select(z: F.zip_with(F.array([1]), F.array([2]), fn x, y -> add(x, y) end))
        |> Map.fetch!(:plan)
        |> Plan.normalize_ids()

      assert plan |> lambda_names() |> Enum.uniq() |> Enum.sort() == ["x_0", "y_1"]
    end
  end

  defp arguments(%Proto.Expression{expr_type: {:lambda_function, lambda}}) do
    Enum.flat_map(lambda.arguments, & &1.name_parts)
  end

  # Every lambda variable name anywhere in a tree, declarations and uses alike.
  #
  # Not `inspect/2` and a regex: `Proto.Expression` has an `Inspect` impl, so it renders as
  # `#Latu.Expression<...>` and the names are nowhere in the output.
  defp lambda_names(%Proto.Expression.UnresolvedNamedLambdaVariable{name_parts: parts}), do: parts

  defp lambda_names(term) when is_struct(term) do
    term |> Map.from_struct() |> Map.values() |> lambda_names()
  end

  defp lambda_names(term) when is_map(term), do: term |> Map.values() |> lambda_names()
  defp lambda_names(term) when is_list(term), do: Enum.flat_map(term, &lambda_names/1)
  defp lambda_names({_tag, value}), do: lambda_names(value)
  defp lambda_names(_other), do: []
end
