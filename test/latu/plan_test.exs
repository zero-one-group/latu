defmodule Latu.PlanTest do
  use ExUnit.Case, async: true

  import Latu.Wire

  alias Latu.DataFrame
  alias Latu.Plan
  alias Latu.Protocol.Spark.Connect, as: Proto
  alias Latu.Session

  # The moduledoc's example is a doctest so the claim it makes — that a plan builds and inspects
  # with no server — is executed rather than asserted in prose.
  doctest Latu.Plan

  # Golden plans come from PySpark: python dev/pyspark_oracle.py --generate

  setup do
    %{session: Session.from_url!("sc://h")}
  end

  describe "range" do
    test "counts from zero", %{session: session} do
      assert_wire(Latu.range(session, 5), "range")
    end

    test "takes a start and a stop", %{session: session} do
      assert_wire(Latu.range(session, 0, 5), "range")
    end

    test "takes a step", %{session: session} do
      assert_wire(Latu.range(session, 0, 10, 2), "range_step")
    end

    test "refuses a zero step, which Spark would reject anyway", %{session: session} do
      assert_raise ArgumentError, ~r/step cannot be 0/, fn -> Latu.range(session, 0, 5, 0) end
    end

    test "builds without a connection", %{session: session} do
      assert %DataFrame{session: %Session{channel: nil}} = Latu.range(session, 5)
    end

    test "num_partitions is Spark's fourth argument", %{session: session} do
      assert_wire(Latu.range(session, 0, 10, 2, num_partitions: 4), "range_partitions")
    end

    test "num_partitions rides on every arity", %{session: session} do
      frames = [
        Latu.range(session, 10, num_partitions: 4),
        Latu.range(session, 0, 10, num_partitions: 4),
        Latu.range(session, 0, 10, 2, num_partitions: 4)
      ]

      for df <- frames do
        assert {:range, %Proto.Range{num_partitions: 4}} = df.plan.rel_type
      end
    end

    test "left out, the field is absent and the server chooses", %{session: session} do
      assert {:range, %Proto.Range{num_partitions: nil}} = Latu.range(session, 10).plan.rel_type
    end

    test "num_partitions is a positive integer", %{session: session} do
      assert_raise ArgumentError, ~r/^num_partitions is a positive integer, not 0$/, fn ->
        apply(Latu, :range, [session, 10, [num_partitions: 0]])
      end
    end

    test "a mistyped range option is refused", %{session: session} do
      assert_raise ArgumentError, ~r/num_parts/, fn ->
        apply(Latu, :range, [session, 10, [num_parts: 4]])
      end
    end
  end

  describe "relations" do
    test "project keeps named columns", %{session: session} do
      relation = Latu.range(session, 10).plan

      assert_wire(Plan.project(relation, [Plan.col("id")]), "project_cols")
    end

    test "project takes a star", %{session: session} do
      relation = Latu.range(session, 10).plan

      assert_wire(Plan.project(relation, [Plan.star()]), "project_star")
    end

    test "filter takes a raw SQL expression", %{session: session} do
      relation = Latu.range(session, 10).plan

      assert_wire(Plan.filter(relation, Plan.expr("id > 3")), "filter_sql_string")
    end

    test "limit bounds the rows", %{session: session} do
      relation = Latu.range(session, 10).plan

      assert_wire(Plan.limit(relation, 3), "limit")
    end

    test "limit refuses a negative bound", %{session: session} do
      relation = Latu.range(session, 10).plan

      assert_raise FunctionClauseError, fn -> Plan.limit(relation, -1) end
    end
  end

  describe "join" do
    test "on a column name uses Spark's using_columns", %{session: session} do
      left = Latu.range(session, 10).plan
      right = Latu.range(session, 5).plan

      assert_wire(Plan.join(left, right, on: "id"), "join_using")
    end

    test "a list of names works the same", %{session: session} do
      left = Latu.range(session, 10).plan
      right = Latu.range(session, 5).plan

      assert_wire(Plan.join(left, right, on: ["id"]), "join_using")
    end

    test "cross takes no condition", %{session: session} do
      left = Latu.range(session, 3).plan
      right = Latu.range(session, 3).plan

      assert_wire(Plan.join(left, right, how: :cross), "join_cross")
    end

    test "on a condition, with a join type", %{session: session} do
      left = session |> Latu.range(10) |> Map.fetch!(:plan) |> Plan.as("l")
      right = session |> Latu.range(5) |> Map.fetch!(:plan) |> Plan.as("r")
      condition = equals(Plan.col("l.id"), Plan.col("r.id"))

      assert_wire(Plan.join(left, right, on: condition, how: :left), "join_on_expr")
    end

    test "the condition and using_columns are never both set", %{session: session} do
      left = Latu.range(session, 3).plan
      right = Latu.range(session, 3).plan

      {:join, on_names} = Plan.join(left, right, on: "id").rel_type
      {:join, on_condition} = Plan.join(left, right, on: Plan.expr("true")).rel_type

      assert {on_names.using_columns, on_names.join_condition} == {["id"], nil}
      assert {on_condition.using_columns, on_condition.join_condition |> is_nil()} == {[], false}
    end

    test "an unknown join type says what it wanted", %{session: session} do
      left = Latu.range(session, 3).plan

      assert_raise ArgumentError, ~r/unknown join type :left_outer, expected one of/, fn ->
        Plan.join(left, left, how: :left_outer)
      end
    end
  end

  describe "as/2" do
    test "names a relation", %{session: session} do
      assert_wire(Plan.as(Latu.range(session, 10).plan, "a"), "alias_df")
    end

    test "takes an atom", %{session: session} do
      assert_wire(Plan.as(Latu.range(session, 10).plan, :a), "alias_df")
    end
  end

  describe "literals" do
    test "the scalar types PySpark emits", %{session: session} do
      relation = Latu.range(session, 1).plan

      aliased = [
        Plan.as(Plan.lit(1), "i"),
        Plan.as(Plan.lit(1.5), "d"),
        Plan.as(Plan.lit(true), "b"),
        Plan.as(Plan.lit("s"), "s")
      ]

      assert_wire(Plan.project(relation, aliased), "lit_scalars")
    end

    test "nil is NullType, not a string default", %{session: session} do
      relation = Latu.range(session, 1).plan

      assert_wire(Plan.project(relation, [Plan.as(Plan.lit(nil), "n")]), "lit_null")
    end

    test "a date is days since the epoch", %{session: session} do
      relation = Latu.range(session, 1).plan

      assert_wire(Plan.project(relation, [Plan.as(Plan.lit(~D[2026-01-02]), "d")]), "lit_date_py")
    end

    test "a DateTime is an instant, in microseconds", %{session: session} do
      relation = Latu.range(session, 1).plan
      literal = Plan.as(Plan.lit(~U[2026-01-02 03:04:00Z]), "t")

      assert_wire(Plan.project(relation, [literal]), "lit_timestamp")
    end

    test "a NaiveDateTime has no zone, so it is Spark's timestamp_ntz" do
      {:literal, literal} = Plan.lit(~N[2026-01-02 03:04:00]).expr_type

      assert literal.literal_type == {:timestamp_ntz, 1_767_323_040_000_000}
    end

    test "integers pick int32 or int64 by magnitude" do
      assert arm(Plan.lit(1)) == {:integer, 1}
      assert arm(Plan.lit(2_147_483_647)) == {:integer, 2_147_483_647}
      assert arm(Plan.lit(2_147_483_648)) == {:long, 2_147_483_648}
      assert arm(Plan.lit(-2_147_483_649)) == {:long, -2_147_483_649}

      assert_raise ArgumentError, ~r/64-bit integer/, fn -> Plan.lit(2 ** 64) end
    end

    test "a float is always double — Elixir has no 32-bit float" do
      assert arm(Plan.lit(1.5)) == {:double, 1.5}
    end

    test "a decimal carries its value as a string", %{session: session} do
      relation = Latu.range(session, 1).plan
      literal = Plan.as(Plan.lit(Decimal.new("1.50")), "m")

      assert_wire(Plan.project(relation, [literal]), "lit_decimal")
    end

    test "precision and scale are PySpark's constants, not derived from the value" do
      # They read as wrong for "1.50" and are not: Spark honours the value string. Measured
      # against a live server, see docs/decisions.md.
      for text <- ["1.50", "1500", "0.05", "-12.345"] do
        {:decimal, decimal} = arm(Plan.lit(Decimal.new(text)))

        assert {decimal.value, decimal.precision, decimal.scale} == {text, 10, 0}
      end
    end

    test "38 digits is Spark's limit" do
      # Decimal's own parser stops at decimal128's 34, so build the coefficients as integers.
      nines = &Decimal.new(String.to_integer(String.duplicate("9", &1)))

      assert {:decimal, decimal} = arm(Plan.lit(nines.(38)))
      assert String.length(decimal.value) == 38

      assert_raise ArgumentError, ~r/more than 38 digits/, fn -> Plan.lit(nines.(39)) end
    end

    test "a decimal that is not a number is refused" do
      assert_raise ArgumentError, ~r/not a finite decimal/, fn -> Plan.lit(Decimal.new("Inf")) end
    end

    test "an unencodable value says what to reach for instead" do
      assert_raise ArgumentError, ~r/use col\/1, and for anything else use expr\/1/, fn ->
        Plan.lit(self())
      end
    end

    test "a list is not a literal — that is the array function, later" do
      assert_raise ArgumentError, fn -> Plan.lit([1, 2, 3]) end
    end
  end

  describe "coercion" do
    test "in an expression position a binary is a literal" do
      assert arm(Plan.to_expr("books")) == {:string, "books"}
    end

    test "in a name position a binary is a column" do
      assert Plan.to_name("books") == Plan.col("books")
    end

    test "an atom is a column in both positions" do
      assert Plan.to_expr(:price) == Plan.col(:price)
      assert Plan.to_name(:price) == Plan.col(:price)
    end

    test "nil and booleans are literals, not columns named nil and true" do
      null = {:null, %Proto.DataType{kind: {:null, %Proto.DataType.NULL{}}}}

      assert arm(Plan.to_expr(nil)) == null
      assert arm(Plan.to_expr(true)) == {:boolean, true}
    end

    test "an expression passes through untouched" do
      expression = Plan.expr("id > 3")

      assert Plan.to_expr(expression) == expression
      assert Plan.to_name(expression) == expression
    end

    test "numbers and dates are literals in either position" do
      assert arm(Plan.to_expr(3)) == {:integer, 3}
      assert arm(Plan.to_expr(~D[2026-01-02])) == {:date, 20_455}
    end
  end

  describe "inspecting an expression" do
    # Per CLAUDE.md there are no assertions on formatting here. The contract being tested is
    # that Inspect never raises, whatever shape it is handed.
    test "never raises, for any shape" do
      shapes = [
        Plan.col(:price),
        Plan.star(),
        Plan.expr("id > 3"),
        Plan.lit(nil),
        Plan.lit(Decimal.new("1.50")),
        Plan.lit(~D[2026-01-02]),
        Plan.lit(~N[2026-01-02 03:04:00]),
        Plan.as(Plan.col(:id), "x"),
        Plan.fun("+", [:id, 1]),
        Plan.fun("not", [Plan.col(:id)]),
        Plan.fun("upper", [:suburb]),
        Plan.fun("array", []),
        %Proto.Expression{},
        Plan.sort_order(:id),
        Plan.sort_order(:id, direction: :desc, nulls: :last),
        %Proto.Expression.SortOrder{},
        %Proto.Expression{expr_type: {:unresolved_regex, %Proto.Expression.UnresolvedRegex{}}}
      ]

      for shape <- shapes do
        assert is_binary(inspect(shape))
      end
    end
  end

  describe "expressions" do
    test "col takes an atom or a string", %{session: session} do
      relation = Latu.range(session, 10).plan

      assert_wire(Plan.project(relation, [Plan.col(:id)]), "project_cols")
    end

    test "col sets is_metadata_column, which PySpark puts on the wire" do
      {:unresolved_attribute, attribute} = Plan.col("id").expr_type

      assert attribute.is_metadata_column == false
    end
  end

  describe "show_string/2" do
    test "its defaults are the plan behind df.show()", %{session: session} do
      relation = Latu.range(session, 5).plan
      assert_wire(Plan.show_string(relation), "show_string_range")
    end

    test "carries the options through", %{session: session} do
      relation = Latu.range(session, 5).plan
      opts = [num_rows: 5, truncate: 8, vertical: true]

      {:show_string, show} = Plan.show_string(relation, opts).rel_type

      assert {show.num_rows, show.truncate, show.vertical} == {5, 8, true}
    end

    test "truncate takes PySpark's booleans", %{session: session} do
      relation = Latu.range(session, 5).plan

      assert truncation(relation, true) == 20
      assert truncation(relation, false) == 0
      assert truncation(relation, 5) == 5
    end

    test "rejects an unknown option", %{session: session} do
      relation = Latu.range(session, 5).plan

      assert_raise ArgumentError, fn -> Plan.show_string(relation, rows: 5) end
    end
  end

  describe "html_string/2" do
    test "its defaults are the plan behind PySpark's _repr_html_", %{session: session} do
      relation = Latu.range(session, 5).plan
      assert_wire(Plan.html_string(relation), "html_string_range")
    end

    test "carries the options through", %{session: session} do
      relation = Latu.range(session, 5).plan
      assert_wire(Plan.html_string(relation, num_rows: 3, truncate: false), "html_string_options")
    end

    test "has no :vertical to give it", %{session: session} do
      relation = Latu.range(session, 5).plan

      assert_raise ArgumentError, fn -> Plan.html_string(relation, vertical: true) end
    end
  end

  describe "aggregate/2" do
    test "an unknown group type says what it accepts", %{session: session} do
      relation = Latu.range(session, 10).plan

      assert_raise ArgumentError, ~r/unknown group type :sets/, fn ->
        Plan.aggregate(relation, type: :sets)
      end
    end

    test "no pivot means no Pivot message", %{session: session} do
      relation = Latu.range(session, 10).plan
      {:aggregate, aggregate} = Plan.aggregate(relation).rel_type

      assert aggregate.pivot == nil
      assert aggregate.group_type == :GROUP_TYPE_GROUPBY
    end
  end

  describe "to_projections/1" do
    test "a keyword pair becomes an alias and everything else a name" do
      assert Plan.to_projections([:id, "x", total: Plan.fun("sum", [:id])]) ==
               [Plan.col(:id), Plan.col("x"), Plan.as(Plan.fun("sum", [:id]), :total)]
    end

    test "a single column needs no list" do
      assert Plan.to_projections(:id) == [Plan.col(:id)]
    end
  end

  describe "sort_order/2" do
    test "defaults follow the direction" do
      assert Plan.sort_order(:id).null_ordering == :SORT_NULLS_FIRST
      assert Plan.sort_order(:id, direction: :desc).null_ordering == :SORT_NULLS_LAST
    end

    test "nulls can be said explicitly either way" do
      assert Plan.sort_order(:id, nulls: :last).null_ordering == :SORT_NULLS_LAST

      order = Plan.sort_order(:id, direction: :desc, nulls: :first)
      assert order.null_ordering == :SORT_NULLS_FIRST
      assert order.direction == :SORT_DIRECTION_DESCENDING
    end

    test "the child goes through to_name/1, so a string is a column" do
      assert Plan.sort_order("id").child == Plan.col("id")
      assert Plan.sort_order(Plan.expr("id + 1")).child == Plan.expr("id + 1")
    end

    test "an unknown spelling says what it accepts" do
      assert_raise ArgumentError, ~r/unknown sort direction :up/, fn ->
        Plan.sort_order(:id, direction: :up)
      end

      assert_raise ArgumentError, ~r/unknown null ordering :top/, fn ->
        Plan.sort_order(:id, nulls: :top)
      end
    end

    test "to_sort_order/1 passes a key through and coerces anything else" do
      order = Plan.sort_order(:id, direction: :desc)

      assert Plan.to_sort_order(order) == order
      assert Plan.to_sort_order(:id) == Plan.sort_order(:id)
    end
  end

  describe "normalize_ids/1" do
    test "numbers relations depth-first from zero" do
      relation =
        Plan.range(0, 5, 1)
        |> Plan.show_string()
        |> Plan.normalize_ids()

      {:show_string, show} = relation.rel_type

      assert show.input.common.plan_id == 0
      assert relation.common.plan_id == 1
    end

    test "remaps a column reference to its relation's new id" do
      source = Plan.range(0, 5, 1)
      relation = filter(source, column("id", source.common.plan_id))

      {:filter, filtered} = Plan.normalize_ids(relation).rel_type
      {:unresolved_attribute, attribute} = filtered.condition.expr_type

      assert filtered.input.common.plan_id == 0
      assert attribute.plan_id == 0
    end

    test "leaves a normalised plan alone" do
      relation =
        Plan.range(0, 5, 1)
        |> Plan.show_string()
        |> Plan.normalize_ids()

      assert Plan.normalize_ids(relation) == relation
    end
  end

  defp arm(%Proto.Expression{expr_type: {:literal, literal}}), do: literal.literal_type

  defp truncation(relation, truncate) do
    {:show_string, show} = Plan.show_string(relation, truncate: truncate).rel_type
    show.truncate
  end

  # M5 builds operators properly; here they are input to the relations under test.

  defp equals(left, right) do
    %Proto.Expression{
      expr_type:
        {:unresolved_function,
         %Proto.Expression.UnresolvedFunction{function_name: "==", arguments: [left, right]}}
    }
  end

  # M5 will build these properly; here they are input to the walk.

  defp filter(input, condition) do
    %Proto.Relation{
      common: %Proto.RelationCommon{plan_id: :erlang.unique_integer([:positive, :monotonic])},
      rel_type: {:filter, %Proto.Filter{input: input, condition: condition}}
    }
  end

  defp column(name, plan_id) do
    %Proto.Expression{
      expr_type:
        {:unresolved_attribute,
         %Proto.Expression.UnresolvedAttribute{unparsed_identifier: name, plan_id: plan_id}}
    }
  end
end
