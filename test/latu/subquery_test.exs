defmodule Latu.SubqueryTest do
  @moduledoc """
  Subqueries: the one cross-DataFrame reference Spark resolves, and the `WithRelations`
  hoisting they force. `test/latu/references_test.exs` has the bare `col/2` case, which Spark
  refuses hoisted or not.
  """

  use ExUnit.Case, async: true

  import Latu.Wire

  alias Latu.Column
  alias Latu.Functions, as: F
  alias Latu.Plan
  alias Latu.Protocol.Spark.Connect, as: Proto
  alias Latu.Session
  alias Latu.Window, as: W

  setup do
    session = Session.from_url!("sc://h")

    %{session: session, a: Latu.range(session, 1), b: Latu.range(session, 5)}
  end

  describe "the wire shape" do
    test "a scalar subquery in a projection", %{a: a, b: b} do
      assert_wire(Latu.select(b, Latu.scalar(a)), "subquery_scalar")
    end

    test "a scalar subquery in a condition", %{a: a, b: b} do
      condition = Column.less(Latu.col(b, :id), Latu.scalar(a))

      assert_wire(Latu.filter(b, condition), "subquery_filter")
    end

    test "an exists subquery", %{a: a, b: b} do
      assert_wire(Latu.filter(b, Latu.exists(a)), "subquery_exists")
    end

    test "an in subquery carries the values it tests", %{session: session, b: b} do
      a = Latu.range(session, 2)

      assert_wire(Latu.filter(b, Column.isin(Latu.col(b, :id), a)), "subquery_in")
    end

    test "a struct on the left sends the struct's children", %{session: session, b: b} do
      a = Latu.range(session, 2)
      id = Latu.col(b, :id)

      assert_wire(Latu.filter(b, Column.isin(F.struct([id, id]), a)), "subquery_in_struct")
    end

    test "a subquery nested inside a function call", %{a: a, b: b} do
      scaled = Column.multiply(Latu.scalar(a), -1)

      assert_wire(Latu.select(b, F.abs(scaled)), "subquery_nested")
    end

    test "two references in one relation", %{session: session, a: a} do
      b = Latu.range(session, 2)
      c = Latu.range(session, 5)

      assert_wire(Latu.select(c, [Latu.scalar(a), Latu.scalar(b)]), "subquery_two_refs")
    end
  end

  describe "the public API" do
    test "scalar/1 and exists/1 are the plan builder", %{a: a} do
      assert Latu.scalar(a) == Plan.subquery(a.plan, :scalar)
      assert Latu.exists(a) == Plan.subquery(a.plan, :exists)
    end

    test "table_arg has no caller yet, but the plan layer builds it", %{a: a} do
      %Latu.Subquery{expr: %{expr_type: {:subquery_expression, node}}} =
        Plan.subquery(a.plan, :table_arg)

      assert node.subquery_type == :SUBQUERY_TYPE_TABLE_ARG
    end
  end

  describe "hoisting" do
    test "wraps the relation, and the wrapper is what a later reference tags", %{a: a, b: b} do
      hoisted = Latu.select(b, Plan.subquery(a.plan, :scalar))

      assert {:with_relations, wrapper} = hoisted.plan.rel_type
      assert {:project, _project} = wrapper.root.rel_type
      assert [%Proto.Relation{}] = wrapper.references
      assert hoisted.plan.common.plan_id != wrapper.root.common.plan_id

      {:unresolved_attribute, reference} = Latu.col(hoisted, :id).expr_type

      assert reference.plan_id == hoisted.plan.common.plan_id
    end

    test "leaves a plan with no reference in it alone", %{b: b} do
      assert {:project, _} = Latu.select(b, :id).plan.rel_type
      assert {:filter, _} = Latu.filter(b, Column.greater(:id, 3)).plan.rel_type
    end

    test "hoists a frame that is already in the tree, as PySpark does", %{b: b} do
      condition = Column.less(Latu.col(b, :id), Plan.subquery(b.plan, :scalar))

      assert {:with_relations, %{references: [_one]}} = Latu.filter(b, condition).plan.rel_type
    end

    test "collects from every argument, in order", %{session: session, a: a, b: b} do
      c = Latu.range(session, 9)
      first = Plan.subquery(a.plan, :scalar)
      second = Plan.subquery(b.plan, :scalar)

      {:with_relations, wrapper} = Latu.select(c, [first, second]).plan.rel_type

      assert Enum.map(wrapper.references, & &1.common.plan_id) ==
               [a.plan.common.plan_id, b.plan.common.plan_id]
    end
  end

  describe "a reference travels through" do
    setup %{a: a} do
      %{scalar: Plan.subquery(a.plan, :scalar)}
    end

    test "a function call", %{scalar: scalar} do
      assert %Latu.Subquery{refs: [_one]} = F.abs(scalar)
    end

    test "an alias and a cast", %{scalar: scalar} do
      assert %Latu.Subquery{refs: [_alias]} = Plan.as(scalar, :s)
      assert %Latu.Subquery{refs: [_cast]} = Column.cast(scalar, "int")
    end

    test "a lambda body", %{scalar: scalar} do
      assert %Latu.Subquery{refs: [_one]} =
               F.transform(:xs, fn x -> Column.multiply(x, scalar) end)
    end

    test "a window function", %{scalar: scalar} do
      assert %Latu.Subquery{refs: [_one]} = Column.over(F.sum(scalar), W.partition_by([:id]))
    end

    test "a sort key, all the way to the relation", %{b: b, scalar: scalar} do
      assert %Latu.Subquery{expr: %Proto.Expression.SortOrder{}} = Plan.sort_order(scalar)
      assert {:with_relations, _} = Latu.sort(b, [scalar]).plan.rel_type
    end

    test "a case-when chain", %{b: b, scalar: scalar} do
      chain = scalar |> Column.greater(0) |> F.when_(1) |> F.otherwise(0)

      assert {:with_relations, _} = Latu.select(b, x: chain).plan.rel_type
    end

    test "an aggregate, grouped or not", %{b: b, scalar: scalar} do
      assert {:with_relations, _} = Latu.agg(b, x: F.sum(scalar)).plan.rel_type

      grouped = b |> Latu.group_by(:id) |> Latu.agg(x: F.sum(scalar))

      assert {:with_relations, _} = grouped.plan.rel_type
    end
  end

  describe "normalising" do
    test "numbers the root before the references, as the oracle does", %{a: a, b: b} do
      hoisted = Latu.select(b, Plan.subquery(a.plan, :scalar))

      {:with_relations, wrapper} = Plan.normalize_ids(hoisted.plan).rel_type
      {:project, project} = wrapper.root.rel_type
      [reference] = wrapper.references

      assert project.input.common.plan_id == 0
      assert wrapper.root.common.plan_id == 1
      assert reference.common.plan_id == 2
    end

    test "remaps the plan_id the subquery carries", %{a: a, b: b} do
      hoisted = Latu.select(b, Plan.subquery(a.plan, :scalar))

      {:with_relations, wrapper} = Plan.normalize_ids(hoisted.plan).rel_type
      {:project, project} = wrapper.root.rel_type
      [%{expr_type: {:subquery_expression, subquery}}] = project.expressions
      [reference] = wrapper.references

      assert subquery.plan_id == reference.common.plan_id
    end
  end

  describe "refusals" do
    test "an unknown subquery type", %{a: a} do
      assert_raise ArgumentError, ~r/unknown subquery type :nope/, fn ->
        Plan.subquery(a.plan, :nope)
      end
    end

    test "values outside an in subquery", %{a: a} do
      assert_raise ArgumentError, ~r/values: belongs to an :in subquery/, fn ->
        Plan.subquery(a.plan, :scalar, values: [:id])
      end
    end

    test "a sort key is still not an expression, carried or bare", %{a: a} do
      key = Plan.sort_order(Plan.subquery(a.plan, :scalar))

      assert_raise ArgumentError, ~r/sort key belongs in sort\/2/, fn -> Plan.to_expr(key) end
    end
  end
end
