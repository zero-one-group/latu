defmodule Latu.ColumnTest do
  use ExUnit.Case, async: true

  import Latu.Column
  import Latu.Wire

  alias Latu.Session

  # Golden plans come from PySpark: python dev/pyspark_oracle.py --generate

  setup do
    %{session: Session.from_url!("sc://h")}
  end

  describe "operators" do
    test "arithmetic carries Spark's names", %{session: session} do
      df =
        Latu.select(Latu.range(session, 10),
          a: add(:id, 1),
          s: subtract(:id, 1),
          m: multiply(:id, 2),
          d: divide(:id, 2),
          r: remainder(:id, 2),
          p: pow(:id, 2)
        )

      assert_wire(df, "op_arith")
    end

    test "comparison carries Spark's names", %{session: session} do
      df =
        Latu.select(Latu.range(session, 10),
          eq: equal(:id, 1),
          ne: not_equal(:id, 1),
          gt: greater(:id, 1),
          ge: greater_equal(:id, 1),
          lt: less(:id, 1),
          le: less_equal(:id, 1),
          ns: equal_null_safe(:id, 1)
        )

      assert_wire(df, "op_compare")
    end

    test "boolean is words where comparison is symbols", %{session: session} do
      df =
        Latu.select(Latu.range(session, 10),
          a: all([greater(:id, 1), less(:id, 5)]),
          o: any([greater(:id, 1), less(:id, 5)]),
          n: not_(greater(:id, 1))
        )

      assert_wire(df, "op_boolean")
    end

    test "an atom operand is a column and everything else is a literal" do
      assert %{expr_type: {:unresolved_function, call}} = greater(:price, 100)

      assert call.function_name == ">"
      assert [left, right] = call.arguments
      assert {:unresolved_attribute, %{unparsed_identifier: "price"}} = left.expr_type
      assert {:literal, %{literal_type: {:integer, 100}}} = right.expr_type
    end

    test "operands can be expressions, so operators nest" do
      assert add(add(:id, 1), 1) == fun("+", [fun("+", [:id, 1]), 1])
    end
  end

  describe "all/1 and any/1" do
    test "fold left, as PySpark's & does", %{session: session} do
      predicates = [greater(:id, 1), less(:id, 8), not_equal(:id, 5)]

      assert_wire(Latu.filter(Latu.range(session, 10), all(predicates)), "op_chain")
    end

    test "one predicate is itself, with no wrapper" do
      assert all([greater(:id, 1)]) == greater(:id, 1)
      assert any([greater(:id, 1)]) == greater(:id, 1)
    end

    test "none is the identity, so a filtered list needs no branch" do
      assert all([]) == lit(true)
      assert any([]) == lit(false)
    end

    test "operands are coerced, so a boolean column needs no wrapping" do
      assert all([:flag, :other]) == fun("and", [col(:flag), col(:other)])
    end
  end

  describe "predicates" do
    test "null tests and between and isin", %{session: session} do
      df =
        Latu.select(Latu.range(session, 10),
          n: is_null(:id),
          nn: is_not_null(:id),
          nan: is_nan(:id),
          b: between(:id, 1, 5),
          i: isin(:id, [1, 2, 3])
        )

      assert_wire(df, "col_predicates")
    end

    test "string predicates", %{session: session} do
      df =
        Latu.select(Latu.range(session, 10),
          c: contains(lit("abc"), "b"),
          s: starts_with(lit("abc"), "a"),
          e: ends_with(lit("abc"), "c"),
          l: like(lit("abc"), "a%"),
          r: rlike(lit("abc"), "^a"),
          il: ilike(lit("abc"), "A%")
        )

      assert_wire(df, "string_predicates")
    end

    test "between composes, it is not a Spark function" do
      assert between(:id, 1, 5) == all([greater_equal(:id, 1), less_equal(:id, 5)])
    end

    test "isin calls the function `in`, and takes one value or many" do
      assert isin(:id, [1]) == fun("in", [:id, 1])
      assert isin(:id, 1) == fun("in", [:id, 1])
      assert isin(:id, [1, 2]) == fun("in", [:id, 1, 2])
    end
  end

  describe "cast/2" do
    test "a string type, as PySpark sends it", %{session: session} do
      df = Latu.select(Latu.range(session, 10), s: cast(:id, "string"))

      assert_wire(df, "cast")
    end

    test "try_cast is the same node with the TRY eval mode", %{session: session} do
      df = Latu.select(Latu.range(session, 10), s: try_cast(:id, "string"))

      assert_wire(df, "try_cast")
    end

    test "the type is a oneof, and plain cast leaves eval_mode alone" do
      {:cast, plain} = cast(:id, "string").expr_type
      {:cast, tried} = try_cast(:id, "string").expr_type

      assert plain.cast_to_type == {:type_str, "string"}
      assert tried.eval_mode == :EVAL_MODE_TRY
      refute plain.eval_mode == :EVAL_MODE_TRY
    end
  end

  describe "fun/2" do
    test "reaches any Spark function by name" do
      assert %{expr_type: {:unresolved_function, call}} = fun("upper", [:suburb])

      assert call.function_name == "upper"
      assert [%{expr_type: {:unresolved_attribute, _}}] = call.arguments
    end

    test "leaves the flags PySpark leaves alone" do
      {:unresolved_function, call} = fun("upper", [:suburb]).expr_type

      # is_internal is proto3_optional, and PySpark does not set it. Absent is not false.
      assert call.is_distinct == false
      assert call.is_user_defined_function == false
      assert call.is_internal == nil
    end

    test "refuses anything but a name and a list" do
      assert_raise FunctionClauseError, fn -> fun(:upper, [:suburb]) end
      assert_raise FunctionClauseError, fn -> fun("upper", :suburb) end
    end
  end
end
