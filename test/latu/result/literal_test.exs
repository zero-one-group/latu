defmodule Latu.Result.LiteralTest do
  use ExUnit.Case, async: true

  alias Latu.Protocol.Spark.Connect, as: Proto
  alias Latu.Result.Literal

  # Nested messages are built as plain maps on purpose: the decoder reads fields and never
  # names these types, so a map is the honest shape of what it depends on. Only the literal
  # itself is a real message.
  defp lit(literal_type, extra \\ []) do
    struct!(%Proto.Expression.Literal{literal_type: literal_type}, extra)
  end

  defp struct_type(names) do
    fields = Enum.map(names, &%Proto.DataType.StructField{name: &1})
    %Proto.DataType{kind: {:struct, %Proto.DataType.Struct{fields: fields}}}
  end

  describe "scalars" do
    test "null is nil, whatever type it claims" do
      assert {:ok, nil} = Literal.value(lit({:null, %Proto.DataType{}}))
    end

    test "every integer width comes back as an integer" do
      for arm <- [:byte, :short, :integer, :long] do
        assert {:ok, 7} = Literal.value(lit({arm, 7}))
      end
    end

    test "float and double both come back as floats" do
      assert {:ok, 1.5} = Literal.value(lit({:float, 1.5}))
      assert {:ok, 1.5} = Literal.value(lit({:double, 1.5}))
    end

    test "booleans, strings and binaries pass through" do
      assert {:ok, true} = Literal.value(lit({:boolean, true}))
      assert {:ok, "hi"} = Literal.value(lit({:string, "hi"}))
      assert {:ok, <<0, 1, 2>>} = Literal.value(lit({:binary, <<0, 1, 2>>}))
    end

    test "a decimal comes back as a Decimal, scale kept" do
      assert {:ok, decimal} = Literal.value(lit({:decimal, %{value: "1.50"}}))
      assert Decimal.equal?(decimal, Decimal.new("1.50"))
    end

    test "a decimal the server mangled is an error, not a crash" do
      assert {:error, error} = Literal.value(lit({:decimal, %{value: "not a number"}}))
      assert error.kind == :decode
      assert error.message =~ "as a decimal"
    end
  end

  describe "times" do
    test "a date is days since the epoch" do
      assert {:ok, ~D[1970-01-01]} = Literal.value(lit({:date, 0}))
      assert {:ok, ~D[2026-09-02]} = Literal.value(lit({:date, 20_698}))
    end

    test "a timestamp is an instant, in UTC, at microsecond precision" do
      assert {:ok, ~U[2025-09-02 08:00:00.000000Z]} =
               Literal.value(lit({:timestamp, 1_756_800_000_000_000}))
    end

    test "a timestamp_ntz is a wall-clock reading with no zone" do
      assert {:ok, ~N[1970-01-01 00:00:01.500000]} =
               Literal.value(lit({:timestamp_ntz, 1_500_000}))
    end

    test "a day-time interval is a count of microseconds" do
      assert {:ok, 90_000_000} = Literal.value(lit({:day_time_interval, 90_000_000}))
    end
  end

  describe "collections" do
    test "an array decodes elementwise" do
      array = %{elements: [lit({:long, 1}), lit({:long, 2})]}
      assert {:ok, [1, 2]} = Literal.value(lit({:array, array}))
    end

    test "a nested array decodes all the way down" do
      inner = lit({:array, %{elements: [lit({:integer, 1})]}})
      assert {:ok, [[1]]} = Literal.value(lit({:array, %{elements: [inner]}}))
    end

    test "a map decodes keys and values" do
      map = %{keys: [lit({:string, "a"})], values: [lit({:long, 1})]}
      assert {:ok, %{"a" => 1}} = Literal.value(lit({:map, map}))
    end

    test "a map whose keys and values disagree in length is an error" do
      map = %{keys: [lit({:string, "a"})], values: []}
      assert {:error, error} = Literal.value(lit({:map, map}))
      assert error.message =~ "1 keys and 0 values"
    end

    test "a struct becomes a map with atom keys, named from data_type" do
      elements = [lit({:long, 1}), lit({:string, "b"})]
      literal = lit({:struct, %{elements: elements}}, data_type: struct_type(["x", "y"]))

      assert {:ok, %{x: 1, y: "b"}} = Literal.value(literal)
    end

    test "a struct falls back to the deprecated struct_type field, as PySpark reads it" do
      elements = [lit({:long, 1})]
      literal = lit({:struct, %{elements: elements, struct_type: struct_type(["x"])}})

      assert {:ok, %{x: 1}} = Literal.value(literal)
    end

    test "a struct with no field names anywhere is an error" do
      literal = lit({:struct, %{elements: [lit({:long, 1})], struct_type: nil}})

      assert {:error, error} = Literal.value(literal)
      assert error.message =~ "no field names"
    end

    test "a specialized array is a flat list, which PySpark refuses to read at all" do
      array = %{value_type: {:longs, %{values: [1, 2, 3]}}}
      assert {:ok, [1, 2, 3]} = Literal.value(lit({:specialized_array, array}))
    end
  end

  describe "refusals" do
    test "the interval arms Latu cannot represent name themselves" do
      assert {:error, error} = Literal.value(lit({:calendar_interval, %{months: 1}}))
      assert error.message =~ "a calendar interval"

      assert {:error, other} = Literal.value(lit({:year_month_interval, 13}))
      assert other.message =~ "a year-month interval"
    end

    test "an arm Latu has never met says so rather than matching something else" do
      assert {:error, error} = Literal.value(lit({:geography, %{}}))
      assert error.message =~ "does not know the literal arm :geography"
    end

    test "a literal with nothing set is an error" do
      assert {:error, error} = Literal.value(lit(nil))
      assert error.message =~ "no value set"
    end

    test "no literal at all is an error rather than a match failure" do
      assert {:error, error} = Literal.value(nil)
      assert error.message =~ "no literal to decode"
    end
  end
end
