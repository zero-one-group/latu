defmodule Latu.Result.SchemaTest do
  use ExUnit.Case, async: true

  alias Latu.Error
  alias Latu.Protocol.Spark.Connect.DataType
  alias Latu.Result.Schema

  @decodable %{
    null: %DataType.NULL{},
    binary: %DataType.Binary{},
    boolean: %DataType.Boolean{},
    byte: %DataType.Byte{},
    short: %DataType.Short{},
    integer: %DataType.Integer{},
    long: %DataType.Long{},
    float: %DataType.Float{},
    double: %DataType.Double{},
    decimal: %DataType.Decimal{precision: 10, scale: 2},
    string: %DataType.String{},
    char: %DataType.Char{length: 3},
    var_char: %DataType.VarChar{length: 3},
    date: %DataType.Date{},
    timestamp: %DataType.Timestamp{},
    timestamp_ntz: %DataType.TimestampNTZ{},
    time: %DataType.Time{},
    day_time_interval: %DataType.DayTimeInterval{}
  }

  @refused %{
    calendar_interval: %DataType.CalendarInterval{},
    year_month_interval: %DataType.YearMonthInterval{},
    variant: %DataType.Variant{},
    geometry: %DataType.Geometry{srid: 4326},
    geography: %DataType.Geography{srid: 4326},
    unparsed: %DataType.Unparsed{}
  }

  test "no schema at all is nothing to check" do
    assert Schema.check(nil) == :ok
  end

  test "every kind the decoder can represent passes" do
    for {kind, message} <- @decodable do
      assert Schema.check(schema(x: type(kind, message))) == :ok, "#{kind} was refused"
    end
  end

  test "every kind it cannot is refused, naming the column" do
    for {kind, message} <- @refused do
      assert {:error, %Error{kind: :decode, message: text}} =
               Schema.check(schema(x: type(kind, message))),
             "#{kind} was let through"

      assert text =~ "column x is", "#{kind}: #{text}"
      assert text =~ "cast it to another type first"
    end
  end

  test "the offender is found inside an array, and the path says so" do
    inner = type(:year_month_interval, %DataType.YearMonthInterval{})

    assert {:error, %Error{message: message}} = Schema.check(schema(xs: array(inner)))
    assert message =~ "column xs[] is a year-month interval"
  end

  test "a variant's refusal admits that it decodes, and says what to do" do
    assert {:error, %Error{message: message}} =
             Schema.check(schema(v: type(:variant, %DataType.Variant{})))

    assert message =~ "column v is a variant, which decodes only as Spark's internal binary"
    assert message =~ "cast it to another type first"
  end

  test "inside a map, on either side" do
    interval = type(:calendar_interval, %DataType.CalendarInterval{})

    assert {:error, %Error{message: bad_value}} =
             Schema.check(schema(m: map(string_type(), interval)))

    assert bad_value =~ "column m.value is"

    assert {:error, %Error{message: bad_key}} =
             Schema.check(schema(m: map(interval, string_type())))

    assert bad_key =~ "column m.key is"
  end

  test "inside a nested struct, with the dotted path" do
    inner = struct_of(b: type(:year_month_interval, %DataType.YearMonthInterval{}))

    assert {:error, %Error{message: message}} = Schema.check(schema(a: inner))
    assert message =~ "column a.b is a year-month interval"
  end

  test "deep nesting of decodable types passes" do
    deep = array(struct_of(xs: array(long()), m: map(string_type(), long())))

    assert Schema.check(schema(rows: deep)) == :ok
  end

  test "a UDT is judged by the SQL type it serialises as" do
    assert Schema.check(schema(u: udt(long()))) == :ok

    assert {:error, %Error{message: message}} = Schema.check(schema(u: udt(nil)))
    assert message =~ "column u is a UDT"
  end

  test "a kind this client has never met is refused, not passed through" do
    assert {:error, %Error{message: message}} = Schema.check(schema(x: %DataType{kind: nil}))
    assert message =~ "column x has a Spark type Latu does not know"
  end

  test "a result schema that is not a struct is a protocol surprise" do
    assert {:error, %Error{kind: :decode, message: message}} = Schema.check(long())
    assert message =~ "expected a struct result schema"
  end

  describe "fields/1" do
    test "names, types and nullability, in the schema's own order" do
      data_type =
        schema(id: long(), tags: array(string_type()), who: struct_of(name: string_type()))

      assert Schema.fields(data_type) ==
               {:ok,
                [
                  %{name: "id", type: "bigint", nullable: true},
                  %{name: "tags", type: "array<string>", nullable: true},
                  %{name: "who", type: "struct<name:string>", nullable: true}
                ]}
    end

    test "no schema at all is an error, not an empty list" do
      assert {:error, %Error{}} = Schema.fields(nil)
    end

    test "a schema that is not a struct is a protocol surprise" do
      assert {:error, %Error{message: message}} = Schema.fields(long())
      assert message =~ "long"
    end
  end

  describe "simple_string/1" do
    test "the scalars carry Spark's SQL names, not the proto's kind names" do
      assert Schema.simple_string(type(:null, %DataType.NULL{})) == "void"
      assert Schema.simple_string(type(:byte, %DataType.Byte{})) == "tinyint"
      assert Schema.simple_string(type(:short, %DataType.Short{})) == "smallint"
      assert Schema.simple_string(type(:integer, %DataType.Integer{})) == "int"
      assert Schema.simple_string(long()) == "bigint"

      assert Schema.simple_string(type(:calendar_interval, %DataType.CalendarInterval{})) ==
               "interval"
    end

    test "and the rest name themselves" do
      assert Schema.simple_string(type(:boolean, %DataType.Boolean{})) == "boolean"
      assert Schema.simple_string(type(:binary, %DataType.Binary{})) == "binary"
      assert Schema.simple_string(type(:date, %DataType.Date{})) == "date"
      assert Schema.simple_string(type(:timestamp, %DataType.Timestamp{})) == "timestamp"
      assert Schema.simple_string(type(:float, %DataType.Float{})) == "float"
      assert Schema.simple_string(type(:variant, %DataType.Variant{})) == "variant"

      assert Schema.simple_string(type(:timestamp_ntz, %DataType.TimestampNTZ{})) ==
               "timestamp_ntz"
    end

    test "the parameterised ones carry their parameters" do
      assert Schema.simple_string(type(:char, %DataType.Char{length: 3})) == "char(3)"
      assert Schema.simple_string(type(:var_char, %DataType.VarChar{length: 3})) == "varchar(3)"

      assert Schema.simple_string(type(:decimal, %DataType.Decimal{precision: 10, scale: 2})) ==
               "decimal(10,2)"
    end

    test "an absent precision and scale are PySpark's DecimalType() defaults" do
      assert Schema.simple_string(type(:decimal, %DataType.Decimal{})) == "decimal(10,0)"
    end

    test "a UTF8_BINARY collation is the unmarked case" do
      assert Schema.simple_string(string_type()) == "string"

      assert Schema.simple_string(type(:string, %DataType.String{collation: "UTF8_BINARY"})) ==
               "string"

      assert Schema.simple_string(type(:string, %DataType.String{collation: "UNICODE_CI"})) ==
               "string collate UNICODE_CI"
    end

    test "an interval names its bounds, and takes the one-field form when they agree" do
      day_time = fn bounds -> Schema.simple_string(type(:day_time_interval, bounds)) end
      year_month = fn bounds -> Schema.simple_string(type(:year_month_interval, bounds)) end

      assert day_time.(%DataType.DayTimeInterval{}) == "interval day to second"
      assert day_time.(%DataType.DayTimeInterval{start_field: 0, end_field: 0}) == "interval day"

      assert day_time.(%DataType.DayTimeInterval{start_field: 1, end_field: 2}) ==
               "interval hour to minute"

      assert year_month.(%DataType.YearMonthInterval{}) == "interval year to month"

      assert year_month.(%DataType.YearMonthInterval{start_field: 1, end_field: 1}) ==
               "interval month"
    end

    test "containers render their element types into the string" do
      assert Schema.simple_string(array(long())) == "array<bigint>"
      assert Schema.simple_string(map(string_type(), long())) == "map<string,bigint>"

      assert Schema.simple_string(array(map(string_type(), array(long())))) ==
               "array<map<string,array<bigint>>>"
    end

    test "a struct names its fields, comma-separated and unspaced" do
      assert Schema.simple_string(struct_of(a: long(), b: string_type())) ==
               "struct<a:bigint,b:string>"
    end

    test "a UDT is udt, whatever it serialises as" do
      assert Schema.simple_string(udt(long())) == "udt"
    end

    test "an unparsed type says whatever the server could not parse" do
      unparsed = %DataType.Unparsed{data_type_string: "wat"}

      assert Schema.simple_string(type(:unparsed, unparsed)) == "wat"
    end
  end

  # =============================================
  # Builders
  # =============================================

  defp schema(fields), do: struct_of(fields)

  defp struct_of(fields) do
    fields =
      for {name, data_type} <- fields do
        %DataType.StructField{name: to_string(name), data_type: data_type, nullable: true}
      end

    %DataType{kind: {:struct, %DataType.Struct{fields: fields}}}
  end

  defp type(kind, message), do: %DataType{kind: {kind, message}}

  defp long, do: type(:long, %DataType.Long{})
  defp string_type, do: type(:string, %DataType.String{})

  defp array(element) do
    type(:array, %DataType.Array{element_type: element, contains_null: true})
  end

  defp map(key, value) do
    type(:map, %DataType.Map{key_type: key, value_type: value, value_contains_null: true})
  end

  defp udt(sql_type) do
    type(:udt, %DataType.UDT{type: "udt", sql_type: sql_type})
  end
end
