defmodule Latu.Result.Schema do
  @moduledoc false
  # What Latu knows about a Spark `DataType`: which ones the Arrow decoder can represent, and
  # what Spark calls each one. `check/1` refuses; `fields/1` and `simple_string/1` report.
  # Internal: every function takes a `Proto.DataType.t()`, which a user never holds; the
  # user-facing type is `Latu.Result.field/0`.
  #
  # An unsupported Arrow dtype panics inside Polars' NIF (`** (ErlangError) :nif_panicked`,
  # naming neither column nor dtype), so `check/1` turns that into an error naming both, from
  # the DataType the server sends ahead of the first batch. Refusals are per kind and measured
  # by `dev/probe_dtypes.exs`: re-run it before changing the table, and update the integration
  # dtype matrix in the same commit. docs/decisions.md (M8.1, M10.1, the probe report).

  alias Latu.Error
  alias Latu.Protocol.Spark.Connect, as: Proto

  @decodable [
    :null,
    :binary,
    :boolean,
    :byte,
    :short,
    :integer,
    :long,
    :float,
    :double,
    :decimal,
    :string,
    :char,
    :var_char,
    :date,
    :timestamp,
    :timestamp_ntz,
    :time,
    :day_time_interval
  ]

  # Spark's interval field names, by their proto index.
  @year_month_fields %{0 => "year", 1 => "month"}
  @day_time_fields %{0 => "day", 1 => "hour", 2 => "minute", 3 => "second"}

  @refused %{
    calendar_interval: "a calendar interval",
    year_month_interval: "a year-month interval",
    geometry: "a geometry",
    geography: "a geography",
    unparsed: "an unparsed type"
  }

  @doc """
  `:ok` when every column can decode; `{:error, _}` naming the first that cannot.

  `nil` means the server sent no schema — nothing to check, and the decode may still succeed.
  """
  @spec check(Proto.DataType.t() | nil) :: :ok | {:error, Error.t()}
  def check(nil), do: :ok

  def check(%Proto.DataType{kind: {:struct, %Proto.DataType.Struct{fields: fields}}}) do
    each(fields, fn field -> walk(field.data_type, field.name) end)
  end

  def check(%Proto.DataType{} = other) do
    {:error, Error.new(:decode, "expected a struct result schema, got: #{describe(other)}")}
  end

  @doc """
  The schema's top-level fields, each with Spark's own name for its type.

  Nested types render *into* the type string — `array<int>`, `struct<a:int,b:string>` — so the
  shape stays flat and pattern-matchable.
  """
  @spec fields(Proto.DataType.t() | nil) ::
          {:ok, [Latu.Result.field()]} | {:error, Error.t()}
  def fields(%Proto.DataType{kind: {:struct, %Proto.DataType.Struct{fields: fields}}}) do
    {:ok,
     Enum.map(fields, fn field ->
       %{name: field.name, type: simple_string(field.data_type), nullable: field.nullable}
     end)}
  end

  def fields(nil), do: {:error, Error.new(:decode, "the server sent no schema")}

  def fields(%Proto.DataType{} = other) do
    {:error, Error.new(:decode, "expected a struct schema, got: #{describe(other)}")}
  end

  @doc """
  Spark's `simpleString` for a type: `bigint`, `array<int>`, `interval day to second`.

  Transcribed from PySpark's `sql/types.py`, because it is what `Latu.dtypes/1` reports and a
  divergence would be invisible offline. `test/integration/analyze_test.exs` cross-checks every
  one of them against the server's own `typeof`.
  """
  @spec simple_string(Proto.DataType.t() | nil) :: String.t()
  def simple_string(%Proto.DataType{kind: {:null, _}}), do: "void"
  def simple_string(%Proto.DataType{kind: {:byte, _}}), do: "tinyint"
  def simple_string(%Proto.DataType{kind: {:short, _}}), do: "smallint"
  def simple_string(%Proto.DataType{kind: {:integer, _}}), do: "int"
  def simple_string(%Proto.DataType{kind: {:long, _}}), do: "bigint"
  def simple_string(%Proto.DataType{kind: {:calendar_interval, _}}), do: "interval"
  # Neither reaches a query's output schema: Catalyst erases char and varchar to StringType
  # there, keeping the spelling in field metadata. They arrive from a DDL parse. See
  # docs/decisions.md (M10.1).
  def simple_string(%Proto.DataType{kind: {:char, %{length: length}}}), do: "char(#{length})"

  def simple_string(%Proto.DataType{kind: {:var_char, %{length: length}}}) do
    "varchar(#{length})"
  end

  # PySpark's DecimalType() defaults, which is also what lit/1 sends — see Latu.Plan.
  def simple_string(%Proto.DataType{kind: {:decimal, %{precision: precision, scale: scale}}}) do
    "decimal(#{precision || 10},#{scale || 0})"
  end

  def simple_string(%Proto.DataType{kind: {:time, %{precision: precision}}}) do
    "time(#{precision || 6})"
  end

  def simple_string(%Proto.DataType{kind: {:string, %{collation: collation}}})
      when collation in [nil, "", "UTF8_BINARY"] do
    "string"
  end

  def simple_string(%Proto.DataType{kind: {:string, %{collation: collation}}}) do
    "string collate #{collation}"
  end

  def simple_string(%Proto.DataType{kind: {:year_month_interval, bounds}}) do
    interval(bounds, @year_month_fields)
  end

  def simple_string(%Proto.DataType{kind: {:day_time_interval, bounds}}) do
    interval(bounds, @day_time_fields)
  end

  def simple_string(%Proto.DataType{kind: {:array, %{element_type: element}}}) do
    "array<#{simple_string(element)}>"
  end

  def simple_string(%Proto.DataType{kind: {:map, %{key_type: key, value_type: value}}}) do
    "map<#{simple_string(key)},#{simple_string(value)}>"
  end

  def simple_string(%Proto.DataType{kind: {:struct, %{fields: fields}}}) do
    inner = Enum.map_join(fields, ",", &"#{&1.name}:#{simple_string(&1.data_type)}")

    "struct<#{inner}>"
  end

  # A UDT is "udt" whatever it serialises as, which is PySpark's answer too.
  def simple_string(%Proto.DataType{kind: {:udt, _}}), do: "udt"

  def simple_string(%Proto.DataType{kind: {:unparsed, %{data_type_string: string}}}), do: string

  # The rest name themselves: boolean, binary, date, timestamp, timestamp_ntz, float, double,
  # variant. So do geometry and geography, where PySpark appends an SRID — neither is reachable
  # from a 4.2 result, and the guard refuses both.
  def simple_string(%Proto.DataType{kind: {kind, _}}), do: to_string(kind)

  def simple_string(_missing), do: "unknown"

  defp walk(nil, path) do
    {:error, Error.new(:decode, "column #{path} carries no type at all")}
  end

  defp walk(%Proto.DataType{kind: {:array, array}}, path) do
    walk(array.element_type, path <> "[]")
  end

  defp walk(%Proto.DataType{kind: {:map, map}}, path) do
    with :ok <- walk(map.key_type, path <> ".key"), do: walk(map.value_type, path <> ".value")
  end

  defp walk(%Proto.DataType{kind: {:struct, %Proto.DataType.Struct{fields: fields}}}, path) do
    each(fields, fn field -> walk(field.data_type, path <> "." <> field.name) end)
  end

  # A UDT serialises as its SQL type, so that is what arrives in the Arrow stream.
  defp walk(%Proto.DataType{kind: {:udt, %Proto.DataType.UDT{sql_type: nil}}}, path) do
    {:error, Error.new(:decode, "column #{path} is a UDT that does not say its SQL type")}
  end

  defp walk(%Proto.DataType{kind: {:udt, %Proto.DataType.UDT{sql_type: sql_type}}}, path) do
    walk(sql_type, path)
  end

  defp walk(%Proto.DataType{kind: {kind, _}}, _path) when kind in @decodable, do: :ok

  # It decodes, but only as Spark's internal binary encoding — a struct of two raw binaries no
  # Elixir code can interpret. A cast to string gives readable JSON instead.
  defp walk(%Proto.DataType{kind: {:variant, _}}, path) do
    {:error,
     Error.new(
       :decode,
       "column #{path} is a variant, which decodes only as Spark's internal binary encoding; " <>
         "cast it to another type first"
     )}
  end

  defp walk(%Proto.DataType{kind: {kind, _}}, path) when is_map_key(@refused, kind) do
    {:error,
     Error.new(
       :decode,
       "column #{path} is #{@refused[kind]}, which the Arrow decoder cannot represent; " <>
         "cast it to another type first"
     )}
  end

  # A kind this client has never met: a re-vendor added one, or the server is newer than the
  # vendored protos. Refusing beats handing Polars a mystery.
  defp walk(%Proto.DataType{} = other, path) do
    {:error,
     Error.new(:decode, "column #{path} has a Spark type Latu does not know: #{describe(other)}")}
  end

  # PySpark defaults both ends when neither is set, and mirrors the start when only it is.
  defp interval(%{start_field: start_field, end_field: end_field}, fields) do
    {start_field, end_field} =
      case {start_field, end_field} do
        {nil, nil} -> {0, map_size(fields) - 1}
        {start_field, nil} -> {start_field, start_field}
        {nil, end_field} -> {0, end_field}
        pair -> pair
      end

    case {Map.get(fields, start_field), Map.get(fields, end_field)} do
      {nil, _} -> "interval"
      {_, nil} -> "interval"
      {same, same} -> "interval #{same}"
      {from, to} -> "interval #{from} to #{to}"
    end
  end

  defp each(fields, fun) do
    Enum.reduce_while(fields, :ok, fn field, :ok ->
      case fun.(field) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp describe(%Proto.DataType{kind: nil}), do: "an empty DataType"
  defp describe(%Proto.DataType{kind: {kind, _}}), do: to_string(kind)
end
