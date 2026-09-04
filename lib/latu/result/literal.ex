defmodule Latu.Result.Literal do
  @moduledoc false
  # An `Expression.Literal` the server sent, back into an Elixir term. `Latu.Plan.lit/1` is the
  # other direction, and the two are deliberately not symmetric: the encoder picks one Spark
  # type per Elixir type, while this has to read back every arm Spark can produce.
  #
  # Transcribed from PySpark's `LiteralExpression._to_value`, with two departures recorded in
  # docs/deviations.md: Latu decodes `specialized_array`, which PySpark refuses, and Latu reads
  # the `data_type` field in preference to the `struct_type` field 4.1 deprecated, which PySpark
  # still reads exclusively.
  #
  # The interval refusals match `Latu.Result.Schema`'s.
  #
  # Clauses below match maps rather than named messages wherever they can, so the offline
  # harness needs one stand-in struct instead of eight.

  alias Latu.Error
  alias Latu.Protocol.Spark.Connect, as: Proto

  @epoch_date ~D[1970-01-01]

  # Microsecond precision on the epoch itself, because `NaiveDateTime.add/4` inherits the
  # precision of what it is adding to: from a zero-precision epoch, 1_500_000us truncates to a
  # whole second with no error. `Latu.Plan`'s epoch can be zero-precision because `diff/3` does
  # not care. Pinned by `test/latu/result/literal_test.exs`.
  @epoch_naive ~N[1970-01-01 00:00:00.000000]

  @refused %{
    calendar_interval: "a calendar interval",
    year_month_interval: "a year-month interval"
  }

  @doc """
  The Elixir term a literal carries, or `{:error, _}` naming the arm that has no term.

  Integers come back as integers whatever their width, and both `float` and `double` as floats,
  because Elixir has neither a byte nor a 32-bit float — the same collapse
  `Latu.Column.lit/1` applies going out. A `day_time_interval` comes back as a count of
  **microseconds**; Elixir's `Duration` is the obvious upgrade and waits on someone wanting
  it.

  A struct literal becomes a map with atom keys, as `Latu.collect/2` does for a row.
  """
  @spec value(Proto.Expression.Literal.t() | nil) :: {:ok, term()} | {:error, Error.t()}
  def value(nil), do: {:error, Error.new(:decode, "no literal to decode")}

  def value(%Proto.Expression.Literal{} = literal) do
    {:ok, decode(literal)}
  rescue
    # The recursion raises, so a bad element deep inside an array does not need every level to
    # thread a tuple. This is the only boundary, and `Error` is the only thing it catches.
    error in Error -> {:error, error}
  end

  # =============================================
  # Scalars
  # =============================================

  defp decode(%{literal_type: nil}) do
    raise Error.new(:decode, "the server sent a literal with no value set")
  end

  defp decode(%{literal_type: {:null, _}}), do: nil
  defp decode(%{literal_type: {:binary, value}}), do: value
  defp decode(%{literal_type: {:boolean, value}}), do: value
  defp decode(%{literal_type: {:byte, value}}), do: value
  defp decode(%{literal_type: {:short, value}}), do: value
  defp decode(%{literal_type: {:integer, value}}), do: value
  defp decode(%{literal_type: {:long, value}}), do: value
  defp decode(%{literal_type: {:float, value}}), do: value
  defp decode(%{literal_type: {:double, value}}), do: value
  defp decode(%{literal_type: {:string, value}}), do: value
  defp decode(%{literal_type: {:date, days}}), do: Date.add(@epoch_date, days)
  defp decode(%{literal_type: {:timestamp, us}}), do: DateTime.from_unix!(us, :microsecond)
  defp decode(%{literal_type: {:day_time_interval, us}}), do: us

  defp decode(%{literal_type: {:timestamp_ntz, us}}) do
    NaiveDateTime.add(@epoch_naive, us, :microsecond)
  end

  defp decode(%{literal_type: {:decimal, %{value: value}}}) do
    case Decimal.parse(value) do
      {decimal, ""} -> decimal
      _ -> raise Error.new(:decode, "the server sent #{inspect(value)} as a decimal")
    end
  end

  # Dormant: Spark 4.2 refuses the TIME type outright, across literals and functions alike. Here
  # for the same reason `Latu.Result.Schema.simple_string/1` renders `time(p)`.
  defp decode(%{literal_type: {:time, %{nano: nano}}}) do
    Time.add(~T[00:00:00.000000], nano, :nanosecond)
  end

  # =============================================
  # Collections
  # =============================================

  defp decode(%{literal_type: {:array, %{elements: elements}}}), do: Enum.map(elements, &decode/1)

  defp decode(%{literal_type: {:map, %{keys: keys, values: values}}}) do
    zip("map", keys, values, fn key, value -> {decode(key), decode(value)} end)
  end

  # The one arm that needs type information: a struct's field *names* are not in the literal.
  # `data_type` is where 4.1+ puts them, `struct_type` the deprecated field PySpark reads.
  defp decode(%{literal_type: {:struct, struct}} = literal) do
    case field_names(literal.data_type) || field_names(struct.struct_type) do
      nil -> raise Error.new(:decode, "the server sent a struct literal with no field names")
      names -> zip("struct", names, struct.elements, &{String.to_atom(&1), decode(&2)})
    end
  end

  # PySpark refuses these; Latu reads them, because they are six flat lists and a value the
  # server may well send is a poor thing to refuse.
  defp decode(%{literal_type: {:specialized_array, %{value_type: value_type}}}) do
    case value_type do
      {_kind, %{values: values}} -> values
      nil -> raise Error.new(:decode, "the server sent an empty specialized array")
    end
  end

  # =============================================
  # Refusals
  # =============================================

  defp decode(%{literal_type: {kind, _}}) when is_map_key(@refused, kind) do
    raise Error.new(:decode, "Latu cannot represent #{@refused[kind]}")
  end

  defp decode(%{literal_type: {kind, _}}) do
    raise Error.new(:decode, "Latu does not know the literal arm #{inspect(kind)}")
  end

  # Two repeated fields the server is meant to keep in step. If it does not, say which.
  defp zip(_what, left, right, pair) when length(left) == length(right) do
    left |> Enum.zip(right) |> Map.new(fn {one, other} -> pair.(one, other) end)
  end

  defp zip(what, left, right, _pair) do
    raise Error.new(
            :decode,
            "the server sent a #{what} literal with #{length(left)} keys and " <>
              "#{length(right)} values"
          )
  end

  defp field_names(%Proto.DataType{kind: {:struct, %{fields: fields}}}) do
    Enum.map(fields, & &1.name)
  end

  defp field_names(_other), do: nil
end
