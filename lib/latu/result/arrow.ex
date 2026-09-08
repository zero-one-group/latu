defmodule Latu.Result.Arrow do
  @moduledoc """
  An Arrow IPC streaming-format reader: the bytes the server sends, without Explorer.

  `Latu.to_explorer/2` hands its batches to Polars, which is the right answer for a DataFrame
  and the wrong one for a tensor — it decodes every column into a Series first. This reads the
  same bytes far enough to hand back, per column, **sub-binaries of the batch**: no copy, and
  no interpretation beyond what Arrow itself says. `Latu.Result.Nx` is what turns those into
  tensors, and is the only caller that needs to.

  It reads only what Spark sends. A dictionary batch, a compressed body, or a stream whose
  schema and batches disagree is refused rather than guessed at; refusals are plain strings,
  because the caller has the column name and the verb and can say more than this can.

  ## What a column comes back as

      %{
        name: "features",
        type: {:float, 64} | {:int, 32, :signed} | :bool | :utf8 | :list | :struct | {:other, _},
        length: 4,
        null_count: 0,
        buffers: [validity_or_nil, ...],
        children: [column]
      }

  Buffers are in Arrow's own order for the type — `[validity, data]` for a primitive,
  `[validity, offsets]` plus one child for a list, `[validity]` plus N children for a struct —
  and a buffer of length zero comes back as `nil`, which is how Arrow spells "no nulls".
  """

  @continuation 0xFFFF_FFFF

  @typedoc "A column's type, as Arrow declares it. Anything not named here is `{:other, name}`."
  @type type ::
          {:int, pos_integer(), :signed | :unsigned}
          | {:float, 16 | 32 | 64}
          | :bool
          | :utf8
          | :list
          | :struct
          | :null
          | {:other, atom()}

  @typedoc "One column of one batch: its type, its Arrow buffers, and its children."
  @type column :: %{
          name: String.t(),
          type: type(),
          length: non_neg_integer(),
          null_count: non_neg_integer(),
          buffers: [binary() | nil],
          children: [column()]
        }

  @typedoc "One record batch: how many rows, and the top-level columns."
  @type batch :: %{rows: non_neg_integer(), columns: [column()]}

  @doc """
  Every record batch in one IPC stream.

  The stream is `[schema][batch]…[end-of-stream]`, which is what one element of
  `Latu.to_arrow/2` holds. A stream with no batches — an empty result still carries its schema
  — answers `{:ok, []}`.
  """
  @spec read(binary()) :: {:ok, [batch()]} | {:error, String.t()}
  def read(stream) when is_binary(stream) do
    with {:ok, messages} <- messages(stream, []) do
      case messages do
        [{1, schema_meta, schema_pos, _body} | rest] ->
          with {:ok, fields} <- schema_fields(schema_meta, schema_pos) do
            batches(rest, fields, [])
          end

        [{header, _, _, _} | _] ->
          {:error, "the stream opens with #{header_name(header)}, not a schema"}

        [] ->
          {:error, "no Arrow messages at all"}
      end
    end
  end

  @doc "The schema's top-level column names and types, without reading a batch."
  @spec schema(binary()) :: {:ok, [{String.t(), type()}]} | {:error, String.t()}
  def schema(stream) when is_binary(stream) do
    with {:ok, [{1, meta, pos, _} | _]} <- messages(stream, []),
         {:ok, fields} <- schema_fields(meta, pos) do
      {:ok, Enum.map(fields, &{&1.name, &1.type})}
    else
      {:ok, _} -> {:error, "the stream does not open with a schema"}
      {:error, _} = error -> error
    end
  end

  # Field numbers are declaration positions in Arrow's own schemas, which is why they are bare
  # integers below. A union declaration takes two of them: its type tag, then its value.
  #
  #   Message        1 header_type, 2 header, 3 bodyLength
  #   RecordBatch    0 length, 1 nodes, 2 buffers, 3 compression
  #   Schema         0 endianness, 1 fields
  #   Field          0 name, 2 type_type, 3 type, 5 children
  #   Int            0 bitWidth, 1 is_signed
  #   FloatingPoint  0 precision
  #
  # https://github.com/apache/arrow/blob/main/format/Message.fbs — Message, RecordBatch
  # https://github.com/apache/arrow/blob/main/format/Schema.fbs — the rest

  # =============================================
  # Framing
  # =============================================
  #
  # Each message is `[0xFFFFFFFF][int32 metadata size][metadata][body]`, the metadata size
  # already padded so the body starts 8-byte aligned, and the body length carried inside the
  # metadata rather than inferred. A zero size is the end-of-stream marker.

  defp messages(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp messages(<<@continuation::little-32, 0::little-32, _rest::binary>>, acc) do
    {:ok, Enum.reverse(acc)}
  end

  defp messages(<<@continuation::little-32, size::little-32, rest::binary>>, acc)
       when size > 0 and byte_size(rest) >= size do
    metadata = binary_part(rest, 0, size)
    tail = binary_part(rest, size, byte_size(rest) - size)
    message = root(metadata)

    header_type = field_u8(metadata, message, 1, 0)
    body_length = field_i64(metadata, message, 3, 0)

    cond do
      byte_size(tail) < body_length ->
        {:error, "a message claims a #{body_length}-byte body and #{byte_size(tail)} remain"}

      true ->
        body = binary_part(tail, 0, body_length)
        rest = binary_part(tail, body_length, byte_size(tail) - body_length)

        case field(metadata, message, 2) do
          nil -> {:error, "a message carries no header"}
          pos -> messages(rest, [{header_type, metadata, indirect(metadata, pos), body} | acc])
        end
    end
  end

  defp messages(_other, _acc), do: {:error, "not an Arrow IPC stream"}

  defp header_name(0), do: "nothing"
  defp header_name(2), do: "a dictionary batch, which Latu does not read"
  defp header_name(3), do: "a record batch"
  defp header_name(other), do: "message header #{other}"

  # =============================================
  # Schema
  # =============================================

  # Endianness is field 0: 0 is little, 1 is big. Every buffer below is handed to
  # `Nx.from_binary/2`, which reads in the machine's own order, so a big-endian stream would be
  # decoded wrong rather than refused. Nothing Spark runs on produces one; this is here so that
  # if one ever arrives it says so.
  defp schema_fields(buf, schema) do
    if field_u16(buf, schema, 0, 0) != 0 do
      {:error, "the stream is big-endian, which Latu does not read"}
    else
      case field(buf, schema, 1) do
        nil -> {:ok, []}
        pos -> collect(buf, pos, &parse_field/2)
      end
    end
  end

  defp parse_field(buf, pos) do
    children =
      case field(buf, pos, 5) do
        nil -> {:ok, []}
        at -> collect(buf, at, &parse_field/2)
      end

    with {:ok, children} <- children do
      {:ok,
       %{
         name: string_at(buf, field(buf, pos, 0)),
         type: parse_type(buf, field_u8(buf, pos, 2, 0), field(buf, pos, 3)),
         children: children
       }}
    end
  end

  # Arrow's `Type` union. Only what a Spark result can hold is named; the rest is refused by
  # `Latu.Result.Nx` with the tag intact, so the message can say what the column actually is.
  defp parse_type(_buf, 1, _pos), do: :null

  defp parse_type(buf, 2, pos) do
    signed = if field_u8(buf, indirect(buf, pos), 1, 0) == 1, do: :signed, else: :unsigned
    {:int, field_i32(buf, indirect(buf, pos), 0, 0), signed}
  end

  defp parse_type(buf, 3, pos) do
    case field_u16(buf, indirect(buf, pos), 0, 0) do
      0 -> {:float, 16}
      1 -> {:float, 32}
      _ -> {:float, 64}
    end
  end

  defp parse_type(_buf, 5, _pos), do: :utf8
  defp parse_type(_buf, 6, _pos), do: :bool
  defp parse_type(_buf, 12, _pos), do: :list
  defp parse_type(_buf, 13, _pos), do: :struct
  defp parse_type(_buf, other, _pos), do: {:other, other}

  # How many buffers Arrow gives a column of this type, before its children.
  defp buffer_count(:null), do: 0
  defp buffer_count(:utf8), do: 3
  defp buffer_count(:list), do: 2
  defp buffer_count(:struct), do: 1
  defp buffer_count(_primitive), do: 2

  # =============================================
  # Record batches
  # =============================================

  defp batches([], _fields, acc), do: {:ok, Enum.reverse(acc)}

  defp batches([{3, meta, pos, body} | rest], fields, acc) do
    with {:ok, batch} <- record_batch(meta, pos, body, fields) do
      batches(rest, fields, [batch | acc])
    end
  end

  defp batches([{header, _, _, _} | _], _fields, _acc) do
    {:error, "the stream carries #{header_name(header)}, which Latu does not read"}
  end

  defp record_batch(buf, pos, body, fields) do
    if field(buf, pos, 3) do
      {:error, "the batch body is compressed, which Latu does not read"}
    else
      rows = field_i64(buf, pos, 0, 0)
      nodes = struct_vector(buf, field(buf, pos, 1))
      buffers = struct_vector(buf, field(buf, pos, 2))

      case walk(fields, nodes, buffers, body, []) do
        {:ok, columns, [], []} -> {:ok, %{rows: rows, columns: columns}}
        {:ok, _columns, nodes, buffers} -> {:error, leftover(nodes, buffers)}
        {:error, _} = error -> error
      end
    end
  end

  defp leftover(nodes, buffers) do
    "the batch has #{length(nodes)} nodes and #{length(buffers)} buffers the schema " <>
      "does not account for"
  end

  # Nodes and buffers are one flat pre-order walk of the field tree, so they are consumed in
  # step with it rather than indexed.
  defp walk([], nodes, buffers, _body, acc), do: {:ok, Enum.reverse(acc), nodes, buffers}

  defp walk([field | rest], nodes, buffers, body, acc) do
    count = buffer_count(field.type)

    cond do
      nodes == [] ->
        {:error, "the batch ran out of field nodes at column #{field.name}"}

      length(buffers) < count ->
        {:error, "the batch ran out of buffers at column #{field.name}"}

      true ->
        [{length, null_count} | nodes] = nodes
        {mine, buffers} = Enum.split(buffers, count)

        with {:ok, children, nodes, buffers} <- walk(field.children, nodes, buffers, body, []) do
          column = %{
            name: field.name,
            type: field.type,
            length: length,
            null_count: null_count,
            buffers: Enum.map(mine, &slice(body, &1)),
            children: children
          }

          walk(rest, nodes, buffers, body, [column | acc])
        end
    end
  end

  # A zero-length buffer is Arrow's way of saying the buffer is not there — an all-valid column
  # carries no validity bitmap — so it comes back as `nil` rather than as `""`.
  defp slice(_body, {_offset, 0}), do: nil
  defp slice(body, {offset, length}), do: binary_part(body, offset, length)

  # =============================================
  # Flatbuffers
  # =============================================
  #
  # Enough of the format to read Arrow's messages: a table is an int32 pointing *backwards* to
  # its vtable, the vtable holds one uint16 offset per field, and zero means absent. Every
  # position here is absolute in `buf`.

  defp root(buf), do: indirect(buf, 0)

  defp indirect(buf, pos), do: pos + u32(buf, pos)

  defp field(buf, table, id) do
    vtable = table - i32(buf, table)
    slot = 4 + id * 2

    if slot + 2 > u16(buf, vtable) do
      nil
    else
      case u16(buf, vtable + slot) do
        0 -> nil
        offset -> table + offset
      end
    end
  end

  defp field_u8(buf, table, id, default) do
    case field(buf, table, id) do
      nil -> default
      pos -> :binary.at(buf, pos)
    end
  end

  defp field_u16(buf, table, id, default) do
    case field(buf, table, id) do
      nil -> default
      pos -> u16(buf, pos)
    end
  end

  defp field_i32(buf, table, id, default) do
    case field(buf, table, id) do
      nil -> default
      pos -> i32(buf, pos)
    end
  end

  defp field_i64(buf, table, id, default) do
    case field(buf, table, id) do
      nil -> default
      pos -> i64(buf, pos)
    end
  end

  defp string_at(_buf, nil), do: ""

  defp string_at(buf, pos) do
    start = indirect(buf, pos)
    binary_part(buf, start + 4, u32(buf, start))
  end

  # A vector of tables or of anything else reached by offset: map over the element positions.
  defp collect(buf, pos, parse) do
    start = indirect(buf, pos)
    count = u32(buf, start)

    Enum.reduce_while(0..(count - 1)//1, {:ok, []}, fn index, {:ok, acc} ->
      at = start + 4 + index * 4

      case parse.(buf, indirect(buf, at)) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      {:error, _} = error -> error
    end
  end

  # `FieldNode` and `Buffer` are both structs of two int64s, stored inline and packed.
  defp struct_vector(_buf, nil), do: []

  defp struct_vector(buf, pos) do
    start = indirect(buf, pos)
    count = u32(buf, start)

    Enum.map(0..(count - 1)//1, fn index ->
      at = start + 4 + index * 16
      {i64(buf, at), i64(buf, at + 8)}
    end)
  end

  defp u16(buf, pos) do
    <<value::little-16>> = binary_part(buf, pos, 2)
    value
  end

  defp u32(buf, pos) do
    <<value::little-32>> = binary_part(buf, pos, 4)
    value
  end

  defp i32(buf, pos) do
    <<value::little-signed-32>> = binary_part(buf, pos, 4)
    value
  end

  defp i64(buf, pos) do
    <<value::little-signed-64>> = binary_part(buf, pos, 8)
    value
  end
end
