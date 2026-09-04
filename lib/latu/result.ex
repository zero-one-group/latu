defmodule Latu.Result do
  @moduledoc """
  Arrow batches in, Explorer out — and, for `Latu.create_dataframe/3`, the other direction.

  This module is Latu's Explorer boundary in both directions; nothing else touches Arrow.

  It also owns the shape a schema comes back as. `Latu.schema/1` and `Latu.parse_ddl/2` return
  a list of `t:field/0`, each carrying **Spark's own name for its type**, as a string: there is
  no client-side type model in either direction — `Latu.create_dataframe/3` takes a DDL string
  going out, and this is what comes back (M8.1 and M10.1; `docs/decisions.md`). Nested types
  render *into* the type string — `array<int>`, `struct<a:int,b:string>` — so a schema stays a
  flat, pattern-matchable list however deep the data is.

  A schema is also checked before any bytes are decoded: Spark's interval types panic inside
  Polars' NIF rather than failing cleanly, so a result carrying one is refused, by column name,
  using the `DataType` the server sends ahead of the first batch.
  """

  alias Explorer.DataFrame
  alias Explorer.Series
  alias Latu.Client
  alias Latu.Error

  @typedoc "One top-level column: its name, Spark's name for its type, and its nullability."
  @type field :: %{name: String.t(), type: String.t(), nullable: boolean()}

  @doc """
  Decode Arrow batches into one Explorer DataFrame.

  One `load_ipc_stream` per batch. Each batch is a complete IPC stream with its own end-of-
  stream marker, so concatenating the binaries first would silently keep the first batch's rows
  and drop the rest — the decoded row count is checked against the server's for that reason.

  Options: `:columns`, passed to Explorer at load time so pruned columns are never deserialized.

  A payload Polars cannot parse raises rather than returning an error; Spark's interval types
  are the live hazard, and the schema guard described above is what heads them off — callers
  run it on the execution's schema before handing bytes here.
  """
  @spec decode([Client.batch()], keyword()) :: {:ok, DataFrame.t()} | {:error, Error.t()}
  def decode(batches, opts \\ [])

  def decode([], _opts), do: {:error, Error.new(:decode, "no Arrow batches to decode")}

  def decode(batches, opts) do
    opts = Keyword.validate!(opts, columns: nil)

    frame =
      batches
      |> Enum.map(&DataFrame.load_ipc_stream!(&1.data, columns: opts[:columns]))
      |> DataFrame.concat_rows()

    expected = Enum.sum(Enum.map(batches, & &1.row_count))
    decoded = DataFrame.n_rows(frame)

    if decoded == expected do
      {:ok, frame}
    else
      {:error, Error.new(:decode, "decoded #{decoded} rows, the server reported #{expected}")}
    end
  end

  @doc """
  The rows of a decoded frame, as maps.

  Streamed out in chunks rather than through `Explorer.DataFrame.to_rows/2`, which converts
  every column to a full list before emitting one row. `:atoms` interns the
  column names — bounded by the schemas ever selected, not by data.
  """
  @spec rows(DataFrame.t(), :atoms | :strings) :: [map()]
  def rows(%DataFrame{} = frame, keys) when keys in [:atoms, :strings] do
    frame |> DataFrame.to_rows_stream(atom_keys: keys == :atoms) |> Enum.to_list()
  end

  @doc "The one cell of a 1x1 result, positionally — PySpark's `table[0][0]`."
  @spec only(DataFrame.t()) :: {:ok, term()} | {:error, Error.t()}
  def only(%DataFrame{} = frame) do
    case DataFrame.names(frame) do
      [column] -> only(frame, column)
      names -> {:error, Error.new(:decode, "expected one column, got #{length(names)}")}
    end
  end

  @doc "The one cell of a 1x1 result, as `ShowString` and `HtmlString` produce."
  @spec only(DataFrame.t(), String.t()) :: {:ok, term()} | {:error, Error.t()}
  def only(%DataFrame{} = frame, column) do
    case DataFrame.n_rows(frame) do
      1 -> {:ok, frame |> DataFrame.pull(column) |> Series.at(0)}
      rows -> {:error, Error.new(:decode, "expected one row of #{column}, got #{rows}")}
    end
  end

  # =============================================
  # Local data, the other direction
  # =============================================

  @doc """
  An Explorer DataFrame from column data — `{name, values}` pairs, in the given order.

  The encode side of `Latu.create_dataframe/3`; ragged or untypeable columns raise, as
  `Explorer.DataFrame.new/1` does.
  """
  @spec from_columns([{atom() | String.t(), list()}] | DataFrame.t()) :: DataFrame.t()
  def from_columns(%DataFrame{} = frame), do: frame
  def from_columns(columns) when is_list(columns), do: DataFrame.new(columns)

  @doc "Row count, so callers outside the Explorer boundary need no Explorer call."
  @spec size(DataFrame.t()) :: non_neg_integer()
  def size(%DataFrame{} = frame), do: DataFrame.n_rows(frame)

  @doc """
  One Arrow IPC stream for the whole frame — exactly the bytes `LocalRelation.data` carries.

  Raises on a frame Explorer cannot serialize; that is an argument problem, not a transport
  one.
  """
  @spec to_ipc(DataFrame.t()) :: binary()
  def to_ipc(%DataFrame{} = frame), do: DataFrame.dump_ipc_stream!(frame)

  @doc """
  The frame as row slices, each dumped as its own complete IPC stream.

  Every chunk must independently decode — the server caches each as a separate artifact and
  reads them back by hash (`ChunkedCachedLocalRelation`) — which is why this slices and
  re-dumps rather than splitting `to_ipc/1`'s bytes.
  """
  @spec to_ipc_chunks(DataFrame.t(), pos_integer()) :: [binary()]
  def to_ipc_chunks(%DataFrame{} = frame, rows_per_chunk) when rows_per_chunk >= 1 do
    total = DataFrame.n_rows(frame)

    0..(total - 1)//rows_per_chunk
    |> Enum.map(fn offset ->
      frame |> DataFrame.slice(offset, min(rows_per_chunk, total - offset)) |> to_ipc()
    end)
  end
end
