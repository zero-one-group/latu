defmodule Latu.DataFrame do
  @moduledoc """
  A lazy DataFrame: a session and an inert plan.

  Building one does no IO and touches no process. Nothing reaches the server until an action,
  such as `Latu.show/2` or `Latu.collect/2`.
  """

  require Logger

  alias Latu.Client
  alias Latu.Error
  alias Latu.ExecutionInfo
  alias Latu.GroupedData
  alias Latu.MergeInto
  alias Latu.Plan
  alias Latu.Result
  alias Latu.Session

  @enforce_keys [:session, :plan]
  defstruct [:session, :plan]

  @type t :: %__MODULE__{session: Session.t(), plan: Plan.relation()}

  # =============================================
  # Sources
  # =============================================

  @doc "See `Latu.range/2`."
  @spec range(Session.t(), integer(), integer(), integer(), keyword()) :: t()
  def range(session, start, stop, step, opts \\ [])

  def range(%Session{} = session, start, stop, step, opts)
      when is_integer(start) and is_integer(stop) and is_integer(step) and step != 0 do
    opts = Keyword.validate!(opts, num_partitions: nil)

    new(session, Plan.range(start, stop, step, partitions!(opts[:num_partitions])))
  end

  def range(%Session{}, _start, _stop, 0, _opts) do
    raise ArgumentError, "range step cannot be 0"
  end

  defp partitions!(nil), do: nil
  defp partitions!(count) when is_integer(count) and count > 0, do: count

  defp partitions!(count) do
    raise ArgumentError, "num_partitions is a positive integer, not #{inspect(count)}"
  end

  @doc "See `Latu.read/2`."
  @spec read(Session.t(), keyword()) :: t()
  def read(%Session{} = session, opts) when is_list(opts) do
    {reserved, options} = Keyword.split(opts, [:format, :schema, :path, :paths])

    if reserved[:path] && reserved[:paths] do
      raise ArgumentError, "read takes :path or :paths, not both"
    end

    plan =
      Plan.read(
        format: reserved[:format],
        schema: reserved[:schema] || "",
        paths: List.wrap(reserved[:paths] || reserved[:path]),
        options: options
      )

    new(session, plan)
  end

  @doc "See `Latu.table/2`."
  @spec table(Session.t(), String.t() | atom(), keyword() | map()) :: t()
  def table(%Session{} = session, name, options \\ []) do
    new(session, Plan.table(name, options))
  end

  @doc "See `Latu.sql/3`."
  @spec sql(Session.t(), String.t(), [term()] | map() | keyword()) ::
          {:ok, t()} | {:error, Error.t()}
  def sql(%Session{} = session, query, bindings \\ []) do
    {args, views} = bindings_and_views(bindings)

    case Client.execute_command(session, Plan.new(Plan.sql_command(query, args, views))) do
      # No result relation — root at the lazy SQL relation, PySpark's own fallback.
      {:ok, %{command_result: nil} = executed} ->
        {:ok, new(executed.session, Plan.sql(query, args, views))}

      # The server answers with the relation to keep querying; the DataFrame roots there, so
      # the query does not run again. PySpark's CachedRelation path.
      {:ok, executed} ->
        {:ok, new(executed.session, Plan.adopt(executed.command_result))}

      {:error, _} = error ->
        error
    end
  end

  # The third argument is bindings — a list for `?`, a map for `:name` — or options, which is
  # how a DataFrame gets named into the query. A keyword list is the one shape that could be
  # read either way, and options win: named bindings are a map, as `sql/3`'s docs say.
  defp bindings_and_views([]), do: {[], []}

  defp bindings_and_views(bindings) when is_list(bindings) do
    if Keyword.keyword?(bindings) do
      case Keyword.validate(bindings, args: [], views: []) do
        {:ok, opts} ->
          {opts[:args], Enum.map(opts[:views], &view/1)}

        {:error, keys} ->
          raise ArgumentError,
                "sql/3 takes options here (:args, :views), got #{inspect(keys)}. Named " <>
                  "bindings are a map: Latu.sql(session, query, %{#{binding_hint(keys)}})"
      end
    else
      {bindings, []}
    end
  end

  defp bindings_and_views(bindings), do: {bindings, []}

  defp binding_hint(keys), do: Enum.map_join(keys, ", ", &"#{&1}: ...")

  defp view({name, %__MODULE__{} = df}), do: {name, df.plan}

  defp view({name, other}) do
    raise ArgumentError, "views: #{inspect(name)} needs a DataFrame, got #{inspect(other)}"
  end

  @doc "Like `sql/3`, raising on failure."
  @spec sql!(Session.t(), String.t(), [term()] | map() | keyword()) :: t()
  def sql!(%Session{} = session, query, bindings \\ []) do
    unwrap!(sql(session, query, bindings))
  end

  # The session configs createDataFrame reads in one batch, PySpark's list minus the
  # Python-conversion ones (Explorer does Latu's conversion).
  @local_relation_configs [
    "spark.sql.session.localRelationCacheThreshold",
    "spark.sql.session.localRelationSizeLimit",
    "spark.sql.session.localRelationChunkSizeRows",
    "spark.sql.session.localRelationChunkSizeBytes",
    "spark.sql.session.localRelationBatchOfChunksSizeBytes"
  ]

  @doc "See `Latu.create_dataframe/3`."
  @spec create_dataframe(Session.t(), term(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def create_dataframe(%Session{} = session, data, opts \\ []) do
    opts = Keyword.validate!(opts, schema: nil)
    schema = opts[:schema] && schema_string!(opts[:schema])

    case columns_for(data) do
      :empty when is_nil(schema) ->
        raise ArgumentError,
              "empty data has no types to infer — pass schema: (DDL, or Spark's JSON form)"

      :empty ->
        {:ok, new(session, Plan.local_relation(nil, schema))}

      columns ->
        frame = Result.from_columns(columns)
        ipc = Result.to_ipc(frame)

        with {:ok, configs, session} <- Client.get_configs(session, @local_relation_configs) do
          threshold = config_int!(configs, "spark.sql.session.localRelationCacheThreshold")

          if byte_size(ipc) < threshold do
            {:ok, new(session, Plan.local_relation(ipc, schema))}
          else
            cache_local_relation(session, frame, ipc, schema, configs)
          end
        end
    end
  end

  @doc "Like `create_dataframe/3`, raising on failure."
  @spec create_dataframe!(Session.t(), term(), keyword()) :: t()
  def create_dataframe!(%Session{} = session, data, opts \\ []) do
    unwrap!(create_dataframe(session, data, opts))
  end

  # Over the threshold: chunk the frame, cache the chunks (and the schema, when given) as
  # session artifacts in size-bounded upload batches, and reference the hashes. PySpark's
  # _cache_local_relation.
  defp cache_local_relation(session, frame, ipc, schema, configs) do
    size_limit = config_int!(configs, "spark.sql.session.localRelationSizeLimit")
    max_rows = config_int!(configs, "spark.sql.session.localRelationChunkSizeRows")
    max_bytes = config_int!(configs, "spark.sql.session.localRelationChunkSizeBytes")
    max_batch = config_int!(configs, "spark.sql.session.localRelationBatchOfChunksSizeBytes")

    rows = rows_per_chunk(Result.size(frame), byte_size(ipc), max_rows, min(max_bytes, max_batch))
    chunks = Result.to_ipc_chunks(frame, rows)
    blobs = if schema, do: [schema | chunks], else: chunks
    total = Enum.reduce(blobs, 0, &(byte_size(&1) + &2))

    if total > size_limit do
      raise ArgumentError,
            "local relation is #{total} bytes serialized, over the server's " <>
              "spark.sql.session.localRelationSizeLimit of #{size_limit}"
    end

    with {:ok, hashes, session} <- upload_batches(session, batch_blobs(blobs, max_batch)) do
      case schema do
        nil -> {:ok, new(session, Plan.chunked_cached_local_relation(hashes))}
        _ -> {:ok, new(session, Plan.chunked_cached_local_relation(tl(hashes), hd(hashes)))}
      end
    end
  end

  defp upload_batches(session, batches) do
    Enum.reduce_while(batches, {:ok, [], session}, fn batch, {:ok, acc, session} ->
      case Client.cache_artifacts(session, batch) do
        {:ok, hashes, session} -> {:cont, {:ok, acc ++ hashes, session}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  @doc false
  # Rows per IPC chunk from the whole frame's serialized size — an estimate, as PySpark's
  # raw-buffer arithmetic is; each chunk stays under both the row and byte bounds.
  def rows_per_chunk(total_rows, total_bytes, max_rows, max_bytes)
      when total_rows > 0 and max_rows > 0 and max_bytes > 0 do
    bytes_per_row = max(1, div(total_bytes, total_rows))
    max(1, min(max_rows, div(max_bytes, bytes_per_row)))
  end

  @doc false
  # Group blobs into upload batches of at most max_batch bytes each — one cache_artifacts
  # call per group, PySpark's middle ground between one RPC per chunk and everything at once.
  # A blob over the bound travels alone.
  def batch_blobs(blobs, max_batch) do
    blobs
    |> Enum.chunk_while(
      {[], 0},
      fn blob, {batch, size} ->
        cond do
          batch == [] ->
            {:cont, {[blob], byte_size(blob)}}

          size + byte_size(blob) > max_batch ->
            {:cont, Enum.reverse(batch), {[blob], byte_size(blob)}}

          true ->
            {:cont, {[blob | batch], size + byte_size(blob)}}
        end
      end,
      fn
        {[], _size} -> {:cont, []}
        {batch, _size} -> {:cont, Enum.reverse(batch), []}
      end
    )
  end

  @doc false
  # Column pairs from what the user has: an Explorer frame passes through; a keyword list is
  # columns in declared order; a map of columns and a list of row maps sort their keys —
  # PySpark's own rule for dicts, and the only deterministic order a map offers. Every row
  # must carry every key (KeyError names the offender).
  def columns_for(%{__struct__: Explorer.DataFrame} = frame) do
    if Result.size(frame) == 0, do: :empty, else: frame
  end

  def columns_for([]), do: :empty
  def columns_for(map) when map == %{}, do: :empty

  def columns_for(data) when is_map(data) and not is_struct(data) do
    data |> Enum.sort() |> Enum.map(fn {name, values} -> {column_name(name), column!(values)} end)
  end

  def columns_for([{name, _} | _] = columns) when is_atom(name) do
    Enum.map(columns, fn {name, values} -> {column_name(name), column!(name, values)} end)
  end

  def columns_for([row | _] = rows) when is_map(row) and not is_struct(row) do
    for name <- row |> Map.keys() |> Enum.sort() do
      {column_name(name), Enum.map(rows, &Map.fetch!(&1, name))}
    end
  end

  def columns_for(other) do
    raise ArgumentError,
          "create_dataframe takes an Explorer.DataFrame, a list of row maps, or column data " <>
            "(a map or keyword list of lists), got: #{inspect(other)}"
  end

  defp column!(values) when is_list(values), do: values

  defp column!(other) do
    raise ArgumentError, "a column's data is a list, got #{inspect(other)}"
  end

  # The keyword form is the one Elixir's trailing-keyword sugar can swallow the options into:
  # `create_dataframe(session, id: [1], schema: "id INT")` is one keyword list, not two, so the
  # `schema:` arrives here as a column. Name the fix rather than let Explorer fail on the value.
  defp column!(_name, values) when is_list(values), do: values

  defp column!(name, other) do
    raise ArgumentError,
          "column #{inspect(name)} is not a list of values, got #{inspect(other)}. If " <>
            "#{inspect(name)} was meant as an option, bracket the data: " <>
            "create_dataframe(session, [name: [...]], #{name}: ...)"
  end

  defp column_name(name) when is_binary(name), do: name
  defp column_name(name) when is_atom(name) and not is_nil(name), do: Atom.to_string(name)

  defp column_name(name) do
    raise ArgumentError, "a column name is a string or an atom, not #{inspect(name)}"
  end

  defp schema_string!(schema) when is_binary(schema), do: schema

  defp schema_string!(schema) do
    raise ArgumentError,
          "a schema is a string — DDL or Spark's JSON schema form — not #{inspect(schema)}"
  end

  defp config_int!(configs, key) do
    case configs[key] do
      value when is_binary(value) -> String.to_integer(value)
      nil -> raise ArgumentError, "the server does not define #{key} — is it Spark 4.2+?"
    end
  end

  # =============================================
  # Transformations
  # =============================================

  @doc """
  Keep these columns, in this order.

  A string or an atom is a column name; anything else is an expression. Trailing keywords name
  what they hold:

      Latu.select(df, [:id, doubled: multiply(:id, 2)])

  A single column needs no list.
  """
  @spec select(t(), term()) :: t()
  def select(%__MODULE__{} = df, columns) do
    %{df | plan: Plan.project(df.plan, Plan.to_projections(columns))}
  end

  @doc "See `Latu.select_expr/2`."
  @spec select_expr(t(), [String.t()] | String.t()) :: t()
  def select_expr(%__MODULE__{} = df, expressions) do
    projections = expressions |> List.wrap() |> Enum.map(&Plan.expr/1)

    %{df | plan: Plan.project(df.plan, projections)}
  end

  @doc "See `Latu.zip_with_index/2`."
  @spec zip_with_index(t(), String.t() | atom()) :: t()
  def zip_with_index(%__MODULE__{} = df, name \\ :index) do
    # PySpark's own definition, and there is no relation for it: a Project of every column plus
    # one internal function. `distributed_sequence_id` is registered with
    # `registerInternalExpression`, so it is resolvable in a plan but hidden from
    # `DESCRIBE FUNCTION` — which is why it is not in Latu's harvested registry and is named
    # here as a string rather than reached through `Latu.Functions`.
    index = Plan.as(Plan.fun("distributed_sequence_id", []), name)

    %{df | plan: Plan.project(df.plan, [Plan.star(), index])}
  end

  @doc "See `Latu.observe/3`."
  @spec observe(t(), String.t() | atom(), keyword() | [Plan.expression()]) :: t()
  def observe(%__MODULE__{} = df, name, metrics) when is_list(metrics) do
    %{df | plan: Plan.collect_metrics(df.plan, name, metrics)}
  end

  @doc """
  Keep the rows the condition holds for.

  A string is SQL, parsed by the server — `filter(df, "id > 3")` is `filter(df, expr("id >
  3"))`. This is the only position where a string means SQL: in `select/2` it is a column name,
  and inside an expression it is a literal. PySpark reads all three the same way.

      Latu.filter(df, greater(:id, 3))
      Latu.filter(df, "id > 3")
  """
  @spec filter(t(), term()) :: t()
  def filter(%__MODULE__{} = df, condition) do
    %{df | plan: Plan.filter(df.plan, condition(condition))}
  end

  @doc "`filter/2`, spelled Spark's other way."
  @spec where(t(), term()) :: t()
  def where(%__MODULE__{} = df, condition), do: filter(df, condition)

  @doc """
  Add or replace columns, keeping the rest.

      Latu.with_columns(df, doubled: multiply(:id, 2))
      Latu.with_columns(df, a: add(:id, 1), b: subtract(:id, 1))

  A keyword list, because it is ordered and a map is not. There is no `with_column`: Spark
  has no singular relation, and the keyword form is already short.
  """
  @spec with_columns(t(), keyword()) :: t()
  def with_columns(%__MODULE__{} = df, columns) do
    if Keyword.keyword?(columns) do
      %{df | plan: Plan.with_columns(df.plan, Enum.map(columns, &named/1))}
    else
      raise ArgumentError,
            "with_columns/2 takes a keyword list, which is ordered, got #{inspect(columns)}"
    end
  end

  @doc """
  Remove columns.

      Latu.drop(df, :x)
      Latu.drop(df, [:x, "y"])
  """
  @spec drop(t(), term()) :: t()
  def drop(%__MODULE__{} = df, columns) do
    %{df | plan: Plan.drop(df.plan, List.wrap(columns))}
  end

  @doc """
  Rename columns.

      Latu.rename(df, id: :n)         # by mapping, leaving the rest
      Latu.rename(df, [:renamed])     # positionally, one name per column

  Two Spark relations behind one verb: `WithColumnsRenamed` for pairs, `ToDF` for a plain list.
  `Explorer.DataFrame.rename/2` reads both shapes the same way.
  """
  @spec rename(t(), keyword() | map() | [String.t() | atom()]) :: t()
  def rename(%__MODULE__{} = df, names) when is_map(names) and not is_struct(names) do
    rename(df, Map.to_list(names))
  end

  def rename(%__MODULE__{} = df, names) when is_list(names) do
    plan =
      if pairs?(names) do
        Plan.with_columns_renamed(df.plan, names)
      else
        Plan.to_df(df.plan, names)
      end

    %{df | plan: plan}
  end

  @doc "Keep at most `count` rows."
  @spec limit(t(), non_neg_integer()) :: t()
  def limit(%__MODULE__{} = df, count), do: %{df | plan: Plan.limit(df.plan, count)}

  @doc "Skip the first `count` rows."
  @spec offset(t(), non_neg_integer()) :: t()
  def offset(%__MODULE__{} = df, count), do: %{df | plan: Plan.offset(df.plan, count)}

  @doc """
  Drop duplicate rows.

      Latu.distinct(df)              # every column is a key
      Latu.distinct(df, [:suburb])   # these columns are
  """
  @spec distinct(t(), term()) :: t()
  def distinct(%__MODULE__{} = df, columns \\ []) do
    %{df | plan: Plan.deduplicate(df.plan, List.wrap(columns))}
  end

  @doc """
  Sort rows.

      Latu.sort(df, :id)                    # ascending, nulls first
      Latu.sort(df, [desc(:price), :id])
      Latu.order_by(df, "id")

  A bare name sorts ascending with nulls first, as PySpark's `orderBy("id")` does.
  `Latu.Column.asc/1`, `Latu.Column.desc/1` and the four explicit `*_nulls_*` spellings are
  the alternatives.
  """
  @spec sort(t(), term()) :: t()
  def sort(%__MODULE__{} = df, columns) do
    %{df | plan: Plan.sort(df.plan, sort_orders(columns))}
  end

  @doc "`sort/2`, spelled Spark's other way."
  @spec order_by(t(), term()) :: t()
  def order_by(%__MODULE__{} = df, columns), do: sort(df, columns)

  @doc """
  Sort within each partition, leaving the partitions unordered.

  The same relation as `sort/2` with `is_global: false`, and cheaper: no shuffle.
  """
  @spec sort_within_partitions(t(), term()) :: t()
  def sort_within_partitions(%__MODULE__{} = df, columns) do
    %{df | plan: Plan.sort(df.plan, sort_orders(columns), global: false)}
  end

  @doc """
  A random fraction of the rows.

      Latu.sample(df, 0.1)
      Latu.sample(df, 0.1, seed: 42, with_replacement: true)

  Without a `:seed` a random one is drawn, as in PySpark, so the plan differs between runs.
  """
  @spec sample(t(), number(), keyword()) :: t()
  def sample(%__MODULE__{} = df, fraction, opts \\ []) do
    %{df | plan: Plan.sample(df.plan, fraction, opts)}
  end

  @doc "See `Latu.random_split/3`."
  @spec random_split(t(), [number()], keyword()) :: [t()]
  def random_split(%__MODULE__{} = df, weights, opts \\ []) when is_list(weights) do
    opts = Keyword.validate!(opts, seed: nil)
    seed = opts[:seed] || Plan.random_seed()

    weights
    |> bounds()
    |> Enum.map(fn {lower, upper} ->
      plan =
        Plan.sample(df.plan, upper,
          lower_bound: lower,
          seed: seed,
          with_replacement: false,
          deterministic_order: true
        )

      %{df | plan: plan}
    end)
  end

  # PySpark's own arithmetic: normalise the weights to proportions, then take the running sums
  # as the slice boundaries, so the slices tile [0.0, 1.0] exactly however the weights are
  # spelled. `[8, 2]` and `[0.8, 0.2]` give the same plans.
  defp bounds(weights) do
    if Enum.any?(weights, &(not is_number(&1) or &1 < 0)) do
      raise ArgumentError,
            "random_split weights are non-negative numbers, got #{inspect(weights)}"
    end

    total = Enum.sum(weights)

    if total <= 0 do
      raise ArgumentError,
            "random_split weights must add up to more than 0, got #{inspect(weights)}"
    end

    cumulative = Enum.scan(weights, 0.0, fn weight, running -> running + weight / total end)

    Enum.zip([0.0 | cumulative], cumulative)
  end

  @doc "See `Latu.hint/3`."
  @spec hint(t(), String.t() | atom(), [term()]) :: t()
  def hint(%__MODULE__{} = df, name, parameters \\ []) do
    %{df | plan: Plan.hint(df.plan, name, parameters)}
  end

  @doc "See `Latu.unpivot/3`."
  @spec unpivot(t(), term(), keyword()) :: t()
  def unpivot(%__MODULE__{} = df, ids, opts) do
    %{df | plan: Plan.unpivot(df.plan, List.wrap(ids), opts)}
  end

  @doc "See `Latu.transpose/2`."
  @spec transpose(t(), term() | nil) :: t()
  def transpose(%__MODULE__{} = df, index_column \\ nil) do
    %{df | plan: Plan.transpose(df.plan, index_column)}
  end

  @doc "See `Latu.to/2`."
  @spec to(t(), Plan.data_type()) :: t()
  def to(%__MODULE__{} = df, schema) do
    %{df | plan: Plan.to_schema(df.plan, schema)}
  end

  @doc "See `Latu.with_metadata/3`."
  @spec with_metadata(t(), String.t() | atom(), map()) :: t()
  def with_metadata(%__MODULE__{} = df, name, metadata) do
    %{df | plan: Plan.with_metadata(df.plan, name, metadata)}
  end

  @doc "See `Latu.repartition_by_range/3`."
  @spec repartition_by_range(t(), term(), keyword()) :: t()
  def repartition_by_range(%__MODULE__{} = df, columns, opts \\ []) do
    opts = Keyword.validate!(opts, num_partitions: nil)
    count = partitions!(opts[:num_partitions])

    %{df | plan: Plan.repartition_by_range(df.plan, sort_orders(columns), count)}
  end

  @doc "See `Latu.cross_join/2`."
  @spec cross_join(t(), t()) :: t()
  def cross_join(%__MODULE__{} = df, %__MODULE__{} = other) do
    same_session!(df, other)

    %{df | plan: Plan.join(df.plan, other.plan, how: :cross)}
  end

  @doc "See `Latu.parse/2`."
  @spec parse(t(), keyword()) :: t()
  def parse(%__MODULE__{} = df, opts) do
    %{df | plan: Plan.parse(df.plan, opts)}
  end

  @doc "See `Latu.table_function/3`."
  @spec table_function(Session.t(), String.t() | atom(), [term()]) :: t()
  def table_function(%Session{} = session, name, arguments \\ []) do
    new(session, Plan.table_function(name, arguments))
  end

  @doc "See `Latu.table_changes/3`."
  @spec table_changes(Session.t(), String.t() | atom(), keyword()) :: t()
  def table_changes(%Session{} = session, table, opts \\ []) do
    new(session, Plan.table_changes(table, opts))
  end

  @doc "See `Latu.checkpoint/2`."
  @spec checkpoint(t(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def checkpoint(%__MODULE__{} = df, opts \\ []) do
    case run_command(df, Plan.checkpoint(df.plan, without_progress(opts)), watch(opts)) do
      {:ok, %{checkpointed: nil}} ->
        {:error, Error.new(:protocol, "the server checkpointed but sent back no relation id")}

      {:ok, executed} ->
        {:ok, new(executed.session, Plan.cached_remote_relation(executed.checkpointed))}

      {:error, _} = error ->
        error
    end
  end

  @doc "Like `checkpoint/2`, raising on failure."
  @spec checkpoint!(t(), keyword()) :: t()
  def checkpoint!(%__MODULE__{} = df, opts \\ []), do: unwrap!(checkpoint(df, opts))

  @doc "See `Latu.release/1`."
  @spec release(t()) :: :ok | {:error, Error.t()}
  def release(%__MODULE__{} = df) do
    case Plan.cached_relation_id(df.plan) do
      {:ok, id} ->
        with {:ok, _executed} <- run_command(df, Plan.remove_cached_relation(id)), do: :ok

      :error ->
        raise ArgumentError,
              "release/1 frees a checkpointed frame, and this one is not checkpointed — " <>
                "checkpoint/2 is what hands one back"
    end
  end

  @doc "Like `release/1`, raising on failure."
  @spec release!(t()) :: :ok
  def release!(%__MODULE__{} = df), do: run!(release(df))

  @doc "See `Latu.with_checkpoint/3`."
  @spec with_checkpoint(t(), keyword(), (t() -> result)) ::
          {:ok, result} | {:error, Error.t()}
        when result: term()
  def with_checkpoint(%__MODULE__{} = df, opts, fun) when is_function(fun, 1) do
    with {:ok, checkpointed} <- checkpoint(df, opts) do
      try do
        {:ok, fun.(checkpointed)}
      after
        # `after`, not `else`: the point of the bracket is that a raise inside the function
        # still frees the resource. A failed release is logged and swallowed, because raising
        # here would replace the caller's own exception with a less interesting one.
        release_quietly(checkpointed)
      end
    end
  end

  @doc "Like `with_checkpoint/3`, raising on failure."
  @spec with_checkpoint!(t(), keyword(), (t() -> result)) :: result when result: term()
  def with_checkpoint!(%__MODULE__{} = df, opts, fun) do
    unwrap!(with_checkpoint(df, opts, fun))
  end

  # Runs in an `after`, so it must not raise: an exception here would replace whatever the
  # caller's function raised with something far less interesting. Rescues rather than only
  # matching `{:error, _}` for that reason — `release/1` raises on a frame that is not
  # checkpointed, which cannot happen from here today and is one refactor away from being able
  # to.
  defp release_quietly(%__MODULE__{} = df) do
    case release(df) do
      :ok -> :ok
      {:error, error} -> Logger.warning(held(Exception.message(error)))
    end
  rescue
    error -> Logger.warning(held(Exception.message(error)))
  end

  defp held(reason) do
    "could not release the checkpoint: #{reason}. It stays until the session ends."
  end

  @doc "See `Latu.merge_into/4`."
  @spec merge_into(t(), String.t() | atom(), term(), keyword()) :: MergeInto.t()
  def merge_into(%__MODULE__{} = source, table, condition, opts \\ []) do
    MergeInto.new(source, table, condition, opts)
  end

  @doc "See `Latu.merge/2`."
  @spec merge(MergeInto.t(), keyword()) :: :ok | {:error, Error.t()}
  def merge(%MergeInto{} = merge, opts \\ []) do
    run_write(merge.source, MergeInto.command(merge), Keyword.validate!(opts, progress: nil))
  end

  @doc "Like `merge/2`, raising on failure."
  @spec merge!(MergeInto.t(), keyword()) :: :ok
  def merge!(%MergeInto{} = merge, opts \\ []), do: run!(merge(merge, opts))

  @doc "See `Latu.join_as_of/3`."
  @spec join_as_of(t(), t(), keyword()) :: t()
  def join_as_of(%__MODULE__{} = df, %__MODULE__{} = other, opts) do
    same_session!(df, other)

    {left, opts} = Keyword.pop(opts, :left_as_of)
    {right, opts} = Keyword.pop(opts, :right_as_of)

    plan =
      Plan.as_of_join(
        df.plan,
        other.plan,
        Keyword.merge(opts, left_as_of: as_of(df, left), right_as_of: as_of(other, right))
      )

    %{df | plan: plan}
  end

  # A bare name is tagged to the frame it belongs to, which is what PySpark's `_col` does and
  # what makes the two as-of columns unambiguous when both sides carry the same name. Anything
  # already built goes through untouched; nil goes through so the plan layer names what is
  # missing.
  defp as_of(%__MODULE__{} = df, name) when is_binary(name), do: col(df, name)

  defp as_of(%__MODULE__{} = df, name) when is_atom(name) and not is_nil(name) do
    col(df, name)
  end

  defp as_of(%__MODULE__{}, built), do: built

  @doc "See `Latu.lateral_join/3`."
  @spec lateral_join(t(), t(), keyword()) :: t()
  def lateral_join(%__MODULE__{} = df, %__MODULE__{} = other, opts \\ []) do
    same_session!(df, other)

    %{df | plan: Plan.lateral_join(df.plan, other.plan, opts)}
  end

  @doc "See `Latu.nearest_by_join/4`."
  @spec nearest_by_join(t(), t(), term(), keyword()) :: t()
  def nearest_by_join(%__MODULE__{} = df, %__MODULE__{} = other, ranking, opts) do
    same_session!(df, other)

    %{df | plan: Plan.nearest_by_join(df.plan, other.plan, ranking, opts)}
  end

  @doc """
  Shuffle into `count` partitions, or partition by these columns, or both.

      Latu.repartition(df, 4)
      Latu.repartition(df, [:suburb])
      Latu.repartition(df, 4, [:suburb])

  Two Spark relations: `Repartition` for a count alone, `RepartitionByExpression` once columns
  are named.
  """
  @spec repartition(t(), pos_integer() | term()) :: t()
  def repartition(%__MODULE__{} = df, count) when is_integer(count) do
    %{df | plan: Plan.repartition(df.plan, count)}
  end

  def repartition(%__MODULE__{} = df, columns) do
    %{df | plan: Plan.repartition_by(df.plan, partitions(columns))}
  end

  @doc "See `repartition/2`."
  @spec repartition(t(), pos_integer(), term()) :: t()
  def repartition(%__MODULE__{} = df, count, columns) when is_integer(count) do
    %{df | plan: Plan.repartition_by(df.plan, partitions(columns), count)}
  end

  @doc "Fewer partitions without a shuffle. `repartition/2` with `shuffle: false`."
  @spec coalesce(t(), pos_integer()) :: t()
  def coalesce(%__MODULE__{} = df, count) do
    %{df | plan: Plan.repartition(df.plan, count, shuffle: false)}
  end

  @doc """
  Group rows, giving a `Latu.GroupedData` that `agg/2` turns back into a DataFrame.

      df |> Latu.group_by(:suburb) |> Latu.agg(total: F.sum(:price))

  Spark has no `group_by` relation, so nothing is built until `agg/2`.
  """
  @spec group_by(t(), term()) :: GroupedData.t()
  def group_by(%__MODULE__{} = df, columns), do: GroupedData.new(df, :group_by, columns)

  @doc "Group by every prefix of these columns, plus the grand total."
  @spec rollup(t(), term()) :: GroupedData.t()
  def rollup(%__MODULE__{} = df, columns), do: GroupedData.new(df, :rollup, columns)

  @doc "Group by every combination of these columns."
  @spec cube(t(), term()) :: GroupedData.t()
  def cube(%__MODULE__{} = df, columns), do: GroupedData.new(df, :cube, columns)

  @doc "See `Latu.grouping_sets/3`."
  @spec grouping_sets(t(), [[term()]], term()) :: GroupedData.t()
  def grouping_sets(%__MODULE__{} = df, sets, columns \\ []) when is_list(sets) do
    GroupedData.new(df, :grouping_sets, columns, grouping_sets: sets)
  end

  @doc """
  Aggregate the whole frame, with no grouping.

      Latu.agg(df, total: F.sum(:price))

  The same `Aggregate` relation `group_by/2` builds, with no grouping expressions — which is
  what PySpark's `df.agg(...)` sends too.
  """
  @spec agg(t(), term()) :: t()
  def agg(%__MODULE__{} = df, aggregates) do
    %{df | plan: Plan.aggregate(df.plan, aggregates: Plan.to_projections(aggregates))}
  end

  @doc """
  All the rows of both, duplicates kept.

    * `:all` — default `true`, as Spark's `union` is `UNION ALL` rather than SQL's `UNION`
    * `:by_name` — match columns by name rather than position
    * `:allow_missing_columns` — fill a missing column with null; needs `:by_name`

      Latu.union(df, other)
      Latu.union(df, other, by_name: true)
  """
  @spec union(t(), t(), keyword()) :: t()
  def union(%__MODULE__{} = df, %__MODULE__{} = other, opts \\ []) do
    set_op(:union, df, other, opts)
  end

  @doc """
  Rows in both. Distinct unless `all: true`, which is PySpark's `intersectAll`.
  """
  @spec intersect(t(), t(), keyword()) :: t()
  def intersect(%__MODULE__{} = df, %__MODULE__{} = other, opts \\ []) do
    set_op(:intersect, df, other, opts)
  end

  @doc """
  Rows in the first and not the second.

  Distinct unless `all: true`. PySpark spells these `subtract` and `exceptAll`; `except` is
  Spark's own Scala name and SQL's.
  """
  @spec except(t(), t(), keyword()) :: t()
  def except(%__MODULE__{} = df, %__MODULE__{} = other, opts \\ []) do
    set_op(:except, df, other, opts)
  end

  @doc """
  Join two DataFrames.

      Latu.join(orders, customers, on: :customer_id)
      Latu.join(orders, customers, on: expr("o.id = c.id"), how: :left)
  """
  @spec join(t(), t(), keyword()) :: t()
  def join(%__MODULE__{} = df, %__MODULE__{} = other, opts \\ []) do
    same_session!(df, other)

    %{df | plan: Plan.join(df.plan, other.plan, opts)}
  end

  @doc """
  A reference to one of this DataFrame's columns, tagged with its identity.

      Latu.col(orders, :id)

  Needed only when two DataFrames in one pipeline share a column name, and it has to be a
  DataFrame the plan already contains: a self-join is fine — both branches *are* the relation —
  but **selecting one frame's column from another is refused by Spark**
  (`CANNOT_RESOLVE_DATAFRAME_COLUMN`), hoisted or not. Measured; `docs/decisions.md` (M9.1).
  For a value from another frame, use a subquery — `scalar/1`, `exists/1`, or
  `Latu.Column.isin/2` over a DataFrame.
  """
  @spec col(t(), String.t() | atom()) :: Plan.expression()
  def col(%__MODULE__{} = df, name), do: Plan.col(name, df.plan)

  @doc "See `Latu.col_regex/2`."
  @spec col_regex(t(), String.t()) :: Plan.expression()
  def col_regex(%__MODULE__{} = df, pattern), do: Plan.col_regex(pattern, df.plan)

  @doc "See `Latu.metadata_column/2`."
  @spec metadata_column(t(), String.t() | atom()) :: Plan.expression()
  def metadata_column(%__MODULE__{} = df, name), do: Plan.metadata_column(name, df.plan)

  @doc """
  This DataFrame as a scalar subquery: a single value, usable wherever a value belongs.

      totals = Latu.agg(orders, total: F.sum(:amount))
      Latu.filter(orders, greater(:amount, Latu.scalar(totals)))

  The frame is hoisted into the plan that uses it, so the whole thing is one query and the two
  frames need no relationship beyond sharing a session. Spark refuses it at analysis if the
  subquery yields more than one row or column.

  This is the reference Spark resolves; a bare `col/2` pointing outside its own tree is not.

  Unlike `join/3` and the set operations, this does **not** check that both frames come from
  one session: the referenced plan travels inline, so a cross-session subquery still executes.
  What does not travel is session-scoped state — a temp view the other frame reads, an artifact
  behind its local data, a conf set on that session — and Spark names whatever is missing.
  `docs/decisions.md` (M9.3).
  """
  @spec scalar(t()) :: Plan.expression()
  def scalar(%__MODULE__{} = df), do: Plan.subquery(df.plan, :scalar)

  @doc """
  A predicate that holds when this DataFrame has any rows at all.

      Latu.filter(orders, Latu.exists(Latu.filter(alerts, :open)))

  Hoisted like `scalar/1`, including its note about sessions. Not to be confused with
  `Latu.Functions.exists/2`, which is Spark's higher-order function over an array — Spark named
  both.
  """
  @spec exists(t()) :: Plan.expression()
  def exists(%__MODULE__{} = df), do: Plan.subquery(df.plan, :exists)

  @doc """
  Name the DataFrame, so its columns can be qualified as `name.column`.

  Spark calls this `alias`, which Elixir cannot use as a function name. See `Latu.Plan.as/2`.
  """
  @spec as(t(), String.t() | atom()) :: t()
  def as(%__MODULE__{} = df, name), do: %{df | plan: Plan.as(df.plan, name)}

  defp condition(sql) when is_binary(sql), do: Plan.expr(sql)
  defp condition(other), do: Plan.to_expr(other)

  defp named({name, value}), do: {name, Plan.to_expr(value)}

  defp partitions(columns), do: columns |> List.wrap() |> Enum.map(&Plan.to_name/1)

  defp sort_orders(columns), do: columns |> List.wrap() |> Enum.map(&Plan.to_sort_order/1)

  defp set_op(kind, df, other, opts) do
    same_session!(df, other)

    %{df | plan: Plan.set_op(kind, df.plan, other.plan, opts)}
  end

  # Two-input verbs are the only place this can go wrong, and the server's error would not say
  # so. Compare ids, not structs: one may be pinned and the other not.
  defp same_session!(%{session: %{session_id: id}}, %{session: %{session_id: id}}), do: :ok

  defp same_session!(df, other) do
    raise ArgumentError,
          "these DataFrames come from different sessions, " <>
            "#{df.session.session_id} and #{other.session.session_id}"
  end

  # `{from, to}` pairs, keyword or not: `[id: :n]` and `[{"id", "n"}]` both rename by mapping.
  defp pairs?(names), do: names != [] and Enum.all?(names, &match?({_from, _to}, &1))

  # =============================================
  # Missing data
  # =============================================

  @doc "See `Latu.fill_na/3`."
  @spec fill_na(t(), term(), keyword()) :: t()
  def fill_na(df, value, opts \\ [])

  def fill_na(%__MODULE__{} = df, pairs, opts) when is_list(pairs) do
    unless Enum.all?(pairs, &match?({_name, _value}, &1)) do
      raise ArgumentError,
            "fill_na: a value, or {column, value} pairs, got #{inspect(pairs)}"
    end

    if opts != [] do
      raise ArgumentError,
            "fill_na: per-column pairs name their own columns, so :subset would be ignored; " <>
              "drop one or the other"
    end

    {cols, values} = Enum.unzip(pairs)

    %{df | plan: Plan.na_fill(df.plan, cols, values)}
  end

  def fill_na(%__MODULE__{} = df, value, opts) do
    opts = Keyword.validate!(opts, subset: [])

    %{df | plan: Plan.na_fill(df.plan, List.wrap(opts[:subset]), [value])}
  end

  @doc "See `Latu.drop_na/2`."
  @spec drop_na(t(), keyword()) :: t()
  def drop_na(%__MODULE__{} = df, opts \\ []) do
    opts = Keyword.validate!(opts, how: :any, min_non_nulls: nil, subset: [])

    %{df | plan: Plan.na_drop(df.plan, List.wrap(opts[:subset]), min_non_nulls!(opts))}
  end

  @doc "See `Latu.replace/3`."
  @spec replace(t(), [{term(), term()}], keyword()) :: t()
  def replace(%__MODULE__{} = df, replacements, opts \\ []) when is_list(replacements) do
    opts = Keyword.validate!(opts, subset: [])

    unless Enum.all?(replacements, &match?({_old, _new}, &1)) do
      raise ArgumentError, "replace: {old, new} pairs, got #{inspect(replacements)}"
    end

    %{df | plan: Plan.na_replace(df.plan, List.wrap(opts[:subset]), replacements)}
  end

  # `:min_non_nulls` overrides `:how`, exactly as PySpark's `thresh` overrides its `how` — and
  # `:any` sends nothing at all, because the field has presence and an absent one already means
  # "every column".
  defp min_non_nulls!(opts) do
    case {opts[:how], opts[:min_non_nulls]} do
      {_how, count} when is_integer(count) and count > 0 ->
        count

      {:any, nil} ->
        nil

      {:all, nil} ->
        1

      {how, nil} ->
        raise ArgumentError, "how: :any or :all, got #{inspect(how)}"

      {_how, count} ->
        raise ArgumentError, "min_non_nulls: a positive count, got #{inspect(count)}"
    end
  end

  # =============================================
  # Statistics
  # =============================================

  @doc "See `Latu.summary/2`."
  @spec summary(t(), [String.t() | atom()] | String.t() | atom()) :: t()
  def summary(%__MODULE__{} = df, statistics \\ []) do
    %{df | plan: Plan.summary(df.plan, List.wrap(statistics))}
  end

  @doc "See `Latu.describe/2`."
  @spec describe(t(), [String.t() | atom()] | String.t() | atom()) :: t()
  def describe(%__MODULE__{} = df, cols \\ []) do
    %{df | plan: Plan.describe(df.plan, List.wrap(cols))}
  end

  @doc "See `Latu.crosstab/3`."
  @spec crosstab(t(), String.t() | atom(), String.t() | atom()) :: t()
  def crosstab(%__MODULE__{} = df, col1, col2) do
    %{df | plan: Plan.crosstab(df.plan, col1, col2)}
  end

  @doc "See `Latu.freq_items/3`."
  @spec freq_items(t(), [String.t() | atom()] | String.t() | atom(), keyword()) :: t()
  def freq_items(%__MODULE__{} = df, cols, opts \\ []) do
    opts = Keyword.validate!(opts, support: 0.01)

    %{df | plan: Plan.freq_items(df.plan, List.wrap(cols), opts[:support])}
  end

  @doc "See `Latu.sample_by/4`."
  @spec sample_by(t(), term(), [{term(), number()}] | map(), keyword()) :: t()
  def sample_by(%__MODULE__{} = df, col, fractions, opts \\ []) do
    %{df | plan: Plan.sample_by(df.plan, col, fraction_pairs!(fractions), opts)}
  end

  @doc "See `Latu.cov/4`."
  @spec cov(t(), String.t() | atom(), String.t() | atom(), keyword()) ::
          {:ok, float()} | {:error, Error.t()}
  def cov(%__MODULE__{} = df, col1, col2, opts \\ []) do
    opts = Keyword.validate!(opts, progress: nil)

    one_cell(df, Plan.cov(df.plan, col1, col2), "cov", opts)
  end

  @doc "Like `cov/4`, raising on failure."
  @spec cov!(t(), String.t() | atom(), String.t() | atom(), keyword()) :: float()
  def cov!(%__MODULE__{} = df, col1, col2, opts \\ []), do: unwrap!(cov(df, col1, col2, opts))

  @doc "See `Latu.corr/4`."
  @spec corr(t(), String.t() | atom(), String.t() | atom(), keyword()) ::
          {:ok, float()} | {:error, Error.t()}
  def corr(%__MODULE__{} = df, col1, col2, opts \\ []) do
    opts = Keyword.validate!(opts, method: :pearson, progress: nil)

    one_cell(df, Plan.corr(df.plan, col1, col2, opts[:method]), "corr", opts)
  end

  @doc "Like `corr/4`, raising on failure."
  @spec corr!(t(), String.t() | atom(), String.t() | atom(), keyword()) :: float()
  def corr!(%__MODULE__{} = df, col1, col2, opts \\ []), do: unwrap!(corr(df, col1, col2, opts))

  @doc "See `Latu.approx_quantile/5`."
  @spec approx_quantile(
          t(),
          [String.t() | atom()] | String.t() | atom(),
          [number()],
          number(),
          keyword()
        ) :: {:ok, [float()] | [[float()]]} | {:error, Error.t()}
  def approx_quantile(df, cols, probabilities, relative_error, opts \\ [])

  def approx_quantile(%__MODULE__{} = df, cols, probabilities, relative_error, opts)
      when is_list(cols) do
    quantiles(df, cols, probabilities, relative_error, opts)
  end

  # One name in, one flat list out — PySpark's own asymmetry, and the shape people expect.
  def approx_quantile(%__MODULE__{} = df, col, probabilities, relative_error, opts) do
    with {:ok, [values]} <- quantiles(df, [col], probabilities, relative_error, opts) do
      {:ok, values}
    end
  end

  @doc "Like `approx_quantile/5`, raising on failure."
  @spec approx_quantile!(
          t(),
          [String.t() | atom()] | String.t() | atom(),
          [number()],
          number(),
          keyword()
        ) :: [float()] | [[float()]]
  def approx_quantile!(%__MODULE__{} = df, cols, probabilities, relative_error, opts \\ []) do
    unwrap!(approx_quantile(df, cols, probabilities, relative_error, opts))
  end

  defp quantiles(df, cols, probabilities, relative_error, opts) do
    opts = Keyword.validate!(opts, progress: nil)
    plan = Plan.approx_quantile(df.plan, cols, probabilities, relative_error)

    with {:ok, lists} <- one_cell(df, plan, "approx_quantile", opts) do
      {:ok, Enum.map(lists, &Enum.to_list/1)}
    end
  end

  # A map is accepted for the shape people reach for, but a list keeps its order, and the order
  # is what reaches the wire.
  defp fraction_pairs!(fractions) when is_list(fractions), do: fractions

  defp fraction_pairs!(fractions) when is_map(fractions) and not is_struct(fractions) do
    Map.to_list(fractions)
  end

  defp fraction_pairs!(other) do
    raise ArgumentError, "sample_by: {stratum, fraction} pairs or a map, got #{inspect(other)}"
  end

  # `cov`, `corr` and `approx_quantile` all collect PySpark's `table[0][0]`: one row, one
  # column, one cell. `count/1`'s shape, over a relation the caller built.
  defp one_cell(%__MODULE__{} = df, plan, what, opts) do
    case fetch(%{df | plan: plan}, opts) do
      {:ok, nil, _} -> {:error, Error.new(:decode, "#{what} came back with no rows at all")}
      {:ok, frame, _} -> Result.only(frame)
      {:error, _} = error -> error
    end
  end

  # =============================================
  # Analysis
  # =============================================

  @doc "See `Latu.schema/1`."
  @spec schema(t()) :: {:ok, [Result.field()]} | {:error, Error.t()}
  def schema(%__MODULE__{} = df) do
    with {:ok, data_type, _session} <- analyzed(df, :schema) do
      Result.Schema.fields(data_type)
    end
  end

  @doc "Like `schema/1`, raising on failure."
  @spec schema!(t()) :: [Result.field()]
  def schema!(%__MODULE__{} = df), do: unwrap!(schema(df))

  @doc "See `Latu.columns/1`."
  @spec columns(t()) :: {:ok, [String.t()]} | {:error, Error.t()}
  def columns(%__MODULE__{} = df) do
    with {:ok, fields} <- schema(df), do: {:ok, Enum.map(fields, & &1.name)}
  end

  @doc "Like `columns/1`, raising on failure."
  @spec columns!(t()) :: [String.t()]
  def columns!(%__MODULE__{} = df), do: unwrap!(columns(df))

  @doc "See `Latu.dtypes/1`."
  @spec dtypes(t()) :: {:ok, [{String.t(), String.t()}]} | {:error, Error.t()}
  def dtypes(%__MODULE__{} = df) do
    with {:ok, fields} <- schema(df), do: {:ok, Enum.map(fields, &{&1.name, &1.type})}
  end

  @doc "Like `dtypes/1`, raising on failure."
  @spec dtypes!(t()) :: [{String.t(), String.t()}]
  def dtypes!(%__MODULE__{} = df), do: unwrap!(dtypes(df))

  @doc "See `Latu.tree_string/2`."
  @spec tree_string(t(), keyword()) :: {:ok, String.t()} | {:error, Error.t()}
  def tree_string(%__MODULE__{} = df, opts \\ []) do
    with {:ok, tree, _session} <- analyzed(df, :tree_string, opts), do: {:ok, tree}
  end

  @doc "Like `tree_string/2`, raising on failure."
  @spec tree_string!(t(), keyword()) :: String.t()
  def tree_string!(%__MODULE__{} = df, opts \\ []), do: unwrap!(tree_string(df, opts))

  @doc "See `Latu.print_schema/2`."
  @spec print_schema(t(), keyword()) :: :ok | {:error, Error.t()}
  def print_schema(%__MODULE__{} = df, opts \\ []) do
    with {:ok, tree} <- tree_string(df, opts), do: IO.write(tree)
  end

  @doc "Like `print_schema/2`, raising on failure."
  @spec print_schema!(t(), keyword()) :: :ok
  def print_schema!(%__MODULE__{} = df, opts \\ []) do
    case print_schema(df, opts) do
      :ok -> :ok
      {:error, error} -> raise error
    end
  end

  @doc "See `Latu.explain/2`."
  @spec explain(t(), keyword()) :: :ok | {:error, Error.t()}
  def explain(%__MODULE__{} = df, opts \\ []) do
    with {:ok, plan} <- explain_string(df, opts), do: IO.write(plan)
  end

  @doc "Like `explain/2`, raising on failure."
  @spec explain!(t(), keyword()) :: :ok
  def explain!(%__MODULE__{} = df, opts \\ []) do
    case explain(df, opts) do
      :ok -> :ok
      {:error, error} -> raise error
    end
  end

  @doc "See `Latu.explain_string/2`."
  @spec explain_string(t(), keyword()) :: {:ok, String.t()} | {:error, Error.t()}
  def explain_string(%__MODULE__{} = df, opts \\ []) do
    with {:ok, string, _session} <- analyzed(df, :explain, opts), do: {:ok, string}
  end

  @doc "Like `explain_string/2`, raising on failure."
  @spec explain_string!(t(), keyword()) :: String.t()
  def explain_string!(%__MODULE__{} = df, opts \\ []), do: unwrap!(explain_string(df, opts))

  @doc "See `Latu.is_local/1`."
  @spec is_local(t()) :: {:ok, boolean()} | {:error, Error.t()}
  def is_local(%__MODULE__{} = df) do
    with {:ok, local?, _session} <- analyzed(df, :is_local), do: {:ok, local?}
  end

  @doc "Like `is_local/1`, raising on failure."
  @spec is_local!(t()) :: boolean()
  def is_local!(%__MODULE__{} = df), do: unwrap!(is_local(df))

  @doc "See `Latu.is_streaming/1`."
  @spec is_streaming(t()) :: {:ok, boolean()} | {:error, Error.t()}
  def is_streaming(%__MODULE__{} = df) do
    with {:ok, streaming?, _session} <- analyzed(df, :is_streaming), do: {:ok, streaming?}
  end

  @doc "Like `is_streaming/1`, raising on failure."
  @spec is_streaming!(t()) :: boolean()
  def is_streaming!(%__MODULE__{} = df), do: unwrap!(is_streaming(df))

  @doc "See `Latu.input_files/1`."
  @spec input_files(t()) :: {:ok, [String.t()]} | {:error, Error.t()}
  def input_files(%__MODULE__{} = df) do
    with {:ok, files, _session} <- analyzed(df, :input_files), do: {:ok, files}
  end

  @doc "Like `input_files/1`, raising on failure."
  @spec input_files!(t()) :: [String.t()]
  def input_files!(%__MODULE__{} = df), do: unwrap!(input_files(df))

  @doc "See `Latu.semantic_hash/1`."
  @spec semantic_hash(t()) :: {:ok, integer()} | {:error, Error.t()}
  def semantic_hash(%__MODULE__{} = df) do
    with {:ok, hash, _session} <- analyzed(df, :semantic_hash), do: {:ok, hash}
  end

  @doc "Like `semantic_hash/1`, raising on failure."
  @spec semantic_hash!(t()) :: integer()
  def semantic_hash!(%__MODULE__{} = df), do: unwrap!(semantic_hash(df))

  @doc "See `Latu.same_semantics/2`."
  @spec same_semantics(t(), t()) :: {:ok, boolean()} | {:error, Error.t()}
  def same_semantics(%__MODULE__{} = df, %__MODULE__{} = other) do
    same_session!(df, other)
    arm = Plan.analyze(:same_semantics, {df.plan, other.plan})

    with {:ok, same?, _session} <- Client.analyzed(df.session, arm), do: {:ok, same?}
  end

  @doc "Like `same_semantics/2`, raising on failure."
  @spec same_semantics!(t(), t()) :: boolean()
  def same_semantics!(%__MODULE__{} = df, %__MODULE__{} = other) do
    unwrap!(same_semantics(df, other))
  end

  @doc "See `Latu.is_empty/1`."
  @spec is_empty(t()) :: {:ok, boolean()} | {:error, Error.t()}
  def is_empty(%__MODULE__{} = df) do
    with {:ok, rows} <- df |> is_empty_plan() |> count(), do: {:ok, rows == 0}
  end

  @doc "Like `is_empty/1`, raising on failure."
  @spec is_empty!(t()) :: boolean()
  def is_empty!(%__MODULE__{} = df), do: unwrap!(is_empty(df))

  # Public so the golden test can pin the plan; not API. PySpark spells this
  # `select().take(1)` — an EMPTY projection, so no column data comes back. Latu counts a
  # one-row limit instead: same answer, same one row of work, and it does not ask the Explorer
  # decode path what a zero-column Arrow batch means. docs/deviations.md.
  @doc false
  @spec is_empty_plan(t()) :: t()
  def is_empty_plan(%__MODULE__{} = df), do: limit(df, 1)

  @doc "See `Latu.persist/2`."
  @spec persist(t(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def persist(%__MODULE__{} = df, opts \\ []) do
    with {:ok, _response, _session} <-
           Client.analyze(df.session, Plan.analyze(:persist, df.plan, opts)) do
      {:ok, df}
    end
  end

  @doc "Like `persist/2`, raising on failure. Returns the DataFrame, so it pipes."
  @spec persist!(t(), keyword()) :: t()
  def persist!(%__MODULE__{} = df, opts \\ []), do: unwrap!(persist(df, opts))

  @doc "See `Latu.cache/1`."
  @spec cache(t()) :: {:ok, t()} | {:error, Error.t()}
  def cache(%__MODULE__{} = df), do: persist(df)

  # Note for both: over Connect this is an AnalyzePlan round trip, where classic Spark's
  # `cache()` is a driver-local CacheManager call that cannot fail. The caching itself is still
  # lazy on the server — `:ok` means registered, not materialised.

  @doc "Like `cache/1`, raising on failure. Returns the DataFrame, so it pipes."
  @spec cache!(t()) :: t()
  def cache!(%__MODULE__{} = df), do: unwrap!(cache(df))

  @doc "See `Latu.unpersist/2`."
  @spec unpersist(t(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def unpersist(%__MODULE__{} = df, opts \\ []) do
    with {:ok, _response, _session} <-
           Client.analyze(df.session, Plan.analyze(:unpersist, df.plan, opts)) do
      {:ok, df}
    end
  end

  @doc "Like `unpersist/2`, raising on failure. Returns the DataFrame, so it pipes."
  @spec unpersist!(t(), keyword()) :: t()
  def unpersist!(%__MODULE__{} = df, opts \\ []), do: unwrap!(unpersist(df, opts))

  @doc "See `Latu.storage_level/1`."
  @spec storage_level(t()) :: {:ok, map()} | {:error, Error.t()}
  def storage_level(%__MODULE__{} = df) do
    with {:ok, level, _session} <- analyzed(df, :get_storage_level) do
      {:ok, Plan.from_storage_level(level)}
    end
  end

  @doc "Like `storage_level/1`, raising on failure."
  @spec storage_level!(t()) :: map()
  def storage_level!(%__MODULE__{} = df), do: unwrap!(storage_level(df))

  defp analyzed(%__MODULE__{} = df, op, opts \\ []) do
    Client.analyzed(df.session, Plan.analyze(op, df.plan, opts))
  end

  # =============================================
  # Actions
  # =============================================

  @doc """
  Print the table Spark renders, and return `:ok`.

  Byte for byte what PySpark's `df.show()` prints: Spark formats it server-side, Latu decodes
  one string cell. Options, all PySpark's:

    * `:num_rows` — how many rows, default 20
    * `:truncate` — cell width, default 20; `true` means 20 and `false` means no truncation
    * `:vertical` — one row per block, default false

  Pipelines want `Kernel.tap/2`:

      df |> tap(&Latu.show!/1) |> Latu.filter(...)
  """
  @spec show(t(), keyword()) :: :ok | {:error, Error.t()}
  def show(%__MODULE__{} = df, opts \\ []) do
    {watched, opts} = Keyword.split(opts, [:progress])
    plan = df.plan |> Plan.show_string(opts) |> Plan.new()

    with {:ok, batches, _execution} <- Client.execute(df.session, plan, watched),
         {:ok, frame} <- Result.decode(batches, columns: ["show_string"]),
         {:ok, table} <- Result.only(frame, "show_string") do
      IO.puts(table)
    end
  end

  @doc "Like `show/2`, raising on failure."
  @spec show!(t(), keyword()) :: :ok
  def show!(%__MODULE__{} = df, opts \\ []) do
    case show(df, opts) do
      :ok -> :ok
      {:error, error} -> raise error
    end
  end

  @doc """
  See `Latu.to_html/2`.
  """
  @spec to_html(t(), keyword()) :: {:ok, String.t()} | {:error, Error.t()}
  def to_html(%__MODULE__{} = df, opts \\ []) do
    {watched, opts} = Keyword.split(opts, [:progress])
    plan = df.plan |> Plan.html_string(opts) |> Plan.new()

    with {:ok, batches, _execution} <- Client.execute(df.session, plan, watched),
         {:ok, frame} <- Result.decode(batches, columns: ["html_string"]),
         do: Result.only(frame, "html_string")
  end

  @doc "Like `to_html/2`, raising on failure."
  @spec to_html!(t(), keyword()) :: String.t()
  def to_html!(%__MODULE__{} = df, opts \\ []), do: unwrap!(to_html(df, opts))

  @doc """
  See `Latu.glimpse/2`.
  """
  @spec glimpse(t(), keyword()) :: :ok | {:error, Error.t()}
  def glimpse(%__MODULE__{} = df, opts \\ []) do
    opts = Keyword.validate!(opts, num_rows: 10, width: 80, count: false)
    num_rows = glimpse_num_rows!(opts[:num_rows])
    width = glimpse_width!(opts[:width])

    with {:ok, types} <- dtypes(df),
         {:ok, rows} <- df |> limit(num_rows) |> collect(keys: :strings),
         {:ok, label} <- glimpse_rows(df, opts[:count], length(rows), num_rows) do
      IO.write(glimpse_text(types, rows, label, width))
    end
  end

  @doc "Like `glimpse/2`, raising on failure."
  @spec glimpse!(t(), keyword()) :: :ok
  def glimpse!(%__MODULE__{} = df, opts \\ []) do
    case glimpse(df, opts) do
      :ok -> :ok
      {:error, error} -> raise error
    end
  end

  # Public so the rendering can be checked with no server — `count_plan/1`'s precedent, and the
  # rendering *is* the feature here. Not API.
  @doc false
  @spec glimpse_text([{String.t(), String.t()}], [map()], String.t(), pos_integer() | :infinity) ::
          iodata()
  def glimpse_text(types, rows, label, width) do
    pad = Enum.reduce(types, 0, fn {name, _type}, wide -> max(String.length(name), wide) end)

    lines =
      Enum.map(types, fn {name, type} ->
        values = Enum.map_join(rows, ", ", &inspect(Map.get(&1, name)))
        line = String.trim_trailing("$ #{String.pad_trailing(name, pad)} <#{type}> #{values}")

        [glimpse_clip(line, width), "\n"]
      end)

    ["Rows: #{label}\nColumns: #{length(types)}\n" | lines]
  end

  # An exact count is free when the sample came back short of what it asked for: there was
  # nothing left to give. Otherwise the honest answer is a lower bound, because a real count on
  # a Spark frame is a full scan — which `count: true` is how you ask for.
  defp glimpse_rows(df, true, _taken, _num_rows) do
    with {:ok, count} <- count(df), do: {:ok, Integer.to_string(count)}
  end

  defp glimpse_rows(_df, false, taken, num_rows) when taken < num_rows do
    {:ok, Integer.to_string(taken)}
  end

  defp glimpse_rows(_df, false, _taken, num_rows), do: {:ok, "at least #{num_rows}"}

  defp glimpse_clip(line, :infinity), do: line

  defp glimpse_clip(line, width) do
    if String.length(line) > width, do: String.slice(line, 0, width - 1) <> "…", else: line
  end

  defp glimpse_num_rows!(count) when is_integer(count) and count > 0, do: count

  defp glimpse_num_rows!(count) do
    raise ArgumentError,
          "num_rows is a positive integer, not #{inspect(count)} — for the columns and their " <>
            "types with no sample at all, dtypes/1 and print_schema/2 are the cheaper calls"
  end

  defp glimpse_width!(:infinity), do: :infinity
  defp glimpse_width!(width) when is_integer(width) and width > 0, do: width

  defp glimpse_width!(width) do
    raise ArgumentError, "width is a positive integer or :infinity, not #{inspect(width)}"
  end

  @doc "See `Latu.write/2`."
  @spec write(t(), keyword()) :: :ok | {:error, Error.t()}
  def write(%__MODULE__{} = df, opts) when is_list(opts) do
    run_write(df, write_command(df, opts), opts)
  end

  # The command builders are public so the golden tests can pin the exact plan without
  # executing anything — `count_plan/1`'s precedent. Not API.
  @doc false
  def write_command(%__MODULE__{} = df, opts) do
    {reserved, options} =
      opts
      |> without_progress()
      |> Keyword.split([
        :format,
        :mode,
        :path,
        :partition_by,
        :sort_by,
        :cluster_by,
        :bucket_by
      ])

    Plan.write(df.plan, Keyword.put(reserved, :options, options))
  end

  @doc "Like `write/2`, raising on failure."
  @spec write!(t(), keyword()) :: :ok
  def write!(%__MODULE__{} = df, opts), do: run!(write(df, opts))

  @doc "See `Latu.save_as_table/3`."
  @spec save_as_table(t(), String.t() | atom(), keyword()) :: :ok | {:error, Error.t()}
  def save_as_table(%__MODULE__{} = df, name, opts \\ []) do
    run_write(df, save_as_table_command(df, name, opts), opts)
  end

  @doc false
  def save_as_table_command(%__MODULE__{} = df, name, opts) do
    {reserved, options} =
      opts
      |> without_progress()
      |> Keyword.split([:format, :mode, :partition_by, :sort_by, :cluster_by, :bucket_by])

    Plan.write(df.plan, reserved ++ [table: {name, :save_as_table}, options: options])
  end

  @doc "Like `save_as_table/3`, raising on failure."
  @spec save_as_table!(t(), String.t() | atom(), keyword()) :: :ok
  def save_as_table!(%__MODULE__{} = df, name, opts \\ []) do
    run!(save_as_table(df, name, opts))
  end

  @doc "See `Latu.insert_into/3`."
  @spec insert_into(t(), String.t() | atom(), keyword()) :: :ok | {:error, Error.t()}
  def insert_into(%__MODULE__{} = df, name, opts \\ []) do
    run_write(df, insert_into_command(df, name, opts), opts)
  end

  @doc false
  def insert_into_command(%__MODULE__{} = df, name, opts) do
    opts = opts |> without_progress() |> Keyword.validate!(overwrite: nil)

    mode =
      case opts[:overwrite] do
        nil -> nil
        true -> :overwrite
        false -> :append
      end

    Plan.write(df.plan, table: {name, :insert_into}, mode: mode)
  end

  @doc "Like `insert_into/3`, raising on failure."
  @spec insert_into!(t(), String.t() | atom(), keyword()) :: :ok
  def insert_into!(%__MODULE__{} = df, name, opts \\ []) do
    run!(insert_into(df, name, opts))
  end

  @doc "See `Latu.write_v2/3`."
  @spec write_v2(t(), String.t() | atom(), keyword()) :: :ok | {:error, Error.t()}
  def write_v2(%__MODULE__{} = df, table, opts) when is_list(opts) do
    run_write(df, write_v2_command(df, table, opts), opts)
  end

  @doc false
  def write_v2_command(%__MODULE__{} = df, table, opts) do
    {reserved, options} =
      opts
      |> without_progress()
      |> Keyword.split([
        :mode,
        :using,
        :partition_by,
        :cluster_by,
        :table_properties,
        :condition
      ])

    Plan.write_v2(df.plan, table, Keyword.put(reserved, :options, options))
  end

  @doc "Like `write_v2/3`, raising on failure."
  @spec write_v2!(t(), String.t() | atom(), keyword()) :: :ok
  def write_v2!(%__MODULE__{} = df, table, opts), do: run!(write_v2(df, table, opts))

  @doc "See `Latu.create_temp_view/3`."
  @spec create_temp_view(t(), String.t() | atom(), keyword()) :: :ok | {:error, Error.t()}
  def create_temp_view(%__MODULE__{} = df, name, opts \\ []) do
    with {:ok, _execution} <- run_command(df, create_temp_view_command(df, name, opts)), do: :ok
  end

  @doc false
  def create_temp_view_command(%__MODULE__{} = df, name, opts \\ []) do
    Plan.create_view(df.plan, name, opts)
  end

  @doc "Like `create_temp_view/3`, raising on failure."
  @spec create_temp_view!(t(), String.t() | atom(), keyword()) :: :ok
  def create_temp_view!(%__MODULE__{} = df, name, opts \\ []) do
    run!(create_temp_view(df, name, opts))
  end

  # A write's result carries no rows — metrics only — so the shared tail of every write is:
  # refuse an observed plan, execute, discard, :ok. The metrics twins call `run_command/3`
  # instead and read the metrics off the execution.
  defp run_write(%__MODULE__{} = df, command, opts) do
    refuse_observed!(df)

    with {:ok, _execution} <- run_command(df, command, watch(opts)), do: :ok
  end

  defp run_command(%__MODULE__{} = df, command, opts \\ []) do
    Client.execute_command(df.session, Plan.new(command), watch(opts))
  end

  # `:progress` is the one option every action can carry down to the transport, and the only
  # one that is not part of the plan. `watch/1` is what reaches the transport; `without_progress/1`
  # is what the command builders drop before they build, since a writer's option list is
  # open-ended — anything it does not reserve becomes a Spark writer option, and anything it
  # *does* reserve is validated by `Latu.Plan` against a closed set. `:progress` belongs in
  # neither, so it leaves before the split.
  defp watch(opts), do: Keyword.take(opts, [:progress])

  defp without_progress(opts), do: Keyword.delete(opts, :progress)

  defp run!(:ok), do: :ok
  defp run!({:error, error}), do: raise(error)

  @doc """
  All the rows, as maps.

      Latu.collect(df)
      #=> {:ok, [%{id: 0}, %{id: 1}]}

  Atom keys by default: they pattern-match, and the atom table only grows by the set of
  column names ever selected. `keys: :strings` when names come out of dynamic SQL.

  The whole result is held at once; `stream/2` is the lazy escape for results that do not
  fit.
  """
  @spec collect(t(), keyword()) :: {:ok, [map()]} | {:error, Error.t()}
  def collect(%__MODULE__{} = df, opts \\ []) do
    opts = Keyword.validate!(opts, keys: :atoms, progress: nil)
    keys = keys!(opts[:keys])

    case fetch(df, opts) do
      {:ok, nil, _} -> {:ok, []}
      {:ok, frame, _} -> {:ok, Result.rows(frame, keys)}
      {:error, _} = error -> error
    end
  end

  @doc "Like `collect/2`, raising on failure."
  @spec collect!(t(), keyword()) :: [map()]
  def collect!(%__MODULE__{} = df, opts \\ []), do: unwrap!(collect(df, opts))

  @doc """
  How many rows, counted by the server.

      Latu.count(df)  #=> {:ok, 10}

  PySpark's own composition — `agg(count(lit(1)))`, unaliased and all — pinned by the
  `count_action` fixture.
  """
  @spec count(t(), keyword()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def count(%__MODULE__{} = df, opts \\ []) do
    opts = Keyword.validate!(opts, progress: nil)

    case fetch(count_plan(df), opts) do
      {:ok, frame, _execution} -> counted(frame)
      {:error, _} = error -> error
    end
  end

  @doc "Like `count/2`, raising on failure."
  @spec count!(t(), keyword()) :: non_neg_integer()
  def count!(%__MODULE__{} = df, opts \\ []), do: unwrap!(count(df, opts))

  # Public so the golden test can pin it to PySpark's plan; not part of the API.
  @doc false
  @spec count_plan(t()) :: t()
  def count_plan(%__MODULE__{} = df), do: agg(df, [Plan.fun("count", [Plan.lit(1)])])

  @doc """
  The first `count` rows, as maps — `limit/2` then `collect/2`, as in PySpark. Options are
  `collect/2`'s.
  """
  @spec take(t(), non_neg_integer(), keyword()) :: {:ok, [map()]} | {:error, Error.t()}
  def take(%__MODULE__{} = df, count, opts \\ []) when is_integer(count) and count >= 0 do
    df |> limit(count) |> collect(opts)
  end

  @doc "Like `take/3`, raising on failure."
  @spec take!(t(), non_neg_integer(), keyword()) :: [map()]
  def take!(%__MODULE__{} = df, count, opts \\ []), do: unwrap!(take(df, count, opts))

  @doc "See `Latu.tail/3`."
  @spec tail(t(), non_neg_integer(), keyword()) :: {:ok, [map()]} | {:error, Error.t()}
  def tail(%__MODULE__{} = df, count, opts \\ []) when is_integer(count) and count >= 0 do
    df |> tail_plan(count) |> collect(opts)
  end

  @doc "Like `tail/3`, raising on failure."
  @spec tail!(t(), non_neg_integer(), keyword()) :: [map()]
  def tail!(%__MODULE__{} = df, count, opts \\ []), do: unwrap!(tail(df, count, opts))

  # Public so the golden test can pin the relation; not API. `count_plan/1`'s precedent.
  @doc false
  @spec tail_plan(t(), non_neg_integer()) :: t()
  def tail_plan(%__MODULE__{} = df, count), do: %{df | plan: Plan.tail(df.plan, count)}

  @doc "The first row, or nil when there are none. Options are `collect/2`'s."
  @spec first(t(), keyword()) :: {:ok, map() | nil} | {:error, Error.t()}
  def first(%__MODULE__{} = df, opts \\ []) do
    with {:ok, rows} <- take(df, 1, opts), do: {:ok, List.first(rows)}
  end

  @doc "Like `first/2`, raising on failure."
  @spec first!(t(), keyword()) :: map() | nil
  def first!(%__MODULE__{} = df, opts \\ []), do: unwrap!(first(df, opts))

  @doc """
  `first/2` under PySpark's other name: one row or nil, not a list. With a count it is
  `take/3`: a list, even for one row. Both shapes are PySpark's. Options are `collect/2`'s.
  """
  @spec head(t(), non_neg_integer() | keyword(), keyword()) ::
          {:ok, map() | nil} | {:ok, [map()]} | {:error, Error.t()}
  def head(df, count_or_opts \\ [], opts \\ [])
  def head(%__MODULE__{} = df, opts, []) when is_list(opts), do: first(df, opts)
  def head(%__MODULE__{} = df, count, opts) when is_integer(count), do: take(df, count, opts)

  @doc "Like `head/3`, raising on failure."
  @spec head!(t(), non_neg_integer() | keyword(), keyword()) :: map() | nil | [map()]
  def head!(%__MODULE__{} = df, count_or_opts \\ [], opts \\ []) do
    unwrap!(head(df, count_or_opts, opts))
  end

  @doc """
  The result as one `Explorer.DataFrame`.

  **Unbounded**, like `collect/2`, `to_arrow/2` and Spark's own `collect`: the whole result
  comes back. To take part of it, bound the *plan* — which is how Spark does it, and what
  `limit/2` is for. A result too large to hold at all is what `stream/2` is for.

      {:ok, frame} = Latu.to_explorer(df)
      {:ok, frame} = df |> Latu.limit(10_000) |> Latu.to_explorer()

  An empty result is a 0-row frame with the right columns and dtypes — the server sends the
  Arrow schema even when there are no rows.
  """
  @spec to_explorer(t(), keyword()) :: {:ok, Explorer.DataFrame.t()} | {:error, Error.t()}
  def to_explorer(%__MODULE__{} = df, opts \\ []) do
    opts = Keyword.validate!(opts, progress: nil)

    with {:ok, frame, _execution} <- fetch_frame(df, opts), do: {:ok, frame}
  end

  @doc "Like `to_explorer/2`, raising on failure."
  @spec to_explorer!(t(), keyword()) :: Explorer.DataFrame.t()
  def to_explorer!(%__MODULE__{} = df, opts \\ []), do: unwrap!(to_explorer(df, opts))

  @doc """
  The result as a lazy stream of `Explorer.DataFrame`s, one per Arrow batch.

  Backpressure for results too large to hold: each batch decodes as it arrives, and stopping
  early releases the execution. Raises `Latu.Error` on failure, since an enumeration has no
  way to return one. The schema guard runs on the `DataType` the server sends ahead of the
  first batch.

      df |> Latu.stream() |> Stream.map(&Explorer.DataFrame.n_rows/1) |> Enum.sum()
  """
  @spec stream(t(), keyword()) :: Enumerable.t()
  def stream(%__MODULE__{} = df, opts \\ []) do
    opts = Keyword.validate!(opts, progress: nil)

    df.session
    |> Client.responses(Plan.new(df.plan))
    |> Client.watched(watch(opts))
    |> Stream.flat_map(fn
      {:progress, _progress} ->
        []

      {:schema, schema} ->
        case Result.Schema.check(schema) do
          :ok -> []
          {:error, error} -> raise error
        end

      {:ok, batch} ->
        case Result.decode([batch]) do
          {:ok, frame} -> [frame]
          {:error, error} -> raise error
        end

      {:done, _execution} ->
        []

      {:error, error} ->
        raise error
    end)
  end

  @doc """
  The raw Arrow IPC streaming-format binaries, one per batch, for doing your own thing.

  Bypasses the decoder AND the schema guard on purpose: these bytes are headed for some other
  Arrow reader, whose capabilities are its own business. Each binary is a complete IPC stream
  — schema, record batch, end marker — and they must never be byte-concatenated.

  """
  @spec to_arrow(t(), keyword()) :: {:ok, [binary()]} | {:error, Error.t()}
  def to_arrow(%__MODULE__{} = df, opts \\ []) do
    opts = Keyword.validate!(opts, progress: nil)

    with {:ok, batches, _execution} <-
           Client.execute(df.session, Plan.new(df.plan), watch(opts)) do
      {:ok, Enum.map(batches, & &1.data)}
    end
  end

  @doc "Like `to_arrow/2`, raising on failure."
  @spec to_arrow!(t(), keyword()) :: [binary()]
  def to_arrow!(%__MODULE__{} = df, opts \\ []), do: unwrap!(to_arrow(df, opts))

  # =============================================
  # Observed metrics
  # =============================================

  # The write twins, in one place so the refusal can name all of them.
  @metrics_twins "write_with_metrics/2, save_as_table_with_metrics/3, " <>
                   "insert_into_with_metrics/3, write_v2_with_metrics/3 or merge_with_metrics/2"

  @doc """
  `collect/2`, and what the run reported besides the rows.

      df = Latu.observe(df, :checks, total: F.count(:id))
      {:ok, rows, info} = Latu.collect_with_metrics(df)

      info.observed  #=> %{checks: %{total: 4}}
      info.metrics   #=> Spark's own per-node SQL metrics

  See `Latu.ExecutionInfo`. Options are `collect/2`'s.
  """
  @spec collect_with_metrics(t(), keyword()) ::
          {:ok, [map()], ExecutionInfo.t()} | {:error, Error.t()}
  def collect_with_metrics(%__MODULE__{} = df, opts \\ []) do
    opts = Keyword.validate!(opts, keys: :atoms, progress: nil)
    keys = keys!(opts[:keys])

    with {:ok, frame, execution} <- fetch(df, opts),
         {:ok, info} <- execution_info(execution) do
      rows = if frame, do: Result.rows(frame, keys), else: []
      {:ok, rows, info}
    end
  end

  @doc "Like `collect_with_metrics/2`, raising on failure and returning `{rows, info}`."
  @spec collect_with_metrics!(t(), keyword()) :: {[map()], ExecutionInfo.t()}
  def collect_with_metrics!(%__MODULE__{} = df, opts \\ []) do
    unwrap!(collect_with_metrics(df, opts))
  end

  @doc "`count/2`, and what the run reported besides the result. See `Latu.ExecutionInfo`."
  @spec count_with_metrics(t(), keyword()) ::
          {:ok, non_neg_integer(), ExecutionInfo.t()} | {:error, Error.t()}
  def count_with_metrics(%__MODULE__{} = df, opts \\ []) do
    opts = Keyword.validate!(opts, progress: nil)

    with {:ok, frame, execution} <- fetch(count_plan(df), opts),
         {:ok, count} <- counted(frame),
         {:ok, info} <- execution_info(execution) do
      {:ok, count, info}
    end
  end

  @doc "Like `count_with_metrics/2`, raising on failure and returning `{count, info}`."
  @spec count_with_metrics!(t(), keyword()) :: {non_neg_integer(), ExecutionInfo.t()}
  def count_with_metrics!(%__MODULE__{} = df, opts \\ []) do
    unwrap!(count_with_metrics(df, opts))
  end

  @doc "`to_explorer/2`, and what the run reported besides the result. See `Latu.ExecutionInfo`."
  @spec to_explorer_with_metrics(t(), keyword()) ::
          {:ok, Explorer.DataFrame.t(), ExecutionInfo.t()} | {:error, Error.t()}
  def to_explorer_with_metrics(%__MODULE__{} = df, opts \\ []) do
    opts = Keyword.validate!(opts, progress: nil)

    with {:ok, frame, execution} <- fetch_frame(df, opts),
         {:ok, info} <- execution_info(execution) do
      {:ok, frame, info}
    end
  end

  @doc "Like `to_explorer_with_metrics/2`, raising on failure and returning `{frame, info}`."
  @spec to_explorer_with_metrics!(t(), keyword()) :: {Explorer.DataFrame.t(), ExecutionInfo.t()}
  def to_explorer_with_metrics!(%__MODULE__{} = df, opts \\ []) do
    unwrap!(to_explorer_with_metrics(df, opts))
  end

  @doc """
  `write/2`, and what the run reported besides the result. See `Latu.ExecutionInfo`.

  This is the shape `observe` was built for: a data-quality aggregate attached on the way in,
  the write done, the counts read back. There are no rows to return, so the metrics are the
  whole result.

      df = Latu.observe(df, :quality, rows: F.count(:id), worst: F.min(:price))
      {:ok, info} = Latu.write_with_metrics(df, path: "/out")

      info.observed  #=> %{quality: %{rows: 1000, worst: -2.5}}
  """
  @spec write_with_metrics(t(), keyword()) :: {:ok, ExecutionInfo.t()} | {:error, Error.t()}
  def write_with_metrics(%__MODULE__{} = df, opts) when is_list(opts) do
    with {:ok, execution} <- run_command(df, write_command(df, opts), watch(opts)) do
      execution_info(execution)
    end
  end

  @doc "Like `write_with_metrics/2`, raising on failure."
  @spec write_with_metrics!(t(), keyword()) :: ExecutionInfo.t()
  def write_with_metrics!(%__MODULE__{} = df, opts), do: unwrap!(write_with_metrics(df, opts))

  @doc "`save_as_table/3`, and what the run reported besides the result. See `Latu.ExecutionInfo`."
  @spec save_as_table_with_metrics(t(), String.t() | atom(), keyword()) ::
          {:ok, ExecutionInfo.t()} | {:error, Error.t()}
  def save_as_table_with_metrics(%__MODULE__{} = df, name, opts \\ []) do
    with {:ok, execution} <- run_command(df, save_as_table_command(df, name, opts), watch(opts)) do
      execution_info(execution)
    end
  end

  @doc "Like `save_as_table_with_metrics/3`, raising on failure."
  @spec save_as_table_with_metrics!(t(), String.t() | atom(), keyword()) :: ExecutionInfo.t()
  def save_as_table_with_metrics!(%__MODULE__{} = df, name, opts \\ []) do
    unwrap!(save_as_table_with_metrics(df, name, opts))
  end

  @doc "`insert_into/3`, and what the run reported besides the result. See `Latu.ExecutionInfo`."
  @spec insert_into_with_metrics(t(), String.t() | atom(), keyword()) ::
          {:ok, ExecutionInfo.t()} | {:error, Error.t()}
  def insert_into_with_metrics(%__MODULE__{} = df, name, opts \\ []) do
    with {:ok, execution} <- run_command(df, insert_into_command(df, name, opts), watch(opts)) do
      execution_info(execution)
    end
  end

  @doc "Like `insert_into_with_metrics/3`, raising on failure."
  @spec insert_into_with_metrics!(t(), String.t() | atom(), keyword()) :: ExecutionInfo.t()
  def insert_into_with_metrics!(%__MODULE__{} = df, name, opts \\ []) do
    unwrap!(insert_into_with_metrics(df, name, opts))
  end

  @doc "`write_v2/3`, and what the run reported besides the result. See `Latu.ExecutionInfo`."
  @spec write_v2_with_metrics(t(), String.t() | atom(), keyword()) ::
          {:ok, ExecutionInfo.t()} | {:error, Error.t()}
  def write_v2_with_metrics(%__MODULE__{} = df, table, opts) when is_list(opts) do
    with {:ok, execution} <- run_command(df, write_v2_command(df, table, opts), watch(opts)) do
      execution_info(execution)
    end
  end

  @doc "Like `write_v2_with_metrics/3`, raising on failure."
  @spec write_v2_with_metrics!(t(), String.t() | atom(), keyword()) :: ExecutionInfo.t()
  def write_v2_with_metrics!(%__MODULE__{} = df, table, opts) do
    unwrap!(write_v2_with_metrics(df, table, opts))
  end

  @doc "`merge/2`, and the metrics an `observe/3` in the source plan asked for."
  @spec merge_with_metrics(MergeInto.t(), keyword()) ::
          {:ok, ExecutionInfo.t()} | {:error, Error.t()}
  def merge_with_metrics(%MergeInto{} = merge, opts \\ []) do
    opts = Keyword.validate!(opts, progress: nil)

    with {:ok, execution} <- run_command(merge.source, MergeInto.command(merge), watch(opts)) do
      execution_info(execution)
    end
  end

  @doc "Like `merge_with_metrics/2`, raising on failure."
  @spec merge_with_metrics!(MergeInto.t(), keyword()) :: ExecutionInfo.t()
  def merge_with_metrics!(%MergeInto{} = merge, opts \\ []) do
    unwrap!(merge_with_metrics(merge, opts))
  end

  # A write consumes the frame, so an observed plan run through a plain write has produced its
  # metrics and dropped them with nothing to show for it. Reads and previews are not refused:
  # the frame is still in hand, and PySpark drops the metrics on `show()` too. Pure and
  # client-side — no round trip pays for it. docs/decisions.md (M12.6).
  defp refuse_observed!(%__MODULE__{} = df) do
    case Plan.observed_names(df.plan) do
      [] ->
        :ok

      names ->
        raise ArgumentError,
              "this plan observes #{Enum.map_join(names, ", ", &inspect/1)}, and a plain write " <>
                "would discard the metrics. Use #{@metrics_twins}, or drop the observe/3."
    end
  end

  # Both halves of what a run reported live on Latu.ExecutionInfo, which owns the decoding.
  defp execution_info(execution), do: ExecutionInfo.new(execution)

  # `count/2`'s own shaping, shared so `count_with_metrics/2` cannot drift from it.
  defp counted(nil), do: {:error, Error.new(:decode, "count came back with no rows at all")}
  defp counted(frame), do: Result.only(frame)

  # Execute, guard, decode — the shared tail of every eager action. Even an empty result
  # arrives as one schema-bearing batch (PySpark's `to_table` asserts as much), so it decodes
  # to a typed 0-row frame; `{:ok, nil, _}` is the defensive arm for a server that sent
  # nothing, and each caller shapes it.
  #
  # The finished execution comes back with the frame because that is what observed metrics ride
  # on; the plain actions drop it, the twins read it.
  defp fetch(%__MODULE__{} = df, opts) do
    with {:ok, batches, execution} <- Client.execute(df.session, Plan.new(df.plan), watch(opts)),
         :ok <- Result.Schema.check(execution.schema) do
      case batches do
        [] -> {:ok, nil, execution}
        batches -> with {:ok, frame} <- Result.decode(batches), do: {:ok, frame, execution}
      end
    end
  end

  # One row past the bound is fetched, so a result that reached it is refused rather than cut.
  # The extra row costs nothing; a silently short frame costs every number computed from it.
  # docs/decisions.md (M12.6).
  # Even an empty result arrives as one schema-bearing Arrow batch — PySpark's own `to_table`
  # asserts it got at least one — so no batches at all is a protocol surprise rather than an
  # empty frame, and the Explorer paths say so instead of handing back a `nil`.
  defp fetch_frame(%__MODULE__{} = df, opts) do
    case fetch(df, opts) do
      {:ok, nil, _execution} -> {:error, Error.new(:decode, "the server sent no batches at all")}
      {:ok, frame, execution} -> {:ok, frame, execution}
      {:error, _} = error -> error
    end
  end

  defp keys!(keys) when keys in [:atoms, :strings], do: keys

  defp keys!(other) do
    raise ArgumentError, "keys: :atoms or :strings, got #{inspect(other)}"
  end

  defp unwrap!({:ok, value}), do: value
  defp unwrap!({:ok, value, metrics}), do: {value, metrics}
  defp unwrap!({:error, error}), do: raise(error)

  @doc false
  def new(%Session{} = session, plan), do: %__MODULE__{session: session, plan: plan}
end

defimpl Inspect, for: Latu.DataFrame do
  import Inspect.Algebra

  def inspect(%{plan: plan}, opts) do
    concat(["#Latu.DataFrame<", Latu.Plan.Inspect.chain(plan, opts), ">"])
  end
end
