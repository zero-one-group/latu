defmodule Latu.Plan do
  @moduledoc """
  Relation and expression trees, built directly as protobuf. Pure: no session, no network, no
  IO.

  `plan_id` is assigned when a node is built rather than when the tree is encoded. The server
  resolves column references by searching the analysed plan for the node carrying that id, so
  it is node identity. `normalize_ids/1` is what that costs, and only tests pay it.

  ## Public and pure, on purpose

  **You can build a plan and look at it without a server, and that is a feature rather than an
  accident of layering.** A `%Latu.DataFrame{}` is a session and one of these trees; every verb
  is a pure function from tree to tree, and nothing reaches the network until an action. So:

      iex> df = Latu.range(Latu.Session.from_url!("sc://h"), 10)
      iex> df |> Latu.filter(Latu.Column.greater(:id, 3)) |> inspect()
      "#Latu.DataFrame<range → filter>"

  That session was never connected. Checking that a pipeline is the one you meant costs nothing
  — no cluster, no credentials, no waiting — and it is what makes `inspect/1` on a DataFrame
  worth reading (`Latu.Plan.Inspect.chain/2`).

  It is also how Latu tests itself: every golden fixture compares a plan built here against the
  bytes PySpark produces for the same pipeline, with no server involved, and every offline check
  in `dev/` rests on the same property. If you are generating Latu code, this is the cheapest
  possible feedback loop — build the plan, inspect it, and only then run it.

  A layering test enforces the purity: nothing under `Latu.Plan` may reference `Latu.Client` or
  `GRPC`.
  """

  alias Latu.Protocol.Spark.Connect, as: Proto

  @typedoc "One node of a plan tree."
  @type relation :: Proto.Relation.t()

  @typedoc """
  A Spark type as the server parsed it.

  Latu holds no client-side type model, so the only way to name one is to have the server do
  it — `Latu.parse_ddl_type/2`. Named here so the layers above can spell it without reaching
  for the generated modules themselves.
  """
  @type data_type :: Proto.DataType.t()

  @typedoc """
  A column reference, a literal, or a function call.

  A `Latu.Subquery` is one too: an expression referencing another DataFrame carries those
  relations beside itself until a relation constructor hoists them. Every builder here takes
  either, and returns a `Latu.Subquery` only when there is something to carry — so a plan with
  no subquery in it is what it always was.
  """
  @type expression :: Proto.Expression.t() | Latu.Subquery.t()

  @typedoc "The eager side of the protocol: a write, and later SQL and catalog operations."
  @type command :: Proto.Command.t()

  @typedoc """
  One sort key: an expression, a direction, and where nulls go. A `Latu.Subquery` when the key
  references another DataFrame.
  """
  @type sort_order :: Proto.Expression.SortOrder.t() | Latu.Subquery.t()

  # Spark's own limits. int32/int64 decide `integer` vs `long`; decimal precision caps at 38.
  @frame_types [rows: :FRAME_TYPE_ROW, range: :FRAME_TYPE_RANGE]

  # Spark names lambda parameters by position, and allows at most three.
  @lambda_names ~w(x y z)

  @int32 -2_147_483_648..2_147_483_647
  @int64 -9_223_372_036_854_775_808..9_223_372_036_854_775_807
  @max_precision 38

  # int64, so a seed is drawn from 0..2^63-1 as PySpark draws from 0..sys.maxsize.
  @max_seed 9_223_372_036_854_775_808

  @epoch_date ~D[1970-01-01]
  @epoch_naive ~N[1970-01-01 00:00:00]

  # One spelling per join, rather than PySpark's ten. Spark's own `outer` is ambiguous — it
  # means full outer — so it is deliberately absent.
  @join_types [
    inner: :JOIN_TYPE_INNER,
    cross: :JOIN_TYPE_CROSS,
    full: :JOIN_TYPE_FULL_OUTER,
    left: :JOIN_TYPE_LEFT_OUTER,
    right: :JOIN_TYPE_RIGHT_OUTER,
    semi: :JOIN_TYPE_LEFT_SEMI,
    anti: :JOIN_TYPE_LEFT_ANTI
  ]

  # `AsOfJoin` and `NearestByJoin` carry the join type as a **string**, not the `Join.JoinType`
  # enum. These are the canonical spellings `JoinType.apply` in catalyst/plans/joinTypes.scala
  # matches on after lowercasing and stripping underscores — read, not guessed, because the
  # fixtures pin the exact bytes.
  @join_type_names [
    inner: "inner",
    cross: "cross",
    full: "fullouter",
    left: "leftouter",
    right: "rightouter",
    semi: "leftsemi",
    anti: "leftanti"
  ]

  # A lateral join takes three of the seven — `LateralJoinType.supported`. Refused here rather
  # than at the server, since the list is fixed and the round trip buys nothing.
  @lateral_join_types [
    inner: :JOIN_TYPE_INNER,
    left: :JOIN_TYPE_LEFT_OUTER,
    cross: :JOIN_TYPE_CROSS
  ]

  # `AsOfJoinDirection.supported`, same file.
  @as_of_directions [backward: "backward", forward: "forward", nearest: "nearest"]

  # `Parse.ParseFormat`. XML is in the enum on 4.2; whether the server implements it is its call.
  @parse_formats [
    csv: :PARSE_FORMAT_CSV,
    json: :PARSE_FORMAT_JSON,
    xml: :PARSE_FORMAT_XML
  ]

  # `NearestByJoinValidation`'s three acceptance lists, mirrored client-side the way PySpark
  # mirrors them — so the error reads the same wherever it fires.
  @nearest_join_types [inner: "inner", left: "leftouter"]
  @nearest_modes [approx: "approx", exact: "exact"]
  @nearest_directions [distance: "distance", similarity: "similarity"]
  @max_nearest_results 100_000

  # Spark's `union` keeps duplicates where `intersect` and `except` do not, so `is_all` defaults
  # per operation rather than globally. These are the un-suffixed methods' behaviour.
  @set_ops [
    union: {:SET_OP_TYPE_UNION, true},
    intersect: {:SET_OP_TYPE_INTERSECT, false},
    except: {:SET_OP_TYPE_EXCEPT, false}
  ]

  # `grouping_sets` exists in the enum and has no builder yet: no fixture, no PySpark
  # DataFrame method that reaches it from a grouped frame.
  @group_types [
    group_by: :GROUP_TYPE_GROUPBY,
    rollup: :GROUP_TYPE_ROLLUP,
    cube: :GROUP_TYPE_CUBE,
    pivot: :GROUP_TYPE_PIVOT,
    grouping_sets: :GROUP_TYPE_GROUPING_SETS
  ]

  # Spark's four subquery shapes. `:table_arg` belongs to table-valued functions and has no
  # Latu caller yet; it is here because one table is cheaper than two places to look.
  @subquery_types [
    scalar: :SUBQUERY_TYPE_SCALAR,
    exists: :SUBQUERY_TYPE_EXISTS,
    in: :SUBQUERY_TYPE_IN,
    table_arg: :SUBQUERY_TYPE_TABLE_ARG
  ]

  # PySpark's Connect client accepts exactly these four spellings; `:error` is Spark's
  # error-if-exists. An omitted mode stays SAVE_MODE_UNSPECIFIED and the server applies its
  # own default.
  @save_modes [
    append: :SAVE_MODE_APPEND,
    overwrite: :SAVE_MODE_OVERWRITE,
    error: :SAVE_MODE_ERROR_IF_EXISTS,
    ignore: :SAVE_MODE_IGNORE
  ]

  @save_methods [
    save_as_table: :TABLE_SAVE_METHOD_SAVE_AS_TABLE,
    insert_into: :TABLE_SAVE_METHOD_INSERT_INTO
  ]

  # One spelling per V2 terminal method, matching PySpark's create/replace/createOrReplace/
  # append/overwrite/overwritePartitions.
  @v2_modes [
    create: :MODE_CREATE,
    replace: :MODE_REPLACE,
    create_or_replace: :MODE_CREATE_OR_REPLACE,
    append: :MODE_APPEND,
    overwrite: :MODE_OVERWRITE,
    overwrite_partitions: :MODE_OVERWRITE_PARTITIONS
  ]

  # What a reference to a relation outside the tree normalises to. Spark refuses such a plan
  # (docs/decisions.md, M9.1), so it appears in one fixture only — the one that pins the shape
  # Latu and PySpark both send. Keeping the raw id would bake a build-specific number into it.
  @unresolved -1

  @directions [asc: :SORT_DIRECTION_ASCENDING, desc: :SORT_DIRECTION_DESCENDING]
  @nulls [first: :SORT_NULLS_FIRST, last: :SORT_NULLS_LAST]

  # SQL's defaults, and asymmetric: ascending puts nulls first, descending puts them last.
  # PySpark's `asc()` and `desc()` are `asc_nulls_first()` and `desc_nulls_last()`.
  @default_nulls [asc: :first, desc: :last]

  # A built expression: a proto, or a `Latu.Subquery` carrying the relations it references.
  defguardp is_expression(value)
            when is_struct(value, Proto.Expression) or is_struct(value, Latu.Subquery)

  # =============================================
  # Relations
  # =============================================

  @doc "Wrap a root relation or a command as a `Plan`, ready to send."
  @spec new(relation() | command()) :: Proto.Plan.t()
  def new(%Proto.Relation{} = relation), do: %Proto.Plan{op_type: {:root, relation}}
  def new(%Proto.Command{} = command), do: %Proto.Plan{op_type: {:command, command}}

  @doc """
  Wrap a `rel_type` arm as a `Relation`, carrying a fresh `plan_id`.

  Public for the same reason `plan_id/0` is: `latu_ml` builds `MlRelation` arms Latu has no verb
  for, and an id is assigned at creation in one place. A second wrapper out of tree would be a
  second allocator. See docs/decisions.md on what Latu owes `latu_ml`.
  """
  @spec relation({atom(), struct()}) :: relation()
  def relation(rel_type) do
    %Proto.Relation{common: %Proto.RelationCommon{plan_id: plan_id()}, rel_type: rel_type}
  end

  @doc """
  One `id` column of longs. `stop` is exclusive, as in Spark.

  `num_partitions` absent leaves the choice to the server:
  `spark.sql.leafNodeDefaultParallelism` if set, else its default parallelism.
  """
  @spec range(integer(), integer(), integer(), pos_integer() | nil) :: Proto.Relation.t()
  def range(start, stop, step, num_partitions \\ nil) do
    range = %Proto.Range{start: start, end: stop, step: step, num_partitions: num_partitions}

    relation({:range, range})
  end

  @doc """
  Read from a data source: `:format`, `:schema`, `:paths` and `:options`, all optional.

      Plan.read(format: "csv", schema: "id INT, name STRING", paths: ["/data/people.csv"],
                options: [header: true])

  The schema is a string the *server* parses — DDL, or Spark's JSON schema form — passed
  verbatim, as PySpark passes a string. It is sent as `""` when absent: PySpark's reader always
  assigns the field, and an absent proto3-optional field is a different message. Options go
  through `to_options/1`.
  """
  @spec read(keyword()) :: relation()
  def read(opts \\ []) do
    opts = Keyword.validate!(opts, format: nil, schema: "", paths: [], options: [])

    data_source = %Proto.Read.DataSource{
      format: format(opts[:format]),
      schema: schema(opts[:schema]),
      paths: Enum.map(opts[:paths], &path/1),
      options: Map.new(to_options(opts[:options]))
    }

    relation({:read, %Proto.Read{read_type: {:data_source, data_source}}})
  end

  # A data source name is open-ended (any registered source), so an atom is spelled out rather
  # than checked against a set; anything else would only fail later, inside protobuf encoding.
  defp format(nil), do: nil
  defp format(format), do: identifier(format, "format")

  defp schema(schema) when is_binary(schema), do: schema

  defp schema(schema) do
    raise ArgumentError,
          "a schema is a string — DDL or Spark's JSON schema form — not #{inspect(schema)}"
  end

  defp path(path) when is_binary(path), do: path
  defp path(path), do: raise(ArgumentError, "a path is a string, not #{inspect(path)}")

  defp table_name(name) when is_binary(name), do: name
  defp table_name(name) when is_atom(name) and not is_nil(name), do: Atom.to_string(name)

  defp table_name(name), do: name_of(name, "table name")

  defp name_of(name, _what) when is_binary(name), do: name
  defp name_of(name, _what) when is_atom(name) and not is_nil(name), do: Atom.to_string(name)

  defp name_of(name, what) do
    raise ArgumentError, "a #{what} is a string or an atom, not #{inspect(name)}"
  end

  @doc "Read a catalog table by name, with `to_options/1` options."
  @spec table(String.t() | atom(), keyword() | map()) :: relation()
  def table(name, options \\ []) do
    named = %Proto.Read.NamedTable{
      unparsed_identifier: table_name(name),
      options: Map.new(to_options(options))
    }

    relation({:read, %Proto.Read{read_type: {:named_table, named}}})
  end

  @doc """
  A SQL query as a relation. `sql_command/2` is the eager wrapper `Latu.sql/3` sends; this is
  its `input`, and what the DataFrame falls back to when the server returns no result relation.

  Parameter markers bind from `args`: a list binds `?` positionally, a map binds `:name`.
  Values are literals through `lit/1`, PySpark's own coercion. An empty list or map stays off
  the wire.
  """
  @spec sql(String.t(), [term()] | map(), [{String.t() | atom(), relation()}]) :: relation()
  def sql(query, args \\ [], views \\ [])

  def sql(query, args, views) when is_binary(query) and is_list(args) do
    with_views(%Proto.SQL{query: query, pos_arguments: Enum.map(args, &lit/1)}, views)
  end

  def sql(query, args, views) when is_binary(query) and is_map(args) and not is_struct(args) do
    named = Map.new(args, fn {name, value} -> {name_of(name, "argument name"), lit(value)} end)

    with_views(%Proto.SQL{query: query, named_arguments: named}, views)
  end

  # A view is a `SubqueryAlias` in `WithRelations.references`, so the query can name it. The
  # same hoisting subqueries use — the server resolves both by looking there.
  defp with_views(%Proto.SQL{} = sql, views) do
    hoist(relation({:sql, sql}), Enum.map(views, fn {name, input} -> as(input, name) end))
  end

  # The catalog operations Latu ships, by oneof arm — the subset is a decision, see
  # docs/decisions.md. Each op's fields are its proto's, snake_case; nil stays off the wire.
  @catalog_ops [
    current_catalog: Proto.CurrentCatalog,
    set_current_catalog: Proto.SetCurrentCatalog,
    list_catalogs: Proto.ListCatalogs,
    current_database: Proto.CurrentDatabase,
    set_current_database: Proto.SetCurrentDatabase,
    list_databases: Proto.ListDatabases,
    list_tables: Proto.ListTables,
    list_columns: Proto.ListColumns,
    database_exists: Proto.DatabaseExists,
    table_exists: Proto.TableExists,
    drop_temp_view: Proto.DropTempView,
    drop_global_temp_view: Proto.DropGlobalTempView,
    drop_table: Proto.DropTable,
    drop_view: Proto.DropView,
    is_cached: Proto.IsCached,
    cache_table: Proto.CacheTable,
    uncache_table: Proto.UncacheTable,
    clear_cache: Proto.ClearCache,
    refresh_table: Proto.RefreshTable
  ]

  @doc """
  A catalog operation. Spark models these as *relations* that answer eagerly — even the void
  ones — and `Latu.Catalog` runs them.

      Plan.catalog(:list_tables, db_name: "default", pattern: "latu_*")
  """
  @spec catalog(atom(), keyword()) :: relation()
  def catalog(op, fields \\ []) do
    module = lookup(@catalog_ops, op, "catalog operation")
    fields = Enum.reject(fields, fn {_key, value} -> is_nil(value) end)
    relation({:catalog, %Proto.Catalog{cat_type: {op, struct!(module, fields)}}})
  end

  @doc """
  Local data as a relation: an Arrow IPC stream, and optionally a schema string — DDL or
  Spark's JSON form, passed verbatim; the server parses either and casts the data to it.
  `nil` data with a schema is an empty frame, PySpark's own encoding of `createDataFrame([],
  schema)`.
  """
  @spec local_relation(binary() | nil, String.t() | nil) :: relation()
  def local_relation(data, schema \\ nil)

  def local_relation(nil, schema) when is_binary(schema) do
    relation({:local_relation, %Proto.LocalRelation{schema: schema}})
  end

  def local_relation(data, schema)
      when is_binary(data) and (is_binary(schema) or is_nil(schema)) do
    relation({:local_relation, %Proto.LocalRelation{data: data, schema: schema}})
  end

  @doc """
  Local data the server already holds: sha-256 hex hashes of Arrow IPC chunks cached as
  session artifacts by the transport, plus the schema chunk's when one was
  uploaded. What `Latu.create_dataframe/3` escalates to past the server's
  `localRelationCacheThreshold`.
  """
  @spec chunked_cached_local_relation([String.t()], String.t() | nil) :: relation()
  def chunked_cached_local_relation(data_hashes, schema_hash \\ nil)
      when is_list(data_hashes) and (is_binary(schema_hash) or is_nil(schema_hash)) do
    chunked = %Proto.ChunkedCachedLocalRelation{dataHashes: data_hashes, schemaHash: schema_hash}
    relation({:chunked_cached_local_relation, chunked})
  end

  @doc """
  A relation the server is holding for us: `CachedRemoteRelation`.

  What a checkpoint hands back. The id is the server's, so this is the one relation Latu builds
  from a value it did not invent — and the one that names a resource somebody has to free.
  """
  @spec cached_remote_relation(String.t()) :: relation()
  def cached_remote_relation(id) when is_binary(id) do
    relation({:cached_remote_relation, %Proto.CachedRemoteRelation{relation_id: id}})
  end

  @doc """
  The server-side id a `cached_remote_relation` names, or `:error` for any other relation.

  Reading a relation's arm is the plan layer's job — the layer above should not pattern-match
  a generated message to find out what it is holding. `Latu.release/1` asks this.
  """
  @spec cached_relation_id(relation()) :: {:ok, String.t()} | :error
  def cached_relation_id(%Proto.Relation{rel_type: {:cached_remote_relation, cached}}) do
    {:ok, cached.relation_id}
  end

  def cached_relation_id(%Proto.Relation{}), do: :error

  @doc false
  # A relation the server handed back (`sql_command_result`), adopted as a local node: a fresh
  # plan_id, so references built on it resolve like any locally built relation's. What
  # PySpark's CachedRelation constructor does.
  @spec adopt(relation()) :: relation()
  def adopt(%Proto.Relation{} = returned) do
    common = returned.common || %Proto.RelationCommon{}
    %{returned | common: %{common | plan_id: plan_id()}}
  end

  @doc """
  The table `Latu.show/2` prints. Spark formats it; the result is one string cell.

  Options are `Latu.show/2`'s. `truncate: true` means 20 characters and `false` means none,
  as in PySpark.
  """
  @spec show_string(Proto.Relation.t(), keyword()) :: Proto.Relation.t()
  def show_string(%Proto.Relation{} = input, opts \\ []) do
    opts = Keyword.validate!(opts, num_rows: 20, truncate: 20, vertical: false)

    show = %Proto.ShowString{
      input: input,
      num_rows: opts[:num_rows],
      truncate: truncate(opts[:truncate]),
      vertical: opts[:vertical]
    }

    relation({:show_string, show})
  end

  defp truncate(true), do: 20
  defp truncate(false), do: 0
  defp truncate(width) when is_integer(width) and width >= 0, do: width

  @doc """
  The same table as `show_string/2`, as HTML. Spark's own `_repr_html_`.

  `Latu.to_html/2`'s relation, and what Kino renders in Livebook. No `:vertical` — `HtmlString`
  has only `num_rows` and `truncate`.
  """
  @spec html_string(Proto.Relation.t(), keyword()) :: Proto.Relation.t()
  def html_string(%Proto.Relation{} = input, opts \\ []) do
    opts = Keyword.validate!(opts, num_rows: 20, truncate: 20)

    html = %Proto.HtmlString{
      input: input,
      num_rows: opts[:num_rows],
      truncate: truncate(opts[:truncate])
    }

    relation({:html_string, html})
  end

  @doc "Keep only these expressions, in this order."
  @spec project(relation(), [expression()]) :: relation()
  def project(%Proto.Relation{} = input, expressions) when is_list(expressions) do
    {expressions, refs} = drain_all(expressions)

    hoist(relation({:project, %Proto.Project{input: input, expressions: expressions}}), refs)
  end

  @doc """
  Add or replace columns, keeping the rest.

  Takes `{name, expression}` pairs. Spark has no singular `withColumn` relation — PySpark's is
  this one with a single alias.
  """
  @spec with_columns(relation(), [{String.t() | atom(), expression()}]) :: relation()
  def with_columns(%Proto.Relation{} = input, columns) when is_list(columns) do
    {aliases, refs} =
      Enum.map_reduce(columns, [], fn {name, expr}, refs ->
        {expr, more} = drain(expr)

        {alias_of(expr, name), refs ++ more}
      end)

    hoist(relation({:with_columns, %Proto.WithColumns{input: input, aliases: aliases}}), refs)
  end

  @doc """
  Attach metadata to an existing column: `WithColumns` with the alias carrying it.

  `metadata` is a map, encoded to the JSON string `Alias.metadata` holds — which is what
  PySpark sends too, via `json.dumps`. The column keeps its own name and its own expression;
  only the metadata is new.
  """
  @spec with_metadata(relation(), String.t() | atom(), map()) :: relation()
  def with_metadata(%Proto.Relation{} = input, name, metadata) when is_map(metadata) do
    alias_node = %{
      alias_of(col(name, input), name)
      | metadata: JSON.encode!(metadata)
    }

    relation({:with_columns, %Proto.WithColumns{input: input, aliases: [alias_node]}})
  end

  @doc """
  Remove columns. Names or expressions; the proto has a field for each.
  """
  @spec drop(relation(), [String.t() | atom() | expression()]) :: relation()
  def drop(%Proto.Relation{} = input, columns) when is_list(columns) do
    {expressions, names} = Enum.split_with(columns, fn column -> is_expression(column) end)
    {expressions, refs} = drain_all(expressions)

    drop = %Proto.Drop{
      input: input,
      columns: expressions,
      column_names: Enum.map(names, &identifier/1)
    }

    hoist(relation({:drop, drop}), refs)
  end

  @doc """
  Observe aggregates over a relation without changing what it returns: `CollectMetrics`.

  `metrics` goes through `to_projections/1`, so a keyword list names each one — and the names
  are load-bearing, not cosmetic: they come back as `ObservedMetrics.keys` and are the only way
  a caller identifies a value. Spark requires aggregate expressions here and refuses a bare
  column at analysis, which no plan-level check can see.
  """
  @spec collect_metrics(relation(), String.t() | atom(), [expression()]) :: relation()
  def collect_metrics(%Proto.Relation{} = input, name, metrics) when is_list(metrics) do
    if metrics == [], do: raise(ArgumentError, "observe needs at least one metric")

    {metrics, refs} = drain_all(to_projections(metrics))

    collect = %Proto.CollectMetrics{
      input: input,
      name: identifier(name, "observation name"),
      metrics: metrics
    }

    hoist(relation({:collect_metrics, collect}), refs)
  end

  @doc """
  Attach a planner hint: `Hint`.

  Parameters go through `to_expr/1`, so Latu's usual rule holds — a binary is a string
  literal, an atom is a column reference. That matches PySpark despite it calling `F.lit` on
  every parameter, because `lit` of a Column returns the column unchanged (measured in
  `connect/functions/builtin.py`).

      Plan.hint(relation, "broadcast")
      Plan.hint(relation, "repartition", [4, "suburb"])
  """
  @spec hint(relation(), String.t() | atom(), [term()]) :: relation()
  def hint(%Proto.Relation{} = input, name, parameters \\ []) when is_list(parameters) do
    {parameters, refs} = drain_all(parameters)
    hint = %Proto.Hint{input: input, name: identifier(name, "hint name"), parameters: parameters}

    hoist(relation({:hint, hint}), refs)
  end

  @doc """
  Wide to long: `Unpivot`.

    * `:values` — the columns to unpivot. **Absent and empty are different messages**: absent
      (the default) means every column that is not an id, and the server works them out;
      `values: []` sends an empty set.
    * `:variable_column_name` — required; the column that will hold the old column names
    * `:value_column_name` — required; the column that will hold their values
  """
  @spec unpivot(relation(), [term()], keyword()) :: relation()
  def unpivot(%Proto.Relation{} = input, ids, opts) when is_list(ids) and is_list(opts) do
    opts =
      Keyword.validate!(opts,
        values: nil,
        variable_column_name: nil,
        value_column_name: nil
      )

    {ids, id_refs} = drain_all(to_projections(ids))
    {values, value_refs} = unpivot_values(opts[:values])

    unpivot = %Proto.Unpivot{
      input: input,
      ids: ids,
      values: values,
      variable_column_name: named!(opts[:variable_column_name], :variable_column_name),
      value_column_name: named!(opts[:value_column_name], :value_column_name)
    }

    hoist(relation({:unpivot, unpivot}), id_refs ++ value_refs)
  end

  defp unpivot_values(nil), do: {nil, []}

  defp unpivot_values(columns) when is_list(columns) do
    {values, refs} = drain_all(to_projections(columns))

    {%Proto.Unpivot.Values{values: values}, refs}
  end

  defp required!(nil, what) do
    raise ArgumentError, "#{what} is required"
  end

  defp required!(value, _what), do: value

  defp named!(value, what), do: value |> required!(what) |> identifier(what)

  # Optional expression: absent stays absent, since a message field with no value is not the
  # same as one holding a default.
  defp optional_expr(nil), do: {nil, []}
  defp optional_expr(term), do: drain(term)

  @doc """
  Rows to columns: `Transpose`.

  `index_column` names the column whose values become the new column names. Without one Spark
  uses the first column, which is its own rule, not a Latu default.
  """
  @spec transpose(relation(), term() | nil) :: relation()
  def transpose(%Proto.Relation{} = input, index_column \\ nil) do
    {columns, refs} =
      case index_column do
        nil -> {[], []}
        column -> drain_all(to_projections([column]))
      end

    hoist(relation({:transpose, %Proto.Transpose{input: input, index_columns: columns}}), refs)
  end

  @doc """
  Reconcile a relation to a target schema: `ToSchema`.

  Matching is **by name**, not by position — see `Latu.to/2` for the rules, including the one
  Spark's own scaladoc gets wrong.

  `schema` is a `DataType` **message**, not a string: `ToSchema.schema` has no string form, and
  a DDL string is refused outright — measured, `DataTypeProtoConverter.toCatalystType` on
  v4.2.0 has no `UNPARSED` case. So the schema comes from `Latu.parse_ddl_type/2`, which is the
  server's own parse of a DDL string.
  """
  @spec to_schema(relation(), data_type()) :: relation()
  def to_schema(%Proto.Relation{} = input, %Proto.DataType{} = schema) do
    relation({:to_schema, %Proto.ToSchema{input: input, schema: schema}})
  end

  @doc """
  Parse a one-string-column relation into a structured one: `Parse`.

    * `:format` — #{inspect(Keyword.keys(@parse_formats))}, required
    * `:schema` — optional, a `DataType` from `Latu.parse_ddl_type/2`. **PySpark parses a DDL
      string client-side here** (`StructType.fromDDL`), which is the type model Latu does not
      have; this is `Latu.to/2`'s route instead.
    * `:options` — reader options, camelCased as everywhere else
  """
  @spec parse(relation(), keyword()) :: relation()
  def parse(%Proto.Relation{} = input, opts) do
    opts = Keyword.validate!(opts, format: nil, schema: nil, options: [])

    parse = %Proto.Parse{
      input: input,
      format: lookup(@parse_formats, required!(opts[:format], :format), "parse format"),
      schema: opts[:schema],
      options: Map.new(to_options(opts[:options]))
    }

    relation({:parse, parse})
  end

  @doc """
  A table-valued function by name: `UnresolvedTableValuedFunction`.

  The generic form, as `fun/3` is for scalar functions — PySpark wraps a fixed handful under
  `spark.tvf`, and one builder covers all of them plus whatever a Spark version adds.

      Plan.table_function("explode", [fun("array", [lit(1), lit(2)])])
      Plan.table_function("sql_keywords")

  Arguments are ordinary expressions. `subquery(relation, :table_arg)` is **not** one of them:
  a table argument is consumed only by a Python UDTF, and `SubqueryExpression.table_arg_options`
  is unbuilt. See `docs/deviations.md`.
  """
  @spec table_function(String.t() | atom(), [term()]) :: relation()
  def table_function(name, arguments \\ []) when is_list(arguments) do
    {arguments, refs} = drain_all(arguments)

    node = %Proto.UnresolvedTableValuedFunction{
      function_name: identifier(name, "function name"),
      arguments: arguments
    }

    hoist(relation({:unresolved_table_valued_function, node}), refs)
  end

  @doc """
  A table's change feed: `RelationChanges`.

  `options` carries the CDC window — `startingVersion`, `endingVersion`, `startingTimestamp`,
  `endingTimestamp`, and the rest the proto lists — camelCased like every other option map.
  `:is_streaming` is a plain bool, so it only reaches the wire when true.
  """
  @spec table_changes(String.t() | atom(), keyword()) :: relation()
  def table_changes(table, opts \\ []) do
    opts = Keyword.validate!(opts, options: [], is_streaming: false)

    changes = %Proto.RelationChanges{
      unparsed_identifier: identifier(table, "table name"),
      options: Map.new(to_options(opts[:options])),
      is_streaming: opts[:is_streaming]
    }

    relation({:relation_changes, changes})
  end

  @doc """
  An as-of join: `AsOfJoin`.

  Matches each left row with the nearest right row by the as-of columns rather than by
  equality. `:left_as_of` and `:right_as_of` are required expressions — the DataFrame layer
  tags a bare name to the frame it belongs to, which is what PySpark's own `_col` does.

    * `:on` — an equality key alongside the as-of match: names become `using_columns`, an
      expression becomes `join_expr`. The two are mutually exclusive on the wire.
    * `:how` — #{inspect(Keyword.keys(@join_type_names))}, default `:inner`; sent as Spark's
      canonical string rather than the enum, because that is what this relation carries.
    * `:tolerance` — how far the match may be; an expression, so an interval goes through
      `expr/1`. **Check the literal rule here**: this is a new literal site.
    * `:allow_exact_matches` — default `true`, where the proto's own default is `false`.
    * `:direction` — #{inspect(Keyword.keys(@as_of_directions))}, default `:backward`.
  """
  @spec as_of_join(relation(), relation(), keyword()) :: relation()
  def as_of_join(%Proto.Relation{} = left, %Proto.Relation{} = right, opts) do
    opts =
      Keyword.validate!(opts,
        left_as_of: nil,
        right_as_of: nil,
        on: nil,
        how: :inner,
        tolerance: nil,
        allow_exact_matches: true,
        direction: :backward
      )

    {left_as_of, left_refs} = drain(required!(opts[:left_as_of], :left_as_of))
    {right_as_of, right_refs} = drain(required!(opts[:right_as_of], :right_as_of))
    {tolerance, tolerance_refs} = optional_expr(opts[:tolerance])

    join = %Proto.AsOfJoin{
      left: left,
      right: right,
      left_as_of: left_as_of,
      right_as_of: right_as_of,
      join_type: lookup(@join_type_names, opts[:how], "join type"),
      tolerance: tolerance,
      allow_exact_matches: opts[:allow_exact_matches],
      direction: lookup(@as_of_directions, opts[:direction], "as-of direction")
    }

    {join, on_refs} = on(join, opts[:on], :join_expr)

    hoist(
      relation({:as_of_join, join}),
      left_refs ++ right_refs ++ tolerance_refs ++ on_refs
    )
  end

  @doc """
  A lateral join: `LateralJoin`.

  The right side may reference the left's columns, which is the whole point and the reason
  `:on` takes a condition only — this relation has no `using_columns`.

    * `:on` — the join condition
    * `:how` — #{inspect(Keyword.keys(@lateral_join_types))}, default `:inner`. Three of the
      seven; `LateralJoinType` refuses the rest and so does this.
  """
  @spec lateral_join(relation(), relation(), keyword()) :: relation()
  def lateral_join(%Proto.Relation{} = left, %Proto.Relation{} = right, opts \\ []) do
    opts = Keyword.validate!(opts, on: nil, how: :inner)
    {condition, refs} = optional_expr(opts[:on])

    join = %Proto.LateralJoin{
      left: left,
      right: right,
      join_condition: condition,
      join_type: lookup(@lateral_join_types, opts[:how], "lateral join type")
    }

    hoist(relation({:lateral_join, join}), refs)
  end

  @doc """
  A nearest-neighbour join: `NearestByJoin`.

  Ranks the right side per left row by `ranking` and keeps the best `:num_results`.

    * `:num_results` — required, 1..#{@max_nearest_results}
    * `:mode` — required, #{inspect(Keyword.keys(@nearest_modes))}
    * `:direction` — required, #{inspect(Keyword.keys(@nearest_directions))}
    * `:how` — #{inspect(Keyword.keys(@nearest_join_types))}, default `:inner`

  Every one of those is checked here rather than at the server, mirroring both PySpark and
  Spark's own `NearestByJoinValidation` — `Latu.approx_quantile/5`'s precedent, and for the same
  reason: `Latu.Plan` is public, so a hand-built plan gets the same check.
  """
  @spec nearest_by_join(relation(), relation(), term(), keyword()) :: relation()
  def nearest_by_join(%Proto.Relation{} = left, %Proto.Relation{} = right, ranking, opts) do
    opts = Keyword.validate!(opts, num_results: nil, mode: nil, direction: nil, how: :inner)
    {ranking, refs} = drain(ranking)

    join = %Proto.NearestByJoin{
      left: left,
      right: right,
      ranking_expression: ranking,
      num_results: num_results!(opts[:num_results]),
      join_type: lookup(@nearest_join_types, opts[:how], "nearest-by join type"),
      mode: lookup(@nearest_modes, required!(opts[:mode], :mode), "nearest-by mode"),
      direction: nearest_direction!(opts[:direction])
    }

    hoist(relation({:nearest_by_join, join}), refs)
  end

  defp nearest_direction!(direction) do
    lookup(@nearest_directions, required!(direction, :direction), "nearest-by direction")
  end

  defp num_results!(count)
       when is_integer(count) and count >= 1 and count <= @max_nearest_results do
    count
  end

  defp num_results!(other) do
    raise ArgumentError,
          "num_results is required, an integer in 1..#{@max_nearest_results}; " <>
            "got #{inspect(other)}"
  end

  @doc """
  Rename every column, positionally. Spark's `ToDF`; needs one name per column.
  """
  @spec to_df(relation(), [String.t() | atom()]) :: relation()
  def to_df(%Proto.Relation{} = input, names) when is_list(names) do
    relation({:to_df, %Proto.ToDF{input: input, column_names: Enum.map(names, &identifier/1)}})
  end

  @doc """
  Rename columns by `{from, to}` pairs, leaving the rest.
  """
  @spec with_columns_renamed(relation(), [{String.t() | atom(), String.t() | atom()}]) ::
          relation()
  def with_columns_renamed(%Proto.Relation{} = input, pairs) when is_list(pairs) do
    # Not `rename_columns_map`, the deprecated map field beside this one. Both reach the
    # server; only `renames` is what PySpark sends, and the golden test says so.
    renames = Enum.map(pairs, &rename/1)

    relation({:with_columns_renamed, %Proto.WithColumnsRenamed{input: input, renames: renames}})
  end

  defp rename({from, to}) do
    %Proto.WithColumnsRenamed.Rename{
      col_name: identifier(from),
      new_col_name: identifier(to)
    }
  end

  @doc "Keep only the rows the condition holds for."
  @spec filter(relation(), expression()) :: relation()
  def filter(%Proto.Relation{} = input, condition) when is_expression(condition) do
    {condition, refs} = drain(condition)

    hoist(relation({:filter, %Proto.Filter{input: input, condition: condition}}), refs)
  end

  @doc "Keep at most `count` rows."
  @spec limit(Proto.Relation.t(), non_neg_integer()) :: Proto.Relation.t()
  def limit(%Proto.Relation{} = input, count) when is_integer(count) and count >= 0 do
    relation({:limit, %Proto.Limit{input: input, limit: count}})
  end

  @doc "Skip the first `count` rows."
  @spec offset(relation(), non_neg_integer()) :: relation()
  def offset(%Proto.Relation{} = input, count) when is_integer(count) and count >= 0 do
    relation({:offset, %Proto.Offset{input: input, offset: count}})
  end

  @doc """
  The last `count` rows: `Tail`.

  A relation, not a client-side trick — Spark collects it on the driver, which is why PySpark
  spells it as an action and `Latu.tail/2` does too.
  """
  @spec tail(relation(), non_neg_integer()) :: relation()
  def tail(%Proto.Relation{} = input, count) when is_integer(count) and count >= 0 do
    relation({:tail, %Proto.Tail{input: input, limit: count}})
  end

  @doc """
  Drop duplicate rows, by these columns or by all of them when none are given.
  """
  @spec deduplicate(relation(), [String.t() | atom()]) :: relation()
  def deduplicate(%Proto.Relation{} = input, columns) when is_list(columns) do
    # Both flags are proto3_optional and PySpark sets both, in both cases. Absent is not false.
    dedup = %Proto.Deduplicate{
      input: input,
      column_names: Enum.map(columns, &identifier/1),
      all_columns_as_keys: columns == [],
      within_watermark: false
    }

    relation({:deduplicate, dedup})
  end

  @doc """
  Sort rows by these keys.

  Global, as `orderBy` is. Spark's `sortWithinPartitions` is the same relation with
  `is_global: false` and is not built yet.
  """
  @spec sort(relation(), [sort_order()], keyword()) :: relation()
  def sort(%Proto.Relation{} = input, orders, opts \\ []) when is_list(orders) do
    opts = Keyword.validate!(opts, global: true)
    {orders, refs} = drain_sorts(orders)

    sort = %Proto.Sort{input: input, order: orders, is_global: opts[:global]}

    hoist(relation({:sort, sort}), refs)
  end

  @doc """
  A random fraction of the rows.

    * `:seed` — an integer; a random one is drawn when absent, as in PySpark, which means the
      plan differs between runs. Pass one to make it reproducible.
    * `:with_replacement` — default `false`
    * `:lower_bound` — where the sampled window starts, default 0.0. Only `Latu.random_split/3`
      passes it; a plain sample takes the window `[0.0, fraction)`.
    * `:deterministic_order` — force a stable row order before sampling, default `false`.
      `Latu.random_split/3` sets it, because its slices only partition the frame if every slice
      sees the rows in the same order.
  """
  @spec sample(relation(), number(), keyword()) :: relation()
  def sample(%Proto.Relation{} = input, fraction, opts \\ []) when is_number(fraction) do
    opts =
      Keyword.validate!(opts,
        seed: nil,
        with_replacement: false,
        lower_bound: 0.0,
        deterministic_order: false
      )

    sample = %Proto.Sample{
      input: input,
      lower_bound: opts[:lower_bound] * 1.0,
      upper_bound: fraction * 1.0,
      with_replacement: opts[:with_replacement],
      seed: opts[:seed] || random_seed(),
      deterministic_order: opts[:deterministic_order]
    }

    relation({:sample, sample})
  end

  @doc """
  Change the partition count.

  `shuffle: true` is `repartition`, `false` is `coalesce` — one Spark relation, two methods.
  """
  @spec repartition(relation(), pos_integer(), keyword()) :: relation()
  def repartition(%Proto.Relation{} = input, count, opts \\ [])
      when is_integer(count) and count > 0 do
    opts = Keyword.validate!(opts, shuffle: true)
    repartition = %Proto.Repartition{input: input, num_partitions: count, shuffle: opts[:shuffle]}

    relation({:repartition, repartition})
  end

  @doc """
  Partition by these expressions, into `count` partitions when given.

  A different relation from `repartition/2`, not an option on it — Spark has two.
  """
  @spec repartition_by(relation(), [expression()], pos_integer() | nil) :: relation()
  def repartition_by(%Proto.Relation{} = input, expressions, count \\ nil)
      when is_list(expressions) do
    {expressions, refs} = drain_all(expressions)

    by = %Proto.RepartitionByExpression{
      input: input,
      partition_exprs: expressions,
      num_partitions: count
    }

    hoist(relation({:repartition_by_expression, by}), refs)
  end

  @doc """
  Range partitioning: the same relation as `repartition_by/3`, carrying **sort orders** rather
  than bare expressions.

  That is the only difference on the wire, and it is what PySpark's `sort=True` produces. A
  range partitioner needs an ordering to cut ranges on; a hash partitioner does not.

  **The sort orders are wrapped in `Expression`s.** `partition_exprs` is `repeated Expression`,
  so a bare `Expression.SortOrder` cannot go in it — `Sort.order` and `Window.order_spec` are
  the fields typed as `SortOrder` directly, and they are the exception, not the rule. Getting
  this wrong fails at *encode* time, not at analysis.
  """
  @spec repartition_by_range(relation(), [term()], pos_integer() | nil) :: relation()
  def repartition_by_range(%Proto.Relation{} = input, orders, count \\ nil)
      when is_list(orders) do
    {orders, refs} = drain_sorts(orders)

    by = %Proto.RepartitionByExpression{
      input: input,
      partition_exprs: Enum.map(orders, &expression({:sort_order, &1})),
      num_partitions: count
    }

    hoist(relation({:repartition_by_expression, by}), refs)
  end

  @doc """
  Group and aggregate. One relation: Spark has no `group_by` node.

    * `:type` — #{inspect(Keyword.keys(@group_types))}, default `:group_by`
    * `:groupings` — expressions to group by; none means aggregate the whole frame
    * `:aggregates` — the aggregate expressions
    * `:pivot` — a column name, `:type` `:pivot` only
    * `:pivot_values` — literals to pivot into columns; without them Spark scans for the
      distinct values first

  The pivot column carries this relation's `plan_id`, because PySpark builds it as `df[name]`
  and the golden test says the tag is on the wire.
  """
  @spec aggregate(relation(), keyword()) :: relation()
  def aggregate(%Proto.Relation{} = input, opts \\ []) do
    opts =
      Keyword.validate!(opts,
        type: :group_by,
        groupings: [],
        aggregates: [],
        pivot: nil,
        pivot_values: [],
        grouping_sets: []
      )

    {groupings, grouping_refs} = drain_all(opts[:groupings])
    {aggregates, aggregate_refs} = drain_all(opts[:aggregates])
    {sets, set_refs} = drain_each(opts[:grouping_sets], &grouping_set/1)

    aggregate = %Proto.Aggregate{
      input: input,
      group_type: lookup(@group_types, opts[:type], "group type"),
      grouping_expressions: groupings,
      aggregate_expressions: aggregates,
      pivot: pivot(opts[:pivot], opts[:pivot_values], input),
      grouping_sets: sets
    }

    hoist(relation({:aggregate, aggregate}), grouping_refs ++ aggregate_refs ++ set_refs)
  end

  # One set of grouping expressions. An empty set is the grand total and is legal, which is
  # most of the point of GROUPING SETS.
  defp grouping_set(columns) when is_list(columns) do
    {expressions, refs} = drain_all(to_projections(columns))

    {%Proto.Aggregate.GroupingSets{grouping_set: expressions}, refs}
  end

  defp grouping_set(other) do
    raise ArgumentError, "each grouping set is a list of columns, got #{inspect(other)}"
  end

  defp pivot(nil, _values, _input), do: nil

  defp pivot(name, values, %Proto.Relation{} = input) do
    # `Pivot.values` holds bare Literals, not Expressions — as `WithColumns.aliases` holds
    # bare Aliases.
    %Proto.Aggregate.Pivot{col: col(name, input), values: Enum.map(values, &literal_of/1)}
  end

  @doc """
  A set operation over two relations: `:union`, `:intersect` or `:except`.

    * `:all` — keep duplicates. Defaults to Spark's own per operation, which is **`true` for
      `:union` and `false` for the other two** — `union` is `UNION ALL`, not SQL's `UNION`.
    * `:by_name` — match columns by name rather than position, `:union` only
    * `:allow_missing_columns` — fill a missing column with null; needs `:by_name`

  PySpark spells the six combinations as `union`, `unionByName`, `intersect`, `intersectAll`,
  `subtract` and `exceptAll`. See `docs/deviations.md`.
  """
  @spec set_op(atom(), relation(), relation(), keyword()) :: relation()
  def set_op(kind, %Proto.Relation{} = left, %Proto.Relation{} = right, opts \\ []) do
    {type, all?} = lookup(@set_ops, kind, "set operation")
    opts = Keyword.validate!(opts, all: all?, by_name: false, allow_missing_columns: false)

    if opts[:allow_missing_columns] and not opts[:by_name] do
      raise ArgumentError, "allow_missing_columns needs by_name: true, as it does in PySpark"
    end

    if opts[:by_name] and kind != :union do
      raise ArgumentError, "by_name applies to :union only, not #{inspect(kind)}"
    end

    set_op = %Proto.SetOperation{
      left_input: left,
      right_input: right,
      set_op_type: type,
      is_all: opts[:all],
      by_name: opts[:by_name],
      allow_missing_columns: opts[:allow_missing_columns]
    }

    relation({:set_op, set_op})
  end

  @doc """
  Join two relations.

    * `:on` — a column name, a list of names, or a condition expression. Names become Spark's
      `using_columns`, which also collapses the duplicate column; a condition does not.
    * `:how` — one of #{inspect(Keyword.keys(@join_types))}, default `:inner`.

      Plan.join(left, right, on: "id")
      Plan.join(left, right, on: Plan.expr("l.id = r.id"), how: :left)
      Plan.join(left, right, how: :cross)
  """
  @spec join(Proto.Relation.t(), Proto.Relation.t(), keyword()) :: Proto.Relation.t()
  def join(%Proto.Relation{} = left, %Proto.Relation{} = right, opts \\ []) do
    opts = Keyword.validate!(opts, on: nil, how: :inner)
    join = %Proto.Join{left: left, right: right, join_type: join_type(opts[:how])}
    {join, refs} = on(join, opts[:on])

    hoist(relation({:join, join}), refs)
  end

  @doc """
  Name a relation, so its columns can be qualified as `name.column`.

  Spark calls this `alias`, which Elixir cannot use as a function name — `import Latu` would
  stop compiling, since `alias` is a special form. `as` is Spark's own Scala spelling.
  """
  @spec as(Proto.Relation.t() | Proto.Expression.t(), String.t() | atom()) ::
          Proto.Relation.t() | Proto.Expression.t()
  def as(%Proto.Relation{} = input, name) do
    relation(
      {:subquery_alias, %Proto.SubqueryAlias{input: input, alias: identifier(name, "alias")}}
    )
  end

  def as(expr, name) when is_expression(expr) do
    {expr, refs} = drain(expr)

    Latu.Subquery.wrap(expression({:alias, alias_of(expr, name)}), refs)
  end

  # `WithColumns.aliases` holds bare `Alias` messages, not `Expression`s, so this is shared
  # rather than going through `as/2`. `Alias.name` is repeated: Spark uses several names when
  # one expression yields several columns, and a single name still goes in a list.
  defp alias_of(%Proto.Expression{} = expr, name) do
    %Proto.Expression.Alias{expr: expr, name: [identifier(name, "alias")]}
  end

  # `on:` takes one shape or the other, so the proto's mutually exclusive fields cannot both be
  # set. `using_columns` stays `[]` when unused: nil is not an empty repeated field. The field
  # name is a parameter because `AsOfJoin` calls the condition `join_expr` where `Join` calls it
  # `join_condition` — one rule, two spellings.
  defp on(join, term), do: on(join, term, :join_condition)

  defp on(join, nil, _field), do: {join, []}

  defp on(join, condition, field) when is_expression(condition) do
    {condition, refs} = drain(condition)

    {Map.put(join, field, condition), refs}
  end

  defp on(join, names, _field) when is_list(names) do
    {%{join | using_columns: Enum.map(names, &identifier/1)}, []}
  end

  defp on(join, name, field), do: on(join, [name], field)

  defp join_type(how), do: lookup(@join_types, how, "join type")

  # `what` is what is being named — a column, a table, an alias — so the refusal can say which
  # argument was wrong rather than only what shape it should have had. Defaulted, because most
  # callers really are naming a column.
  defp identifier(name, what \\ "column name")
  defp identifier(name, _what) when is_binary(name), do: name

  defp identifier(name, _what) when is_atom(name) and not is_nil(name) do
    Atom.to_string(name)
  end

  defp identifier(name, what) do
    raise ArgumentError, "a #{what} is a string or an atom, not #{inspect(name)}"
  end

  # Spark's enums as atoms, with a message that lists the spellings Latu accepts.
  defp lookup(table, key, what) do
    Keyword.get(table, key) ||
      raise ArgumentError,
            "unknown #{what} #{inspect(key)}, expected one of #{inspect(Keyword.keys(table))}"
  end

  # Hoist the relations an expression referenced, so the server can find them by plan_id.
  #
  # PySpark wraps the relation it has just built, at construction — there is no rewrite pass
  # over the finished tree — and the wrapper takes a plan_id of its own, which then *is* the
  # DataFrame's identity for a later `col/2`. It hoists without asking whether the relation is
  # already in the tree; so does Latu, because that is what the fixtures say.
  defp hoist(root, []), do: root

  defp hoist(root, refs) do
    relation({:with_relations, %Proto.WithRelations{root: root, references: refs}})
  end

  # =============================================
  # Commands
  # =============================================

  @doc """
  Materialise a relation on the server: `CheckpointCommand`.

    * `:local` — a local checkpoint (executor storage, no reliable location), default `false`
    * `:eager` — do it now rather than at the next action, default `true`, as in PySpark
    * `:storage_level` — a name `storage_level/1` knows; **a local checkpoint only**

  The answer is a `CachedRemoteRelation` on `CheckpointCommandResult`, and it is **a resource**:
  driver or executor memory held until it is released or the session ends.
  `remove_cached_relation/1` is the release. See `docs/decisions.md`.

  A storage level without `local: true` is refused rather than sent. `handleCheckpointCommand`
  reads `storage_level` inside its `if (getLocal)` branch and calls `checkpoint(eager)` with
  nothing else in the other, so the server would ignore it in silence; PySpark cannot express
  the combination at all, since `storageLevel` is `localCheckpoint`'s parameter.
  """
  @spec checkpoint(relation(), keyword()) :: Proto.Command.t()
  def checkpoint(%Proto.Relation{} = input, opts \\ []) do
    opts = Keyword.validate!(opts, local: false, eager: true, storage_level: nil)
    level = opts[:storage_level] && storage_level(opts[:storage_level])

    if level && not opts[:local] do
      raise ArgumentError,
            ":storage_level applies to a local checkpoint only — a reliable checkpoint writes " <>
              "to the checkpoint directory and the server ignores the level. Pass local: true."
    end

    checkpoint = %Proto.CheckpointCommand{
      relation: input,
      local: opts[:local],
      eager: opts[:eager],
      storage_level: level
    }

    %Proto.Command{command_type: {:checkpoint_command, checkpoint}}
  end

  @doc """
  Free a checkpointed relation: `RemoveCachedRemoteRelationCommand`.

  Takes the id the server gave, not a relation Latu built, because that is what identifies the
  resource. PySpark sends this from a `__del__` on the plan node — a finalizer that holds a
  session inside a plan, which Latu's layering forbids and which PySpark's own source labels a
  hack. Latu releases explicitly.
  """
  @spec remove_cached_relation(String.t()) :: Proto.Command.t()
  def remove_cached_relation(id) when is_binary(id) do
    remove = %Proto.RemoveCachedRemoteRelationCommand{
      relation: %Proto.CachedRemoteRelation{relation_id: id}
    }

    %Proto.Command{command_type: {:remove_cached_remote_relation_command, remove}}
  end

  # A merge's three clauses, and what each one may do. Not a Latu invention: PySpark's
  # `MergeIntoWriter` exposes exactly these methods on each of its three nested builders, and
  # the shape *is* the semantics — a row with no match in the target cannot be updated or
  # deleted, and a row with no match in the source cannot be inserted.
  @merge_clauses [
    matched: [:update, :update_all, :delete],
    not_matched: [:insert, :insert_all],
    not_matched_by_source: [:update, :update_all, :delete]
  ]

  @merge_actions [
    update: :ACTION_TYPE_UPDATE,
    update_all: :ACTION_TYPE_UPDATE_STAR,
    insert: :ACTION_TYPE_INSERT,
    insert_all: :ACTION_TYPE_INSERT_STAR,
    delete: :ACTION_TYPE_DELETE
  ]

  # The two actions that assign. `MergeAction.assignments` says so in the proto itself
  # ("Required for ActionTypes INSERT and UPDATE"); the `_all` forms take the source row whole
  # and `delete` has nothing to set.
  @merge_assigning [:update, :insert]

  @doc """
  One `WHEN ... THEN` clause of a merge: a `MergeAction` inside an `Expression`.

      Plan.merge_action(:matched, :update, set: [n: Plan.col("s.n")], on: Plan.expr("s.op = 'U'"))
      Plan.merge_action(:not_matched, :insert_all)

  `clause` is `:matched`, `:not_matched` or `:not_matched_by_source`, and it decides which
  actions are legal — the restriction is PySpark's own builder shape, so refusing here costs
  no round trip. `:on` narrows the clause; `:set` is required for `:update` and `:insert` and
  refused for the other three.

  **An assignment key is a target column name**, and it travels as an `ExpressionString`:
  PySpark writes `expr(k)` for the key and Latu follows, so a keyword key and a string key are
  the same bytes. Anything that is not a name is refused rather than sent.
  """
  @spec merge_action(atom(), atom(), keyword()) :: expression()
  def merge_action(clause, action, opts \\ []) do
    allowed = lookup(@merge_clauses, clause, "merge clause")
    opts = Keyword.validate!(opts, on: nil, set: nil)

    if action not in allowed do
      raise ArgumentError,
            "a #{clause} clause cannot #{inspect(action)}, only #{inspect(allowed)} — " <>
              "the three clauses take different actions, as they do in SQL"
    end

    merge = %Proto.MergeAction{
      action_type: lookup(@merge_actions, action, "merge action"),
      condition: opts[:on] && grounded(opts[:on], :on),
      assignments: assignments(action, opts[:set])
    }

    expression({:merge_action, merge})
  end

  defp assignments(action, nil) when action in @merge_assigning do
    raise ArgumentError,
          "a #{action} clause needs :set — the #{action}_all form is what takes the source " <>
            "row whole"
  end

  defp assignments(_action, nil), do: []

  defp assignments(action, _set) when action not in @merge_assigning do
    raise ArgumentError, "a #{action} clause assigns nothing, so :set has no meaning for it"
  end

  defp assignments(action, []) do
    raise ArgumentError, "a #{action} clause's :set is empty, which assigns nothing"
  end

  defp assignments(_action, set) do
    Enum.map(set, fn {key, value} ->
      %Proto.MergeAction.Assignment{
        key: expr(name_of(key, "merge assignment key")),
        value: grounded(value, :set)
      }
    end)
  end

  @doc """
  A merge condition, coerced and checked.

  `Latu.MergeInto` calls this when the merge is started rather than when it is sent, so a
  condition that cannot travel is refused at the call site that wrote it.
  """
  @spec merge_condition(term()) :: expression()
  def merge_condition(condition), do: grounded(condition, :condition)

  @doc """
  Merge a source relation into a target table: `MergeIntoTableCommand`.

      Plan.merge_into(source, "people", Plan.expr("t.id = s.id"),
        matched: [Plan.merge_action(:matched, :update_all)],
        not_matched: [Plan.merge_action(:not_matched, :insert_all)]
      )

  The three action lists hold `merge_action/3` results. **At least one action is required**:
  `MergeIntoWriter.mergeCommand` throws `NO_MERGE_ACTION_SPECIFIED` for an empty merge, so the
  answer is fixed and the round trip buys nothing.

  Note what is *not* checked. Spark's rule that only the last clause of a list may omit its
  condition is thrown from `AstBuilder` — it is a rule about SQL text, and the DataFrame path
  never applies it. Enforcing it here would refuse a plan the server accepts.
  """
  @spec merge_into(relation(), String.t() | atom(), term(), keyword()) :: Proto.Command.t()
  def merge_into(%Proto.Relation{} = source, table, condition, opts \\ []) do
    opts =
      Keyword.validate!(opts,
        matched: [],
        not_matched: [],
        not_matched_by_source: [],
        schema_evolution: false
      )

    if Enum.all?([:matched, :not_matched, :not_matched_by_source], &(opts[&1] == [])) do
      raise ArgumentError,
            "a merge needs at least one clause — when_matched/3, when_not_matched/3 or " <>
              "when_not_matched_by_source/3 — or it would do nothing and the server would " <>
              "refuse it (NO_MERGE_ACTION_SPECIFIED)"
    end

    merge = %Proto.MergeIntoTableCommand{
      target_table_name: table_name(table),
      source_table_plan: source,
      merge_condition: grounded(condition, :condition),
      match_actions: opts[:matched],
      not_matched_actions: opts[:not_matched],
      not_matched_by_source_actions: opts[:not_matched_by_source],
      with_schema_evolution: opts[:schema_evolution]
    }

    %Proto.Command{command_type: {:merge_into_table_command, merge}}
  end

  @doc """
  Write to a path or a table: `WriteOperation`, the eager side of `read/1`.

      Plan.write(relation, format: "parquet", path: "/data/out", mode: :overwrite)
      Plan.write(relation, table: {"people", :save_as_table}, mode: :append)

    * `:mode` — `:append`, `:overwrite`, `:error` (Spark's error-if-exists) or `:ignore`.
      Omitted, the wire carries no mode and the server applies its default (error-if-exists).
    * `:path` or `:table` — a oneof on the wire, never both. `:table` is
      `{name, :save_as_table | :insert_into}`.
    * `:partition_by`, `:sort_by`, `:cluster_by` — column *names*; `:bucket_by` —
      `{buckets, names}`, sent only when given.
    * `:options` — `to_options/1`.

  Returns a command; `new/1` wraps it into the `Plan` the transport executes.
  """
  @spec write(relation(), keyword()) :: command()
  def write(%Proto.Relation{} = input, opts \\ []) do
    opts =
      Keyword.validate!(opts,
        format: nil,
        mode: nil,
        path: nil,
        table: nil,
        partition_by: [],
        sort_by: [],
        cluster_by: [],
        bucket_by: nil,
        options: []
      )

    write = %Proto.WriteOperation{
      input: input,
      source: format(opts[:format]),
      mode: save_mode(opts[:mode]),
      save_type: save_type(opts[:path], opts[:table]),
      partitioning_columns: Enum.map(opts[:partition_by], &identifier/1),
      sort_column_names: Enum.map(opts[:sort_by], &identifier/1),
      clustering_columns: Enum.map(opts[:cluster_by], &identifier/1),
      bucket_by: bucket(opts[:bucket_by]),
      options: Map.new(to_options(opts[:options]))
    }

    %Proto.Command{command_type: {:write_operation, write}}
  end

  defp save_mode(nil), do: :SAVE_MODE_UNSPECIFIED
  defp save_mode(mode), do: lookup(@save_modes, mode, "save mode")

  defp save_type(nil, nil), do: nil
  defp save_type(path, nil) when is_binary(path), do: {:path, path}

  defp save_type(nil, {name, method}) do
    table = %Proto.WriteOperation.SaveTable{
      table_name: table_name(name),
      save_method: lookup(@save_methods, method, "table save method")
    }

    {:table, table}
  end

  defp save_type(nil, other) do
    raise ArgumentError,
          ":table is {name, :save_as_table | :insert_into}, not #{inspect(other)}"
  end

  defp save_type(_path, _table) do
    raise ArgumentError, "a write goes to :path or to :table, never both"
  end

  defp bucket(nil), do: nil

  defp bucket({buckets, columns}) when is_integer(buckets) and buckets > 0 and is_list(columns) do
    %Proto.WriteOperation.BucketBy{
      num_buckets: buckets,
      bucket_column_names: Enum.map(columns, &identifier/1)
    }
  end

  defp bucket(other) do
    raise ArgumentError, ":bucket_by is {buckets, [columns]}, not #{inspect(other)}"
  end

  @doc """
  Write to a table through the v2 API: `WriteOperationV2`.

      Plan.write_v2(relation, "people", mode: :create, using: "parquet")

    * `:mode` — required; `:create`, `:replace`, `:create_or_replace`, `:append`, `:overwrite`
      or `:overwrite_partitions` — one spelling per PySpark terminal method.
    * `:condition` — the overwrite predicate, `:overwrite` only.
    * `:partition_by` — *expressions* here, where `write/2` takes names, so a transform
      like `Latu.Plan.fun("years", [:ts])` works; `:cluster_by` stays names.
    * `:table_properties` — keys verbatim (they are user-defined, so no camelCase), values
      stringified as option values are.
  """
  @spec write_v2(relation(), String.t() | atom(), keyword()) :: command()
  def write_v2(%Proto.Relation{} = input, table, opts \\ []) do
    opts =
      Keyword.validate!(opts,
        mode: nil,
        using: nil,
        partition_by: [],
        cluster_by: [],
        table_properties: [],
        condition: nil,
        options: []
      )

    mode = opts[:mode] || raise(ArgumentError, "write_v2 needs a :mode")

    if opts[:condition] != nil and mode != :overwrite do
      raise ArgumentError,
            ":condition is the overwrite predicate and needs mode: :overwrite, " <>
              "got mode: #{inspect(mode)}"
    end

    write = %Proto.WriteOperationV2{
      input: input,
      table_name: table_name(table),
      provider: format(opts[:using]),
      mode: lookup(@v2_modes, mode, "write_v2 mode"),
      partitioning_columns: Enum.map(opts[:partition_by], &grounded(&1, :partition_by)),
      clustering_columns: Enum.map(opts[:cluster_by], &identifier/1),
      table_properties: Map.new(to_properties(opts[:table_properties])),
      options: Map.new(to_options(opts[:options])),
      overwrite_condition: opts[:condition] && grounded(opts[:condition], :condition)
    }

    %Proto.Command{command_type: {:write_operation_v2, write}}
  end

  # A command is not a relation, so there is no `WithRelations` to hoist a reference into: a
  # subquery in a write would encode as a carrier where an expression belongs and fail somewhere
  # unhelpful. Refuse it here, where the argument still has a name.
  defp grounded(%Latu.Subquery{}, key) do
    raise ArgumentError,
          "#{key}: a subquery cannot travel in a write command, which has nowhere to hoist " <>
            "the frame it references — collect the value first and pass a literal"
  end

  # The keys whose value is an expression rather than a name. `:on` and `:set` are the merge
  # clause condition and its assignment values; the rest of `grounded/2`'s callers pass names.
  @expression_keys [:condition, :on, :set]

  defp grounded(value, key) when key in @expression_keys, do: to_expr(value)
  defp grounded(value, _key), do: to_name(value)

  # Property keys pass verbatim — they are user-defined names, so camelCasing would corrupt
  # them. Values follow option values.
  defp to_properties(properties) do
    properties
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Enum.map(fn {key, value} -> {property_key(key), option_value(value, key)} end)
  end

  defp property_key(key) when is_binary(key), do: key

  defp property_key(key) when is_atom(key) and not is_nil(key) and not is_boolean(key) do
    Atom.to_string(key)
  end

  defp property_key(key) do
    raise ArgumentError, "a table property key is an atom or a string, not #{inspect(key)}"
  end

  @doc """
  `Latu.sql/3`'s command: `SqlCommand` over the `sql/2` relation, run eagerly — PySpark's
  choice, so DDL executes when called. The response usually carries a result relation back;
  `adopt/1` is how the DataFrame takes it.
  """
  @spec sql_command(String.t(), [term()] | map()) :: command()
  def sql_command(query, args \\ [], views \\ []) do
    input = sql(query, args, views)

    %Proto.Command{command_type: {:sql_command, %Proto.SqlCommand{input: input}}}
  end

  @doc """
  Register a DataFrame as a view: `CreateDataFrameViewCommand`. One call with `:global` and
  `:replace` flags stands in for PySpark's four `create*TempView` methods — docs/deviations.md.
  """
  @spec create_view(relation(), String.t() | atom(), keyword()) :: command()
  def create_view(%Proto.Relation{} = input, name, opts \\ []) do
    opts = Keyword.validate!(opts, global: false, replace: false)

    view = %Proto.CreateDataFrameViewCommand{
      input: input,
      name: name_of(name, "view name"),
      is_global: flag(opts[:global], :global),
      replace: flag(opts[:replace], :replace)
    }

    %Proto.Command{command_type: {:create_dataframe_view, view}}
  end

  defp flag(value, _key) when is_boolean(value), do: value
  defp flag(value, key), do: raise(ArgumentError, "#{key}: is a flag, got #{inspect(value)}")

  # =============================================
  # Expressions
  # =============================================

  @doc """
  A column reference.

  Unresolved: the server matches it against the plan it is used in, so a name that is not there
  fails at analysis rather than here.
  """
  @spec col(String.t() | atom()) :: Proto.Expression.t()
  def col(name) do
    # PySpark sets is_metadata_column explicitly, so it is present on the wire. Leaving it nil
    # is not the same message, and the golden tests say so.
    attribute = %Proto.Expression.UnresolvedAttribute{
      unparsed_identifier: identifier(name),
      is_metadata_column: false
    }

    expression({:unresolved_attribute, attribute})
  end

  @doc """
  A column reference tagged with the relation it came from.

  The server resolves a tagged reference by searching the analysed plan for that `plan_id`,
  which is how `df1.a` and `df2.a` stay apart. **A reference to a relation that is not in this
  tree is refused by Spark, hoisted or not**: the analyser searches downward from the operator,
  so a relation in `WithRelations.references` is never found (`docs/decisions.md`, M9.1). The
  reference that does resolve across frames is a subquery — `Latu.Subquery`.
  """
  @spec col(String.t() | atom(), relation()) :: expression()
  def col(name, %Proto.Relation{common: %{plan_id: id}}) do
    {:unresolved_attribute, attribute} = col(name).expr_type

    expression({:unresolved_attribute, %{attribute | plan_id: id}})
  end

  @doc """
  Columns whose names match a Java regex: `UnresolvedRegex`.

  Always tagged to a relation, as PySpark's `colRegex` is — the pattern is resolved against
  that frame's columns, so an untagged form has nothing to match against.

      Plan.col_regex("`(id)?+.+`", relation)

  Spark wants the pattern in backticks; it is passed through untouched.
  """
  @spec col_regex(String.t(), relation()) :: Proto.Expression.t()
  def col_regex(pattern, %Proto.Relation{common: %{plan_id: id}}) when is_binary(pattern) do
    regex = %Proto.Expression.UnresolvedRegex{col_name: pattern, plan_id: id}

    expression({:unresolved_regex, regex})
  end

  @doc """
  A hidden metadata column, such as `_metadata` on a file source.

  `col/2` with `is_metadata_column` set, which is exactly how PySpark spells it.
  """
  @spec metadata_column(String.t() | atom(), relation()) :: Proto.Expression.t()
  def metadata_column(name, %Proto.Relation{} = input) do
    {:unresolved_attribute, attribute} = col(name, input).expr_type

    expression({:unresolved_attribute, %{attribute | is_metadata_column: true}})
  end

  @doc """
  Every column, as `select("*")` means it.
  """
  @spec star() :: Proto.Expression.t()
  def star, do: expression({:unresolved_star, %Proto.Expression.UnresolvedStar{}})

  @doc """
  A raw SQL expression, parsed by the server.

      Plan.filter(input, Plan.expr("id > 3"))

  The escape hatch for anything Latu has no builder for, and it stays useful after it does.
  """
  @spec expr(String.t()) :: Proto.Expression.t()
  def expr(sql) when is_binary(sql) do
    expression({:expression_string, %Proto.Expression.ExpressionString{expression: sql}})
  end

  @doc """
  A function call, resolved by name on the server.

      Plan.fun(">", [:id, 3])
      Plan.fun("upper", [:name])

  Nearly all of Spark's function library is this one node, so this is also the escape hatch
  for a function Latu has no wrapper for. Arguments go through `to_expr/1`.

  `distinct: true` sets `is_distinct`, which is how Spark spells `count(DISTINCT x)` — there is
  no separate function name for it. See `Latu.Functions`.
  """
  @spec fun(String.t(), [term()], keyword()) :: expression()
  def fun(name, arguments, opts \\ []) when is_binary(name) and is_list(arguments) do
    {arguments, refs} = drain_all(arguments)

    call = %Proto.Expression.UnresolvedFunction{
      function_name: name,
      arguments: arguments,
      is_distinct: Keyword.get(opts, :distinct, false)
    }

    Latu.Subquery.wrap(expression({:unresolved_function, call}), refs)
  end

  @doc """
  A call to a function by name, resolved by Spark's catalog rather than by its parser.

      Plan.call_function("my_udf", [:id])

  A different node from `fun/3`: `CallFunction` is how Spark reaches a registered or
  user-defined function, where `UnresolvedFunction` is how it reaches a builtin. The one place
  Latu can call code that is not part of the function library.
  """
  @spec call_function(String.t(), [term()]) :: expression()
  def call_function(name, arguments) when is_binary(name) and is_list(arguments) do
    {arguments, refs} = drain_all(arguments)

    call = %Proto.CallFunction{function_name: name, arguments: arguments}

    Latu.Subquery.wrap(expression({:call_function, call}), refs)
  end

  @doc """
  A subquery over another relation: `:scalar`, `:exists`, `:in` or `:table_arg`.

      Plan.subquery(other, :scalar)
      Plan.subquery(other, :in, values: [:id])

  Returns a `Latu.Subquery`, because the wire carries only the referenced relation's `plan_id`
  and the relation itself has to reach the verb some other way. `:in` takes the values to test,
  and is the only shape that does.

  This is the one cross-DataFrame reference Spark resolves: a bare `col/2` pointing outside its
  own tree is refused whether or not it is hoisted — `docs/decisions.md`, M9.1.
  """
  @spec subquery(relation(), atom(), keyword()) :: Latu.Subquery.t()
  def subquery(%Proto.Relation{} = referenced, type, opts \\ []) do
    opts = Keyword.validate!(opts, values: [])

    if opts[:values] != [] and type != :in do
      raise ArgumentError, "values: belongs to an :in subquery, not #{inspect(type)}"
    end

    {values, refs} = drain_all(opts[:values])
    values = in_values(values)

    node = %Proto.SubqueryExpression{
      plan_id: referenced.common.plan_id,
      subquery_type: lookup(@subquery_types, type, "subquery type"),
      in_subquery_values: values
    }

    # The subquery's own relation first, as PySpark collects it: `foreach` visits a node before
    # its children. Values carrying references of their own is a shape PySpark drops on the
    # floor; keeping them costs nothing and the alternative is a plan that cannot resolve.
    Latu.Subquery.wrap(expression({:subquery_expression, node}), [referenced | refs])
  end

  # `struct(a, b)` tested against a two-column subquery sends the struct's children rather than
  # the struct itself, so the arity matches. PySpark's rule, in `Column.isin`.
  defp in_values([%Proto.Expression{expr_type: {:unresolved_function, call}}] = values) do
    if call.function_name == "struct", do: call.arguments, else: values
  end

  defp in_values(values), do: values

  @doc """
  A seed, drawn the way PySpark draws one.

  Several of Spark's functions take an optional seed and send a random one when it is omitted —
  `sample`, `rand`, `shuffle` and friends — which makes the *plan* differ between builds, not
  just the result. One definition, so they cannot drift apart.
  """
  @spec random_seed() :: non_neg_integer()
  def random_seed, do: :rand.uniform(@max_seed) - 1

  @doc """
  A lambda, for Spark's higher-order functions.

      Plan.lambda(fn x -> Plan.fun("+", [x, 1]) end)

  The anonymous function receives one expression per parameter and returns one. Spark allows one
  to three parameters, and names them `x`, `y` and `z` by position — Elixir cannot see what you
  called yours, and Spark would ignore it anyway.

  Each variable gets a unique suffix, because a nested lambda that reused a name would shadow the
  outer one. That makes the plan differ between builds, so `normalize_ids/1` renumbers them for
  golden tests, exactly as it does `plan_id`.
  """
  @spec lambda(function()) :: expression()
  def lambda(fun) when is_function(fun) do
    {:arity, arity} = Function.info(fun, :arity)
    variables = lambda_variables(arity)

    body =
      variables
      |> Enum.map(&expression({:unresolved_named_lambda_variable, &1}))
      |> then(&apply(fun, &1))

    {body, refs} = drain(body)
    lambda = %Proto.Expression.LambdaFunction{function: body, arguments: variables}

    Latu.Subquery.wrap(expression({:lambda_function, lambda}), refs)
  end

  defp lambda_variables(arity) when arity in 1..3 do
    @lambda_names
    |> Enum.take(arity)
    |> Enum.map(fn name ->
      suffix = :erlang.unique_integer([:positive, :monotonic])

      %Proto.Expression.UnresolvedNamedLambdaVariable{name_parts: ["#{name}_#{suffix}"]}
    end)
  end

  defp lambda_variables(arity) do
    raise ArgumentError, "a Spark lambda takes one to three arguments, not #{arity}"
  end

  @doc """
  A higher-order function call: some columns, then some lambdas, in one argument list.

      Plan.higher_order("transform", [:xs], [fn x -> Plan.fun("+", [x, 1]) end])

  That flat shape is Spark's — a lambda is an ordinary argument to an ordinary
  `UnresolvedFunction`, not a field of its own. See `Latu.Functions`.
  """
  @spec higher_order(String.t(), [term()], [function()]) :: expression()
  def higher_order(name, columns, funs)
      when is_binary(name) and is_list(columns) and is_list(funs) do
    fun(name, columns ++ Enum.map(funs, &lambda/1))
  end

  @doc """
  A window function call: an expression evaluated over `Latu.Window`'s frame.

      Plan.over(Plan.fun("row_number", []), Latu.Window.partition_by([:suburb]))

  There is no window relation. `Expression.Window` rides inside whatever relation the projection
  belongs to, which is why `over/2` returns an expression like any other builder.
  """
  @spec over(expression(), Latu.Window.t()) :: expression()
  def over(function, %Latu.Window{} = window) when is_expression(function) do
    {function, refs} = drain(function)
    {partitions, partition_refs} = drain_all(window.partitions || [])
    {orders, order_refs} = drain_sorts(window.orders)

    spec = %Proto.Expression.Window{
      window_function: function,
      partition_spec: partitions,
      order_spec: orders,
      frame_spec: frame(window.frame)
    }

    Latu.Subquery.wrap(expression({:window, spec}), refs ++ partition_refs ++ order_refs)
  end

  defp frame(nil), do: nil

  defp frame({type, lower, upper}) do
    %Proto.Expression.Window.WindowFrame{
      frame_type: lookup(@frame_types, type, "frame type"),
      lower: boundary(lower, type),
      upper: boundary(upper, type)
    }
  end

  # The wire carries no direction: `unbounded` is one flag and the side it sits on says which.
  # `Latu.Window` rejects a boundary naming the wrong direction before it gets here.
  defp boundary(:current_row, _type), do: bound({:current_row, true})
  defp boundary(0, _type), do: bound({:current_row, true})
  defp boundary(:unbounded_preceding, _type), do: bound({:unbounded, true})
  defp boundary(:unbounded_following, _type), do: bound({:unbounded, true})

  # A row offset is int32 and a range offset is int64 — Spark's asymmetry, and not one `lit/1`
  # can express, since that picks the width by magnitude. `docs/decisions.md`.
  defp boundary(offset, :rows) when is_integer(offset) and offset in @int32 do
    bound({:value, literal({:integer, offset})})
  end

  defp boundary(offset, :rows) when is_integer(offset) do
    raise ArgumentError, "a rows_between/3 offset is 32-bit; #{offset} is not"
  end

  defp boundary(offset, :range) when is_integer(offset) and offset in @int64 do
    bound({:value, literal({:long, offset})})
  end

  defp bound(boundary) do
    %Proto.Expression.Window.WindowFrame.FrameBoundary{boundary: boundary}
  end

  defp literal(literal_type) do
    expression({:literal, %Proto.Expression.Literal{literal_type: literal_type}})
  end

  @doc """
  A typed literal.

  Elixir's types pick Spark's for you. Integers become `integer` or `long` by magnitude; floats
  are always `double`, since Elixir has no 32-bit float. `%DateTime{}` is an instant and becomes
  `timestamp`; `%NaiveDateTime{}` is a wall-clock reading with no zone and becomes
  `timestamp_ntz`, which is what Spark's own type means. Nothing here applies a timezone.

  A list is deliberately not accepted: Spark has no array literal in practice, and PySpark
  compiles `[1, 2, 3]` into an `array` function call over scalar literals. That arrives with the
  function library.
  """
  @spec lit(term()) :: expression()
  def lit(value), do: expression({:literal, literal_of(value)})

  # Shared with `Aggregate.Pivot.values`, which holds the bare message.
  defp literal_of(value), do: %Proto.Expression.Literal{literal_type: scalar(value)}

  # PySpark emits NullType here rather than inventing a default. Match it.
  defp scalar(nil), do: {:null, %Proto.DataType{kind: {:null, %Proto.DataType.NULL{}}}}
  defp scalar(value) when is_boolean(value), do: {:boolean, value}
  defp scalar(value) when is_integer(value) and value in @int32, do: {:integer, value}
  defp scalar(value) when is_integer(value) and value in @int64, do: {:long, value}
  defp scalar(value) when is_float(value), do: {:double, value}
  defp scalar(value) when is_binary(value), do: {:string, value}
  defp scalar(%Date{} = value), do: {:date, Date.diff(value, @epoch_date)}

  defp scalar(%DateTime{} = value), do: {:timestamp, DateTime.to_unix(value, :microsecond)}

  defp scalar(%NaiveDateTime{} = value) do
    {:timestamp_ntz, NaiveDateTime.diff(value, @epoch_naive, :microsecond)}
  end

  defp scalar(value) when is_struct(value, Decimal), do: {:decimal, decimal(value)}

  defp scalar(value) when is_integer(value) do
    raise ArgumentError, "#{value} does not fit in a 64-bit integer"
  end

  defp scalar(value) do
    raise ArgumentError,
          "cannot make a Spark literal of #{inspect(value)}; for a column reference use " <>
            "col/1, and for anything else use expr/1"
  end

  # precision 10 / scale 0 look wrong for a value like "1.50" but are PySpark's, and Spark reads
  # the value string regardless. Measured, not assumed — see docs/decisions.md before changing.
  defp decimal(%{coef: coef} = value) when is_integer(coef) do
    if length(Integer.digits(coef)) > @max_precision do
      raise ArgumentError, "#{Decimal.to_string(value)} needs more than #{@max_precision} digits"
    end

    %Proto.Expression.Literal.Decimal{
      value: Decimal.to_string(value, :normal),
      precision: 10,
      scale: 0
    }
  end

  defp decimal(value) do
    raise ArgumentError, "#{Decimal.to_string(value)} is not a finite decimal"
  end

  @doc """
  Coerce a value used where an *expression* belongs: a condition, a function argument, a
  `with_column` value.

  An atom is a column reference; **a binary is a string literal**. That is PySpark's rule, so
  `filter(df, eq(:suburb, "Reservoir"))` compares a column to a string. Use `col/1` when a name
  needs to be a binary.
  """
  @spec to_expr(term()) :: expression()
  def to_expr(%Proto.Expression{} = expr), do: expr

  def to_expr(%Latu.Subquery{expr: %Proto.Expression.SortOrder{}}), do: sort_key_error()
  def to_expr(%Latu.Subquery{} = subquery), do: subquery

  def to_expr(%Proto.Expression.SortOrder{}), do: sort_key_error()

  def to_expr(%Latu.CaseWhen{} = chain), do: fun("when", Latu.CaseWhen.arguments(chain))

  def to_expr(value) when is_nil(value) or is_boolean(value), do: lit(value)
  def to_expr(value) when is_atom(value), do: col(value)
  def to_expr(value), do: lit(value)

  @doc """
  One sort key.

    * `:direction` — `:asc` (default) or `:desc`
    * `:nulls` — `:first` or `:last`; the default follows the direction, `:first` for `:asc`
      and `:last` for `:desc`, which is SQL's rule and PySpark's

  The child goes through `to_name/1`, so `sort_order("id")` sorts by the column.
  """
  @spec sort_order(term(), keyword()) :: sort_order()
  def sort_order(column, opts \\ []) do
    opts = Keyword.validate!(opts, direction: :asc, nulls: nil)
    direction = opts[:direction]

    {child, refs} = drain_name(column)

    key = %Proto.Expression.SortOrder{
      child: child,
      direction: lookup(@directions, direction, "sort direction"),
      null_ordering: lookup(@nulls, opts[:nulls] || @default_nulls[direction], "null ordering")
    }

    Latu.Subquery.wrap(key, refs)
  end

  @doc """
  Cast to a Spark type, spelled as SQL spells it: `"string"`, `"int"`, `"decimal(10,2)"`.

  `try_cast/2` is the same node with Spark's TRY eval mode, which gives null where a cast would
  fail instead of raising.
  """
  @spec cast(term(), String.t()) :: expression()
  def cast(column, type) when is_binary(type), do: cast_node(column, type, [])

  @doc "See `cast/2`."
  @spec try_cast(term(), String.t()) :: expression()
  def try_cast(column, type) when is_binary(type) do
    cast_node(column, type, eval_mode: :EVAL_MODE_TRY)
  end

  # `type` and `type_str` are a oneof, and PySpark sends the string for a string. A DataType
  # needs a builder Latu does not have yet.
  defp cast_node(column, type, extra) do
    {expr, refs} = drain(column)
    cast = %Proto.Expression.Cast{expr: expr, cast_to_type: {:type_str, type}}

    Latu.Subquery.wrap(expression({:cast, struct!(cast, extra)}), refs)
  end

  @doc """
  Coerce the mixed list `Latu.select/2` and `Latu.agg/2` both take.

  A `{name, expression}` pair becomes an alias; everything else goes through `to_name/1`. A
  single column needs no list.

      Plan.to_projections([:id, doubled: Plan.fun("*", [:id, 2])])
  """
  @spec to_projections(term()) :: [expression()]
  def to_projections(columns) when is_list(columns), do: Enum.map(columns, &to_projection/1)
  def to_projections(column), do: to_projections([column])

  defp to_projection({name, value}) when is_atom(name) and not is_nil(name) do
    as(to_expr(value), name)
  end

  defp to_projection(column), do: to_name(column)

  defp sort_key_error do
    raise ArgumentError, "a sort key belongs in sort/2, not in an expression"
  end

  # Coerce, then separate a built value from the relations it references. Every builder that
  # takes expressions goes through one of these, which is what makes a reference propagate
  # through a whole expression tree the way `Expression.foreach` does in PySpark.
  defp drain(term), do: split(to_expr(term))
  defp drain_name(term), do: split(to_name(term))
  defp drain_sort(term), do: split(to_sort_order(term))

  defp drain_all(terms), do: drain_each(terms, &drain/1)
  defp drain_sorts(orders), do: drain_each(orders, &drain_sort/1)

  defp drain_each(terms, drain) do
    Enum.map_reduce(terms, [], fn term, refs ->
      {value, more} = drain.(term)

      {value, refs ++ more}
    end)
  end

  defp split(%Latu.Subquery{expr: expr, refs: refs}), do: {expr, refs}
  defp split(built), do: {built, []}

  @doc """
  Coerce a value used where a *sort key* belongs: `sort`, `order_by`, a window's ordering.

  A bare name sorts ascending with nulls first, as PySpark's `orderBy("id")` does.
  """
  @spec to_sort_order(term()) :: sort_order()
  def to_sort_order(%Proto.Expression.SortOrder{} = order), do: order
  def to_sort_order(%Latu.Subquery{expr: %Proto.Expression.SortOrder{}} = key), do: key
  def to_sort_order(column), do: sort_order(column)

  @doc """
  Coerce a value used where a *name* belongs: `select`, `drop`, `group_by`, `order_by`.

  Here a binary is a column name, not a literal — the other half of PySpark's rule. `select(df,
  ["price", :suburb])` selects two columns.

  `"*"` is every column, not a column called `*`. PySpark special-cases it the same way, and a
  column reference to `*` would be a silently different node.
  """
  @spec to_name(term()) :: expression()
  def to_name(%Proto.Expression{} = expr), do: expr
  def to_name(%Latu.Subquery{} = subquery), do: subquery
  def to_name(name) when name in ["*", :*], do: star()
  def to_name(name), do: col(name)

  @doc """
  Coerce reader/writer options to what the wire wants: string keys, string values.

  A snake_case atom key becomes Spark's camelCase (`:infer_schema` → `"inferSchema"`); a binary
  key passes verbatim — the escape hatch for a key no atom spells. Values follow PySpark's
  `to_str`: booleans lowercase, numbers stringified, atoms and binaries as they are, and a `nil`
  value drops its pair. Order is kept, because the `from_json` family sends options as a `map`
  *function call*, where argument order reaches the wire.
  """
  @spec to_options(keyword() | map()) :: [{String.t(), String.t()}]
  def to_options(options) when is_list(options) or (is_map(options) and not is_struct(options)) do
    options
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Enum.map(fn {key, value} -> {option_key(key), option_value(value, key)} end)
  end

  defp option_key(key) when is_binary(key), do: key

  defp option_key(key) when is_atom(key) and not is_nil(key) and not is_boolean(key) do
    [first | rest] = key |> Atom.to_string() |> String.split("_")
    IO.iodata_to_binary([first | Enum.map(rest, &String.capitalize/1)])
  end

  defp option_key(key) do
    raise ArgumentError, "an option key is an atom or a string, not #{inspect(key)}"
  end

  # The key rides along only so the refusal can name it: an options map is written all at once
  # and "an option value is a string" without saying *which* option sends you reading the whole
  # thing.
  defp option_value(value, _key) when is_binary(value), do: value
  defp option_value(true, _key), do: "true"
  defp option_value(false, _key), do: "false"
  defp option_value(value, _key) when is_integer(value), do: Integer.to_string(value)
  defp option_value(value, _key) when is_float(value), do: Float.to_string(value)
  defp option_value(value, _key) when is_atom(value), do: Atom.to_string(value)

  defp option_value(value, key) do
    raise ArgumentError,
          "#{inspect(key)} is a string, number, boolean or atom, not #{inspect(value)} — " <>
            "Spark takes options as strings, and Latu converts only those"
  end

  defp expression(expr_type), do: %Proto.Expression{expr_type: expr_type}

  # =============================================
  # Missing data
  # =============================================

  @doc """
  Fill nulls with a literal, column by column.

  An empty `cols` means every column **whose type matches the value** — Spark's own rule, and
  the reason filling a string column with a number does nothing at all rather than erroring.
  """
  @spec na_fill(relation(), [String.t() | atom()], [term()]) :: relation()
  def na_fill(%Proto.Relation{} = input, cols, values) when is_list(cols) and is_list(values) do
    fill = %Proto.NAFill{
      input: input,
      cols: Enum.map(cols, &identifier/1),
      values: Enum.map(values, &fill_value/1)
    }

    relation({:fill_na, fill})
  end

  @doc """
  Drop rows by how many non-null values they carry.

  `min_non_nulls` is `nil` for PySpark's `how="any"` — the field has presence and an absent one
  means "every column must be non-null", which is not the same message as any number.
  """
  @spec na_drop(relation(), [String.t() | atom()], pos_integer() | nil) :: relation()
  def na_drop(%Proto.Relation{} = input, cols, min_non_nulls) when is_list(cols) do
    drop = %Proto.NADrop{
      input: input,
      cols: Enum.map(cols, &identifier/1),
      min_non_nulls: min_non_nulls
    }

    relation({:drop_na, drop})
  end

  @doc """
  Replace values with other values, column by column, as `{old, new}` pairs.

  Both sides are bare `Literal`s, as `Aggregate.Pivot`'s values are — not `Expression`s.
  """
  @spec na_replace(relation(), [String.t() | atom()], [{term(), term()}]) :: relation()
  def na_replace(%Proto.Relation{} = input, cols, replacements)
      when is_list(cols) and is_list(replacements) do
    replace = %Proto.NAReplace{
      input: input,
      cols: Enum.map(cols, &identifier/1),
      replacements: Enum.map(replacements, &replacement/1)
    }

    relation({:replace, replace})
  end

  defp replacement({old, new}) do
    %Proto.NAReplace.Replacement{
      old_value: literal_of(replace_value(old)),
      new_value: literal_of(replace_value(new))
    }
  end

  # `NAFill` takes four value types and no others — the server refuses an int32 by name
  # ("Unsupported value type java.lang.Integer"), so an Elixir integer goes as a long. This is
  # why `lit/1`, which picks int32 by magnitude, cannot be reused here.
  defp fill_value(value) when is_boolean(value), do: fill_literal({:boolean, value})
  defp fill_value(value) when is_integer(value), do: fill_literal({:long, value})
  defp fill_value(value) when is_float(value), do: fill_literal({:double, value})
  defp fill_value(value) when is_binary(value), do: fill_literal({:string, value})

  defp fill_value(other) do
    raise ArgumentError,
          "fill_na: a boolean, integer, float or string — Spark fills with nothing else, " <>
            "got #{inspect(other)}"
  end

  defp fill_literal(literal_type) do
    %Proto.Expression.Literal{literal_type: literal_type}
  end

  # PySpark converts a non-boolean integer to a float before building the literal, so the two
  # sides of a pair cannot disagree about width. Mirrored, or the plans differ.
  defp replace_value(value) when is_integer(value), do: value * 1.0
  defp replace_value(value), do: value

  # =============================================
  # Statistics
  # =============================================

  @doc """
  One row per statistic, one column per numeric or string column.

  An empty `statistics` sends none and the server applies its own list — count, mean, stddev,
  min, the three quartiles, max. Any name Spark's `StatFunctions` knows is accepted, percentiles
  (`"25%"`) included.
  """
  @spec summary(relation(), [String.t() | atom()]) :: relation()
  def summary(%Proto.Relation{} = input, statistics \\ []) when is_list(statistics) do
    summary = %Proto.StatSummary{input: input, statistics: Enum.map(statistics, &statistic/1)}

    relation({:summary, summary})
  end

  @doc """
  `summary/2`'s fixed five — count, mean, stddev, min, max — over the columns named.

  An empty `cols` describes every column Spark can describe. A separate relation from
  `summary/2`, not an option on it: Spark has two.
  """
  @spec describe(relation(), [String.t() | atom()]) :: relation()
  def describe(%Proto.Relation{} = input, cols \\ []) when is_list(cols) do
    relation({:describe, %Proto.StatDescribe{input: input, cols: Enum.map(cols, &identifier/1)}})
  end

  @doc """
  A contingency table: distinct `col1` values down the rows, distinct `col2` across the columns.
  """
  @spec crosstab(relation(), String.t() | atom(), String.t() | atom()) :: relation()
  def crosstab(%Proto.Relation{} = input, col1, col2) do
    crosstab = %Proto.StatCrosstab{
      input: input,
      col1: identifier(col1),
      col2: identifier(col2)
    }

    relation({:crosstab, crosstab})
  end

  @doc """
  Frequent items, one array column of candidates per column named.

  `support` is the minimum frequency. PySpark defaults it to 0.01 and sends it either way, so
  Latu does too — the field has presence, and an absent one is not the same message.
  """
  @spec freq_items(relation(), [String.t() | atom()], number()) :: relation()
  def freq_items(%Proto.Relation{} = input, cols, support \\ 0.01)
      when is_list(cols) and is_number(support) do
    freq_items = %Proto.StatFreqItems{
      input: input,
      cols: Enum.map(cols, &identifier/1),
      support: support * 1.0
    }

    relation({:freq_items, freq_items})
  end

  @doc """
  A stratified sample: a fraction of the rows per stratum of `col`.

    * `:seed` — an integer; a random one is drawn when absent, as in `sample/3`, which means the
      plan differs between runs.

  `fractions` are `{stratum, fraction}` pairs. A stratum is a **value**, so it becomes a bare
  `Literal` under `lit/1`'s own rule — not a column name, which is what an atom would be
  everywhere else in Latu.
  """
  @spec sample_by(relation(), term(), [{term(), number()}], keyword()) :: relation()
  def sample_by(%Proto.Relation{} = input, col, fractions, opts \\ []) when is_list(fractions) do
    opts = Keyword.validate!(opts, seed: nil)
    {col, refs} = drain_name(col)

    sample_by = %Proto.StatSampleBy{
      input: input,
      col: col,
      fractions: Enum.map(fractions, &fraction/1),
      seed: opts[:seed] || random_seed()
    }

    hoist(relation({:sample_by, sample_by}), refs)
  end

  @doc "Sample covariance of two numeric columns: a relation of one row and one column."
  @spec cov(relation(), String.t() | atom(), String.t() | atom()) :: relation()
  def cov(%Proto.Relation{} = input, col1, col2) do
    relation({:cov, %Proto.StatCov{input: input, col1: identifier(col1), col2: identifier(col2)}})
  end

  @doc """
  Correlation of two numeric columns: a relation of one row and one column.

  Spark has exactly one method, `:pearson`. PySpark refuses any other before the wire and sends
  the field regardless; so does Latu.
  """
  @spec corr(relation(), String.t() | atom(), String.t() | atom(), atom() | String.t()) ::
          relation()
  def corr(input, col1, col2, method \\ :pearson)

  def corr(%Proto.Relation{} = input, col1, col2, method) when method in [:pearson, "pearson"] do
    corr = %Proto.StatCorr{
      input: input,
      col1: identifier(col1),
      col2: identifier(col2),
      method: "pearson"
    }

    relation({:corr, corr})
  end

  def corr(%Proto.Relation{}, _col1, _col2, method) do
    raise ArgumentError, "corr: :pearson is the only method Spark has, got #{inspect(method)}"
  end

  @doc """
  Approximate quantiles: one row, one column, one array of quantiles per column named.

  `relative_error` is the accuracy Spark may trade away for speed; 0.0 asks for exact quantiles
  and is expensive.
  """
  @spec approx_quantile(relation(), [String.t() | atom()], [number()], number()) :: relation()
  def approx_quantile(%Proto.Relation{} = input, cols, probabilities, relative_error)
      when is_list(cols) and is_list(probabilities) and is_number(relative_error) do
    unless relative_error >= 0 do
      raise ArgumentError, "approx_quantile: a relative error is not negative"
    end

    quantile = %Proto.StatApproxQuantile{
      input: input,
      cols: Enum.map(cols, &identifier/1),
      probabilities: Enum.map(probabilities, &probability/1),
      relative_error: relative_error * 1.0
    }

    relation({:approx_quantile, quantile})
  end

  # A statistic is neither a column name nor a value, so it gets its own coercion and its own
  # message: "25%" is a perfectly good one and would read oddly as a column.
  defp statistic(name) when is_binary(name), do: name
  defp statistic(name) when is_atom(name) and not is_nil(name), do: Atom.to_string(name)

  defp statistic(other) do
    raise ArgumentError,
          "a summary statistic is a name like \"count\" or \"25%\", got #{inspect(other)}"
  end

  defp fraction({stratum, fraction}) when is_number(fraction) do
    %Proto.StatSampleBy.Fraction{stratum: stratum(stratum), fraction: fraction * 1.0}
  end

  defp fraction(other) do
    raise ArgumentError, "sample_by: {stratum, fraction} pairs, got #{inspect(other)}"
  end

  # PySpark takes a stratum of exactly these types, and so must Latu: an atom would be a column
  # name under `to_expr/1`'s rule, which a stratum never is.
  defp stratum(value) when is_binary(value) or is_number(value), do: literal_of(value)

  defp stratum(other) do
    raise ArgumentError, "sample_by: a stratum is a string or a number, got #{inspect(other)}"
  end

  defp probability(p) when is_number(p) and p >= 0 and p <= 1, do: p * 1.0

  defp probability(p) do
    raise ArgumentError, "a probability is between 0 and 1, got #{inspect(p)}"
  end

  # =============================================
  # Analysis
  # =============================================

  @typedoc "One `AnalyzePlanRequest.analyze` arm: the oneof tag and its message."
  @type analysis :: {atom(), struct()}

  # The arms whose whole message is a Plan and nothing else.
  @plan_arms %{
    schema: Proto.AnalyzePlanRequest.Schema,
    is_local: Proto.AnalyzePlanRequest.IsLocal,
    is_streaming: Proto.AnalyzePlanRequest.IsStreaming,
    input_files: Proto.AnalyzePlanRequest.InputFiles,
    semantic_hash: Proto.AnalyzePlanRequest.SemanticHash
  }

  @analyze_ops ~w(schema tree_string explain is_local is_streaming input_files semantic_hash
                  same_semantics persist unpersist get_storage_level ddl_parse json_to_ddl)a

  @explain_modes [
    simple: :EXPLAIN_MODE_SIMPLE,
    extended: :EXPLAIN_MODE_EXTENDED,
    codegen: :EXPLAIN_MODE_CODEGEN,
    cost: :EXPLAIN_MODE_COST,
    formatted: :EXPLAIN_MODE_FORMATTED
  ]

  # Spark's own storage levels, as {use_disk, use_memory, use_off_heap, deserialized,
  # replication}. Transcribed from pyspark/storagelevel.py; `:memory_and_disk_deser` is what
  # `persist` and `cache` default to, in Scala since 3.0 and in PySpark to match.
  @storage_levels [
    none: {false, false, false, false, 1},
    disk_only: {true, false, false, false, 1},
    disk_only_2: {true, false, false, false, 2},
    disk_only_3: {true, false, false, false, 3},
    memory_only: {false, true, false, false, 1},
    memory_only_2: {false, true, false, false, 2},
    memory_and_disk: {true, true, false, false, 1},
    memory_and_disk_2: {true, true, false, false, 2},
    memory_and_disk_deser: {true, true, false, true, 1},
    off_heap: {true, true, true, false, 1}
  ]

  @doc """
  An `AnalyzePlan` request arm, ready for the transport to send.

      Plan.analyze(:schema, relation)
      Plan.analyze(:tree_string, relation, level: 2)
      Plan.analyze(:explain, relation, mode: :extended)
      Plan.analyze(:same_semantics, {relation, other})
      Plan.analyze(:persist, relation, level: :memory_and_disk)
      Plan.analyze(:ddl_parse, "a INT, b STRING")

  The second argument is whatever the arm takes: a relation, a pair of them, or a string.
  Mind which is which on the wire — `persist`, `unpersist` and `get_storage_level` carry a bare
  `Relation` where every other arm carries a `Plan`.
  """
  @spec analyze(atom(), relation() | {relation(), relation()} | String.t(), keyword()) ::
          analysis()
  def analyze(op, input, opts \\ [])

  def analyze(op, %Proto.Relation{} = relation, opts) when is_map_key(@plan_arms, op) do
    Keyword.validate!(opts, [])

    {op, struct!(Map.fetch!(@plan_arms, op), plan: new(relation))}
  end

  def analyze(:tree_string, %Proto.Relation{} = relation, opts) do
    opts = Keyword.validate!(opts, level: nil)

    arm = %Proto.AnalyzePlanRequest.TreeString{
      plan: new(relation),
      level: tree_level!(opts[:level])
    }

    {:tree_string, arm}
  end

  def analyze(:explain, %Proto.Relation{} = relation, opts) do
    opts = Keyword.validate!(opts, mode: :simple)
    mode = lookup(@explain_modes, opts[:mode], "explain mode")

    {:explain, %Proto.AnalyzePlanRequest.Explain{plan: new(relation), explain_mode: mode}}
  end

  def analyze(:same_semantics, {%Proto.Relation{} = target, %Proto.Relation{} = other}, opts) do
    Keyword.validate!(opts, [])

    arm = %Proto.AnalyzePlanRequest.SameSemantics{
      target_plan: new(target),
      other_plan: new(other)
    }

    {:same_semantics, arm}
  end

  def analyze(:persist, %Proto.Relation{} = relation, opts) do
    opts = Keyword.validate!(opts, level: :memory_and_disk_deser)

    arm = %Proto.AnalyzePlanRequest.Persist{
      relation: relation,
      storage_level: storage_level(opts[:level])
    }

    {:persist, arm}
  end

  def analyze(:unpersist, %Proto.Relation{} = relation, opts) do
    # PySpark's `blocking` defaults to false and is sent either way; the field has presence, so
    # an absent one is a different message.
    opts = Keyword.validate!(opts, blocking: false)

    arm = %Proto.AnalyzePlanRequest.Unpersist{
      relation: relation,
      blocking: flag(opts[:blocking], :blocking)
    }

    {:unpersist, arm}
  end

  def analyze(:get_storage_level, %Proto.Relation{} = relation, opts) do
    Keyword.validate!(opts, [])

    {:get_storage_level, %Proto.AnalyzePlanRequest.GetStorageLevel{relation: relation}}
  end

  def analyze(:ddl_parse, ddl, opts) when is_binary(ddl) do
    Keyword.validate!(opts, [])

    {:ddl_parse, %Proto.AnalyzePlanRequest.DDLParse{ddl_string: ddl}}
  end

  def analyze(:json_to_ddl, json, opts) when is_binary(json) do
    Keyword.validate!(opts, [])

    {:json_to_ddl, %Proto.AnalyzePlanRequest.JsonToDDL{json_string: json}}
  end

  def analyze(op, input, _opts) when op in @analyze_ops do
    raise ArgumentError, "analyze: #{inspect(op)} does not take #{inspect(input)}"
  end

  def analyze(op, _input, _opts) do
    raise ArgumentError,
          "unknown analysis #{inspect(op)}, expected one of #{inspect(@analyze_ops)}"
  end

  @doc """
  A `StorageLevel` message from one of Spark's own level names.

  `Latu.storage_level/1` reads the same table the other way, so the two cannot drift.
  """
  @spec storage_level(atom()) :: Proto.StorageLevel.t()
  def storage_level(name) do
    {disk, memory, off_heap, deserialized, replication} =
      lookup(@storage_levels, name, "storage level")

    %Proto.StorageLevel{
      use_disk: disk,
      use_memory: memory,
      use_off_heap: off_heap,
      deserialized: deserialized,
      replication: replication
    }
  end

  @doc """
  A `StorageLevel` message as flags, plus the name when Spark has one for that combination.

  The server can answer with a combination no name covers, so `:name` is `nil` rather than a
  guess.
  """
  @spec from_storage_level(Proto.StorageLevel.t()) :: map()
  def from_storage_level(%Proto.StorageLevel{} = level) do
    tuple =
      {level.use_disk, level.use_memory, level.use_off_heap, level.deserialized,
       level.replication}

    name = Enum.find_value(@storage_levels, fn {name, fields} -> fields == tuple && name end)

    %{
      name: name,
      use_disk: level.use_disk,
      use_memory: level.use_memory,
      use_off_heap: level.use_off_heap,
      deserialized: level.deserialized,
      replication: level.replication
    }
  end

  defp tree_level!(nil), do: nil
  defp tree_level!(level) when is_integer(level) and level > 0, do: level

  defp tree_level!(other) do
    raise ArgumentError, "level: a positive depth or none at all, got #{inspect(other)}"
  end

  # =============================================
  # Ids
  # =============================================

  @doc """
  A fresh `plan_id`, from the one allocator.

  Public so a `Relation` arm built outside Latu — `latu_ml`'s `MlRelation` — draws its ids from
  the same monotonic sequence rather than starting a second one that collides with this.
  `normalize_ids/1` renumbers whatever it finds, so an out-of-tree node normalises like any
  other. See docs/decisions.md on what Latu owes `latu_ml`.
  """
  @spec plan_id() :: pos_integer()
  def plan_id, do: :erlang.unique_integer([:positive, :monotonic])

  @doc """
  Renumber `plan_id`s depth-first from 0, remapping column references to match.

  Ids come from `:erlang.unique_integer/1`, so no two runs build the same tree. Golden fixtures
  are numbered from 0, so tests normalise first. Mirrors `normalize_plan_ids` in
  `dev/pyspark_oracle.py`; keep the two in step.

  Takes any protobuf message holding relations: a `Relation`, a `Command`, or an `AnalyzePlan`
  request arm.
  """
  @spec normalize_ids(struct()) :: struct()
  def normalize_ids(%{__protobuf__: true} = message), do: normalize_tree(message)

  defp normalize_tree(tree) do
    {tree, {mapping, _next}} = walk(tree, {%{}, 0}, &renumber/2)
    {tree, _mapping} = walk(tree, mapping, &remap/2)

    normalize_lambda_names(tree)
  end

  # Lambda variables carry a unique suffix, so the same plan built twice differs by name.
  # Renumbering is by **creation order, which the suffix already records**, not by traversal
  # order — deliberately, so this and `normalize_lambda_names` in `dev/pyspark_oracle.py` cannot
  # drift on the order they happen to visit nodes in.
  defp normalize_lambda_names(relation) do
    {_relation, names} = walk(relation, MapSet.new(), &collect_lambda_name/2)

    mapping =
      names
      |> Enum.filter(&created_at/1)
      |> Enum.sort_by(&created_at/1)
      |> Enum.with_index()
      |> Map.new(fn {name, index} -> {name, Regex.replace(~r/_\d+$/, name, "_#{index}")} end)

    {relation, _mapping} = walk(relation, mapping, &rename_lambda_name/2)

    relation
  end

  defp created_at(name) do
    case Regex.run(~r/_(\d+)$/, name) do
      [_match, digits] -> String.to_integer(digits)
      nil -> nil
    end
  end

  defp collect_lambda_name(%Proto.Expression.UnresolvedNamedLambdaVariable{} = message, names) do
    {message, Enum.into(message.name_parts, names)}
  end

  defp collect_lambda_name(message, acc), do: {message, acc}

  @doc """
  The `observe` names a plan carries, innermost first.

  Pure — it reads the tree Latu built, with no round trip. `Latu.DataFrame` uses it to refuse
  an action that would run an observed plan and then drop its metrics on the floor, which is
  the one thing a partially-observed API can get silently wrong.

  Goes through the same tree walk every id pass uses, which rebuilds the tree to read it. A
  wasted allocation per action, deliberately accepted: two traversals with the same rules drift,
  and the cost is nothing beside the RPC that follows.
  """
  @spec observed_names(struct()) :: [String.t()]
  def observed_names(%{__protobuf__: true} = message) do
    {_message, names} = walk(message, [], &collect_observed_name/2)

    Enum.reverse(names)
  end

  defp collect_observed_name(%Proto.CollectMetrics{} = message, names) do
    {message, [message.name | names]}
  end

  defp collect_observed_name(message, acc), do: {message, acc}

  # One clause covers both the declaration in `LambdaFunction.arguments` and every use in its
  # body: they are the same message.
  defp rename_lambda_name(%Proto.Expression.UnresolvedNamedLambdaVariable{} = message, mapping) do
    {%{message | name_parts: Enum.map(message.name_parts, &Map.get(mapping, &1, &1))}, mapping}
  end

  defp rename_lambda_name(message, acc), do: {message, acc}

  defp renumber(%Proto.Relation{common: %{plan_id: old}} = relation, {mapping, next})
       when is_integer(old) do
    {put_in(relation.common.plan_id, next), {Map.put(mapping, old, next), next + 1}}
  end

  defp renumber(message, acc), do: {message, acc}

  # A RelationCommon's plan_id is the id itself, renumbered by the first pass. Every other
  # plan_id in the tree is a reference to one.
  defp remap(%Proto.RelationCommon{} = message, mapping), do: {message, mapping}

  defp remap(%{plan_id: old} = message, mapping) when is_integer(old) do
    {%{message | plan_id: Map.get(mapping, old, @unresolved)}, mapping}
  end

  defp remap(message, acc), do: {message, acc}

  # `WithRelations` before the generic clause, because a struct decomposes into a map ordered
  # by term order and `references` sorts before `root` — which would number a hoisted relation
  # before the plan it belongs to, where the oracle's `ListFields` walks in field order and
  # numbers the root first. Add a clause here if another message ever holds two relations whose
  # names sort against their field numbers.
  defp walk(%Proto.WithRelations{} = message, acc, fun) do
    {root, acc} = walk(message.root, acc, fun)
    {references, acc} = walk(message.references, acc, fun)

    fun.(%{message | root: root, references: references}, acc)
  end

  # The same hazard, second instance: term order here is (other_plan, target_plan), the reverse
  # of the field order the oracle walks.
  defp walk(%Proto.AnalyzePlanRequest.SameSemantics{} = message, acc, fun) do
    {target, acc} = walk(message.target_plan, acc, fun)
    {other, acc} = walk(message.other_plan, acc, fun)

    fun.(%{message | target_plan: target, other_plan: other}, acc)
  end

  # Post-order, so children are numbered before the node holding them. `__protobuf__` is the
  # marker every generated message struct carries.
  defp walk(%{__protobuf__: true} = message, acc, fun) do
    {fields, acc} =
      message
      |> Map.from_struct()
      |> Enum.map_reduce(acc, fn {key, value}, acc ->
        {value, acc} = walk(value, acc, fun)
        {{key, value}, acc}
      end)

    fun.(struct(message, fields), acc)
  end

  defp walk(list, acc, fun) when is_list(list), do: Enum.map_reduce(list, acc, &walk(&1, &2, fun))

  defp walk({tag, value}, acc, fun) when is_atom(tag) do
    {value, acc} = walk(value, acc, fun)
    {{tag, value}, acc}
  end

  defp walk(%{} = map, acc, fun) do
    {pairs, acc} = map |> Map.to_list() |> Enum.map_reduce(acc, &walk(&1, &2, fun))
    {Map.new(pairs), acc}
  end

  defp walk(other, acc, _fun), do: {other, acc}
end
