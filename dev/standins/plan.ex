defmodule Latu.Plan do
  @moduledoc """
  Stand-in for `lib/latu/plan.ex`, for `dev/check_offline.exs` only. Never compiled into Latu.

  The real module builds protobuf structs, and `:protobuf` needs Elixir 1.15+ while the offline
  container has 1.14. So the layers above it are compiled against this instead, which is enough
  to catch a bad `def`, a wrong arity or a coercion that never fires.

  Two rules keep it honest, both enforced by the runner:

    * it must export everything the real module exports, at every arity;
    * its types must carry the same protocol implementations the real ones do. A stand-in
      without them lets a test pass here and fail under `mix`.
  """

  defmodule Expression do
    @moduledoc false
    defstruct [:form]
  end

  defmodule SortOrder do
    @moduledoc false
    defstruct [:form]
  end

  defmodule Relation do
    @moduledoc false
    defstruct [:form]
  end

  @type expression :: Expression.t()
  @type sort_order :: SortOrder.t()
  @type relation :: Relation.t()

  defp expression(form), do: %Expression{form: form}
  # Public in the real module (`latu_ml` needs the one allocator), so public here: the runner
  # checks the stand-in exports everything the source does.
  def relation(form), do: %Relation{form: form}

  # --- relations ---------------------------------------------------------------------------
  def new(input), do: {:plan, input}
  def range(start, stop, step, n \\ nil), do: relation({:range, start, stop, step, n})
  def read(opts \\ []), do: relation({:read, Keyword.put_new(opts, :schema, "")})
  def table(name, options \\ []), do: relation({:table, name, options})
  def write(input, opts \\ []), do: {:command, {:write, input, opts}}
  def write_v2(input, table, opts \\ []), do: {:command, {:write_v2, input, table, opts}}
  def sql(query, args \\ [], views \\ []), do: relation({:sql, query, args, views})

  def sql_command(query, args \\ [], views \\ []) do
    {:command, {:sql_command, query, args, views}}
  end

  def create_view(input, name, opts \\ []), do: {:command, {:create_view, input, name, opts}}
  def catalog(op, fields \\ []), do: relation({:catalog, op, fields})
  def analyze(op, relation, opts \\ []), do: {op, {:analyze, relation, opts}}
  def storage_level(name), do: {:storage_level, name}
  def na_fill(input, cols, values), do: relation({:fill_na, input, cols, values})
  def na_drop(input, cols, min_non_nulls), do: relation({:drop_na, input, cols, min_non_nulls})
  def na_replace(input, cols, pairs), do: relation({:replace, input, cols, pairs})
  def summary(input, statistics \\ []), do: relation({:summary, input, statistics})
  def describe(input, cols \\ []), do: relation({:describe, input, cols})
  def crosstab(input, col1, col2), do: relation({:crosstab, input, col1, col2})
  def freq_items(input, cols, support \\ 0.01), do: relation({:freq_items, input, cols, support})
  def cov(input, col1, col2), do: relation({:cov, input, col1, col2})

  def corr(input, col1, col2, method \\ :pearson),
    do: relation({:corr, input, col1, col2, method})

  def sample_by(input, col, fractions, opts \\ []) do
    relation({:sample_by, input, col, fractions, opts})
  end

  def approx_quantile(input, cols, probabilities, error) do
    relation({:approx_quantile, input, cols, probabilities, error})
  end

  def from_storage_level(_level), do: %{name: nil}
  def adopt(relation), do: relation
  def local_relation(data, schema \\ nil), do: relation({:local_relation, data, schema})

  def chunked_cached_local_relation(hashes, schema_hash \\ nil) do
    relation({:chunked_cached_local_relation, hashes, schema_hash})
  end

  def show_string(input, opts \\ []), do: relation({:show_string, input, opts})
  def html_string(input, opts \\ []), do: relation({:html_string, input, opts})
  def project(input, expressions), do: relation({:project, input, expressions})
  def with_columns(input, columns), do: relation({:with_columns, input, columns})
  def drop(input, columns), do: relation({:drop, input, columns})
  def collect_metrics(input, name, ms), do: relation({:collect_metrics, input, name, ms})
  def tail(input, count), do: relation({:tail, input, count})
  def hint(input, name, params \\ []), do: relation({:hint, input, name, params})
  def unpivot(input, ids, opts), do: relation({:unpivot, input, ids, opts})
  def transpose(input, index \\ nil), do: relation({:transpose, input, index})
  def to_schema(input, schema), do: relation({:to_schema, input, schema})
  def as_of_join(left, right, opts), do: relation({:as_of_join, left, right, opts})
  def col_regex(pattern, input), do: expression({:col_regex, pattern, input})
  def metadata_column(name, input), do: expression({:metadata_column, name, input})
  def with_metadata(input, name, meta), do: relation({:with_metadata, input, name, meta})
  def repartition_by_range(input, orders, n \\ nil), do: relation({:by_range, input, orders, n})
  def parse(input, opts), do: relation({:parse, input, opts})
  def table_function(name, args \\ []), do: relation({:table_function, name, args})
  def table_changes(table, opts \\ []), do: relation({:table_changes, table, opts})
  def cached_remote_relation(id), do: relation({:cached_remote_relation, id})
  def cached_relation_id(%Relation{form: {:cached_remote_relation, id}}), do: {:ok, id}
  def cached_relation_id(%Relation{}), do: :error
  def checkpoint(input, opts \\ []), do: relation({:checkpoint, input, opts})
  def remove_cached_relation(id), do: relation({:remove_cached_relation, id})
  def merge_action(clause, action, opts \\ []), do: {:merge_action, clause, action, opts}
  def merge_condition(condition), do: {:merge_condition, condition}

  def merge_into(source, table, condition, opts \\ []) do
    relation({:merge_into, source, table, condition, opts})
  end

  def lateral_join(left, right, opts \\ []), do: relation({:lateral_join, left, right, opts})

  def nearest_by_join(left, right, ranking, opts) do
    relation({:nearest_by_join, left, right, ranking, opts})
  end

  def to_df(input, names), do: relation({:to_df, input, names})
  def with_columns_renamed(input, pairs), do: relation({:renamed, input, pairs})
  def filter(input, condition), do: relation({:filter, input, condition})
  def limit(input, count), do: relation({:limit, input, count})
  def offset(input, count), do: relation({:offset, input, count})
  def deduplicate(input, columns), do: relation({:deduplicate, input, columns})
  def sort(input, orders, opts \\ []), do: relation({:sort, input, orders, opts})
  def sample(input, fraction, opts \\ []), do: relation({:sample, input, fraction, opts})
  def repartition(input, count, opts \\ []), do: relation({:repartition, input, count, opts})

  def repartition_by(input, expressions, count \\ nil) do
    relation({:repartition_by, input, expressions, count})
  end

  def aggregate(input, opts \\ []), do: relation({:aggregate, input, opts})
  def set_op(kind, left, right, opts \\ []), do: relation({:set_op, kind, left, right, opts})
  def join(left, right, opts \\ []), do: relation({:join, left, right, opts})

  # --- expressions -------------------------------------------------------------------------
  def as(%Relation{} = input, name), do: relation({:as, input, name})
  def as(expr, name), do: expression({:alias, expr, name})
  def col(name), do: expression({:col, name})
  def col(name, %Relation{} = input), do: expression({:col, name, input})
  def star, do: expression({:star})
  def expr(sql), do: expression({:sql, sql})
  def fun(name, arguments, opts \\ []), do: expression({:fun, name, arguments, opts})
  def lit(value), do: expression({:lit, value})
  def cast(column, type), do: expression({:cast, column, type})
  def try_cast(column, type), do: expression({:try_cast, column, type})
  def over(function, window), do: expression({:over, function, window})

  # Enough of the real carrier to keep the layers above honest: a subquery is a Latu.Subquery,
  # and every builder that takes expressions passes one through.
  def subquery(referenced, type, opts \\ []) do
    Latu.Subquery.wrap(expression({:subquery, referenced, type, opts}), [referenced])
  end

  def lambda(fun) when is_function(fun) do
    {:arity, arity} = Function.info(fun, :arity)

    unless arity in 1..3 do
      raise ArgumentError, "a Spark lambda takes one to three arguments, not #{arity}"
    end

    variables = ~w(x y z) |> Enum.take(arity) |> Enum.map(&expression({:lambda_var, &1}))

    expression({:lambda, apply(fun, variables), variables})
  end

  def higher_order(name, columns, funs) do
    fun(name, columns ++ Enum.map(funs, &lambda/1))
  end

  # --- coercions ---------------------------------------------------------------------------
  def to_expr(%Expression{} = expr), do: expr
  def to_expr(%Latu.Subquery{} = subquery), do: subquery
  def to_expr(%SortOrder{}), do: raise(ArgumentError, "a sort key belongs in sort/2")
  def to_expr(%Latu.CaseWhen{} = chain), do: fun("when", Latu.CaseWhen.arguments(chain))
  def to_expr(value) when is_nil(value) or is_boolean(value), do: lit(value)
  def to_expr(value) when is_atom(value), do: col(value)
  def to_expr(value), do: lit(value)

  def to_name(%Latu.Subquery{} = subquery), do: subquery
  def to_name(name), do: name

  # Faithful copy of the real coercion (pure, proto-free), so `options_arg` runs here for real.
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

  def to_projections(columns) when is_list(columns), do: Enum.map(columns, &to_projection/1)
  def to_projections(column), do: to_projections([column])

  defp to_projection({name, value}) when is_atom(name) and not is_nil(name) do
    as(to_expr(value), name)
  end

  defp to_projection(value), do: to_expr(value)

  def sort_order(column, opts \\ []), do: %SortOrder{form: {column, opts}}
  def to_sort_order(%SortOrder{} = order), do: order
  def to_sort_order(%Latu.Subquery{expr: %SortOrder{}} = key), do: key
  def to_sort_order(column), do: sort_order(column)

  def call_function(name, arguments), do: expression({:call_function, name, arguments})

  def random_seed, do: 42

  def normalize_ids(input), do: input
  def plan_id, do: :erlang.unique_integer([:positive, :monotonic])
  def observed_names(_message), do: []
end

defmodule Latu.Plan.Inspect do
  @moduledoc """
  Stand-in for `lib/latu/plan/inspect.ex`.

  Two functions are used from outside: `describe/1`, which `Latu.GroupedData`'s own `Inspect`
  impl calls — the call that made this module necessary rather than optional — and `chain/2`,
  which `Latu.DataFrame`'s does. The real `chain/2` walks a `Proto.Relation` spine and cannot
  run here; this one names the stand-in relation's own form and no more.
  """

  def chain(relation, opts \\ %Inspect.Opts{})

  def chain(%Latu.Plan.Relation{form: form}, _opts) when is_tuple(form) do
    form |> elem(0) |> to_string()
  end

  def chain(other, _opts), do: Kernel.inspect(other)

  def describe(%Latu.Plan.Expression{}), do: "<expression>"
  def describe(%Latu.Plan.SortOrder{}), do: "<sort key>"
  def describe(nil), do: "?"
  def describe(other), do: Kernel.inspect(other)
end

# The real Expression and SortOrder are rendered by Latu.Plan.Inspect, so their contents never
# appear in inspect/2's output. A stand-in without these impls lies about that.
defimpl Inspect, for: Latu.Plan.Expression do
  def inspect(_expression, _opts), do: "#Latu.Expression<stand-in>"
end

defimpl Inspect, for: Latu.Plan.SortOrder do
  def inspect(_order, _opts), do: "#Latu.SortKey<stand-in>"
end
