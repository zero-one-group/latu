# Runnable example. With a server up:
#
#     docker compose up -d spark-connect
#     mix run dev/example.exs
#
# Or, to keep the bindings and poke at them:
#
#     iex -S mix
#     iex> import_file "dev/example.exs"
#
# Deliberately does not disconnect, so the REPL keeps a live `session`.

import Latu.Column

# The function library is aliased, not imported: Spark's names are kept exactly, and `quote`
# is one of them. `F.` costs nothing and is PySpark's idiom.
alias Latu.Functions, as: F
alias Latu.Window, as: W

session = Latu.connect!("sc://localhost:15002")

IO.puts("connected to Spark #{Latu.spark_version!(session)}")

df = Latu.range(session, 5)

Latu.show(df)
Latu.show(df, num_rows: 2, vertical: true)

# A frame inspects as its plan's spine, which costs no round trip: what you piped, not what
# the schema is. `to_html!/2` is the same table Spark renders for a notebook.
IO.inspect(df |> Latu.filter(greater(:id, 1)) |> Latu.limit(2), label: "plan")
IO.puts(String.slice(Latu.to_html!(df, num_rows: 2), 0, 60) <> "...")

# `glimpse/2` transposes it: one line per column, so a wide frame stays readable where `show/2`
# wraps. `Rows:` is exact only when the sample proved it — `count: true` pays for the real one.
session
|> Latu.range(3)
|> Latu.select(id: :id, label: F.lpad(cast(:id, "string"), 3, "0"), root: F.round(F.sqrt(:id), 3))
|> Latu.glimpse()

# A string is a column name in `select` and SQL in `filter`; inside an expression it is a
# literal. Expressions are values, so this one can be named and reused.
big = greater(:id, 2)

session
|> Latu.range(10)
|> Latu.filter(all([big, not_equal(:id, 5)]))
|> Latu.select([:id, doubled: multiply(:id, 2)])
|> Latu.show()

session
|> Latu.range(10)
|> Latu.filter("id > 7")
|> Latu.select("*")
|> Latu.show()

session
|> Latu.range(20)
|> Latu.with_columns(bucket: remainder(:id, 3))
|> Latu.distinct([:bucket])
|> Latu.rename(bucket: :b)
|> Latu.drop(:id)
|> Latu.limit(2)
|> Latu.show()

# `asc` puts nulls first and `desc` puts them last, as SQL and PySpark do.
session
|> Latu.range(20)
|> Latu.sample(0.5, seed: 42)
|> Latu.sort(desc(:id))
|> Latu.limit(3)
|> Latu.show()

# `union` keeps duplicates and `except` does not, matching Spark; `all:` flips either.
evens = session |> Latu.range(0, 10, 2)
threes = session |> Latu.range(0, 10, 3)

evens |> Latu.union(threes) |> Latu.sort(:id) |> Latu.show()
evens |> Latu.except(threes) |> Latu.sort(:id) |> Latu.show()

# `group_by` builds nothing on its own: Spark has one Aggregate relation, so the plan appears
# when `agg` or `count` is called.
session
|> Latu.range(20)
|> Latu.with_columns(bucket: remainder(:id, 3))
|> Latu.group_by(:bucket)
|> Latu.agg(n: F.count(:id), distinct_ids: F.count_distinct(:id), total: F.sum(:id))
|> Latu.sort(:bucket)
|> Latu.show()

# A variadic Spark function takes a list here, and an optional trailing argument is a second
# clause rather than a default — `round/1` and `round/2` send different plans.
session
|> Latu.range(5)
|> Latu.select(
  id: :id,
  label: F.when_(greater(:id, 2), "big") |> F.otherwise("small"),
  padded: F.concat_ws("-", [F.upper("row"), :id]),
  approx: F.round(F.sqrt(:id), 3),
  pick: F.coalesce([:id, 0])
)
|> Latu.show()

# A window is client-side: Spark has no window relation, so `over/2` folds the specification
# into an ordinary projection. A row frame counts rows; a range frame counts values of the
# ordering column, and Spark sends their offsets at different widths.
by_bucket = W.partition_by([:bucket]) |> W.order_by([desc(:id)])
running = by_bucket |> W.rows_between(:unbounded_preceding, :current_row)

session
|> Latu.range(12)
|> Latu.with_columns(bucket: remainder(:id, 3))
|> Latu.with_columns(rn: over(F.row_number(), by_bucket), total: over(F.sum(:id), running))
|> Latu.sort([:bucket, :id])
|> Latu.show()

# A higher-order function takes an ordinary Elixir anonymous function. Spark names the lambda
# parameters by position, so what you call yours is your business.
session
|> Latu.range(1)
|> Latu.select(
  doubled: F.transform(F.array([1, 2, 3]), fn x -> multiply(x, 2) end),
  big: F.filter(F.array([1, 2, 3]), fn x -> greater(x, 1) end),
  indexed: F.transform(F.array([10, 20, 30]), fn x, i -> add(x, i) end),
  total: F.aggregate(F.array([1, 2, 3]), 0, fn acc, x -> add(acc, x) end)
)
|> Latu.show()

# For a function with no wrapper yet, `Latu.Column.fun/3` is the escape hatch.
session
|> Latu.range(3)
|> Latu.select(s: fun("soundex", [F.upper("smith")]))
|> Latu.show()

# Results come out as Elixir data. `collect` gives maps with atom keys; `count`, `take` and
# `first` are the actions PySpark makes them; `to_explorer` hands the columns to Explorer
# whole, bounded at 100k rows unless told otherwise.
{:ok, rows} = session |> Latu.range(3) |> Latu.collect()
IO.inspect(rows, label: "collect")

{:ok, n} = session |> Latu.range(10) |> Latu.count()
IO.puts("count: #{n}")

{:ok, first_even} = session |> Latu.range(0, 10, 2) |> Latu.first()
IO.inspect(first_even, label: "first")

{:ok, frame} =
  session
  |> Latu.range(1000)
  |> Latu.with_columns(sq: pow(:id, 2))
  |> Latu.to_explorer()

IO.inspect(Explorer.DataFrame.n_rows(frame), label: "to_explorer rows")

# A result too large to hold streams as one Explorer frame per Arrow batch, and stopping
# early releases the execution on the server.
total =
  session
  |> Latu.range(100_000)
  |> Latu.stream()
  |> Stream.map(&Explorer.DataFrame.n_rows/1)
  |> Enum.sum()

IO.puts("streamed rows: #{total}")

# Reading is one call, not a builder chain: `:format`, `:schema` and `:path` are Latu's keys,
# everything else is a reader option — snake_case atoms become Spark's camelCase. The schema is
# a string the server parses. Data files: dev/.venv/bin/python dev/make_data_fixtures.py (once),
# and the containers mount ./fixtures at /fixtures.
session
|> Latu.read(
  format: "csv",
  schema: "id INT, name STRING",
  path: "/fixtures/people.csv",
  header: true
)
|> Latu.filter(greater(:id, 1))
|> Latu.show()

# The same schema-and-options rules parse columns in place.
session
|> Latu.range(1)
|> Latu.select(j: F.to_json(F.struct([:id])))
|> Latu.with_columns(back: F.from_json(:j, "id INT"))
|> Latu.show(truncate: false)

# Writing is an action — one call per PySpark terminal method, returning :ok. The path is the
# server's filesystem, so /tmp here is inside the container.
out = "/tmp/latu_example/out"

session
|> Latu.range(5)
|> Latu.write!(format: "parquet", path: out, mode: :overwrite)

session
|> Latu.read(format: "parquet", path: out)
|> Latu.sort(:id)
|> Latu.show()

# SQL runs eagerly — DDL works — and binds args as literals, a list for `?` and a map for
# `:name`. The DataFrame that comes back queries the result, not the query again.
session
|> Latu.sql!("SELECT id, id * :factor AS scaled FROM range(5) WHERE id > :min", %{
  factor: 10,
  min: 1
})
|> Latu.show()

# A temp view makes any DataFrame reachable from SQL; the catalog answers questions about it.
session
|> Latu.range(20)
|> Latu.with_columns(bucket: remainder(:id, 3))
|> Latu.create_temp_view!("buckets", replace: true)

session
|> Latu.sql!("SELECT bucket, count(*) AS n FROM buckets GROUP BY bucket ORDER BY bucket")
|> Latu.show()

# A view needs no registration when the query is the only place it is used: name the frame in
# `views:` and write that name in the SQL.
session
|> Latu.sql!("SELECT bucket, count(*) AS n FROM raw GROUP BY bucket ORDER BY bucket",
  views: [raw: Latu.with_columns(Latu.range(session, 20), bucket: remainder(:id, 3))]
)
|> Latu.show()

IO.inspect(Latu.Catalog.list_tables!(session, pattern: "buckets"), label: "list_tables")
IO.inspect(Latu.Catalog.drop_temp_view!(session, "buckets"), label: "drop_temp_view")

# A subquery is how one DataFrame reaches into another: the frame is hoisted into the plan
# that uses it, so this is one query. `scalar/1` is a value, `exists/1` a predicate, and
# `isin/2` over a frame is an IN subquery.
busiest = session |> Latu.range(10) |> Latu.agg(m: F.max(:id))

session
|> Latu.range(10)
|> Latu.filter(less(:id, Latu.scalar(busiest)))
|> Latu.select([:id, top: Latu.scalar(busiest)])
|> Latu.show()

session
|> Latu.range(5)
|> Latu.filter(isin(:id, Latu.range(session, 2)))
|> Latu.show()

# A frame can say what it is without running: AnalyzePlan rather than ExecutePlan. The schema
# comes back as data, with Spark's own name for each type; the tree is Spark's own rendering.
described = Latu.select(Latu.range(session, 5), id: :id, label: F.concat([lit("row-"), :id]))

IO.inspect(Latu.schema!(described), label: "schema")
IO.inspect(Latu.dtypes!(described), label: "dtypes")
Latu.print_schema!(described)

# The rest of the analysis surface: what Spark would run, what the frame reads, whether two
# frames mean the same thing, and caching — which over Connect is a round trip, so the bang
# form hands the frame back and pipes.
Latu.explain!(described, mode: :formatted)
IO.inspect(Latu.is_empty!(described), label: "is_empty")
IO.inspect(Latu.same_semantics!(described, described), label: "same_semantics")
IO.inspect(Latu.parse_ddl!(session, "id INT, tags ARRAY<STRING>"), label: "parse_ddl")

cached = Latu.cache!(described)
IO.inspect(Latu.storage_level!(cached), label: "storage_level")
IO.inspect(cached |> Latu.unpersist!() |> Latu.count!(), label: "count after unpersist")

# Missing data. The type is the filter, not an error: filling a string column with a number
# does nothing at all, which is Spark's rule and the reason to name columns when you mean it.
measurements =
  Latu.create_dataframe!(
    session,
    [
      %{id: 1, score: 10.0, team: "red"},
      %{id: 2, score: nil, team: "blue"},
      %{id: 3, score: nil, team: nil}
    ],
    schema: "id INT, score DOUBLE, team STRING"
  )

measurements |> Latu.drop_na() |> Latu.show()
measurements |> Latu.drop_na(min_non_nulls: 2) |> Latu.show()
measurements |> Latu.fill_na(score: 0.0, team: "unknown") |> Latu.show()
measurements |> Latu.replace([{"red", "crimson"}], subset: [:team]) |> Latu.show()

# The reshaping verbs. `unpivot` needs both column names; `values:` absent means every column
# that is not an id. `transpose` and `grouping_sets` are Spark 4's own; the empty grouping set
# is the grand total.
wide =
  Latu.create_dataframe!(
    session,
    [%{id: 1, jan: 10.0, feb: 20.0}, %{id: 2, jan: 30.0, feb: 40.0}],
    schema: "id INT, jan DOUBLE, feb DOUBLE"
  )

wide
|> Latu.unpivot([:id], variable_column_name: "month", value_column_name: "sales")
|> Latu.sort([:id, :month])
|> Latu.show()

measurements
|> Latu.grouping_sets([[:team], []], [:team])
|> Latu.agg(total: F.sum(:score))
|> Latu.show()

# random_split's slices partition the frame: one seed, non-overlapping windows, stable order.
[train, test] = Latu.random_split(measurements, [0.7, 0.3], seed: 42)
IO.inspect({Latu.count!(train), Latu.count!(test)}, label: "train/test")

# tail is an action, like take — Spark's Tail relation collects on the driver.
IO.inspect(Latu.tail!(Latu.range(session, 10), 3), label: "tail")

session
|> Latu.range(5)
|> Latu.hint("broadcast")
|> Latu.select_expr(["id * 2 as n"])
|> Latu.show()

# The last of the relation surface. `col_regex` picks columns by pattern (backticks are
# Spark's syntax); `_metadata` is a hidden column a file source carries; `parse` turns a frame
# of strings into a structured one, and its schema comes from the server, not a client-side
# type model.
wide = Latu.select_expr(Latu.range(session, 3), ["id", "id * 2 as id_doubled", "1 as other"])
wide |> Latu.select(Latu.col_regex(wide, "`id.*`")) |> Latu.show()

session
|> Latu.read(format: "csv", path: "/fixtures/people.csv", header: true)
|> Latu.select(path: expr("_metadata.file_path"))
|> Latu.limit(1)
|> Latu.show()

session
|> Latu.range(3)
|> Latu.select_expr([~s|concat('{"n": ', id, '}') as value|])
|> Latu.parse(format: :json)
|> Latu.show()

session
|> Latu.table_function("explode", [F.array([lit(1), lit(2)])])
|> Latu.show()

# The joins. An as-of join matches the nearest earlier row instead of an equal one — PySpark
# keeps this one private. A lateral join lets the right side reference the left by qualified
# name, which is what `as/2` is for.
quotes =
  Latu.create_dataframe!(session, [t: [1, 3, 5], price: [10.0, 30.0, 50.0]],
    schema: "t BIGINT, price DOUBLE"
  )

trades =
  Latu.create_dataframe!(session, [t: [2, 4, 6], size: [100, 200, 300]],
    schema: "t BIGINT, size INT"
  )

trades
|> Latu.join_as_of(quotes, left_as_of: :t, right_as_of: :t, tolerance: 1)
|> Latu.select([:size, :price])
|> Latu.show()

quotes
|> Latu.as("l")
|> Latu.lateral_join(Latu.filter(Latu.range(session, 10), expr("id < l.t")))
|> Latu.show()

# Observing. The frame is unchanged and still shows, joins and collects like any other; the
# metrics come back from a `*_with_metrics` action. Only a plain *write* refuses an observed
# frame, since a write would produce the metrics and drop them. This is the write-pipeline
# shape: count what you wrote without a second pass over it.
observed = Latu.observe(measurements, :quality, all: F.count(lit(1)), scored: F.count(:score))

observed |> Latu.limit(2) |> Latu.show()
{:ok, rows, info} = Latu.collect_with_metrics(observed)

IO.inspect(length(rows), label: "rows collected")
IO.inspect(info.observed, label: "observed")
IO.inspect(length(info.metrics), label: "plan nodes Spark reported metrics for")

# A checkpoint is the one thing here that allocates: the server holds the result and hands back
# a frame that reads it, so everything above the checkpoint is computed once. The bracket form
# frees it on the way out even if the function raises. `local: true` keeps it in executor
# storage, which needs no checkpoint directory on the server.
{:ok, summary} =
  measurements
  |> Latu.filter(Latu.Column.is_not_null(:score))
  |> Latu.with_checkpoint([local: true], fn scored ->
    %{rows: Latu.count!(scored), best: Latu.collect!(Latu.agg(scored, top: F.max(:score)))}
  end)

IO.inspect(summary, label: "over one materialisation")

# Local data goes the other way: collect's inverse. Rows, column data or an Explorer frame
# ship as Arrow; a `schema:` string casts server-side. Past the server's 64 MiB threshold the
# data is cached as session artifacts instead — same call, no code change.
session
|> Latu.create_dataframe!([%{id: 1, name: "Ada"}, %{id: 2, name: "Grace"}],
  schema: "id INT, name STRING"
)
|> Latu.filter(greater(:id, 1))
|> Latu.show()

# Watching a long query. The server reports on a timer — default two seconds, so anything
# quicker reports nothing — and the handler runs in this process, between batches.
measurements
|> Latu.count(progress: fn p -> IO.write("\r#{Latu.Progress.percent(p)}% ") end)
|> IO.inspect(label: "\ncounted")

# An error carries Spark's own structured detail, unpacked from the gRPC trailers with no
# extra round trip: the error class is what Spark's documentation is indexed by.
{:error, refused} = session |> Latu.range(5) |> Latu.select(:nope) |> Latu.collect()
IO.inspect({refused.error_class, refused.sql_state}, label: "refused")

# A clone is a second session on the same channel, isolated from this one: register whatever
# you like in it and release it. `release_session/1` ends a session without closing the
# transport, which is what makes that cheap. `disconnect/1` only closes the channel — pass
# `release: true` if you also want the session gone.
scratch = Latu.clone_session!(session)
scratch |> Latu.range(3) |> Latu.create_temp_view!("scratch_rows")
IO.inspect(Latu.Catalog.table_exists!(session, "scratch_rows"), label: "visible to the parent")
Latu.release_session!(scratch)

# Tag a session and every execution built from it carries the tag, so a runaway query can be
# cancelled from another process by name rather than by a handle nobody kept.
IO.inspect(Latu.interrupt!(Latu.Session.add_tag(session, "example")), label: "interrupted")

# Telemetry: plain function calls, so a handler runs in this process. The names follow
# SparkEx's; `[:latu, :execute, :stop]` is the one that times the whole query, because the rpc
# span around ExecutePlan only measures opening the stream.
:telemetry.attach_many(
  "latu-example",
  [[:latu, :execute, :stop], [:latu, :result, :batch]],
  fn event, measurements, _metadata, _config ->
    IO.puts("  #{inspect(event)} #{inspect(measurements)}")
  end,
  nil
)

IO.puts("telemetry:")
Latu.count!(Latu.range(session, 1_000))
:telemetry.detach("latu-example")

# Config, both ways. Three reads that differ where it matters: `conf/2` is Map.get — the set
# value, else Spark's own default, else nil; `fetch_conf/2` is Map.fetch, an error for a key
# Spark does not define; and `conf/3` overrides Spark's default rather than following it.
# `confs/2` is only what the session has *set*, so a conf sitting at its default is not in it.
IO.inspect(Latu.conf!(session, "spark.sql.session.timeZone"), label: "timezone")
IO.inspect(Latu.conf!(session, "latu.example.nope"), label: "never set")
IO.inspect(Map.keys(Latu.confs!(session, prefix: "spark.sql.")), label: "set, under spark.sql.")

# Setting takes an integer, boolean or atom and returns :ok — the conf lives on the server.
# `is_modifiable/2` asks first — false means Spark does not define the key, or it is static
# and cannot be changed at all.
:ok = Latu.set_conf!(session, "spark.sql.shuffle.partitions", 8)
IO.inspect(Latu.conf!(session, "spark.sql.shuffle.partitions"), label: "partitions")
IO.inspect(Latu.is_modifiable!(session, "spark.sql.warehouse.dir"), label: "static, so")

# The tuning knobs are session fields and `connect/2` options: the retry policy (PySpark's own
# numbers by default), the HTTP/2 window and the keepalive pair. A result is bounded in the plan,
# with `limit/2`, never by the action.
brisk = Latu.connect!("sc://localhost:15002", retry: [max_retries: 3])
{:ok, ten} = brisk |> Latu.range(100) |> Latu.limit(10) |> Latu.to_explorer()
IO.inspect(Explorer.DataFrame.n_rows(ten), label: "bounded in the plan")
Latu.disconnect!(brisk)

# `range` takes Spark's partition count as a keyword, on any arity.
session
|> Latu.range(0, 12, 1, num_partitions: 3)
|> Latu.select(p: F.spark_partition_id())
|> Latu.distinct()
|> Latu.show()

df
