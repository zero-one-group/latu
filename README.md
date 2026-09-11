<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/zero-one-group/latu/main/assets/latu-lockup-dark.svg">
    <img alt="Latu" src="https://raw.githubusercontent.com/zero-one-group/latu/main/assets/latu-lockup.svg" width="360">
  </picture>
</p>

<p align="center">
  <a href="https://hex.pm/packages/latu"><img alt="Hex version" src="https://img.shields.io/hexpm/v/latu.svg"></a>
  <a href="https://hexdocs.pm/latu"><img alt="Hexdocs" src="https://img.shields.io/badge/hex-docs-blue.svg"></a>
  <a href="https://github.com/zero-one-group/latu/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/hexpm/l/latu.svg"></a>
  <a href="https://livebook.dev/run?url=https%3A%2F%2Fgithub.com%2Fzero-one-group%2Flatu%2Fblob%2Fmain%2Fnotebooks%2Fquick_start.livemd"><img alt="Run in Livebook" src="https://livebook.dev/badge/v1/blue.svg"></a>
</p>

A native Elixir DataFrame API for Apache Spark, over [Spark
Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html).

Latu builds a query plan on your machine and Spark runs it. There is no JVM in your project and
no cluster on your laptop. A session is a plain struct wrapping a gRPC channel.

`latu` is Javanese for *spark*; [`geni`](https://github.com/zero-one-group/geni), its Clojure
predecessor, is Javanese for *fire*.

## Install

```elixir
def deps do
  [{:latu, "~> 0.7"}]
end
```

Requires Elixir 1.18 or newer and a Spark **4.2.0** Connect server. Another Spark:
[Spark versions](https://hexdocs.pm/latu/spark-versions.html).

## A server to talk to

Point at your cluster's `sc://` URL, or run one locally:

```bash
docker run -p 15002:15002 apache/spark:4.2.0 /opt/spark/sbin/start-connect-server.sh --wait
```

## The three lines at the top of your file

`Latu` is called qualified, the way `Enum` is. `Latu.Column` is small and gets composed by hand,
so it is imported. The other two are aliased, as in PySpark, because their names collide with the
verbs on purpose. `Latu.count/1` counts a DataFrame; `F.count/1` is the aggregate.

```elixir
import Latu.Column              # operators, predicates, casts, sort keys, over/2
alias Latu.Functions, as: F     # Spark's ~500 functions, under Spark's own names
alias Latu.Window, as: W        # window specifications
```

## A first pipeline

```elixir
{:ok, session} = Latu.connect("sc://localhost:15002")

session
|> Latu.range(10)
|> Latu.filter(all([greater(:id, 2), not_equal(:id, 5)]))
|> Latu.with_columns(doubled: multiply(:id, 2))
|> Latu.distinct([:doubled])
|> Latu.rename(id: :n)
|> Latu.select([:n, :doubled])
|> Latu.limit(3)
|> Latu.show()
```

`show` prints the table Spark renders, not one Latu formats, so the output matches PySpark byte
for byte. This page shows the shape of the API. The [quick
start](https://hexdocs.pm/latu/quick-start.html) shows results, and every line of it is executed
by the test suite.

## What this is

A slim Spark Connect client with a DataFrame API designed for Elixir rather than transliterated
from PySpark. Two things are load-bearing.

**Ergonomics over fidelity.** Where Spark's DataFrame API and idiomatic Elixir disagree, Elixir
wins. Aggressive coercion, no mandatory `col/1`, `show` prints to stdout, keyword lists for
aliased projections.

**No runtime ownership.** Latu defines no GenServer, supervisor, registry or pool, and declares
no application callback module. Adding it to your deps starts nothing. `%Latu.Session{}` is a
struct, and you decide where it lives. The one process Latu causes to exist is the gRPC channel
that `connect/2` opens and `disconnect/2` closes. The one server-side resource it allocates is a
checkpoint, which is why `release/1` exists.

A string is a column name in `select` and SQL in `filter`, which is PySpark's rule, so
`Latu.filter(df, "id > 3")` works too. Inside an expression a string is a literal:
`equal(:suburb, "Reservoir")` compares a column to text.

Results come out as Elixir data:

```elixir
{:ok, rows} = df |> Latu.limit(2) |> Latu.collect()
#=> {:ok, [%{id: 0}, %{id: 1}]}

{:ok, n} = Latu.count(df)
{:ok, frame} = Latu.to_explorer(df)            # unbounded, like toPandas: bound the plan
df |> Latu.stream() |> Enum.each(&handle/1)    # lazy: one Explorer frame per Arrow batch
```

A schema comes back as data, under Spark's own name for each type. There is no client-side type
model in either direction.

```elixir
{:ok, fields} = Latu.schema(df)
#=> {:ok, [%{name: "id", type: "bigint", nullable: false}]}

Latu.dtypes!(df)       #=> [{"id", "bigint"}]
Latu.print_schema!(df) # root
                       #  |-- id: long (nullable = false)
```

Reading is one call, not a builder chain. The schema is a string the server parses, and
snake_case option keys become Spark's camelCase (`infer_schema:` becomes `"inferSchema"`).

```elixir
Latu.read(session, format: "csv", schema: "id INT, name STRING",
  path: "/data/people.csv", header: true)

df |> Latu.write(format: "parquet", path: "/data/out", mode: :overwrite)
```

Expressions are plain functions, not macros. That makes one a value you can name, pass around
and fold.

```elixir
big = greater(:price, 1_000_000)
Latu.filter(df, all([big | extra_predicates]))
```

There is no macro DSL, and the omission is deliberate. A macro expression is not a value, so
extracting a fragment or folding a list of predicates would need a second construct bolted on.

Window specifications compose the same way:

```elixir
by_suburb = W.partition_by([:suburb]) |> W.order_by([desc(:price)])

df
|> Latu.with_columns(rank: over(F.rank(), by_suburb))
|> Latu.group_by(:suburb)
|> Latu.agg(avg: F.avg(:price), sold: F.count_distinct(:id))
|> Latu.show()
```

`Latu.connect/0` reads `SPARK_REMOTE`, and `Latu.connect/1` accepts Spark's URL parameters:
`sc://host:15002/;use_ssl=true;token=...;user_id=...`. Unrecognised parameters become gRPC
metadata headers, as PySpark does.

## What is in it

**Relational verbs.** `select`, `filter`, `with_columns`, `drop`, `sort`, `distinct`, `rename`,
set operations, every join type plus as-of, lateral and nearest-by, and `group_by`/`agg` with
rollup, cube, pivot and grouping sets.

**Expressions.** ~500 functions in `Latu.Functions` under Spark's own names, with Spark's own
documentation harvested into them, so `h F.regexp_replace` tells you what Spark says. Window
specifications, higher-order functions that take an ordinary Elixir lambda, and subqueries that
reach across frames.

**Results.** `show`, `collect` into maps, `to_explorer` into an `Explorer.DataFrame`, a lazy
`stream` of one frame per Arrow batch, and raw Arrow. `to_nx` reads the same bytes as `Nx`
tensors. Behind all of them is a schema guard that turns the types the decoder cannot represent
into errors naming the column.

**Reading and writing.** `read/2` and `write/2` for any format the cluster has, JDBC included.
`create_dataframe/3` ships rows, columns or an Explorer frame the other way, escalating past
64 MiB to server-cached artifacts with no change to the call. `sql/3` binds parameters as
literals rather than splicing text.

**Running things.** `observe/3`, checkpointing, interrupting a query by tag from another process,
session config both ways, per-action progress callbacks, errors carrying Spark's own error class
and SQLSTATE, `:telemetry` events, and Livebook rendering behind an optional `:kino`.

**Structured streaming.** `read/2` with `is_streaming: true`, `with_watermark/3`, and
`write_stream/2` with the `:available_now`, processing-time and continuous triggers, returning a
`%Latu.StreamingQuery{}` whose verbs are Spark's own: `await_termination`, `status`,
`last_progress`, `stop`. `with_stream/3` is the bracket, and `events/1` is the listener bus as a
lazy stream. What is out is `foreachBatch`, which carries a closure no client but Python can
build.

The [cheatsheet](https://hexdocs.pm/latu/cheatsheet.html) is the whole surface, one line each.

**Elsewhere.** MLlib is [`latu_ml`](https://hexdocs.pm/latu_ml), a companion package. It is a
surface of its own: a server-side model cache, Spark's on-disk model format, its own operator
registry, and no reason to share Latu's release cadence. There are no UDFs written in Elixir, no
RDDs and no `SparkContext`. Spark Connect offers no client in any language a path to them.

## Custom code on the cluster

Latu **calls** a user-defined function by name with `Latu.Column.fun/3`. A SQL UDF, a Hive UDF
and a registered Java class all resolve the same way, and `CREATE FUNCTION` through `Latu.sql/3`
is how you register one. `Latu.add_jar/3` puts a jar on the session for such a class to resolve
from, and `Latu.copy_to_fs/3` puts a file on the cluster's filesystem. Both take bytes: Latu
reads nothing from your disk.

## Where to go next

  * [Quick start](https://hexdocs.pm/latu/quick-start.html). Connect, build, run. Every line of
    it is executed. The same steps as a
    [Livebook notebook](https://livebook.dev/run?url=https%3A%2F%2Fgithub.com%2Fzero-one-group%2Flatu%2Fblob%2Fmain%2Fnotebooks%2Fquick_start.livemd),
    one click from the badge above.
  * [Cookbook](https://hexdocs.pm/latu/cookbook.html). Recipes for the things you actually do.
  * [Cheatsheet](https://hexdocs.pm/latu/cheatsheet.html). Every verb, one line each.
  * [Coming from PySpark](https://hexdocs.pm/latu/from-pyspark.html). The five differences, and
    a translation table for the calls you make every day.
  * [Coming from Explorer](https://hexdocs.pm/latu/from-explorer.html). Where the local Elixir
    dataframe ends and the cluster begins, and how to move frames across the seam.
  * [`usage-rules.md`](https://hexdocs.pm/latu/usage-rules.html). The short set of rules that are
    not guessable from the function names, in the
    [`usage_rules`](https://github.com/ash-project/usage_rules) convention, so an agent can sync
    it into its context.
  * [Spark versions](https://hexdocs.pm/latu/spark-versions.html). What a 4.2 client does
    against 4.1 and 4.0, measured; 3.5, newer servers, and the managed platforms.
  * [`docs/deviations.md`](https://hexdocs.pm/latu/deviations.html). A reference: every place the
    API departs from PySpark, and why.
  * [`CONTRIBUTING.md`](https://hexdocs.pm/latu/contributing.html). The servers, the proto
    oracle, the golden fixtures.

## SparkEx, and why Latu exists

[SparkEx](https://github.com/lukaszsamson/spark_ex) is an independent Elixir Spark Connect
client. **It got here first, it is on Hex, and it does one thing Latu does not**: UDF/UDTF
registration. If you need that today, use SparkEx.

The two made different bets. SparkEx keeps close to PySpark's shape, with mandatory `col/1` and
`lit/1`, module namespaces standing in for method chains, positional arguments and string keys,
and its session is a `GenServer`. Latu's API is designed for Elixir, with atoms as columns,
keyword lists for aliases and options, and one namespace of verbs called the way `Enum` is. Its
session is a plain struct, and it defines no process at all.

```elixir
# SparkEx
DataFrame.filter(df, Column.gt(col("salary"), lit(120)))
DataFrame.join(departments, ["dept"], :inner)
#=> {:ok, [%{"name" => "Bob", "salary" => 200}]}

# Latu
Latu.filter(df, greater(:salary, 120))
Latu.join(df, departments, on: :dept, how: :inner)
#=> {:ok, [%{name: "Bob", salary: 200}]}
```

The runtime shape is the choice everything else follows from. A session process gives you
supervision and somewhere to put shared state; doing without one is why Latu's metrics come back
from actions and its progress handler runs in your own process. If SparkEx's spelling reads
better to you, that is a good reason to use SparkEx. The rules behind Latu's are in
[`docs/deviations.md`](https://hexdocs.pm/latu/deviations.html).

## Acknowledgements

Design and test suite draw heavily on [Geni](https://github.com/zero-one-group/geni)
(Apache-2.0, Copyright 2020 Zero One Group).

Studying SparkEx shaped early design decisions, and `docs/decisions.md` records where the two
projects part ways.

## How this was built

**Claude (Anthropic) wrote the overwhelming majority of the code, the tests and the
documentation.** The maintainers set the scope, made the design calls, ran every gate and
reviewed the result. That division is worth stating plainly rather than leaving to be guessed
at.

The apparatus is what holds it up. Every plan Latu builds is diffed against the protobuf PySpark
builds for the same pipeline, every documented example is executed, and `docs/decisions.md`
records the reasoning behind every non-obvious choice.

## License

Apache License 2.0. See the `LICENSE` file. Arrow decoding uses
[Explorer](https://github.com/elixir-explorer/explorer), which is MIT.
