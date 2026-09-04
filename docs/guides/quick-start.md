# Quick start

Latu builds a query plan on your machine and Spark runs it. Nothing here needs a JVM in your
project — only a Spark Connect server to talk to.

This page follows Spark's own [quick start](https://spark.apache.org/docs/latest/quick-start.html)
move for move: make a frame, count it, filter it, aggregate it, count words, cache it. The data
is three lines of text rather than a file, so the page runs as written with nothing on disk.

Every `elixir` snippet here is executed by `mix check.all`
(`test/integration/guides_test.exs`), in order, sharing one set of bindings. The pattern matches
are the assertions: if a result stopped looking like this, the suite would say so.

## A server to talk to

Point at a cluster's `sc://` URL, or run one locally:

```bash
docker run -p 15002:15002 apache/spark:4.2.0 \
  /opt/spark/sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.13:4.2.0
```

In this repo, `docker compose up -d spark-connect` does it.

## Connect

A session is a plain struct wrapping a gRPC channel. Latu adds nothing to your supervision
tree, so where the session lives is your application's business. The channel itself *is* a
process — the adapter starts one — and `Latu.disconnect/2` is what stops it.

```elixir
{:ok, session} = Latu.connect("sc://localhost:15002")
```

`Latu.connect/0` reads `SPARK_REMOTE` instead, and `Latu.connect!/2` raises rather than
returning a tuple — every action in Latu has that pair.

## Some data

Verbs are called qualified, the way `Enum` is. Expressions come from `Latu.Column`, which is
small enough to import, and Spark's function library is aliased as `F`.

`Latu.create_dataframe/3` ships local rows to the cluster as Arrow. It is `Latu.collect/2`'s
inverse, and the quickest way to have something to work on.

```elixir
import Latu.Column
alias Latu.Functions, as: F

{:ok, df} =
  Latu.create_dataframe(session, [
    %{id: 1, line: "Latu builds a plan"},
    %{id: 2, line: "Spark runs the plan on a cluster"},
    %{id: 3, line: "the plan is just data"}
  ])
```

Row maps are sorted by key, so the columns are `id` then `line`. Pass `schema:` when you want
the types decided rather than inferred.

## Count it, and look at a row

An action returns `{:ok, value}` or `{:error, %Latu.Error{}}`.

```elixir
{:ok, 3} = Latu.count(df)

{:ok, %{id: 1, line: "Latu builds a plan"}} = df |> Latu.sort(:id) |> Latu.first()
```

Rows come back as maps with atom keys. `keys: :strings` is there for names that come out of
dynamic SQL, where an unbounded atom table would be a leak.

## Filter, and look before you run

A verb is a pure function from one plan to the next, so nothing has run yet — and you can see
what you built before paying for it:

```elixir
about_spark = Latu.filter(df, contains(:line, "Spark"))

"#Latu.DataFrame<local_relation → filter>" = inspect(about_spark)
```

Then chain an action onto it:

```elixir
{:ok, 1} = Latu.count(about_spark)
```

## The longest line

`Latu.agg/2` on an ungrouped frame aggregates the whole thing. A keyword list names the result
column, which is why there is no `max(numWords)` to quote back at you:

```elixir
{:ok, [%{longest: 7}]} =
  df
  |> Latu.select(words: F.size(F.split(:line, "\\s+")))
  |> Latu.agg(longest: F.max(:words))
  |> Latu.collect()
```

Two things worth noticing. A **string inside an expression is a literal**, so `"\\s+"` is a
regex and not a column name — an atom is what makes it a column. And `Latu.select/2` takes a
keyword list to alias a projection, the same way `agg` does.

## Word count

Spark's own quick start ends with MapReduce, and it is the same three verbs here:

```elixir
counts =
  df
  |> Latu.select(word: F.explode(F.split(:line, "\\s+")))
  |> Latu.group_by(:word)
  |> Latu.count()
```

`Latu.count/1` on a grouped frame is **lazy** — a transformation that adds a `count` column.
`Latu.count/2` on a plain frame is an action that returns a number. Spark overloads the name the
same way; here the structs are what tell them apart.

```elixir
{:ok, [%{word: "plan", count: 3} | _rest]} =
  counts |> Latu.sort([desc(:count)]) |> Latu.collect()
```

## Cache it

Caching is a round trip here, where classic Spark's is a driver-local call that cannot fail — so
it returns a tuple, and `cache!/1` is the one that pipes. It stays lazy on the server: success
means the query is registered, not that anything is materialised.

```elixir
cached = Latu.cache!(counts)

{:ok, 12} = Latu.count(cached)
{:ok, 12} = Latu.count(cached)
```

## Ask a frame about itself

Analysing a plan does not run it. A schema comes back as data, using Spark's own name for each
type — there is no client-side type model in either direction.

```elixir
["word", "count"] = Latu.columns!(counts)

[%{name: "word", type: "string"} | _] = Latu.schema!(counts)
```

## Out to the cluster, and back

`Latu.write/2` writes **on the cluster**, not on your machine — the path is the server's. One
call per destination rather than a builder chain, and any key Latu does not recognise is passed
through to Spark as a writer option.

```elixir
out = "/tmp/latu_quick_start"

:ok = Latu.write(counts, format: "parquet", path: out, mode: :overwrite)

{:ok, 12} = session |> Latu.read(format: "parquet", path: out) |> Latu.count()
```

## SQL, and back again

SQL runs eagerly, binds its parameters as literals rather than splicing text, and hands back a
frame that queries the *result*:

```elixir
{:ok, added} = Latu.sql(session, "SELECT 1 + :n AS n", %{n: 1})

{:ok, [%{n: 2}]} = Latu.collect(added)
```

A frame can also be named into a query without registering anything on the server:

```elixir
{:ok, named} = Latu.sql(session, "SELECT max(count) AS top FROM words", views: [words: counts])

{:ok, [%{top: 3}]} = Latu.collect(named)
```

## Disconnect

```elixir
{:ok, _closed} = Latu.disconnect(session)
```

Closing the channel leaves the server's own session to time out. `Latu.release_session/2` ends
it now, and takes its temp views, cached frames and confs with it.

## Where to go next

  * [Cookbook](cookbook.md) — recipes for the things you actually do
  * [Coming from PySpark](from-pyspark.md) — five differences, and a translation table
  * [Coming from Explorer](from-explorer.md) — the two together, and where they differ
  * `Latu` — the verbs, and how the three expression modules are reached
  * `Latu.Column` — operators, predicates, casts, sort keys
  * `Latu.Functions` — Spark's ~500 built-ins, under Spark's own names
  * [Cheatsheet](../cheatsheet.cheatmd) — the whole surface, one line each
  * [`usage-rules.md`](../../usage-rules.md) — the short set of rules that are not guessable
    from the names
  * [`docs/deviations.md`](../deviations.md) — every place the API departs from PySpark, and
    why
