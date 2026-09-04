# Cookbook

Short recipes for things people actually do. The [quick start](quick-start.md) is the tour; this
is the reference you come back to.

Every `elixir` snippet here is executed by `mix check.all`
(`test/integration/guides_test.exs`), in order, sharing one set of bindings — with two
exceptions, each marked on the page and each saying why. The pattern matches are the assertions.

Where a recipe departs from PySpark, [`docs/deviations.md`](../deviations.md) has the reason.

## Setting up

```elixir
import Latu.Column
alias Latu.Functions, as: F
alias Latu.Window, as: W

{:ok, session} = Latu.connect("sc://localhost:15002")
```

Two small frames carry the page. Local rows go to the cluster as Arrow, so nothing here needs a
file on disk:

```elixir
{:ok, sales} =
  Latu.create_dataframe(session, [
    %{region: "north", year: 2025, units: 10},
    %{region: "north", year: 2026, units: 12},
    %{region: "south", year: 2025, units: 8},
    %{region: "south", year: 2026, units: 5}
  ])

{:ok, readings} =
  Latu.create_dataframe(
    session,
    [
      %{id: 1, score: 10.0, team: "red"},
      %{id: 2, score: nil, team: "red"},
      %{id: 3, score: 30.0, team: nil},
      %{id: 4, score: nil, team: nil}
    ],
    schema: "id INT, score DOUBLE, team STRING"
  )
```

**Pass `schema:` when a column has nulls in it.** Inference reads the values, and a column that
is null in the rows you happened to write has nothing to infer from.

## Grouping, and naming what comes out

`Latu.agg/2` takes a keyword list, and the keys are the output column names — so there is no
`sum(units)` to quote back at you later.

```elixir
{:ok, by_region} =
  sales
  |> Latu.group_by(:region)
  |> Latu.agg(total: F.sum(:units), n: F.count(lit(1)))
  |> Latu.sort(:region)
  |> Latu.collect()

[%{region: "north", total: 22, n: 2}, %{region: "south", total: 13, n: 2}] = by_region
```

`Latu.rollup/2`, `Latu.cube/2` and `Latu.grouping_sets/3` take the same `agg`, and add the
subtotal rows their names suggest.

## Reshaping with a pivot

```elixir
pivoted =
  sales
  |> Latu.group_by(:region)
  |> Latu.pivot(:year, [2025, 2026])
  |> Latu.agg(units: F.sum(:units))

["region", "2025", "2026"] = Latu.columns!(pivoted)

{:ok, [%{region: "north", "2025": 10, "2026": 12} | _]} =
  pivoted |> Latu.sort(:region) |> Latu.collect()
```

**Pass the values when you know them.** Without them Spark runs a separate query first to find
the distinct ones, which is a second pass over the data to learn something you already knew.

## Rolling and ranking windows

A window specification is a value: build it once, name it, use it in as many columns as you like.

```elixir
window = W.partition_by([:g]) |> W.order_by([:id])
rolling = W.rows_between(window, -1, 1)

{:ok, rows} =
  session
  |> Latu.range(0, 10, 3)
  |> Latu.with_columns(g: 1)
  |> Latu.with_columns(s: over(F.sum(:id), rolling))
  |> Latu.sort(:id)
  |> Latu.collect()

[3, 9, 18, 15] = Enum.map(rows, & &1.s)
```

The ids are `0, 3, 6, 9` — gapped on purpose, because that is what separates the two frame
kinds. **`rows_between` counts rows and `range_between` counts values**, and with consecutive
ids the two agree and you would never find out:

```elixir
{:ok, ranged} =
  session
  |> Latu.range(0, 10, 3)
  |> Latu.with_columns(g: 1)
  |> Latu.with_columns(s: over(F.sum(:id), W.range_between(window, -1, 1)))
  |> Latu.sort(:id)
  |> Latu.collect()

[0, 3, 6, 9] = Enum.map(ranged, & &1.s)
```

Each row is its own window there, because no other row's `id` falls within 1 of it.

Ranking works the same way, and `row_number` needs the running frame Spark gives it by default:

```elixir
{:ok, ranked} =
  session
  |> Latu.range(4)
  |> Latu.with_columns(rn: over(F.row_number(), W.order_by([:id])))
  |> Latu.sort(:id)
  |> Latu.collect()

[1, 2, 3, 4] = Enum.map(ranked, & &1.rn)
```

## Missing data: count it, drop it, fill it

**Counting nulls per column is a `count` of the column against a `count` of the rows.**
`count(col)` skips nulls and `count(1)` does not, so the gap between them is the null count:

```elixir
{:ok, [%{rows: 4, scored: 2, teamed: 2}]} =
  readings
  |> Latu.agg(rows: F.count(lit(1)), scored: F.count(:score), teamed: F.count(:team))
  |> Latu.collect()
```

Dropping reads four ways, and they are genuinely different:

```elixir
{:ok, 1} = readings |> Latu.drop_na() |> Latu.count()
{:ok, 4} = readings |> Latu.drop_na(how: :all) |> Latu.count()
{:ok, 2} = readings |> Latu.drop_na(subset: [:score]) |> Latu.count()
{:ok, 1} = readings |> Latu.drop_na(min_non_nulls: 3) |> Latu.count()
```

`how: :all` keeps every row here because `id` is never null — a row has to be null *all the way
across* to go. And `:min_non_nulls` overrides `:how` rather than combining with it.

Filling has a rule worth knowing before it surprises you: **a fill value only reaches the columns
whose type it fits**, and Spark says nothing about the ones it skips.

```elixir
{:ok, filled} = readings |> Latu.fill_na(-1.0) |> Latu.sort(:id) |> Latu.collect()

[%{id: 1, score: 10.0, team: "red"}, %{id: 2, score: -1.0, team: "red"} | _] = filled
```

`team` is still null there — a number does not fit a string column, so Spark passes it over. A
string fills that one and leaves `score` alone:

```elixir
{:ok, named} = readings |> Latu.fill_na("unknown") |> Latu.sort(:id) |> Latu.collect()

[_, _, _, %{id: 4, score: nil, team: "unknown"}] = named
```

## Data quality with `observe`

`Latu.observe/3` attaches aggregates to a plan and the metrics come back **from the action**, so
checking a frame costs no second pass over it.

```elixir
{:ok, _rows, info} =
  readings
  |> Latu.observe(:quality, rows: F.count(lit(1)), scored: F.count(:score))
  |> Latu.collect_with_metrics()

%{quality: %{rows: 4, scored: 2}} = info.observed
```

Every action has a `_with_metrics` twin — `count_with_metrics/2`, `write_with_metrics/2`,
`merge_with_metrics/2` — which is how you find out how many rows a write touched without
counting them again afterwards.

## Checkpointing a long pipeline

A checkpoint materialises the frame on the server and hands back one rooted at the result, so
everything above it is computed once. It is **the one resource in Latu with a release call of
its own**, and `with_checkpoint/3` is the bracket that frees it even when your function raises.

```elixir
{:ok, counts} =
  Latu.with_checkpoint(sales, [], fn base ->
    {Latu.count!(base), base |> Latu.filter(greater(:units, 7)) |> Latu.count!()}
  end)

{4, 3} = counts
```

Use `Latu.checkpoint/2` plus `Latu.release/1` when the frame has to outlive one function — in a
REPL it usually does. Nothing frees a checkpoint for you: Latu holds no processes and has no
finalizer, so the session ending is the only other thing that bounds it.

## Results too large to hold

`Latu.stream/2` decodes one Explorer frame per Arrow batch and stops the execution when you stop
reading, so a result that will not fit in memory never has to.

```elixir
total =
  session
  |> Latu.range(1_000)
  |> Latu.stream()
  |> Stream.map(&Explorer.DataFrame.n_rows/1)
  |> Enum.sum()

1_000 = total
```

`Latu.to_explorer/2` is the eager form: it brings the whole result back, so bound the plan
first when you want part of it — `Latu.limit/2` is Spark's own way to ask for that.

```elixir
{:ok, frame} = session |> Latu.range(100) |> Latu.to_explorer()

100 = Explorer.DataFrame.n_rows(frame)
```

## Watching a slow query

Every action that reaches the server takes `progress:`, a 1-arity function called with a
`Latu.Progress` as the query runs.

```elixir
{:ok, 5} =
  session
  |> Latu.range(5)
  |> Latu.count(progress: fn p -> IO.write("\r#{Latu.Progress.percent(p)}%") end)
```

Two things the shape does not tell you. **A fast query may report nothing at all**, which is the
server's timer and not an error — so a handler must not be where your result comes from. And the
handler runs **in your own process**, because Latu holds no process to isolate it in: if it
raises, the query fails.

## Interrupting a query from another process

The process running a query cannot cancel it — it is blocked in the query. That is what tags are
for: tag a session, run the work from a `Task`, and interrupt by tag from anywhere.

```elixir
worker = Latu.connect!("sc://localhost:15002", tags: ["cookbook"])

[] = Latu.interrupt!(worker, tag: "cookbook")

{:ok, _} = Latu.disconnect(worker)
```

Nothing was running, so nothing matched, and an empty list is the honest answer rather than an
error. With work in flight you get back the operation ids the server cancelled;
`test/integration/control_test.exs` runs the full dance, and it is `async: false` because a
query slow enough to interrupt occupies the server while it runs.

**Interrupt rather than killing the process.** A Latu execution is reattachable — a client may
vanish and come back — so a killed client leaves the query *running on the server*, holding
cluster resources until the detached timeout expires.

## Subqueries

A subquery is a frame used as a value. `Latu.scalar/1` takes its one cell, `Latu.exists/1` asks
whether it has rows, and `Latu.Column.isin/2` over a frame is an `IN`.

```elixir
one = Latu.select(Latu.range(session, 1), x: lit(10))
five = Latu.range(session, 5)

{:ok, rows} = five |> Latu.select(x: Latu.scalar(one)) |> Latu.collect()
[10, 10, 10, 10, 10] = Enum.map(rows, & &1.x)

{:ok, 5} = five |> Latu.filter(Latu.exists(one)) |> Latu.count()
```

The frames reach across without either one being registered on the server.

## Partitioned Parquet

`partition_by:` writes a directory per distinct value, and reading the dataset back gives the
partition columns as ordinary columns.

```elixir
out = "/tmp/latu_cookbook_sales"

:ok =
  Latu.write(sales, format: "parquet", path: out, mode: :overwrite, partition_by: [:region])

{:ok, 2} =
  session
  |> Latu.read(format: "parquet", path: out)
  |> Latu.filter(equal(:region, "north"))
  |> Latu.count()
```

The path is the **cluster's**, not your machine's. `:bucket_by` takes `{buckets, columns}` and
pairs with `:sort_by`; `:cluster_by` is the Spark 4 spelling.

## Talking to a database over JDBC

There is no JDBC builder: `format: "jdbc"` and the driver's own options go straight through, the
same way any unrecognised key does.

> **Not executed.** JDBC needs a database the test server does not have.

```elixir
Latu.write(sales,
  format: "jdbc",
  url: "jdbc:postgresql://db:5432/warehouse",
  driver: "org.postgresql.Driver",
  dbtable: "sales",
  user: "app",
  password: password,
  mode: :append
)

Latu.read(session,
  format: "jdbc",
  url: "jdbc:postgresql://db:5432/warehouse",
  driver: "org.postgresql.Driver",
  query: "SELECT region, sum(units) AS total FROM sales GROUP BY region"
)
```

`test/integration/jdbc_test.exs` does prove the option passing, against embedded Derby — but
that works only because `--master local[1]` puts the executor in the driver's JVM, which is a
property of the test rig and not advice.

The driver jar has to be on the **cluster's** classpath — Latu ships nothing from your machine,
and `docs/decisions.md` records why. `query:` is Spark's own option for pushing a query down,
and it reaches the server because Latu passes through what it does not recognise rather than
validating a list it would have to keep current.

## Upserting with `merge_into`

A merge is built as inert data and sent by `Latu.merge/2`. The frame is the *source*, the table
is the target, and both are in scope in the condition — so the names need qualifying, the source
by `Latu.as/2`.

> **Not executed.** A merge needs an Iceberg or Delta target, and the test server has neither.

```elixir
sales
|> Latu.as("s")
|> Latu.merge_into("warehouse.sales", expr("warehouse.sales.region = s.region"))
|> Latu.when_matched(:update, set: [units: col("s.units")])
|> Latu.when_not_matched(:insert_all)
|> Latu.merge()
```

**A stock Spark cannot run a merge at all.** `RewriteMergeIntoTable` only rewrites a target that
supports row-level operations; Iceberg and Delta provide such tables and Spark's own built-in
sources do not, so the plan is refused at analysis. The plan Latu builds is the same either way,
which is why the verb ships.

Clauses apply in the order you add them and **only the first matching clause runs**, so an
unconditional one belongs last. `Latu.merge_with_metrics/2` is the form that tells you how many
rows it touched.

## Where to go next

  * [Quick start](quick-start.md) — the tour, if you have not taken it
  * [Coming from PySpark](from-pyspark.md) — five differences, and a translation table
  * [Coming from Explorer](from-explorer.md) — the two together, and where they differ
  * `Latu` — every verb, with its options
  * [Cheatsheet](../cheatsheet.cheatmd) — the same verbs, one line each
  * [`usage-rules.md`](../../usage-rules.md) — the rules that are not guessable from the names
  * [`docs/deviations.md`](../deviations.md) — every place the API departs from PySpark, and
    why
