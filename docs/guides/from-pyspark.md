# Coming from PySpark

Latu builds the same plans PySpark builds and sends them down the same protocol, so what you
know about Spark transfers whole. What changes is the language around it.

Five differences account for nearly everything; the rest is a spelling table. Where a departure
has a reason worth reading, [`docs/deviations.md`](../deviations.md) has it.

Every `elixir` snippet here is executed by `mix check.all`
(`test/integration/guides_test.exs`).

```elixir
import Latu.Column
alias Latu.Functions, as: F

{:ok, session} = Latu.connect("sc://localhost:15002")
```

## 1. There is no JVM in your project

PySpark's client starts a Python process that talks to a JVM. Latu is a gRPC client and nothing
else: `Latu.connect/2` returns a plain struct wrapping a channel, Latu adds nothing to your
supervision tree, and where the session lives is your application's business. The channel is a
process — the adapter starts one, and `Latu.disconnect/2` stops it — but Latu defines none of
its own and holds no state between calls.

The practical consequences are all pleasant. There is no `SparkContext`, no RDD, and no
`spark-submit` — a Latu program is an Elixir program. There is also nothing to configure at
startup: `Latu.connect/2`'s options are the session's own, and Spark's own config is
`Latu.set_conf/3` afterwards.

## 2. A plan is a value, and verbs are functions over it

`df.filter(...)` is a method on an object; `Latu.filter(df, ...)` is a function from one plan to
the next. That means a DataFrame is inert data you can name, pass around and inspect without
touching the server — printing one never does IO, where PySpark's `repr(df)` pays a round trip.

```elixir
lazy = session |> Latu.range(10) |> Latu.filter(greater(:id, 4))

"#Latu.DataFrame<range → filter>" = inspect(lazy)
```

The same property is why an expression is a value rather than a macro: you can build one, name
it, and fold a list of them.

```elixir
predicates = [greater(:id, 4), not_equal(:id, 7)]

# range(10) is 0..9; `> 4` leaves 5, 6, 7, 8, 9; `!= 7` takes 7 out. Four.
{:ok, 4} = session |> Latu.range(10) |> Latu.filter(all(predicates)) |> Latu.count()
```

`lazy` above is still usable and still unrun — building on a named plan does not consume it.

**There is no builder chain.** `spark.read.format(...).option(...).load(...)` is one call here,
and so is the writer. A builder is a mutable object pretending to be a pipeline, and Elixir
already has a pipeline.

## 3. An action returns a tuple, and every one has a `!` twin

Transformations return a DataFrame; **actions return `{:ok, value}` or
`{:error, %Latu.Error{}}`**. The `!` form raises instead, and exists for every action — which is
what makes a pipe read well.

```elixir
{:ok, 10} = session |> Latu.range(10) |> Latu.count()

10 = session |> Latu.range(10) |> Latu.count!()
```

The error is structured, not a string: it carries Spark's own error class and SQLSTATE, the JVM
exception hierarchy and the message parameters, all off the gRPC trailers at no extra cost. See
`Latu.Error`.

## 4. An atom is a column; a string depends on where it is

This is the one rule that catches people, and it is PySpark's own rule made explicit.

**In a verb, a string is a column name — except in `filter`, where it is SQL.** That asymmetry
is PySpark's (`df.filter("id > 3")` works there too), kept for exactly that reason.

**Inside an expression, a string is a literal.** `equal(:suburb, "Reservoir")` compares a column
to text, and there is no `F.lit` to remember — though `lit/1` is there when you want to be
explicit.

```elixir
{:ok, rows} =
  session
  |> Latu.range(5)
  |> Latu.select([:id, label: F.concat([lit("row-"), cast(:id, "string")])])
  |> Latu.filter("id > 3")
  |> Latu.collect()

[%{id: 4, label: "row-4"}] = rows
```

**There is no mandatory `col/1`.** An atom is a column reference everywhere one is expected, so
`F.sum(:price)` is what PySpark spells `F.sum(F.col("price"))`.

## 5. Aliasing is a keyword list

PySpark names an output column with `.alias(...)` on the expression. Latu names it with the
keyword key, in `select`, `with_columns` and `agg` alike — so the name reads first, where you
look for it.

```elixir
{:ok, [%{total: 45, n: 10}]} =
  session
  |> Latu.range(10)
  |> Latu.agg(total: F.sum(:id), n: F.count(lit(1)))
  |> Latu.collect()
```

## The daily twenty

| PySpark | Latu |
|---|---|
| `spark.range(10)` | `Latu.range(session, 10)` |
| `df.select("a", "b")` | `Latu.select(df, [:a, :b])` |
| `df.select(F.sum("b").alias("t"))` | `Latu.select(df, t: F.sum(:b))` |
| `df.filter(df.id > 3)` | `Latu.filter(df, greater(:id, 3))` |
| `df.filter("id > 3")` | `Latu.filter(df, "id > 3")` |
| `df.withColumn("x", F.lit(1))` | `Latu.with_columns(df, x: lit(1))` |
| `df.withColumnRenamed("a", "b")` | `Latu.rename(df, a: :b)` |
| `df.drop("a")` | `Latu.drop(df, :a)` |
| `df.orderBy(F.desc("a"))` | `Latu.sort(df, [desc(:a)])` |
| `df.distinct()` | `Latu.distinct(df)` |
| `df.limit(5)` | `Latu.limit(df, 5)` |
| `df.groupBy("a").count()` | `df \|> Latu.group_by(:a) \|> Latu.count()` |
| `df.groupBy("a").agg(F.sum("b"))` | `df \|> Latu.group_by(:a) \|> Latu.agg(t: F.sum(:b))` |
| `df.join(other, "id", "left")` | `Latu.join(df, other, on: :id, how: :left)` |
| `df.union(other)` | `Latu.union(df, other)` |
| `spark.read.csv(p, header=True)` | `Latu.read(session, format: "csv", path: p, header: true)` |
| `df.write.parquet(p)` | `Latu.write(df, format: "parquet", path: p)` |
| `df.write.mode("overwrite")` | `mode: :overwrite` — an option, not a call |
| `df.collect()` | `Latu.collect(df)` |
| `df.show()` | `Latu.show(df)` |
| `df.count()` | `Latu.count(df)` |
| `df.printSchema()` | `Latu.print_schema(df)` |
| `df.cache()` | `Latu.cache(df)` |
| `spark.sql(q)` | `Latu.sql(session, q)` |
| `df.na.drop()` | `Latu.drop_na(df)` |
| `df.na.fill(0)` | `Latu.fill_na(df, 0)` |
| `F.col("a")` | `:a`, or `col(:a)` |
| `F.expr("a + 1")` | `expr("a + 1")` |
| `F.lit(1)` | `lit(1)`, or just `1` |

**Every `Latu.` call in that table is checked** — `test/latu/examples_test.exs` parses each cell
and resolves the call at the arity shown, so a rename breaks the page rather than rotting it.

Two shapes to notice. **`df.na` and `df.stat` have no namespace here** — PySpark's six
`DataFrameStatFunctions` methods and four `na` methods are all verbs on `Latu`, because a
namespace whose only job is grouping is a method chain by another name. And **`groupBy().count()`
is lazy while `df.count()` is an action**, exactly as in Spark; the structs are what tell them
apart.

## Naming, when the two disagree

The precedence is **Spark > Elixir > Polars > dplyr**, and it is decided in that order every
time.

**Spark's own spelling wins wherever Spark has one**, which is why the function library is
`F.regexp_replace` and not a re-invention, and why `join_as_of` keeps Spark's concept name. The
only change Spark's names get is snake_case, since camelCase in Elixir would be worse than
either.

**Where Spark has no name, Elixir's conventions win.** `{:ok, _}` tuples with `!` twins,
keyword options instead of positional booleans, `Latu.Error` as a struct. This is also why
`Latu.range/2`'s fifth argument became `num_partitions:` — `Latu.range(session, 0, 10, 2, 4)`
gives a reader no way to tell the step from the partitions.

**Below that, Polars and then dplyr**, which is where `glimpse/2` comes from: Spark has no
method like it, and a transposed preview is the right way to look at a wide frame.

What the precedence rules *out* is the interesting half. There is no `Latu.Ops` operator
shadowing, no macro DSL, no kebab-case columns, and no pandas-shaped conveniences — each one
was proposed, and each one lost to a rung above it. `docs/decisions.md` has the arguments.

## Things that are deliberately not here

**MLlib and structured streaming** are separate packages, not omissions, and for different
reasons. Latu hands out resources and never keeps them, and the test is whether one can be
honestly bracketed: a checkpoint can (`with_checkpoint/3`, the way `File.open/3` does it), and a
streaming query cannot — it runs after you stop looking. ML is a fitted model, which brackets
fine; what it lacks is an oracle, since its 103 operators' parameters appear nowhere
machine-readable and every plan in Latu is checked against the one PySpark builds. `Latu`'s
moduledoc states the first rule; `docs/deviations.md` lists what is missing.

**UDFs written in Elixir** cannot exist — Spark Connect offers no client in any language a path
to them. *Calling* a UDF that is already registered works fine: `Latu.Column.fun/3` sends the
name, and a SQL UDF, a Hive UDF and a registered Java class all resolve identically.

**RDDs and `SparkContext`** are not part of Spark Connect at all, for any client.

The full list, with reasons, is under "In PySpark, not in Latu" in
[`docs/deviations.md`](../deviations.md).

## Where to go next

  * [Quick start](quick-start.md) — the tour, in ten minutes
  * [Cookbook](cookbook.md) — recipes for the things you actually do
  * [Coming from Explorer](from-explorer.md) — the local Elixir dataframe, and the seam
  * [Cheatsheet](../cheatsheet.cheatmd) — the whole surface, one line each
  * [`docs/deviations.md`](../deviations.md) — every departure, and why
  * [`usage-rules.md`](../../usage-rules.md) — the same rules in the form a coding agent
    reads

```elixir
{:ok, _closed} = Latu.disconnect(session)
```
