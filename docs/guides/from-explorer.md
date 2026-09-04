# Coming from Explorer

Latu is not a replacement for [Explorer](https://hexdocs.pm/explorer). It is a client for a
Spark cluster, and it **depends** on Explorer: results decode into `Explorer.DataFrame`s, local
frames ship the other way, and twelve of Latu's operators are named after
`Explorer.Series`'. If you already write Explorer, most of this page is about where the two
meet rather than how to leave one behind.

The honest division: **Explorer is terser for a quick look, and it is the right tool while the
data fits in memory.** Latu is for the frame that does not fit, the join across two sources the
cluster already has, or the pipeline you keep editing — because a Latu plan is a value you can
name, fold and inspect without running anything.

Every `elixir` snippet here is executed by `mix check.all`
(`test/integration/guides_test.exs`). The Explorer half of each comparison is a comment beside
it, so the two shapes sit side by side without the page pretending to run two libraries.
Explorer examples assume `require Explorer.DataFrame, as: DF`.

```elixir
import Latu.Column
alias Latu.Functions, as: F

{:ok, session} = Latu.connect("sc://localhost:15002")
```

## Some data to compare on

`Latu.create_dataframe/3` ships local rows to the cluster as Arrow. Row maps have their columns
sorted by key, so these are `price` then `year`.

```elixir
# Explorer:  DF.new(price: [1_500_000, 900_000, 2_000_000], year: [2021, 2022, 2019])
{:ok, sales} =
  Latu.create_dataframe(session, [
    %{price: 1_500_000, year: 2021},
    %{price: 900_000, year: 2022},
    %{price: 2_000_000, year: 2019}
  ])
```

## 1. You already know twelve of the operators

`add`, `subtract`, `multiply`, `divide`, `remainder`, `pow`, `equal`, `not_equal`, `greater`,
`greater_equal`, `less` and `less_equal` are **`Explorer.Series`' names, on purpose**. Latu's
naming precedence is Spark > Elixir > Polars > dplyr, and Spark names none of these twelve —
PySpark exposes them as dunders only — so the first rung is silent and Latu falls to the one
every Latu user already has in scope.

Two could not follow Explorer: `divide` cannot be `div` and `remainder` cannot be `rem`,
because `Kernel.div/2` and `Kernel.rem/2` are auto-imported and `import Latu.Column` would stop
compiling.

Everything else does have a Spark name and follows it instead, which is where the vocabularies
part: `is_null/1` rather than Explorer's `is_nil`, `contains/2`, `starts_with/2`, `between/3`,
`isin/2`, `cast/2`.

## 2. A query is a value, not a macro

This is the difference everything else follows from. `Explorer.DataFrame.filter/2`,
`mutate/3`, `summarise/2` and `sort_by/3` are **macros**: inside them a bare identifier is a
column name and `^` escapes back to Elixir. Latu has no query DSL at all. An atom is a column
reference, anything else is an ordinary Elixir value, and an expression is a struct you can
bind to a variable.

So a predicate is a **value**, which means you can name one, pass it around, and fold a list of
them:

```elixir
# Explorer:  DF.filter(df, price > 1_000_000 and year > 2020)
expensive = greater(:price, 1_000_000)
recent = greater(:year, 2020)

# 1.5M/2021 passes both; 900k/2022 fails on price; 2M/2019 fails on year. One row.
{:ok, 1} = sales |> Latu.filter(all([expensive, recent])) |> Latu.count()
```

`expensive` is reusable, and so is its negation — `not_/1` takes the same value:

```elixir
{:ok, 1} = sales |> Latu.filter(not_(expensive)) |> Latu.count()
```

**Explorer's macros structurally cannot do this**, because the predicate only exists during
macro expansion. The escape hatch is the `_with` family — `filter_with/2`, `mutate_with/3`,
`summarise_with/2`, `sort_with/3` — which take a function of a lazy frame and are the right
answer when you need composition. Worth knowing they exist, and worth knowing that a closure
over a lazy frame is a heavier thing to pass around than a struct.

`all/1` and `any/1` fold left and return a single predicate, and both are total: `all([])` is
`lit(true)` and `any([])` is `lit(false)`, so a filter built from an empty list of user-supplied
conditions is a no-op rather than an error.

## 3. There is no `^`

Explorer needs `^` because a query is a macro and an outside variable has to be marked as one.
Nothing in Latu is a macro, so there is nothing to pin:

```elixir
floor = 1_000_000

# Explorer:  DF.filter(df, price > ^floor)
{:ok, 2} = sales |> Latu.filter(greater(:price, floor)) |> Latu.count()
```

The rule that replaces it is one sentence: **an atom is a column, and everything else is a
value.** `col/1` is there for a name an atom cannot spell, the way Explorer's `col/1` is inside
a query.

## 4. `collect` means something else here

The one false friend, and it is worth reading twice. Both libraries have `collect`, `lazy` and
`stream`, and they sit on different axes.

| word | Explorer | Latu |
|---|---|---|
| `collect` | run a lazy frame; still local | **run on the cluster, bring rows back** |
| `lazy` | opt in to the lazy backend | nothing to opt into — every verb is lazy |
| `stream` | `to_rows_stream/2` — local rows | one Explorer frame per Arrow batch, off the wire |
| `new` | build a frame from local data | `create_dataframe/3` — the data is **sent** |

`Latu.collect/2` is an action: it runs the plan on the cluster and hands back a list of row maps
with atom keys.

```elixir
# Explorer:  df |> DF.filter(price > 1_000_000) |> DF.arrange(price) |> DF.to_rows()
{:ok, rows} = sales |> Latu.filter(expensive) |> Latu.sort(:price) |> Latu.collect()

[%{price: 1_500_000}, %{price: 2_000_000}] = rows
```

A map pattern matches partially, which is why `year` need not appear above.

## 5. Everything is lazy, and the work is somewhere else

Explorer is eager by default and `DF.lazy/1` opts in. Latu has no eager mode: every verb is a
pure function from one plan to the next, and only an action talks to the server. So a mistyped
pipeline costs nothing, and you can look at what you built before paying for it:

```elixir
lazy = Latu.filter(sales, expensive)

"#Latu.DataFrame<local_relation → filter>" = inspect(lazy)
```

Printing a Latu frame never does IO. Printing an eager Explorer frame shows you data because it
already has it; printing a lazy one shows you the plan, which is the closer analogue.

The other half of "somewhere else" is memory. An Explorer frame lives in your VM's process
memory (outside the BEAM heap, in Polars' allocator — `Explorer.DataFrame.estimated_size/1` is
your handle on it). A Latu frame holds a plan and nothing else, and the rows never reach your
machine unless you ask for them.

## 6. There is no Series, and no client-side type model

Explorer's series are typed and the dtype is part of the API: `:s64`, `:f64`, `:string`,
`{:s, 32}`, `{:duration, :microsecond}`. Latu has no type model on the client in either
direction. `Latu.dtypes/1` gives Spark's own type names as strings, and a schema you supply is
a string too — DDL, exactly as Spark spells it.

```elixir
# Explorer:  DF.dtypes(df)  #=> %{"price" => {:s, 64}, "year" => {:s, 64}}
{:ok, [{"price", _price_type}, {"year", _year_type}]} = Latu.dtypes(sales)

["price", "year"] = Latu.columns!(sales)
```

There is also no `Explorer.Series` equivalent and no `pull/2`: a single column is a one-column
frame, and `Latu.select/2` plus `Latu.collect/2` is how it reaches you. This is deliberate — a
client-side series would be a second type system to keep in step with Spark's, and Spark's is
the authority.

## The daily twenty

| Explorer | Latu |
|---|---|
| `DF.new(a: [1, 2])` | `Latu.create_dataframe(session, a: [1, 2])` |
| `DF.from_csv("f.csv")` | `Latu.read(session, format: "csv", path: "f.csv")` |
| `DF.from_parquet(p)` | `Latu.read(session, format: "parquet", path: p)` |
| `DF.n_rows(df)` | `Latu.count(df)` — an action, and it returns a tuple |
| `DF.names(df)` | `Latu.columns(df)` |
| `DF.dtypes(df)` | `Latu.dtypes(df)` |
| `DF.select(df, ["a", "b"])` | `Latu.select(df, [:a, :b])` |
| `DF.discard(df, ["a"])` | `Latu.drop(df, :a)` |
| `DF.filter(df, price > 100)` | `Latu.filter(df, greater(:price, 100))` |
| `DF.mutate(df, x: price * 2)` | `Latu.with_columns(df, x: multiply(:price, 2))` |
| `DF.summarise(df, t: sum(price))` | `Latu.agg(df, t: F.sum(:price))` |
| `DF.group_by(df, "a")` | `Latu.group_by(df, :a)` |
| `DF.sort_by(df, desc: price)` | `Latu.sort(df, [desc(:price)])` |
| `DF.distinct(df)` | `Latu.distinct(df)` |
| `DF.rename(df, a: "b")` | `Latu.rename(df, a: :b)` |
| `DF.head(df, 5)` | `Latu.limit(df, 5)`, or `Latu.take(df, 5)` to get rows |
| `DF.join(other, on: "id")` | `Latu.join(df, other, on: :id)` |
| `DF.concat_rows(df, other)` | `Latu.union(df, other)` |
| `DF.drop_nil(df)` | `Latu.drop_na(df)` |
| `DF.pivot_longer(df, ["a"])` | `Latu.unpivot/3` |
| `DF.pivot_wider(df, "k", "v")` | `Latu.group_by/2` then `Latu.pivot/3` then `Latu.agg/2` |
| `DF.print(df)` | `Latu.show(df)` |
| `DF.describe(df)` | `Latu.describe(df)`, or `Latu.summary(df)` for percentiles |
| `DF.sql(df, "...")` | `Latu.sql(session, "...")` |
| `DF.to_parquet(df, p)` | `Latu.write(df, format: "parquet", path: p)` — a **server** path |
| `DF.collect(lazy)` | nothing: already lazy. The action is what runs it |
| `DF.to_rows(df)` | `Latu.collect(df)` |
| `DF.to_rows_stream(df)` | `Latu.stream(df)` — but one frame per batch, not one row |

**Every `Latu.` call in that table is checked** — `test/latu/examples_test.exs` parses each cell
and resolves the call at the arity shown.

## Using both: the round trip

This is the half that has no equivalent in the PySpark guide. Explorer is a hard dependency of
Latu, and the Arrow boundary runs in both directions.

### Down: `to_explorer/2`, and why it refuses

```elixir
{:ok, frame} = sales |> Latu.filter(expensive) |> Latu.to_explorer()

2 = Explorer.DataFrame.n_rows(frame)
["price", "year"] = Explorer.DataFrame.names(frame)
```

The result is a real Explorer frame, decoded from the Arrow batches Spark already sends — no
row-by-row conversion, and nothing lands on the BEAM heap.

**It is unbounded, and so are `collect/2` and `to_arrow/2`.** To take part of a result, bound
the plan rather than the action — which is Spark's own idiom, `df.limit(n).collect()`:

```elixir
{:ok, frame} = session |> Latu.range(10_000) |> Latu.limit(3) |> Latu.to_explorer()

3 = Explorer.DataFrame.n_rows(frame)
```

Explorer has the same split: `DF.head/2` narrows the frame and `DF.collect/1` runs it. What
neither library does is take a row count as an argument to the action.

### Down without holding it all: `stream/2`

```elixir
total =
  session
  |> Latu.range(1_000)
  |> Latu.stream()
  |> Stream.map(&Explorer.DataFrame.n_rows/1)
  |> Enum.sum()

1_000 = total
```

One Explorer frame per Arrow batch, decoded as it arrives, and stopping early releases the
execution on the server. This is the shape for "larger than memory, but I only need a running
total". It raises rather than returning a tuple, since an enumeration has nowhere to put an
error.

### Up: `create_dataframe/3` takes an Explorer frame

```elixir
local = Explorer.DataFrame.new(city: ["Melbourne", "Hobart"], pop: [5_200_000, 250_000])

{:ok, cities} = Latu.create_dataframe(session, local)

{:ok, [%{city: "Melbourne"}]} =
  cities |> Latu.filter(greater(:pop, 1_000_000)) |> Latu.collect()
```

Explorer writes the same Arrow IPC stream format Spark reads, so this is a dump and a send. Big
enough frames escalate to session artifacts automatically, which is PySpark's own behaviour.

One cost worth knowing: below the server's threshold the Arrow bytes travel **inside the plan**,
so the frame — and every frame derived from it — retains them, and they are re-sent on every
action. A local frame that is small but not tiny is the one that sits in your memory.

### Where to put the boundary

The division that works: **Spark for the scan, the shuffle and the join; Explorer for the last
mile.** Aggregate on the cluster until the result is small, then bring it down and stay in
Elixir for plotting, `Nx`, or a Livebook table. `Latu.agg/2` before `Latu.to_explorer/2` is the
whole pattern; nothing stops you doing it the other way round, so this is the one place to be
deliberate — `Latu.count/1` costs one round trip and tells you what you are about to pull.

## Why isn't Latu an Explorer backend?

Explorer's own README lists remote backends, Spark included, as forthcoming — so this is a fair
question, and the answer is that they are different contracts rather than that it would be hard.

**The API is Spark's vocabulary, by a recorded precedence.** An Explorer backend has to present
Explorer's API, which is dplyr's and Polars': `mutate`, `summarise`, `arrange`, `discard`. Latu
resolves every naming question as Spark > Elixir > Polars > dplyr, so it says `with_columns`,
`agg`, `sort`, `drop` — and a user who knows Spark can guess them. Both are defensible; they
cannot both be the same library.

**The query DSL cannot carry Spark's expression surface.** Explorer's macros support a bounded
set of `Explorer.Series` operations. Spark has ~500 functions, windows, `MergeInto`, subqueries
and SQL parsing, and Latu's answer is expressions-as-values precisely so that surface stays
ordinary Elixir. A macro would have to grow a case per feature.

**A backend is in-process; a session is not.** An `Explorer.Backend` is a local computation.
Latu hands out a gRPC channel and server-side resources with lifecycles — `disconnect/2`,
`release_session/2`, `with_checkpoint/3`. That belongs in the caller's hands, not behind a
uniform local API.

What Latu *does* implement of that seam is the useful part: the Arrow boundary, in both
directions, with no conversion layer in between. `docs/decisions.md` has the argument at length.

## Things that are deliberately not here

**No `Explorer.Series`, no `pull/2`, no lazy/eager distinction** — covered above, each for its
own reason.

**No ADBC.** Explorer reaches databases through
[ADBC](https://github.com/elixir-explorer/adbc); Latu reaches them through Spark's own JDBC
sources, which means the cluster connects rather than your VM. The cookbook has the recipe.

**MLlib and structured streaming** are separate packages, for reasons
[`docs/deviations.md`](../deviations.md) and `Latu`'s moduledoc give.

## Where to go next

  * [Quick start](quick-start.md) — the tour, in ten minutes
  * [Cookbook](cookbook.md) — recipes, including the streaming and JDBC ones
  * [Coming from PySpark](from-pyspark.md) — if you know Spark as well
  * [Cheatsheet](../cheatsheet.cheatmd) — the whole surface, one line each
  * [`docs/deviations.md`](../deviations.md) — every departure, and why
  * [`usage-rules.md`](../../usage-rules.md) — the rules that are not guessable from the names

```elixir
{:ok, _closed} = Latu.disconnect(session)
```
