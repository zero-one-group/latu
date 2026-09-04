# Using Latu

Latu is a Spark Connect client: it builds a plan locally and the cluster runs it. This file is
the short set of rules that are not guessable from the function names. It follows the
`usage_rules` convention, so a consuming project can sync it into an agent's context.

Full reference is in the docs; every deliberate departure from PySpark is in
`docs/deviations.md`, with why.

## The two shapes

**A builder takes a DataFrame and returns a DataFrame.** It is pure, does no IO, and nothing
reaches the server. `select`, `filter`, `join`, `group_by`, `with_columns`, `sort` — all of them.

**An action runs the query** and returns `{:ok, value} | {:error, %Latu.Error{}}`, with a `!`
twin that raises instead. `collect`, `count`, `show`, `write`, `schema`.

**Every action that can fail has a `!` twin, and that is enforced by a test**
(`test/latu/twins_test.exs`). If you can call `foo/2`, you can call `foo!/2`. Guessing works.

    {:ok, rows} = Latu.collect(df)
    rows = Latu.collect!(df)

## Coercion: what an atom, a string and a function mean

This is the one thing worth reading twice, because the same value means different things in
different positions.

- **An atom is a column reference.** `:price` is the column named `price`.
- **A string is a column *name* where a name is expected, and a *literal* inside an
  expression.** `Latu.select(df, "price")` selects the column; `F.upper("price")` upcases the
  five-character string.
- **`Latu.Column.col/1`** is the explicit column reference, and **`expr/1`** takes SQL anywhere
  an expression goes: `Latu.select(df, [:id, big: expr("price > 100")])`. In `filter/2` a bare
  string is already SQL.
- **A keyword list projects and names**: `Latu.select(df, total: F.sum(:price))`.

There is no macro DSL and no operator overloading. `Latu.Column.greater(:a, :b)`, not `a > b`.

## Import and alias discipline

    alias Latu.Functions, as: F
    alias Latu.Window, as: W
    import Latu.Column

**Call `Latu` qualified** — `Latu.filter`, `Latu.show` — the way `Enum` is. Nothing in it
collides with `Latu.Column` or `Kernel`, so `import Latu` compiles, but the verbs read better
with the module on. There is no `Latu.inspect` and no `Latu.alias`: `Latu.as/2` is the alias.

`lit`, `col`, `expr` and `star` are in `Latu.Column`, **not** `Latu.Functions` — a call to
`F.col/1` is the single most common mistake and there is a test that catches it in this repo
(`test/latu/function_calls_test.exs`).

## observe: the metrics come back from the action

`Latu.observe/3` asks the server for metrics alongside the result. Because a `%Latu.DataFrame{}`
is inert and Latu holds no processes, the metrics come back **from the action**, through a
`*_with_metrics` twin:

    df = Latu.observe(df, :checks, total: F.count(:id))
    {:ok, rows, info} = Latu.collect_with_metrics(df)
    info.observed  #=> %{checks: %{total: 8}}

Every other action runs the observed frame and simply does not report — `show`, `collect`, a
`join` — as PySpark does when nobody reads the `Observation`. The one exception is a plain
**write** (`write`, `save_as_table`, `insert_into`, `write_v2`, `merge`), which **raises** and
names the twin: a write consumes the frame, so its metrics would be produced and dropped.

## Bound the plan, not the action

`Latu.collect/2`, `Latu.to_explorer/2` and `Latu.to_arrow/2` all bring the **whole** result back
and none of them takes a row limit — Spark's `collect` takes no arguments either. To take part
of a result, bound the plan: `df |> Latu.limit(10_000) |> Latu.to_explorer()`, or
`Latu.take(df, 10_000)` for rows. For a result too large to hold, `Latu.stream/2` gives one
`Explorer.DataFrame` per Arrow batch.

## Errors tell you what went wrong

A `%Latu.Error{}` from the server carries Spark's own structured detail — no extra call needed:

    {:error, error} = Latu.collect(df)

    error.kind         #=> :rpc — Spark refused; :protocol means the server answered out of shape
    error.error_class  #=> "UNRESOLVED_COLUMN.WITH_SUGGESTION"
    error.sql_state    #=> "42703"
    error.classes      #=> ["org.apache.spark.sql.AnalysisException", ...]
    error.parameters   #=> %{"objectName" => "`nope`", ...}

**Match on `error_class`, not on the message.** `Latu.error_details/2` fetches the full cause
chain when you need it; it is a round trip, so it is a call rather than automatic.

## Session config: three reads, and they are not the same read

    Latu.conf!(session, "spark.sql.shuffle.partitions")        # else Spark's default, else nil
    Latu.fetch_conf!(session, "nope")                            # else Spark's default, else ERROR
    Latu.conf!(session, "spark.sql.shuffle.partitions", "200")   # else YOUR default

`conf/2` is `Map.get`, `fetch_conf/2` is `Map.fetch`. **`conf/3` overrides Spark's own default;
it does not fall back to it.** For a conf Spark defines but nobody set, `conf/2` and
`fetch_conf/2` give you Spark's default and `conf/3` gives you yours. Spark type-checks your
default against the conf.

`set_conf/3`, `set_confs/2` and `unset_conf/2` return `:ok` — the conf lives on the server, the
session struct is unchanged. Values may be a string, number, boolean or atom.

Two more that read wrong until you know them:

- **`confs/1` is only what the session has *set*.** A conf at its default is absent from it
  while `conf/2` answers for it.
- **`is_modifiable/2` returning false does not mean `set_conf/3` will fail.** It is false for
  every key Spark does not define, and those are stored happily. What it reliably catches is a
  *static* conf, which is refused.

The session also carries the tuning knobs, all `Latu.connect/2` options: `:retry`
(a `Latu.Retry`, defaulted to PySpark's own policy), `:window_size`, `:keepalive` and
`:keepalive_tolerance`.

## Check your work without a server

**`Latu.Plan` is public and pure**, and a session that was never connected still builds plans.
So a pipeline can be checked for nothing:

    session = Latu.Session.from_url!("sc://localhost:15002")
    df = session |> Latu.range(10) |> Latu.filter(Latu.Column.greater(:id, 3))

    inspect(df)  #=> "#Latu.DataFrame<range → filter>"

That is how Latu tests itself — every golden test compares a locally built plan against
PySpark's bytes — and it is the cheapest feedback loop available when generating Latu code.
Build it, inspect it, then run it.

## Naming

**Spark > Elixir > Polars > dplyr.** Latu uses Spark's own spelling wherever Spark has one, so
if you know the PySpark name, snake_case it and you are usually right. Where Elixir forbids it,
the deviation is recorded:

- `df.alias("a")` → `Latu.as/2` (`alias` is a special form)
- `df.show()` → `Latu.show/2`, returning `:ok` like `File.write/2`
- `col != other` → `Latu.Column.not_equal/2`

## Things Latu deliberately does not do

- **It adds nothing to your supervision tree.** No GenServer, no pool, no application callback
  module. A session is a plain struct; nothing is supervised, nothing is mutated. That is why
  metrics come from actions and why a progress handler runs in your process. Two things it does
  hold: the **gRPC channel is a process** (`connect/2` opens it, `disconnect/2` closes it), and a
  **checkpoint is a server-side resource** with `release/1` to free it.
- **It has no client-side type model.** A schema is Spark's own `simpleString`, and a
  `DataType` comes from `Latu.parse_ddl_type/2` — the server parses it.
- **No UDFs in Elixir, and Latu ships no jars.** Spark Connect has no path for client-side
  code. But a function already registered on the session is callable by name —
  `Latu.Column.fun("my_udf", [:price])` — whether a SQL UDF, a Hive UDF or a Java class put it
  there, and `CREATE FUNCTION` through `Latu.sql/3` registers one. Otherwise: SQL expressions,
  and the ~500 built-ins in `Latu.Functions`.
- **Structured streaming and MLlib are separate packages, for different reasons.** Latu hands
  out resources and never keeps them, and the test is whether one can be honestly bracketed: a
  checkpoint can (`with_checkpoint/3`), a streaming query cannot, because it runs after you
  stop looking. ML is excluded on verification instead — its 103 operators' parameters are
  nowhere machine-readable, so nothing could check them the way plans are checked.

## Observability

Latu emits `:telemetry` events; `Latu.Telemetry`'s moduledoc is the list. Two that read wrong
until you know them:

- **`[:latu, :rpc, :stop]` for `ExecutePlan` measures opening the stream, not draining it**,
  because a result is a lazy stream. For how long a query took, use
  `[:latu, :execute, :stop]`.
- **`[:latu, :execute, :stop]` has an `:abandoned` outcome**, for a stream the caller stopped
  reading. Worth alerting on: the execution keeps running on the server until it times out.

A handler runs in the process talking to Spark — yours — so a slow handler slows the query.
Metadata is ids only; the session's token is never in it.

## Stopping a query

`Latu.interrupt/2`, not killing the process. An execution is *reattachable*, so a killed client
leaves the query running on the server until it times out.

    session = Latu.Session.add_tag(session, "report")
    task = Task.async(fn -> df |> Latu.count() end)
    # ... once status/2 shows it running; an interrupt that arrives first matches nothing
    Latu.interrupt(session, tag: "report")
    Task.await(task)
