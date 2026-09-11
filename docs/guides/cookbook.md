# Cookbook

Short recipes for things people actually do. The [quick start](quick-start.md) is the tour; this
is the reference you come back to.

Every `elixir` snippet here is executed by `mix check.all`, in order, sharing one set of
bindings. Some are not, and each says so where it stands.

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

`Latu.agg/2` takes a keyword list, and the keys are the output column names. There is no
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

**Pass the values when you know them.** Without them Spark runs a separate query to find the
distinct ones. That is a second pass over the data to learn something you already knew.

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

The ids are `0, 3, 6, 9`, gapped on purpose, because that is what separates the two frame
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

`how: :all` keeps every row here because `id` is never null. A row has to be null *all the way
across* to go. And `:min_non_nulls` overrides `:how` rather than combining with it.

Filling has a rule worth knowing before it surprises you: **a fill value only reaches the columns
whose type it fits**, and Spark says nothing about the ones it skips.

```elixir
{:ok, filled} = readings |> Latu.fill_na(-1.0) |> Latu.sort(:id) |> Latu.collect()

[%{id: 1, score: 10.0, team: "red"}, %{id: 2, score: -1.0, team: "red"} | _] = filled
```

`team` is still null there. A number does not fit a string column, so Spark passes it over. A
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

Every action has a `_with_metrics` twin: `count_with_metrics/2`, `write_with_metrics/2`,
`merge_with_metrics/2`. That is how you learn how many rows a write touched without counting
them again.

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

Use `Latu.checkpoint/2` plus `Latu.release/1` when the frame has to outlive one function. In a
REPL it usually does. Nothing frees a checkpoint for you. Latu holds no processes and no
finaliser, so the session ending is the only other thing that bounds it.

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

`Latu.to_explorer/2` is the eager form. It brings the whole result back, so bound the plan when
you want part of it. `Latu.limit/2` is Spark's own way to ask.

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

**A fast query may report nothing at all.** That is the server's timer, not an error, so a
handler must not be where your result comes from. The handler runs **in your own process**,
because Latu holds no process to isolate it in. If it raises, the query fails.

## Interrupting a query from another process

The process running a query cannot cancel it. It is blocked in the query. That is what tags are
for: tag a session, run the work from a `Task`, and interrupt by tag from anywhere.

```elixir
worker = Latu.connect!("sc://localhost:15002", tags: ["cookbook"])

[] = Latu.interrupt!(worker, tag: "cookbook")

{:ok, _} = Latu.disconnect(worker)
```

Nothing was running, so nothing matched. An empty list is the honest answer rather than an
error. With work in flight you get back the operation ids the server cancelled.

**Interrupt rather than killing the process.** A Latu execution is reattachable, so a killed
client leaves the query *running on the server*. It holds cluster resources until the detached
timeout expires.

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

The driver jar has to be on the **cluster's** classpath. Latu ships nothing from your machine,
and `docs/decisions.md` records why. `query:` is Spark's own option for pushing a query down. It
reaches the server because Latu passes through what it does not recognise, rather than
validating a list it would have to keep current.

## Upserting with `merge_into`

A merge is built as inert data and sent by `Latu.merge/2`. The frame is the *source* and the
table is the target. Both are in scope in the condition, so the names need qualifying, and
`Latu.as/2` is what qualifies the source.

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
supports row-level operations. Iceberg and Delta provide them; Spark's built-in sources do not,
so the plan is refused at analysis. Latu builds the same plan either way, which is why the verb
ships.

Clauses apply in the order you add them and **only the first matching clause runs**, so an
unconditional one belongs last. `Latu.merge_with_metrics/2` is the form that tells you how many
rows it touched.

## A supervised session

Latu holds no processes and manages no session lifecycle. That is the library's rule, and it
leaves one job to you: keep a live session around, and rebuild it when the transport dies. A
small GenServer is the whole answer.

> **Not executed.** A supervised session needs a supervision tree and a server that drops, which the guide runner has neither.

```elixir
defmodule MyApp.Spark do
  @moduledoc "Owns one `Latu.Session`, hands out a live copy, rebuilds it when the link drops."
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  # The session callers run their own queries with.
  def session, do: GenServer.call(__MODULE__, :session)

  # Ask for a fresh session, passing the one that just failed. The rebuild happens once even if
  # every caller asks at the same moment: a stale session is rebuilt only while it is still the
  # one this process holds.
  def refresh(stale), do: GenServer.call(__MODULE__, {:refresh, stale})

  # The two error shapes that mean the session itself is gone. `status: 14` is Spark's
  # UNAVAILABLE, and it reaches you here only after the session's retry policy gave up.
  def reconnect?(%Latu.Error{kind: :connect}), do: true
  def reconnect?(%Latu.Error{kind: :rpc, status: 14}), do: true
  def reconnect?(%Latu.Error{}), do: false

  @impl true
  def init(opts) do
    state = %{url: Keyword.fetch!(opts, :url), opts: Keyword.get(opts, :connect, []), session: nil}
    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    {:noreply, %{state | session: Latu.connect!(state.url, state.opts)}}
  end

  @impl true
  def handle_call(:session, _from, state), do: {:reply, state.session, state}

  def handle_call({:refresh, stale}, _from, %{session: current} = state) do
    if current && current.session_id == stale.session_id do
      Latu.disconnect(stale)
      fresh = Latu.connect!(state.url, state.opts)
      {:reply, fresh, %{state | session: fresh}}
    else
      {:reply, current, state}
    end
  end

  @impl true
  def terminate(_reason, %{session: %Latu.Session{} = session}), do: Latu.disconnect(session)
  def terminate(_reason, _state), do: :ok
end
```

To use it, get a session with `MyApp.Spark.session/0`, run your query, and check any error
against `reconnect?/1`. When it says yes, hand the session to `MyApp.Spark.refresh/1` and try
again with the session it returns. Everything else is a real error the query has to answer for.

You might reach for a monitor on the channel process instead, and rebuild when it goes down. Do
not. `session.channel.adapter_payload.conn_pid` is the gRPC adapter's own shape, not Latu's, so
nothing keeps it stable across versions. It is `nil` between a disconnect and the next connect.
The adapter already retries the socket for you, on its own backoff, so a monitor would fire on a
blip it is about to heal. The signal you can trust is an error from a call you made. Match on
`reconnect?/1` and rebuild then.

A rebuilt session is a new session on the server, with a new id. Temp views, cached plans, and
anything else keyed on the old session are gone with it. Register what a fresh session needs
again, or keep that state in a table where a reconnect cannot reach it.

## A supervised query

A streaming query is the other thing that outlives your process, and it needs the same care from
the other side. `Latu.write_stream/2` starts one on the server and hands back a handle. If your
process crashes and restarts, the query keeps running and the handle is gone. Recover it rather
than start a second one.

> **Not executed.** Recovering a query across a restart needs a restart, which the guide runner cannot stage.

```elixir
defmodule MyApp.Rollups do
  @moduledoc "Owns one streaming query. After a restart it recovers the running one, not a copy."
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    state = %{
      session: MyApp.Spark.session(),
      frame: Keyword.fetch!(opts, :frame),
      write: Keyword.fetch!(opts, :write),
      query: nil
    }

    {:ok, state, {:continue, :ensure}}
  end

  @impl true
  def handle_continue(:ensure, state) do
    {:noreply, %{state | query: recover(state.session) || start(state)}}
  end

  # A query survives the process that started it, so after a crash the old one is still on the
  # server. `get/2` hands back a handle with the *current* run id. A remembered run id goes
  # stale when a query restarts from its checkpoint, and the server refuses a command that
  # carries the old one, so recover the handle rather than reuse a saved struct.
  defp recover(session) do
    with id when is_binary(id) <- remembered_id(),
         {:ok, %Latu.StreamingQuery{} = query} <- Latu.StreamingQuery.get(session, id) do
      query
    else
      _ -> nil
    end
  end

  defp start(state) do
    {:ok, query} = Latu.write_stream(state.frame.(state.session), state.write)
    remember_id(query.id)
    query
  end

  # The id is stable across restarts from the checkpoint, so persist it where it outlives the
  # process: a file, a table, your own config. A file is shown here for one moving part.
  defp remembered_id do
    case File.read("priv/rollups.qid") do
      {:ok, id} -> String.trim(id)
      {:error, _} -> nil
    end
  end

  defp remember_id(id), do: File.write!("priv/rollups.qid", id)
end
```

Start it under your supervisor with two options: `:frame`, a one-argument function that builds
the streaming frame from a session, and `:write`, the `write_stream/2` options with a
`:checkpoint_location` set. The checkpoint is what lets Spark resume the query after a restart,
and it is what makes the id worth remembering.

This module does not stop the query when it terminates, on purpose. The query is meant to
outlive a crash, which is the reason to recover it at all. Stop it from somewhere with a longer
life than one process, or on a deliberate shutdown, with `Latu.StreamingQuery.stop/1`. And its
failure is asynchronous: a query can die on the server long after it started, and nothing here
raises when it does. Watch for that with `Latu.StreamingQuery.await_termination/2` in a task, or
poll `Latu.StreamingQuery.exception/1`.

## A function Latu has no wrapper for

`Latu.Functions` wraps about five hundred of Spark's functions, and every Spark release adds
more. When there is no wrapper, call the function by name. `luhn_check` is one Latu does not
wrap, and it is a Spark built-in all the same:

```elixir
{:ok, [%{valid: true, mistyped: false}]} =
  session
  |> Latu.range(1)
  |> Latu.select(
    valid: fun("luhn_check", [lit("79927398713")]),
    mistyped: fun("luhn_check", [lit("79927398714")])
  )
  |> Latu.collect()
```

`fun/3` builds an ordinary column, so it composes with everything else and takes `distinct:
true` where you would write `count(DISTINCT x)`. `expr/1` takes the same call as SQL text, which
reads better once a few operators are in it:

```elixir
{:ok, [%{ok: true}]} =
  session
  |> Latu.range(1)
  |> Latu.select(ok: expr("luhn_check('79927398713')"))
  |> Latu.collect()
```

`Latu.sql/3` goes all the way to a whole query, for when the expression is the least of what you
are writing:

```elixir
{:ok, df} = Latu.sql(session, "SELECT luhn_check('79927398713') AS ok")
{:ok, [%{ok: true}]} = Latu.collect(df)
```

Three rungs, and you climb only as far as you need. `Latu.Column.fun/3` for one call that stays
a column. `Latu.Column.expr/1` when SQL reads more clearly than the builders. `Latu.sql/3` when
the query is the point. `fun/3` is the one you reach for most: latu_ml's whole `Latu.ML.Functions`
is two of them, `vector_to_array` and `array_to_vector`, each a single `fun/3` call that never
needed a wrapper of its own.

## Where to go next

  * [Quick start](quick-start.md). The tour, if you have not taken it.
  * [Cheatsheet](../cheatsheet.cheatmd). Every verb, one line each.
  * [`usage-rules.md`](../../usage-rules.md). The rules that are not guessable from the names.
