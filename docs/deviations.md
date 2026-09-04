# Where Latu differs from PySpark

Latu follows PySpark unless there is a reason not to. Naming precedence is **Spark > Elixir >
Polars > dplyr**: Spark's own spelling wins, and Latu only reaches further down the list when
Spark has no name to follow. Every entry below is a place Latu does differ, with why. Rationale
in full is in `docs/decisions.md`.

Behaviour differences — where Latu produces a different *plan* or a different *result* — are
marked. Everything unmarked is a rename, and the wire format is identical.

## Forced by Elixir

### `df.alias("a")` → `Latu.as/2`

`alias` is an Elixir special form. `def alias/2` compiles and works qualified, but a module
exporting it cannot be imported. `as` is Spark's own Scala spelling.

### `col.alias("x")`

none — name it where it is projected: `select(df, x: expr)`, `agg/2`, `with_columns/2`,
`observe/3`. Every projecting verb takes a keyword list, so a free-standing alias had one job and
one name — and that name was the only one `Latu` and `Latu.Column` shared, which made importing
both a trap. `Latu.Plan.as/2` is the bare alias for a hand-built plan.

### `df.show()` → `show/2`, `show!/2`

An action, so it returns `:ok` or `{:error, _}`, like `File.write/2`. `tap(df, &Latu.show!/1)`
covers mid-pipe use.

### `df["id"]`, `df.id` → `col/2` — `Latu.col(orders, :id)`

Elixir has no attribute access on a struct field, and `df[...]` would need `Access`, which reads
as a lookup rather than a reference. `Latu.Column.col/1` stays the untagged form.

### `col.cast(DoubleType())`

`cast/2` with a type name — `cast(:x, "double")`. `Cast.type` and `Cast.type_str` are a oneof and
PySpark sends the string for a string. Latu has no client-side type model, so the string is
the type; `parse_ddl_type/2` exists for the two relations whose proto takes nothing else.

### `col != other` → `not_equal/2`

Not a deviation in the plan — PySpark also compiles this to `not(a == b)`, since Spark has no `!=`
function. Named because Latu does not shadow `Kernel`'s operators, and never will (`CLAUDE.md`).

### `~col`, `col & other` → `not_/1`, `all/1`, `any/1`

`not`, `and` and `or` are Elixir **operators**: `def not(a, b)` is a syntax error, so not even a
qualified call is possible — hence the underscore. Not the same mechanism as `alias`,
which is a special form and *can* be defined. Never shadowed (`CLAUDE.md`).

### `F.when(c, v).otherwise(x)` → `when_/2,3` and `otherwise/2`

`when` is an **operator**, not a special form: unlike `alias` it is a *syntax* error to define, so
not even a qualified call is possible. The plan is identical: one `when` call whose arguments are
the branches followed by the else value.

## Ergonomics over fidelity

Where Spark's API and idiomatic Elixir disagree, Elixir wins.

### `spark.sql("... {df} ...", df=df)` → `sql/3` with `views: [name: df]`

PySpark formats the query string, substituting a generated `_pyspark_connect_temp_view_<uuid>` for
each `{df}`. Elixir has no f-strings, and the generated name is an artefact of Python's API: Latu
takes the name, you write it in the SQL, and no text is rewritten. Neither client registers
anything on the server.

### `withColumn(name, col)` and `withColumns({...})` → `with_columns/2`

Both compile to the same `WithColumns` relation — Spark has no singular one. Python needs two
methods because it has no keyword-list literal; Elixir does.

### `toDF(*names)` and `withColumnRenamed(old, new)` → `rename/2`

A plain list renames positionally (`ToDF`), `{from, to}` pairs rename by mapping
(`WithColumnsRenamed`). `toDF` means nothing outside Python.

### `distinct()` and `dropDuplicates(cols)` → `distinct/1,2`

One `Deduplicate` relation, so one verb.

### `union`, `unionByName`, `intersect`, `intersectAll`, `subtract`, `exceptAll`

`union/3`, `intersect/3`, `except/3` with `all:` and `by_name:`. One `SetOperation` relation with
three flags. Spark's six method names disagree about `all`: `union` keeps duplicates while
`intersect` and `subtract` drop them, with nothing in the name to say so. Latu's defaults match
each un-suffixed method, so `union/3` still keeps duplicates. `except` is Spark's own Scala name,
and SQL's; PySpark calls it `subtract`.

### `repartition(n)` and `coalesce(n)`

`repartition/3` with `shuffle:`, plus `coalesce/2`. One `Repartition` relation. Both spellings
kept, since `coalesce` is the well-known one.

### `how="left_outer"`, `"outer"`, … (ten spellings) → `how: :left`, `:full`, … (one each)

Spark's `outer` means *full* outer, which is ambiguous, so it is deliberately absent. Atoms, since
protobuf enums decode to atoms anyway.

### `join(on=...)` accepting either shape → same, never normalised

Names go to `using_columns` and collapse the join column; a condition goes to `join_condition` and
does not. Different output schemas, so `on:` dispatches on the argument.

### `orderBy("a", ascending=False)` → `sort(df, desc(:a))`

One way to say a direction, not two. PySpark's keyword form cannot mix directions across keys
without a list of booleans.

### `groupBy(...).agg(...)` → `group_by/2` returns `%Latu.GroupedData{}`

`Aggregate` is a single relation holding the group type, the groupings and the aggregates; the
two-call API is a client-side fiction in every Spark client.

### `agg({"id": "sum"})` — the dict form → not accepted

The keyword form names its output (`agg(df, total: ...)`); the dict form cannot, and leaves Spark
to invent `sum(id)`. Reach for `Latu.Column.fun/3` for the unaliased shape.

### `F.lit([1, 2, 3])` → `array/1` — a list is not a literal

Not a deviation: PySpark also compiles a list to an `array` *function call*, so `Literal.Array`
never reaches the wire. Latu refuses the list rather than pretending, and `Latu.Functions.array/1`
is the function.

### `import pyspark.sql.functions as F` → `alias Latu.Functions, as: F`

Aliased, not imported — and it cannot be: `F.quote/1` is Spark's name and `quote` is a special
form, which Elixir refuses to import over. Nine more (`abs`, `ceil`, `floor`, `length`, `max`,
`min`, `round`, `struct`, `trunc`) collide with `Kernel`'s auto-imports. A qualified call is
unambiguous, so Spark's names survive intact. `Latu.Column` stays import-first — twelve operators,
no collisions.

### `F.coalesce(a, b, c)` — variadic → `F.coalesce([a, b, c])` — a list

Elixir has no varargs, and a list composes with `Enum`. Same reasoning as `all/1` and `any/1`.

### `F.round(col, scale=None)` → `round/1` and `round/2`

Two clauses, never a default argument. Spark distinguishes an absent argument from a present one,
and its own defaults are not uniform — `split`'s `limit` defaults to `-1` and is always sent.

### `Column.isNull()` and `F.isnull(col)` → `Latu.Column.is_null/1` only

Spark ships two spellings of the same idea for nine predicates — `isNull`/`isnull`,
`startsWith`/`startswith`, `contains`/`contains`, `rlike`, `like`, `ilike`, `isNaN`/`isnan`,
`isNotNull`/`isnotnull`, `endsWith`/`endswith`. Two of them have the *same* wire name in both.
`Latu.Column` owns all nine and the registry excludes the twins, so there is one way to say each.

### `F.like(str, pattern, escapeChar)` → `Latu.Column.like/3`

The escape character lives on the Column spelling, since that is the one Latu keeps. `like/2` and
`like/3` send different plans, so it is a second clause, not a default.

### `Window.partitionBy(*cols)`, `orderBy(*cols)`

`Latu.Window.partition_by/1,2`, `order_by/1,2` — a list. Elixir has no varargs, and a list
composes with `Enum`. Aliased as `W`, since `order_by` would collide with `Latu.order_by/2` on
import.

### `col.over(window)` → `Latu.Column.over/2`

A `Column` method in PySpark, so it lives with the other ones.

### `F.transform(col, lambda x: ...)` → `F.transform(col, fn x -> ... end)`

An ordinary Elixir anonymous function. Its arity picks how many parameters Spark is given; one to
three, which is Spark's limit. Parameter *names* are ignored by both — Spark names them `x`, `y`,
`z` by position.

### `F.aggregate(col, init, merge, finish)` → `aggregate/3` and `aggregate/4`

Two clauses rather than an optional argument, since the finish lambda either reaches the wire or
does not. Same for `array_sort/1,2`.

### `F.trim(col, trim)` → `trim/2` — same call order

Spark's wire order is `trim(chars, col)`; PySpark's Python order is the reverse of it and Latu
keeps PySpark's, so the column comes first as in every other function.

### `F.log(arg1, arg2)` → `log/1` and `log/2`

One argument sends `ln`, two send `log`, and the first argument means a different thing in each.
Kept for portability; `ln/1` is the unambiguous spelling.

### `F.convert_timezone(sourceTz, targetTz, ts)` → `convert_timezone/2` and `/3`

The optional argument is the *first* one, so it is an arity rather than a `nil` — passing `nil`
would send a NULL literal, which is a different plan.

### `F.rand()`, `F.shuffle(col)` → same, seed optional

A random seed is drawn when omitted, exactly as `Latu.sample/3` does, so the plan is not
reproducible between builds. Pass a seed to fix it.

### `F.window(col, duration, startTime=...)` → `window/4` — pass the slide explicitly

PySpark repeats `windowDuration` as the slide when you give a start time without one. Latu has no
keyword arguments to make that convenience possible, so the four-argument form takes the slide you
mean.

### `Window.rowsBetween(start, end)` — a static

`rows_between/3`, which needs a specification. A frame with neither partitioning nor ordering
means nothing; `W.partition_by([])` covers the global case explicitly.

### `df.collect()` → `[Row]` → `collect/2` → `{:ok, [map]}`

Elixir has no Row, and maps pattern-match. Atom keys by default — bounded by the column names ever
selected — with `keys: :strings` for names out of dynamic SQL. An action, so a tuple, with
`collect!/2` raising. `take/2`, `first/1` and `head/1,2` return the same shapes.

### `df.toPandas()` — unbounded

`to_explorer/2` — an `Explorer.DataFrame` rather than a pandas one, and **unbounded, exactly as
`toPandas()` is**. A rename, not a behaviour difference. Spark bounds a result in the plan
(`df.limit(n).collect()`) and `Latu.limit/2` is that; `collect/2`, `to_arrow/2` and `stream/2`
are unbounded too.

### `df.toLocalIterator()` → `Row`s → `stream/1` → `Explorer.DataFrame`s

One decoded frame per Arrow batch, so batch boundaries stay visible and the columns stay columnar;
rows are one `Explorer.DataFrame.to_rows_stream/2` away. Raises rather than returns, since an
enumeration cannot return an error.

### `df.toArrow()` → one `pa.Table` → `to_arrow/1` → `{:ok, [binary]}`

The raw per-batch IPC streams, deliberately untouched — no decoder, no schema guard, and never
concatenated (each is a complete stream with its own end marker). Assembling a table is the
downstream reader's business.

### `spark.read.format("csv").schema(s).option("header", True).load(path)`

`Latu.read(session, format: "csv", schema: s, header: true, path: path)`. One call over a keyword
list instead of a mutable builder. `:format`, `:schema`, `:path`/`:paths` are Latu's keys; every
other key is a reader option — the shape of PySpark's own per-format readers
(`spark.read.csv(path, schema=..., header=...)`).

### `option("inferSchema", True)` — keys as written

`infer_schema: true` — snake_case atoms, camelCased on the wire. One vocabulary with the rest of
Latu. A string key passes verbatim, the escape hatch for keys no atom spells. Values follow
PySpark's `to_str`: booleans lowercase, numbers stringified, `nil` drops the pair.

### `F.from_json(col, StructType(...))`

a string — DDL or Spark's JSON schema form — or a built expression. No client-side schema model
(`docs/decisions.md`). PySpark sends the StructType as a JSON *string* anyway; Latu takes the
string.

### `df.write.format("csv").mode("overwrite").option(...).save(path)`

`Latu.write(df, format: "csv", mode: :overwrite, path: path, ...)`. One call per terminal method —
`write`, `save_as_table`, `insert_into`, `write_v2` — instead of a mutable builder. All are
actions returning `:ok`, like `show/2`; a write has no payload. Reserved keys aside, every keyword
is a writer option under `read/2`'s rules.

### `df.writeTo(t).using(p).create()` / `.append()` / `.overwrite(cond)` … → `write_v2(df, t, mode: :create, using: p)`

Six terminal methods become one `mode:` option. `:condition` pairs with `:overwrite` only and is
refused elsewhere, since only `overwrite(condition)` takes one.

### `bucketBy(4, "id", "name")` → `bucket_by: {4, [:id, :name]}`

Elixir has no varargs; the tuple keeps the bucket count from reading as a column.

### `createTempView` / `createOrReplaceTempView` / `createGlobalTempView` / `createOrReplaceGlobalTempView`

`create_temp_view(df, name, global:, replace:)`. Four methods, one proto with two booleans — verbs
collapse where the relation underneath is one thing.

### `spark.sql(query, args, **kwargs)`

`Latu.sql(session, query, args)`, and `views:` for the frames. Same eager `SqlCommand`, same arg
binding (a list binds `?`, a map binds `:name`). The `kwargs` form — Python f-string formatting
that registers DataFrames as temp views — is the `views:` option above, with the caller's name
rather than a generated one.

### `spark.catalog.listTables()` → `Table` namedtuples → `Latu.Catalog.list_tables/2` → plain maps

Rows exactly as `Latu.collect/2` spells them: atom keys in the server's own camelCase
(`:tableType`, `:isTemporary`). No client-side row types to maintain or mistranslate.

### `createDataFrame(data, schema=StructType(...))`

`create_dataframe(session, data, schema: "id INT")`. No client-side schema model: the
string goes verbatim — the proto takes DDL or Spark's JSON form — where PySpark ddl-parses it over
an extra RPC. Rows/columns/Explorer frames instead of pandas/numpy; row-map columns sort by key,
PySpark's own rule for dicts.

### `createDataFrame` threshold on `pa.Table.nbytes` → the Arrow IPC stream's byte size

The size Latu can measure without Arrow internals; slightly larger than raw buffers, so Latu
escalates to cached artifacts marginally earlier. Behaviour past the threshold is the same 4.2
chunked-artifact path.

### `cacheTable(name, storageLevel)` → `cache_table/2` — no storage level

The `StorageLevel` proto for one rarely-passed argument; the server default is almost always
right. `Latu.sql/2` reaches `CACHE TABLE ... OPTIONS` if it matters.

### `df.schema` → `StructType` → `schema/1` → `[%{name:, type:, nullable:}]`

No client-side type model in either direction (`docs/decisions.md`). `type` is
Spark's own `simpleString`, and a nested type renders into that string (`array<int>`,
`struct<a:int,b:string>`) rather than into a nested Elixir shape. `columns/1` and `dtypes/1` are
the same answer, narrowed.

### `df.printSchema()` renders client-side → `print_schema/2` prints the server's render

PySpark 4.2 calls its own `StructType.treeString`; Latu has no type model to render from, so it
asks for the `tree_string` analyze arm. Same string, one round trip, and no renderer to keep in
step with Spark's. `tree_string/2` returns it instead of printing.

### `df.persist()`, `df.cache()`, `df.unpersist()` return the DataFrame

**`persist!/2`, `cache!/1`, `unpersist!/2`** return it; `persist/2`, `cache/1`, `unpersist/2`
return `{:ok, df}` — The one to know: what you write as `.persist()` in PySpark is `persist!/1`
here. **Over Connect these are `AnalyzePlan` round trips that can fail** — classic Spark's are
driver-local `CacheManager` calls that cannot, which is why PySpark can return `self` and let an
exception fly. Latu's rule stands (an action returns a tuple), and because there *is* something to
hand back, the tuple carries the frame rather than `:ok`. So `df \ — > Latu.cache!() \ — >
Latu.count!()` pipes exactly as Scala's does. Caching stays lazy on the server either way: a
success means registered, not materialised.

### `persist(StorageLevel.MEMORY_AND_DISK_2)` → `persist(df, level: :memory_and_disk_2)`

Spark's ten level names as atoms, mapped to the five proto flags by one table `storage_level/1`
reads in both directions. An unnamed combination can be read back but not asked for;
`storage_level/1` reports `name: nil` for one.

### `df.storageLevel` → a `StorageLevel` object → `storage_level/1` → a map, with `:name`

Flags as data, plus Spark's name for that combination where it has one — the same "report as data"
rule as `schema/1`.

### `df.explain(True)` and `df.explain(mode="extended")` → `explain(df, mode: :extended)`

PySpark accepts a bool, a mode string in the first position, or a keyword — and refuses two of
them together with `CANNOT_SET_TOGETHER`. One spelling, five atoms, nothing to refuse.
`explain_string/2` returns it where `explain/2` prints, as `tree_string/2` is to `print_schema/2`.

### `df.isEmpty()` — `select().take(1)` → `is_empty/1` — a one-row limit, counted

Same answer and the same one row of work. PySpark's empty projection returns a zero-column Arrow
batch, which is not a thing Latu's Explorer decode path is known to survive, and the question is
not worth asking for a boolean.

### `spark._parse_ddl(...)` — private → `Latu.parse_ddl/2` — public

It earns public surface here that it lacks in PySpark: Latu sends schemas *as strings* (`read/2`,
`create_dataframe/3`), so "does this DDL mean what I think" is a real question with no
`StructType` to answer it. The reverse arm (`_to_ddl`) stays internal — nothing in Latu holds a
schema as JSON.

### `dropna(thresh=n)` → `drop_na(df, min_non_nulls: n)`

The wire field's own name, where PySpark abbreviates. It overrides `:how` exactly as `thresh`
overrides PySpark's, and `how: :any` sends *nothing* — the field has presence and an absent one
already means "every column must be non-null".

### `fillna({"a": 0})` — a dict → `fill_na(df, a: 0)` — pairs

A list of `{column, value}` pairs, so the columns keep their order and a string key works for
names that are not atoms. A bare value still fills every column whose type fits.

### `df.summary("count", "min")` — variadic → `summary(df, ["count", "min"])`

A list, as every other Latu verb takes; a single name is wrapped. Elixir has no variadic call, and
a keyword-list tail here would read as options.

### `df.freqItems(cols, 0.4)` — positional support → `freq_items(df, cols, support: 0.4)`

An option, because it is one, and because the arity would otherwise collide with the two-name
shape of `crosstab/3`. Still 0.01 by default, and still sent either way.

### `df.corr(a, b, "pearson")` — a method string → `corr(df, a, b, method: :pearson)`

Spark has exactly one method, so this is an option only because PySpark's signature has one.
Anything else is refused by name rather than at the server.

### `df.sampleBy(col, {"red": 0.5})` — a dict

`sample_by(df, col, [{"red", 0.5}])`, or a map. A list keeps the order the wire carries; a map is
accepted for the shape people reach for. **Strata are values, not column names**, so they are
strings or numbers — an atom is a column reference everywhere else in Latu and is refused here
rather than guessed at.

### `df.stat.cov(...)` — a `DataFrameStatFunctions` namespace → `Latu.cov/4`

PySpark exposes the eight both ways (`df.cov` and `df.stat.cov`) and Latu has one namespace of
verbs, so they sit beside every other verb.

### `df.checkpoint(eager)` and `df.localCheckpoint(eager, storageLevel)` → one `checkpoint/2` with `local:`

The wire has one command with a `local` field. Two Elixir functions differing by a boolean is the
pair that gets called wrongly, and `local: true` reads as what it is at the call site.

### `df.mergeInto(t, c).whenMatched(cond).update({...})` — a builder per clause

`when_matched(merge, :update, set: [...], on: cond)`. PySpark needs a nested object per clause
because the action is a *method* on it. One call carrying both the match and the action is three
verbs instead of three classes, and the action is an atom like every other closed set in Latu.

### `.withSchemaEvolution()` → `schema_evolution: true` on `merge_into/4`

A chained setter that flips a boolean on an inert struct is an option with extra steps.

### `CachedRemoteRelation.__del__` frees the checkpoint → `release/1`, or `with_checkpoint/3`

**Behaviour.** PySpark's release is a finalizer its own source labels `!!HACK ALERT!!`: it needs a
special unary channel, swallows failures into a warning, and holds a session inside a plan node —
which Latu's layering forbids outright. A `%Latu.DataFrame{}` is inert data with no process to
finalize. So the end of the resource is said out loud: `with_checkpoint/3` is the bracket, and
`checkpoint/2` plus `release/1` is the REPL form. The session still bounds a leak, because the
server drops its cached relations when the session ends.

### `spark.interruptAll()`, `interruptTag(t)`, `interruptOperation(id)`

one `interrupt/2` — `interrupt(session)`, `interrupt(session, tag: t)`, `interrupt(session,
operation_id: id)`. One RPC with an `InterruptType` enum, so one function with the scope as an
option. Three functions differing only in which enum value they send is the set that gets called
wrongly, and the wire has one shape.

### Tags live in a thread-local on the client, mutated by `addTag`/`removeTag`/`getTags`/`clearTags`

tags are a field on `%Latu.Session{}`, set by `Latu.Session.add_tag/2` and `remove_tag/2`; read as
`session.tags`, cleared as `%{session \ — tags: []}` — **Behaviour.** A thread-local in Elixir
would be the process dictionary, and Latu holds no mutable state anywhere else. A session you pass
explicitly carries its tags to every execution built from it, which is the same guarantee without
the process — and it is what makes `Task.async` with a tagged session work, where PySpark's
thread-local does not follow a thread it did not create. No getter and no clearer, because it is a
struct.

### `repr(df)` shows `DataFrame[id: bigint]`

`inspect(df)` shows the plan's spine — `#Latu.DataFrame<range → filter → project(2)>`.
**Behaviour.** PySpark's repr needs the schema and therefore a round trip. `Inspect` cannot do IO,
and a DataFrame that talks to a server when you print it is a trap, so Latu shows what it can know
for free: what you piped. The spine only — a two-input relation ends it — because a tree is
`tree_string/2`'s job. `Latu.Plan.Inspect.chain/2`.

### `df._repr_html_()` is private → `to_html/2`, `to_html!/2`

Spark has no public name for it, so Elixir's slot in the precedence takes it: an action returning
`{:ok, html}`, the same shape as every other action. It is what Kino renders in Livebook.

### `spark.conf.get(k)`, `.get(k, d)`, `.set(k, v)`, `.unset(k)`, `.isModifiable(k)`, `.getAll`

`conf/2`, `conf/3`, `fetch_conf/2`, `set_conf/3`, `set_confs/2`, `unset_conf/2`,
`is_modifiable/2`, `confs/2`. PySpark hangs a `RuntimeConf` object off the session; Latu's session
is a plain struct, so the verbs go on the facade with the noun in the name. **`conf/2` is
`Map.get`** — set value, else Spark's default, else nil (`GetOption`, which PySpark never sends) —
and **`fetch_conf/2` is `Map.fetch`**, PySpark's raising `get` (`Get`). Elixir's lookup idiom wins
over PySpark's default read. `isModifiable` keeps Spark's spelling, following
`is_cached/2`. The setters return `:ok`: the conf lives on the server and the struct does not
change.

### `spark.range(start, end, step, numPartitions)`

`num_partitions:` as a trailing keyword on every arity. Spark's fourth argument is positional, and
`Latu.range(session, 0, 10, 2, 4)` gives a reader no way to tell the step from the partitions. The
keyword is accepted last on `range/2` through `range/4` and explicitly on `range/5`; leaving it
out is an absent proto field, so the server chooses as it always did.

### `spark.stop()` → `disconnect/2`, which closes the channel and nothing else

**Behaviour.** PySpark's `stop()` releases the server session and then closes the connection, and
its own source marks turning the release off as "testing only" — a PySpark session *is* its one
client, so ending one is ending the other. Latu keeps them apart: a clone shares its parent's
channel, and a second `connect/2` can carry an existing `session_id:`, which is how one process
interrupts what another is running. Closing a channel there must not end the session it borrowed,
so `disconnect/2` closes the channel only. `release_session/2` ends a session, and
`disconnect(session, release: true)` does both at once. What nobody releases, the server times
out. `docs/decisions.md`.

## Stricter than PySpark

**Behaviour differences.** Each of these accepts something PySpark accepts, or refuses
something PySpark allows.

### `sc://h/;use_ssl=ture` silently disables TLS → error

Any value but `true`/`false` is a typo, and the failure mode is sending credentials in cleartext.

### `sc://h/;token=` sends a bare `Bearer` header → error

An empty token is never intended.

### `rowsBetween(Window.unboundedFollowing, ...)` encodes, or raises a range error → error naming the side

The wire has one `unbounded` flag and reads the direction from which bound it is. PySpark's
boundaries are plain integers, so putting one on the wrong side sends a valid plan meaning the
opposite. Latu takes atoms and checks the position.

### a token to a remote host over cleartext → error, before any socket opens

The message names `use_ssl=true` and does not echo the token.

### `df.collect()` on an incomplete stream → error

An execution the server never marked complete is refused rather than returned short. Spark's own
`spark-connect-go` truncates here.

### `%NaiveDateTime{}` converted using the client's timezone → Spark's `timestamp_ntz`, no timezone applied

PySpark cannot express a zoneless literal, so it silently uses the client machine's zone. Elixir
has the type, so Latu uses Spark's.

### a Spark literal for any Python value → `ArgumentError` naming `col/1` and `expr/1`

Plan building is local, so it can fail immediately and precisely.

### a set operation over two different sessions → `ArgumentError` naming both session ids

The server's error would not say that this is what went wrong.

### `unionByName(other, allowMissingColumns=True)` on a non-`union` operation → `ArgumentError`

`by_name` and `allow_missing_columns` apply to `:union` only, and the latter needs the former, as
PySpark's signature couples them.

### `from_json(col, schema)` with any expression as the schema → a bare atom is refused

An atom is Latu's column reference, and a column where a schema belongs builds a plan the server
rejects as non-foldable. The string *is* the schema.

### interval and variant columns collect through pandas

year-month and calendar intervals, and variants, are refused, naming the column; cast first.
**Behaviour difference.** Measured by `dev/probe_dtypes.exs`: year-month and calendar
intervals panic inside Polars' NIF, naming neither column nor type, so the guard refuses them
first; a variant decodes only as Spark's internal binary encoding, so it is refused with a message
that says so. A **day-time interval decodes fine**, as `{:duration, :microsecond}`. See
`docs/decisions.md`.

### `printSchema(level=0)` prints the whole tree → `ArgumentError`

The field has presence and PySpark guards it with `if level and isinstance(level, int)`, so its 0
silently means "unset". A depth of zero means nothing; Latu says so rather than ignoring it.

### `fillna({"a": 0}, subset=["b"])` silently ignores the subset → `ArgumentError`

Per-column pairs already name their columns, so a `:subset` beside them can only be a mistake.
PySpark drops it without a word.

### `sampleBy(col, {…})` with an unhashable or wrongly typed stratum → `ArgumentError` naming what a stratum may be

PySpark checks the same three types; Latu's message also says *why* an atom is not one of them,
since an atom means a column everywhere else here.

### `approxQuantile(col, probabilities, relativeError)` validated in the verb

validated in `Latu.Plan`, so a hand-built plan gets the same check. Same rules as PySpark's —
probabilities in 0..1, a non-negative error — one layer lower, because `Latu.Plan` is public and
pure.

### `df.observe(obs, *exprs)` then `obs.get`; `df.executionInfo.metrics`

`observe/3`, then one of the eight `*_with_metrics` twins, which return a `%Latu.ExecutionInfo{}`
carrying **both** kinds of metric. **Behaviour difference.** PySpark fills a mutable `Observation`
from inside the response loop, so *any* action reports. Latu's frames are inert and it holds no
processes, so the metrics come back from the action. Every other action runs the frame and does
not report — as PySpark does when nobody calls `obs.get` — except a plain **write**, which
**raises** and names the twin: a write consumes the frame, so its metrics would be produced and
dropped. `docs/decisions.md`.

### `df.unpivot(ids, values, variableColumnName, valueColumnName)` → `unpivot/3` — ids, then options

Four positional arguments where the last two are required strings reads badly in Elixir, and Latu
takes options everywhere else. **`values:` absent and `values: []` are different messages**, which
the positional form hides behind `None`.

### `df.hint(name, *parameters)` documented as `str, float, int, Column, list`

`hint/3` with Latu's usual coercion: a binary is a string literal, an atom is a column. Not a
behaviour difference. PySpark calls `F.lit` on every parameter and `lit` of a Column returns the
column unchanged (`connect/functions/builtin.py`), so both clients can send either; Latu just says
which is which. A list parameter is refused, as `lit/1` refuses every list.

### `df.tail(n)` returns `List[Row]`

`tail/3` returns `{:ok, [map()]}`, with `tail!/3`. An action, so it follows every other action's
shape, options included.

### `df.to(StructType([...]))` → `to/2` over `parse_ddl_type/2`'s result

`ToSchema.schema` is a `DataType` message with no string form:
`DataTypeProtoConverter.toCatalystType` on 4.2.0 has no `UNPARSED` case. PySpark builds the type
from its client-side type model; Latu has none by decision, so it asks the server to
parse a DDL string and passes the answer straight back. The one generated protobuf struct in
Latu's public surface, and opaque by contract.

### `df.to(schema)` documented as "missing columns lead to failures"

`to/2` fills a missing **nullable** target field with nulls, and only raises for a non-nullable
one. Not a Latu deviation — Spark's own scaladoc is wrong and Latu's docs say what the code does.
`Project.reorderFields` in `basicLogicalOperators.scala`: `if (matched.isEmpty) if (f.nullable)
Literal.create(null, f.dataType) else throw unresolvedColumnError(...)`. DDL declares nullable by
default, so `parse_ddl_type!(session, "a INT")` against a frame with no `a` gives nulls; write
`NOT NULL` to get the error.

### `spark.read.json(df)` / `.csv(df)` / `.xml(df)`, overloading the reader on its argument → `parse/2` with `format:`

The relation is `Parse`; Latu names the verb after it rather than overloading `read/2`, whose
first argument is a session. **PySpark parses a DDL `:schema` client-side**
(`StructType.fromDDL`), the type model Latu does not have, so `:schema` takes a
`parse_ddl_type/2` result as `to/2` does.

### `spark.tvf.<name>(args)` — a fixed set of named methods → `table_function/3`, one generic builder

`Latu.Column.fun/3`'s precedent: one function by name covers `spark.tvf`'s whole surface plus
whatever a Spark release adds, with no wrapper to keep in step.

### `spark.read.changes(table)` → `table_changes/3`

`changes` alone says too little at the top level of a facade, and the relation is
`RelationChanges`.

### `df.repartitionByRange(numPartitions_or_col, *cols)`

`repartition_by_range/3` with `num_partitions:`. PySpark overloads its first argument as an int
*or* a column; Latu keeps the columns in one place and the count in a keyword, as `range/2` does.
(`repartition/3` takes the count positionally because there the count is the common case and the
columns are the option.)

### `whenNotMatched()` exposes only `insert`/`insertAll` by class shape; a wrong pairing is a missing method

`when_not_matched/3` refuses `:update`, `:update_all` and `:delete` by name. Same restriction,
said out loud: the clause tables come from PySpark's three builder classes, and Latu names the
actions the clause does take. A merge with no clauses at all is refused too —
`MergeIntoWriter.mergeCommand` throws `NO_MERGE_ACTION_SPECIFIED`, so the answer is fixed.

### `df.localCheckpoint(storageLevel=...)` is the only way to pass a level

`checkpoint/2` refuses `:storage_level` without `local: true`. `handleCheckpointCommand` reads the
level inside its `if (getLocal)` branch and calls `checkpoint(eager)` with nothing else in the
other, so a level on a reliable checkpoint is dropped in silence. PySpark cannot express the
combination at all; Latu can, and refuses it.

### `df.withMetadata(col, {...})` sends `json.dumps`'s spelling → `with_metadata/3` sends `JSON.encode!`'s

`Alias.metadata` is a JSON *string*: Python writes `{"a": 1}`, Elixir writes `{"a":1}`, and
multi-key order differs on top. Spark parses the string, so neither spelling is more correct.

**Where Latu is deliberately not stricter: a subquery across two sessions.** `join/3` and the
set operations refuse one, because the wire cannot express two sessions in a single plan. A
subquery can: the referenced relation travels inline in `WithRelations`, so the plan is
self-contained and executes. What stays behind is session-scoped state — a temp view the other
frame reads, an artifact behind its local data, a conf set on that session — and Spark's error
names whatever is missing. PySpark checks nothing here either.

## In PySpark, not in Latu

### `df.melt(...)`

Spark's own alias for `unpivot`, kept in PySpark for pandas users. One name is enough, and
`unpivot` is the one Spark's SQL and Scala use.

### `df.asTable()`

A table argument is consumed **only by a Python UDTF** — `UserDefinedTableFunction.__call__` is
the one place PySpark checks for a `TableArg`, and `spark.tvf._fn` refuses one outright. A Python
UDTF needs a Python worker, so it is out of Latu's scope by construction.
`SubqueryExpression.table_arg_options` (partition spec, ordering, single-partition) is unbuilt for
the same reason. SQL's `TABLE(...)` through `Latu.sql/3` is the route that works.

### `df1.select(df2.a)`

A bare reference to a frame outside the plan. Spark refuses it —
`CANNOT_RESOLVE_DATAFRAME_COLUMN`, hoisted or not (`docs/decisions.md`) — so
PySpark's version does not work either. Use a subquery: `Latu.scalar/1`, `Latu.exists/1`,
`Latu.Column.isin/2` over a DataFrame.

## Not in PySpark at all

### `Latu.Column.fun/3`

Any Spark function by name, before a wrapper exists.

### `Latu.join_as_of/3`

An as-of join. The `AsOfJoin` relation is public protocol, but PySpark keeps its wrapper private
as `DataFrame._joinAsOf` and exposes it only through pandas-on-Spark's `merge_asof`. Latu ships it
as a verb, with Spark's own `joinAsOf` spelling.

### `Latu.Plan`

The plan layer is public and pure, so plans can be built and golden-tested with no session.

### `Latu.glimpse/2`

dplyr's and Polars' transposed preview: one `$` line per column with its type and first few
values, which reads where `show/2` wraps. Spark has no such method, so the name comes from the
Polars slot in the precedence. **The transpose is client-side on purpose** — Spark's own
`transpose` requires every non-index column to share a type, so it would refuse exactly the wide
mixed-type frame this is for. And `Rows:` is a *lower bound* unless the sample proved otherwise or
`count: true` was passed, because a row count on a Spark frame is a full scan; dplyr and Polars
already hold the whole frame and pay nothing for it.

### `Latu.error_details/2`

The full server-side cause chain, root cause last. PySpark fetches `FetchErrorDetails` eagerly
inside its exception conversion; Latu makes it a call, because most of what it would give you is
already on the error and an expected refusal should not cost a round trip.

### `Latu.Progress` and the `:progress` option

PySpark registers progress *handlers on the client* and ships a terminal progress bar; Latu takes
a function on the action instead. Same data, no hidden state, and no bar — writing one is four
lines and what it should look like is the caller's business.

### `Latu.status/2`

What the session is running, per operation. `GetStatus` is public protocol; PySpark keeps it
private as `_get_operation_statuses`. Named for what it returns rather than the RPC's verb,
following `get_storage_level` → `storage_level/1`.

### `Latu.clone_session/2`

Fork the session. PySpark's `_clone_session` is a developer API that deep-copies the channel
builder and constructs a whole new client; Latu's clone is a `%Latu.Session{}` **over the same
channel**, because a Connect session is server-side state keyed by an id and not a connection.

### `Latu.release_session/2`

End the server session without closing the channel — which is how you end a clone. PySpark reaches
`ReleaseSession` only through `SparkSession.stop()`.

### `Latu.Session.pin/2`

`server_side_session_id` latching, as a pure function on the struct. `@doc false`: the
transport calls it on every response, and nothing public hands back an id to pin by hand.

### `Latu.union(df, other, all: false)`

SQL's `UNION DISTINCT`. Valid on the server, but no PySpark DataFrame method emits it, so it is
the one set-operation combination with no golden test.

### `Latu.Result.Literal` decodes `specialized_array`

PySpark's `LiteralExpression._to_value` raises `UNSUPPORTED_LITERAL` on it. It is six flat lists,
and refusing a value the server may well send is worse than reading it.

### `Latu.Result.Literal` prefers `Literal.data_type`

PySpark still reads only the `array.element_type` / `map.key_type` / `struct.struct_type` fields
that Spark 4.1 deprecated. Latu reads `data_type` first and falls back, so a literal built the new
way decodes.

### `Latu.conf/2` is `GetOption`

`ConfigRequest.GetOption` — a config value, or nil where Spark has nothing at all. Public
protocol, and PySpark's connect `RuntimeConf` never builds that arm: `get(key, None)` sends
`GetWithDefault` instead, which ignores Spark's own default rather than falling back to it. The
two are different questions and Latu asks both — `conf/2` and `conf/3`.

### `Latu.set_confs/2`

Several configs in one `Set`. PySpark has this as the private `_set_all`, reached only from the
session builder. The proto field is public and the round trip is one instead of many. The proto's
`silent` flag is not exposed: it turns a refusal into a server-side warning, and a refusal Latu
cannot return is not one it offers.

### `Latu.confs(session, prefix: _)`

`GetAll`'s optional prefix, which PySpark never sends. **Behaviour**: the server strips the prefix
off the keys it returns; Latu puts it back, so the keys stay usable with `conf/2`.

### `Latu.Telemetry`

`:telemetry` events on every RPC, retry, reattach, batch and progress message. PySpark has no
equivalent — its observability is a Python logger and a progress bar. The event **names follow
[SparkEx](https://github.com/lukaszsamson/spark_ex)'s** rather than being invented, because a
reporter written for one should work for the other with a prefix change; `[:latu, :execute,
:start\. :stop]` is Latu's own addition, for the query duration the rpc span cannot honestly
report on a lazy stream.

### `%Latu.Retry{}` on the session

PySpark's `DefaultPolicy` is a class you subclass and hand to the client's constructor. Latu's is
a validated struct on `%Latu.Session{}`, defaulted to PySpark's exact numbers, so
`Latu.connect(url, retry: [max_retries: 3])` is the whole of it. Which errors are retryable is not
configurable in either.

### `Latu.Ops`

Opt-in operator shadowing inside a function body. Will not ship — functions and values, no
operator overloading (`CLAUDE.md`).

