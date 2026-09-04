# Decisions

One entry per non-obvious choice, oldest first: *what* was decided and *why*, so the same
question is not re-litigated. How a decision was reached stays in git history. When a later
entry reverses an earlier one, the new entry carries the reasoning and the earlier one gets one
line saying so — no corrections stacked under old text.

Format: date — decision — why — where it lives. Milestone tags (M5.2, M12.6) are the grep
handles the source comments use.

---

## 2026-08-28 — Spark 4.2.0 as the target

4.2.0 is the current release and gets a six-month maintenance window (ending ~January 2027,
like every non-LTS release under the quarterly-release SPIP). Re-vendor at 4.5.0, the designated
4.x LTS with the 18-month window. Proto churn 4.0 → 5.0-SNAPSHOT is zero removals, zero
renumberings, additions only, enforced by `buf breaking use: [FILE]` against `branch-4.0`.
Gate any 4.3+ feature off `AnalyzePlan{spark_version:}`, the protocol's only capability
negotiation.

## 2026-08-28 — No macro DSL

An Ecto/Explorer-style `filter(df, price > 100)` macro was considered and rejected. Macro
expressions are not values, so fragments cannot be extracted into functions or folded over a
list without a second construct (why Ecto needs `dynamic/1`). Predicates are built from named
functions with `all/1`/`any/1`, or from `expr/1` (raw SQL, parsed server-side). A third
register, `Latu.Ops` operator shadowing, was sketched and later cut (2026-08-31 below).

## 2026-08-28 — PySpark coercion semantics

A binary at the top level of a name-taking list is a column name; a binary as an operand inside
an expression is a string literal; an atom is always a column reference. The strict alternative
(binaries always literals) was rejected — PySpark's rule costs less muscle memory. Coercion is
plan-layer work (`Latu.Plan.to_expr/1`, `to_name/1`) so it stays testable offline.

## 2026-08-28 — plan_id assigned at construction

`:erlang.unique_integer([:positive, :monotonic])` when a node is built, not during encoding.
`plan_id` encodes node identity, which the server resolves column references against, and
encode-time assignment cannot express identity when the same DataFrame appears twice in a tree.
Cost: golden tests need `Latu.Plan.normalize_ids/1`.

## 2026-08-28 — Explorer for Arrow decoding

A pure-Elixir IPC decoder means FlatBuffers vtables, dictionary encoding, nested types, decimals
and five timestamp variants. Explorer decodes outside the BEAM heap and makes results first-class
in Nx/Kino/Livebook. It is a hard dependency (nothing in Latu returns a result without it) and
MIT, noted in the README. A `Latu.Decoder` behaviour was planned and never needed: M7 shipped
every result shape as a plain function.

## 2026-08-30 — normalize_ids runs on the proto tree, not on an IR

Golden tests compare decoded protos, so normalising there is one generic post-order walk keyed on
protobuf-elixir's `__protobuf__` marker, with no per-relation clauses. It mirrors
`normalize_plan_ids` in `dev/pyspark_oracle.py`, including the second pass that remaps column
references — **keep the two in step**; if they drift, every golden test fails at once.

## 2026-08-31 — ExecutePlan carries no deadline; the channel carries the liveness check

No `:timeout` on `ExecutePlan` (elixir-grpc's streaming default is `:infinity`). PySpark sets no
deadline either; a query may run for hours, and elixir-grpc applies `recv`'s timeout *per
message*, so any value here would be an inactivity timeout rather than a deadline. `AnalyzePlan`
keeps `session.timeout`, a single round trip where a deadline means what it says.

Liveness is the connection's: `keepalive` plus `keepalive_tolerance`. Gun reads the tolerance
with `map_get/2` inside a guard, so when it is absent Gun pings forever and never closes — set it.
Bounding a query is the caller's `Task`; the gRPC stream process is linked to whoever consumes
it, so `Task.shutdown` cancels the RPC and leaves the channel usable.

Two guards turn silence into an error: a batch carrying `chunk_index` is refused (Latu never
requests result chunking), and `start_offset` is checked against rows seen, as PySpark does.
Unhandled response arms are skipped, not rejected — `schema`/`metrics` sit outside the oneof.

## 2026-08-31 — `show` returns `:ok`, and there is no pipe-through twin

`show/2` prints and returns `:ok | {:error, _}`, `show!/2` raises. PySpark's returns `None`, and
`File.write/2` is the stdlib shape for a payload-free action. A `Latu.inspect/2` returning the
frame was rejected: `Kernel.inspect/2` shares name and arity, so `import Latu` would stop
compiling. `tap(&Latu.show!/1)` covers the pipeline case. Same shape for `print_schema/2`.

## 2026-08-31 — ReleaseUntil goes on the reattach, not on every response

PySpark releases after every response through a thread pool. Latu starts no processes, so that
cadence would be a synchronous round trip per batch. Latu releases at each reattach — the
moment the server buffer has demonstrably grown and a round trip is already being spent. The
server trims to `observerRetryBufferSize` regardless, so the buffer is bounded either way.
`ReleaseAll` fires from the stream's `after_fun`, which runs on completion, early halt and raise.

## 2026-08-31 — No IR: relations are built as protobuf directly

`Latu.Plan` constructs `Proto.Relation` and `Proto.Expression` values itself; there is no
intermediate representation and no encoder. `Join` was the falsification test — three shapes in
one message, an enum, mutually exclusive fields — and a hand-written constructor came out at six
lines plus a seven-line table. What did the work was a data table, not a macro. The rule: **a
registry where the data repeats, plain functions where the structure varies.** A hand-written
constructor also encodes invariants the proto cannot (`join/3` takes one `:on`, so
`using_columns` and `join_condition` can never both be set). SparkEx's 3,896-line
`plan_encoder.ex` is the price of choosing a different IR.

## 2026-08-31 — `as/2`, not `alias/2`, and one spelling per join type

A module exporting `alias/2` cannot be imported ("conflicts with Elixir special forms"). `as/2`
is Spark's own Scala spelling. `join/3` takes seven join types with one spelling each (`:inner`,
`:cross`, `:full`, `:left`, `:right`, `:semi`, `:anti`) against PySpark's ten; Spark's `"outer"`
is absent because it means *full* outer. Enum fields hold the **atom**, not the integer —
golden tests compare decoded structs, and protobuf-elixir decodes enums through `key/1`.

## 2026-08-31 — Literals: Elixir's types choose Spark's, and decimals match PySpark

`lit/1` reads the type off the Elixir value: integers are `integer` or `long` by magnitude,
floats are always `double`, `nil` is NullType (PySpark's choice, not a StringType default), a
`Date` is int32 days. **`%DateTime{}` → `timestamp`, `%NaiveDateTime{}` → `timestamp_ntz`**, no
timezone applied anywhere; PySpark cannot express the zoneless case and converts using the
client machine's zone, which is why the oracle passes an explicit `tzinfo=timezone.utc`.

**Decimals send `precision: 10, scale: 0`** — PySpark's `DecimalType()` defaults, not derived
from the value. Measured: a live server prints `Decimal("1.50")` as `1.50`, so Spark reads the
value string and the two fields are not load-bearing. Deriving them was implemented and reverted
for parity. Latu refuses a value past Spark's 38 digits.

**A list is not a literal.** PySpark compiles `[1, 2, 3]` into an `array` function call over
scalar literals, so `Literal.Array` never reaches the wire; `lit/1` refuses a list and points at
`F.array/1`.

## 2026-08-31 — Operators take Explorer's names, and one string means SQL

`UnresolvedFunction{function_name, arguments}` is the one node behind every operator and nearly
the whole function library, so it gets one constructor (`Plan.fun/3`) and everything else is a
wrapper. Spark's spellings are pinned by fixtures, not reasoned about — which caught **there is
no `!=` function**: PySpark spells `a != b` as `not(a == b)`.

**Spark names none of the twelve operators** (`Column` exposes them as dunders only), so they
take `Explorer.Series`' names: `equal`, `not_equal`, `greater`, `greater_equal`, `less`,
`less_equal`, `add`, `subtract`, `multiply`, `divide`, `remainder`, `pow`. `divide`/`remainder`
could not be Explorer's `div`/`rem` — `Kernel` auto-imports those and `import Latu.Column` would
stop compiling. Everything else on `Column` has a PySpark name and follows it exactly.

`all/1` and `any/1` take a list, fold left (PySpark's `a & b & c`), return one predicate
unwrapped, and return `lit(true)`/`lit(false)` only for an empty list — the identity never
appears when there is something to combine, which is what lets `all(Enum.filter(...))` compose.
`not_/1` carries an underscore because `not` cannot be defined.

**A string passed to `filter/2` is SQL**, PySpark's own exception to the coercion rule; a
constant predicate would mean nothing, so no reading is given up. **`"*"` in a name position is
`UnresolvedStar`**, not a column called `*`. Operators live in `Latu.Column` (L3) and reference
no proto; coercion happens at the verb, so golden tests pin the wire shape rather than the sugar.

## 2026-08-31 — Naming precedence: Spark > Elixir > Polars > dplyr

Spark's own spelling wins; reach further down only where Spark has no name, and record every
deviation in `docs/deviations.md`. Explorer's names for the operators are consistent with this
only because PySpark names none of them; its relational vocabulary (`discard`, `mutate`,
`head`) is dplyr's and is not followed.

**Verbs collapse where the relation underneath is one thing.** No `with_column/3`
(`WithColumns` is the only relation); `distinct/1,2` covers `dropDuplicates`; one `rename/2`
covers `ToDF` (a positional list) and `WithColumnsRenamed` (pairs or a map), as
`Explorer.DataFrame.rename/2` does. `drop/2` splits its argument between `columns` and
`column_names` rather than normalising, as `join(on:)` does.

Presence traps pinned by fixtures: `WithColumns.aliases` is a repeated `Expression.Alias`, not
`Expression`; `WithColumnsRenamed` has a deprecated `rename_columns_map` beside the live
`renames`; `Deduplicate` sets `all_columns_as_keys` and `within_watermark` explicitly, `false`
included. **When PySpark writes a proto3-optional field, it writes it whatever the value.**

`Latu` re-exports none of `Latu.Column`'s functions, so `import Latu` beside `import
Latu.Column` compiles (later made one story: call `Latu` qualified, import `Latu.Column`).

## 2026-08-31 — Sort keys are not expressions, and their nulls default asymmetrically

`Sort.order` is a repeated `SortOrder`, its own message, so `Latu.Column.asc/1` returns a sort
key — a third type on `Latu.Plan` beside `relation()` and `expression()` — and a sort key in an
expression position raises naming `sort/2`. **`asc` means nulls first, `desc` means nulls
last**, read from PySpark: `Column.asc()` is `asc_nulls_first()` and `desc()` is
`desc_nulls_last()`. A flat `nulls: :first` default would have been wrong for every descending
sort and no fixture would have caught it. `orderBy("a", ascending=False)` has no equivalent:
direction is said once, with `desc/1`.

`sample/3` draws a random seed when none is given, as PySpark does; the docstring says the plan
is not reproducible, and fixtures pass a seed. `repartition/2,3` covers `Repartition` and
`RepartitionByExpression`, because Spark has two relations.

## 2026-08-31 — Set operations: three verbs, because Spark's six names disagree about `all`

`SetOperation` is one relation with a type and three flags, so Latu has one verb per type and
the flags are options. `:all` defaults **per operation** to whatever the un-suffixed PySpark
method does — `true` for `:union` (it is `UNION ALL`), `false` for `:intersect` and `:except`.
`except` is Spark's Scala name and SQL's. `allow_missing_columns` without `by_name`, and either
flag on `:intersect`/`:except`, are refused as PySpark's signatures couple them.
`union(all: false)` is `UNION DISTINCT`, which PySpark cannot express — a real server capability,
kept uniform, and the one combination with no golden test.

**Two DataFrames from different sessions raise** (`same_session!/2`, comparing `session_id` so
a pinned and an unpinned session still match); `join/3` uses the same check.

## 2026-08-31 — Aggregation, and the first tagged column reference

`Aggregate` is one relation holding the group type, groupings and aggregates; `groupBy().agg()`
is a client-side fiction in every Spark client, and `%Latu.GroupedData{}` is where Latu keeps
that state. `agg/2` on a plain DataFrame is `GROUP_TYPE_GROUPBY` with no groupings, so
`group_by(df, [])` and `agg(df, ...)` produce the same plan. `select/2` and `agg/2` share one
coercion, `Plan.to_projections/1`. `count/1` on a grouped frame is `count(1)` aliased `"count"`;
on a plain DataFrame (M7) it is the unaliased action, PySpark's own asymmetry.

**The pivot column carries the input relation's `plan_id`** — PySpark builds it as
`self._df[pivot_col]`, a tagged self-reference — while the grouping beside it is untagged. First
real use of `UnresolvedAttribute.plan_id`. `Aggregate.Pivot.values` holds bare `Literal`s.
`pivot` is refused after `rollup`, `cube` or another `pivot`, as PySpark refuses it.

## 2026-08-31 — Column predicates, casts, and the reference that needs no hoisting

Spark's predicate names are camelCase where its function names are not (`isNull`, `startsWith`
beside `contains`, `like`) — per-function, pinned by fixtures. `between/3` composes `(c >=
lower) and (c <= upper)`, as PySpark does; `isin` calls the function `in`, which cannot be an
Elixir name, so PySpark's spelling stays. `Expression.Cast.type`/`type_str` are a oneof; Latu
sends `type_str` and has no client-side `DataType` builder. `try_cast/2` is `EVAL_MODE_TRY`;
plain `cast/2` leaves `eval_mode` at its struct default to match the fixture.

`col/2` tags a reference with the relation it came from. **A self-join is not the case that
needs hoisting**: both branches are the same relation, so the `plan_id` search resolves and
PySpark emits no `WithRelations`. The case that would is a reference to a relation not in the
tree (`b.select(a.id)`) — see M9.1 for what Spark does with that.

**`normalize_ids/1` destroys shared-subtree identity**: it renumbers depth-first with one mapping
entry per original id, so a relation appearing twice gets two ids. The oracle does the same, so
golden tests agree, but a normalised fixture cannot prove a hoisting decision.
`test/latu/references_test.exs` pins the artefact.

## 2026-08-31 — The function library: the Connect client is the source, and the registry covers only uniform shapes

**Read `pyspark/sql/connect/functions/builtin.py`, never the classic client.** The classic one
names Catalyst functions that Connect spells differently — `count_distinct` there is `count`
with `is_distinct` on the wire. Nothing is vendored: `dev/extract_functions.py` reads the copy in
`dev/.venv`, which `dev/requirements.txt` pins to the targeted Spark.

**`Latu.Functions` is aliased, never imported, and keeps Spark's names exactly.** Nine of
Spark's functions collide with `Kernel` (`abs`, `ceil`, `floor`, `length`, `max`, `min`,
`round`, `struct`, `trunc`) and `quote/1` is a special form, so `import Latu.Functions` cannot
compile at all; under `F.` the collision costs nothing and is PySpark's own idiom. The renames
in `Latu.Column` (`divide`, `not_`) were forced by that module being import-first and must not
be generalised here. `when` cannot be defined at all, hence `when_/2,3`.

**The registry generates five uniform shapes and nothing else**: fixed arity, variadic (a list),
one required argument plus a list, an omit-or-append optional tail, and (M6.5) a `default_tail`
whose absent form Spark lacks, so a constant is always sent. Everything else is hand-written,
because "optional argument" is at least five behaviours — omit-or-append (`round`), argument
order reversing (`trim`), a non-nil default always sent (`split`), a mix (`lag`), and an arity
that changes the wire name (`log` → `ln`). An optional argument is two clauses, never a default
argument: absent and present-with-a-default are different plans. The extractor names every
function it refuses, so the hand-written list is enumerated rather than discovered.

**Docs are harvested, not written.** `dev/harvest_docs.py` runs `DESCRIBE FUNCTION EXTENDED`
and writes `priv/function_docs.exs`, checked in and read at compile time. Escaping is the whole
risk: a Spark example containing `#{` is an Elixir interpolation.

## 2026-08-31 — Two namespaces, two access styles, and no operator shadowing

**`Latu.Ops` will not ship.** The maintainer's principle — functions and values, no macros, no
operator overloading — also explains the `defrelation` rejection and the no-DSL decision, so it
is one CLAUDE.md bullet rather than three rejections inviting a fourth proposal.

**`Latu.Column` is imported; `Latu.Functions` is aliased**, and the reason is not `Kernel`. The
namespaces genuinely overlap — Spark names the same thing twice on purpose (`df.count()` and
`F.count(col)`, `df.filter(pred)` and `F.filter(arr, fn)`) — so a flat import cannot exist and
`F.` is disambiguation, as in PySpark. And cardinality: a dozen operators composed by hand are
worth importing; a ~500-function catalogue is browsed by `F.<TAB>`. The split follows PySpark's:
`Latu.Column` is the `Column` methods, `Latu.Functions` is `pyspark.sql.functions`.

**Where Spark offers both spellings, `Latu.Column` wins.** Nine predicates existed twice
(`isNull`/`isnull` …) plus `pow`/`power`; the registry excludes them so there is one way to say
each. `Latu.Column.like/3` and `ilike/3` gained PySpark's escape character so nothing is lost.

**`when_/2` returns a `%Latu.CaseWhen{}`**, an inert struct coerced by `to_expr/1` — the same
treatment as `%Latu.GroupedData{}`, for the same reason: a client-side chain over one wire node.
A `case_when([{cond, value}], else:)` form was rejected as data describing control flow. This is
the house pattern for "a thing Spark models as one node and every client models as a chain".

## 2026-08-31 — Windows: a client-side spec, and Spark's int32/int64 frame asymmetry

There is no window relation: `Expression.Window` rides inside whatever projection it belongs to,
so `Latu.Window` is client-side state, the third struct of that kind. Aliased as `W`, because
`order_by/1` would collide with `Latu.order_by/2`.

**A ROW frame offset encodes as `integer` and a RANGE offset as `long`, for the same number**
(read from `pyspark/sql/connect/expressions.py`), so `lit/1` cannot be reused for a boundary —
it picks width by magnitude. **Zero is the current row**: `rows_between(0, 2)` and
`rows_between(:current_row, 2)` are the same plan. **The wire has one `unbounded` flag and takes
the direction from the side it sits on**, so `:unbounded_following` as a lower bound would encode
a valid plan meaning the opposite; Latu refuses it by position, stricter than PySpark. An
unpartitioned window warns rather than raises (PySpark's behaviour; the one place `lib/` logs).
`row_number()` requires `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, so a ranking
function and an aggregate cannot share one framed spec — encodes fine, refused at analysis.

Name clash: `Latu.Window` is the specification, `Latu.Functions.window/2` is Spark's tumbling
time window. Spark named them both.

## 2026-08-31 — Higher-order functions, and lambda names as the second thing to normalise

A lambda is an ordinary argument: `LambdaFunction` sits in the `UnresolvedFunction`'s argument
list, so `Plan.higher_order/3` is four lines over `Plan.fun/3`. **The Elixir function's arity
picks the parameters**: Spark names them `x`, `y`, `z` by position and ignores the caller's
names; one to three is Spark's limit.

**Lambda variable names are normalised, like `plan_id`.** PySpark generates them from a
process-global counter (`x_0`, `x_1`, `y_2` …), so generating one more fixture rewrote every
earlier one. The suffix cannot be dropped — a nested lambda reuses the position letter and Spark
resolves by name, so the suffix is what stops the inner `x` shadowing the outer. Both sides
assign uniquely at construction and renumber for golden tests, **by creation order, not
traversal order**, so the two hand-written walks cannot drift on visit order.

`registered/0` gained `handwritten/0`: every export is a registry row or a declared hand-written
function, and one that is neither fails the build. The doc harvest covers irregulars too.

## 2026-09-01 — The registry is generated, and the offline check is checked in

`lib/latu/functions/registry.ex` is written by `dev/extract_functions.py --write`; `--check`
fails if it is stale. A generated row sends its parameters in declared order, and comparing every
invocation against its declaration found `convert_timezone`, whose *first* parameter is optional
with no default — it is irregular. Two classifier rules came out of it: a shape with no optional
argument must invoke the function exactly one way, and that branch must pass as many arguments as
the wrapper declares. Every run also fails if a public PySpark function is neither a row nor in
`EXCLUDE`, which is the check that matters at a Spark bump.

**Only sixteen words cannot be an Elixir function name**: `after and catch do else end false fn
in nil or rescue true when __aliases__ __block__`. `not`, `alias`, `if`, `case`, `for` and
`with` can all be defined — what they break is an *import*.

`dev/check_offline.exs` and `dev/standins/` are Claude's pre-flight compile of the modules the
offline container can build. A stand-in carries no protocol implementations, so anything relying
on `Inspect` passes there and fails under `mix`; the harness checks stand-in drift for that
reason. `mix check.all` on a real toolchain remains the authority.

## 2026-09-01 — The fifth shape: an argument Spark always sends, and where its default lives

Where Spark has no absent form, PySpark substitutes a constant and always sends it — and **the
constant is in the body, not the signature**: `mask`'s four parameters default to `None` and
send `"X"`, `"x"`, `"n"` and NULL; `sentences` sends `""` twice; `str_to_map` sends `","` and
`":"`; the `aes_*` family sends `"GCM"` and `"DEFAULT"`. Reading `ast.literal_eval` off the
signature would have sent NULL for thirty-five of fifty-four parameters, each encoding cleanly.
The extractor reads the `x is None` substitution and falls back to the signature only when there
is none. Parameters whose default is computed (`shuffle` and `count_min_sketch` draw a random
seed; the `make_*_interval` trio sends `Decimal(0)`) stay hand-written.

`Kernel` is qualified throughout the generated section of `Latu.Functions`: the registry defines
`length/1`, `count/1`, `abs/1` and `round/1`, which shadow `Kernel`'s inside that module.

## 2026-09-01 — The irregulars that are about shape, and one seed rule for all of them

Each hand-written and pinned by a fixture, because each encodes cleanly when wrong. `trim`,
`ltrim`, `rtrim` reverse their arguments on the wire (Latu keeps the column first and reverses
inside — the docstring says so). `log` sends `ln` with one argument and `log` with two; `ln/1` is
the unambiguous spelling. `lag`/`lead` always send the offset (default 1) and send the fallback
only when supplied — three clauses, no default arguments. `mean` sends `avg`;
`unix_timestamp/0` sends nothing and `unix_timestamp/1` fills in `yyyy-MM-dd HH:mm:ss`.

**One seed rule**: `rand`, `randn`, `uuid`, `randstr`, `shuffle`, `uniform`, `count_min_sketch`
and `sample/3` share `Latu.Plan.random_seed/0` — the seed is optional, a random one is drawn when
omitted, the docstring says the plan is not reproducible. Seedless forms have no golden test by
construction; a unit test asserts exactly that. PySpark's deprecated camelCase aliases are
excluded.

## 2026-09-01 — Forty-six functions that were never irregular, and the last twelve that are

The `kll_*`, `theta_*` and `tuple_*` sketch functions open with `fn = "kll_sketch_agg_double"`
and call `_invoke_function_over_columns(fn, ...)`; reading only literal first arguments made all
46 look irregular. Following a name held in a local is nine lines and every one was an ordinary
row. Also: a leading underscore is PySpark's rebinding convention, not a reordering; and
`lit(decimal.Decimal(0))` is a constant, rendered as `Decimal.new(0)`. Every time the classifier
has been wrong it has erred toward calling something irregular that was not.

Of the twelve real irregulars, `make_timestamp` and friends are overloaded on arity alone and one
name serves all; `call_function` is the one function that is not an `UnresolvedFunction` —
Spark's `CallFunction` node reaches a *registered* function, so it has its own `Latu.Plan`
builder. Latu cannot ship a UDF but can call one somebody registered.

## 2026-09-01 — Results: the schema is opaque cargo, and the guard refuses rather than casts

`ExecutePlanResponse.schema` (a `DataType`, outside the oneof) is latched by
`Latu.Client.Execution` as opaque cargo and emitted as `{:schema, dt}` ahead of the first batch,
so the lazy path can guard before its first decode. `Latu.Result.Schema.check/1` refuses, by
kind, what the Arrow decoder cannot represent — otherwise an unsupported dtype surfaces as
`** (ErlangError) :nif_panicked` naming neither column nor type.

Rules: **refuse, never auto-cast** (a silent server-side cast changes the result to fix a client
limitation; the error names the column and says to cast); **refuse until measured** (a kind
leaves the refused set only on `dev/probe_dtypes.exs`'s say-so); a UDT is judged by its
`sql_type`; **an unknown kind is refused**, so a re-vendor that adds one fails with a name. The
module is internal and proto-licensed in the layering test.

## 2026-09-01 — The eager actions, and M7's scope decisions

`collect/2` is unbounded, matching PySpark; `stream/2` is the escape for a result too large to
hold; `to_arrow/2` returns raw per-batch IPC binaries and **bypasses the decoder and the guard**
on purpose — the bytes are for some other Arrow reader. *(`to_explorer/2` was bounded at 100,000
rows here; reversed 2026-09-04, see "Spark bounds a result in the plan".)*

Facts read from PySpark and pinned: `df.count()` is `agg(count(lit(1)))` unaliased, where
`GroupedData.count` aliases as `"count"`; the result cell is read positionally
(`Result.only/1`), so nothing depends on Spark's column name. **Even an empty result arrives as
one schema-bearing Arrow batch** — PySpark's `to_table` asserts as much — so an empty
`to_explorer` is a typed 0-row frame for free and no DataType → dtype mapping runs at runtime.
`head/1` returns one row or nil where `head/2` returns a list, PySpark's own shapes. `collect`
streams rows out in chunks (`to_rows_stream`, never `to_rows`), atom keys by default, `keys:
:strings` for dynamic SQL.

## 2026-09-01 — check.all: warnings-as-errors moved into the compiler, --force dropped

`elixirc_options: [warnings_as_errors: true]` for every env but prod, and both aliases run an
incremental `compile` instead of `compile --force --warnings-as-errors`, which recompiled the
~7k generated proto lines on every check. What is given up is re-checking files untouched since
their last green compile — the same trust every Elixir project without `--force` extends. CI gets
one `mix compile --force` job as the backstop.

## 2026-09-01 — The probe report: one relaxation, two confirmed panics, one honest refusal

`dev/probe_dtypes.exs` against Spark 4.2.0 / Explorer 0.12 (polars-core 0.52): **day-time
interval decodes** as `{:duration, :microsecond}` and moved to the allowed set; **year-month and
calendar intervals panic** inside the NIF and stay refused; **variant decodes only as Spark's
internal binary encoding** (a struct of two raw binaries) and stays refused with a message that
says to cast to string; **Spark 4.2 refuses TIME literals outright** (`UNSUPPORTED_TIME_TYPE`),
so the guard's `time` allowance is dormant. Re-run the probe at any Spark or Explorer bump.

## 2026-09-01 — M8.1: a schema is a string, and options are one coercion

**Latu has no client-side schema model.** A schema is a string the server parses — DDL or
Spark's JSON form — passed verbatim. Measured from PySpark 4.2: `Read.DataSource.schema` is a
plain string field, and the `from_json` family sends the schema as `lit(schema_string)`. The
reader sends `schema: ""` when unset (PySpark always assigns the proto3-optional field), and a
bare atom where a schema belongs is refused rather than coerced to a column reference.

**Options are one rule, `Latu.Plan.to_options/1`**: snake_case atom keys become Spark's
camelCase (`infer_schema` → `inferSchema`), binary keys pass verbatim, values follow PySpark's
`to_str` (booleans lowercase, numbers stringified, `nil` drops the pair). It returns an ordered
list because the same coercion feeds a `map<string,string>` field and, in the `from_json`
family, a `map` *function call* whose argument order reaches the wire.

**`Latu.read/2` is one call, not a builder chain**: `:format`, `:schema`, `:path`/`:paths` are
Latu's keys and everything else is a reader option — PySpark's per-format reader shape minus the
mutable `DataFrameReader`.

## 2026-09-01 — M8.2: writes are one call each, and the oracle grew a command path

A write is a `Command`, and PySpark's terminal methods both finish the builder and execute it, so
the oracle carries `save`, `save_as_table`, `insert_into` and `v2` — verbatim copies of those
methods minus `execute_command` — and both normalisers walk the `Plan` message rather than
`plan.root`. **One verb per PySpark terminal method**: `write` (path), `save_as_table`,
`insert_into`, `write_v2`, all returning `:ok | {:error, _}` since a write has no payload. Mode
atoms are PySpark-Connect's accepted strings (`:append`, `:overwrite`, `:error`, `:ignore`); an
omitted mode stays `SAVE_MODE_UNSPECIFIED`. V2's six terminal methods become `mode:`, with
`:condition` valid only under `:overwrite`. Command builders on `DataFrame` are `@doc false`
public so golden tests pin the exact command.

Wire facts: `path`/`table` are a oneof (Latu refuses both); `bucket_by` appears only when
`num_buckets > 0`; V1 partition/sort/cluster columns are strings while V2's partitioning columns
are expressions; V2 `table_properties` keys pass verbatim — user-defined names, so no camelCase.

## 2026-09-01 — The write-stall verdict: `--master local[1]`

Write commands intermittently stalled ~60 s in the compose server, and PySpark against the same
container stalled identically, so it was the environment: concurrent write tasks contending in
Docker Desktop's I/O layer, scaling with `local[N]`. The dev server runs `local[1]`, which
eliminates it — the right trade for a correctness fixture, since the tests assert plans and
values, never speed, and `spark.sql.shuffle.partitions=4` still exercises multi-partition
results after a shuffle. Consequence: pre-shuffle results arrive as one batch; the standing rule
"assert totals, never batch counts" makes that a non-event. `dev/probe_writes.{exs,py}` stay
checked in for if it reappears.

## 2026-09-01 — M8.3: sql is a Command, views are one verb, the catalog is a table of relations

**`Latu.sql/3` sends `Command.sql_command`, eagerly** — PySpark's choice, so DDL runs when the
function is called. The response's `sql_command_result` carries a relation back, and the returned
DataFrame roots at it with a fresh local plan_id (`Plan.adopt/1`, PySpark's `CachedRelation`), so
collecting twice does not run the query twice; no result arm falls back to the lazy `SQL`
relation. Args bind parameter markers as `lit/1` literals, never spliced text: a list binds `?`,
a map binds `:name`.

**Temp views are one verb**: PySpark's four `create*TempView` methods are one proto with two
booleans, so `create_temp_view/3` takes `:global` and `:replace`.

**The catalog is one function over a table.** Every catalog operation is a relation that answers
eagerly, so `Plan.catalog/2` maps op atom → proto module with `nil` fields dropped, and
`Latu.Catalog` returns rows exactly as `collect/2` spells them (atom keys in the server's own
camelCase, `:tableType`). Left out, each one `Latu.sql/2` away: `create_table`, function/UDF
listing, partitions, the `get_*` singletons, `cache_table`'s storage level.

## 2026-09-01 — M8.4: local data ships as Arrow, and escalates the way 4.2 actually does

`Latu.create_dataframe/3` is `collect/2`'s inverse. Under `spark.sql.session.localRelation
CacheThreshold` (64 MiB) it sends one Arrow IPC stream in `LocalRelation.data`; over it, 4.2
escalates to **`ChunkedCachedLocalRelation`** — the data chunked into independently valid IPC
streams, each cached as a session artifact by sha-256, five session configs fetched in one
`Config` RPC — not the legacy single-blob `CachedLocalRelation` the design sketched. Latu
implements the 4.2 shape only. The schema stays a string (the proto takes DDL or JSON verbatim),
and empty data plus a schema is a schema-only relation needing no connection.

`Client.cache_artifacts/2` does one `ArtifactStatus` round (re-sending identical data uploads
nothing) then one `AddArtifacts` client-streaming call: blobs ≤ 32 KiB pack into `Batch`
requests, larger ones stream as `BeginChunkedArtifact` plus crc'd chunks. Latu compares the IPC
byte size where PySpark compares `nbytes`, so it escalates marginally earlier. Input shapes: row
maps (columns sorted by key, PySpark's rule for dicts), column data (a keyword list keeps order, a
map sorts), or an `Explorer.DataFrame`. Rows must be uniform. **A data-bearing golden fixture
cannot exist** — PyArrow's IPC bytes are not Explorer's — so the oracle carries a schema-only
fixture and the integration suite is the authority for the data path, with escalation exercised
by lowering the threshold via `SET`.

## 2026-09-01 — M9.1: PySpark hoists subqueries, not column references, and the server agrees

Measured by `dev/probe_references.py`.

**A bare cross-frame column reference is not hoisted, by anyone, and hoisting cannot rescue
it.** `LogicalPlan._collect_references` collects `SubqueryExpression`s and nothing else, so
`b.select(a.id)` goes on the wire as a plain `UnresolvedAttribute` with a dangling `plan_id`.
Spark's `ColumnResolutionHelper` searches from the operator's own children *downward*, so a
relation hoisted into `WithRelations.references` above it is never found: the hand-hoisted plan
fails with the same `CANNOT_RESOLVE_DATAFRAME_COLUMN`, and Spark's own message calls the pattern
illegal. **Latu matches PySpark: no hoisting of bare references**, and `cross_frame_col` pins
parity in the failure case. What `WithRelations` is for is subquery expressions and `sql` with
DataFrame views.

**Hoisting is per node, at construction, and the wrapper becomes the frame's identity** — no
whole-tree rewrite pass of the kind SparkEx has. **References propagate through the whole
expression tree** (`F.abs(one.scalar() * -1)` round-trips), so whatever carries them in Latu has
to survive every expression combinator. A referenced frame already in the tree is hoisted anyway,
as PySpark does; the plan works either way.

## 2026-09-01 — M9.2: a subquery is an expression that carries its relations

`Latu.Subquery` is the carrier — the built proto plus the relations to hoist — because
`SubqueryExpression` sends only a `plan_id` and no proto field can hold the relation. It is an
inert struct coerced at the point of use, `Latu.CaseWhen`'s precedent. A builder returns the
carrier only when there is something to carry, so plans without a subquery are unchanged. Every
coercion point drains and every expression builder unions what its arguments carried; eight
relation constructors hoist into `WithRelations`. A sort key is a carrier too.

**A struct's map order is not the proto's field order.** `normalize_ids/1` decomposes with
`Map.from_struct/1`, and `references` sorts before `root`, while the oracle walks field order.
`walk/3` has a `WithRelations` clause ahead of the generic one; any message holding two relations
whose names sort against their field numbers needs the same (`SameSemantics`, M10.2). The oracle
needed the mirror fix: `SubqueryExpression.plan_id` is a plain `int64` where
`UnresolvedAttribute.plan_id` is `optional`, so `HasField` raises on it.

One deliberate divergence: a subquery *inside* an `:in` subquery's values is collected too.
PySpark never looks there and would send a plan that cannot resolve.

## 2026-09-01 — M9.3: the subquery API, and the session question it raises

`Latu.scalar/1`, `Latu.exists/1` and a DataFrame clause on `Latu.Column.isin/2`, all PySpark's
names. `F.exists/2` (higher-order) and `Latu.exists/1` (subquery) are different things because
Spark's are. A struct on the left of `isin` sends its children, so a two-column subquery matches.

**`table_arg` is plan-layer only, permanently.** The only consumer PySpark has is a Python UDTF,
out of scope by construction; `spark.tvf` refuses a `TableArg` outright. SQL's `TABLE(...)`
through `Latu.sql/3` is the route that works.

**No same-session check on a subquery, deliberately.** The referenced relation travels inline in
`WithRelations.references`, so a plan built from another session's frame executes correctly;
what does not travel is session-scoped state (a temp view, a cached artifact, a conf), and that
failure is Spark's own and names the missing thing. PySpark checks nothing here either.

## 2026-09-01 — M9.4: an unresolvable reference normalises to −1, and Latu does not pre-empt it

Both normalisers map a `plan_id` that resolves to no relation to `-1`, so the one fixture that
contains one says "points at nothing" instead of carrying PySpark's raw counter value. The
server's own answers are integration tests: `CANNOT_RESOLVE_DATAFRAME_COLUMN` for a cross-frame
reference, `AMBIGUOUS_COLUMN_REFERENCE` for a self-join reference, and `alias` fixing the latter.

**Latu does not refuse a cross-frame reference locally.** It would trade a measured server
error whose message names the pattern for a client-side predicate never measured against the
analysed plan Spark searches, and it would put Latu in the business of resolving references —
the plan layer validates shape, the server resolves names. The `:pending` tag machinery went
with it: with nothing tagged, an excluded tag is a trap.

## 2026-09-01 — M9.5: a view is a name you choose, not one PySpark invents

`Latu.sql(session, query, views: [orders: df])` hoists the frame as a `SubqueryAlias` in
`WithRelations.references` and the query names it. PySpark's connect client generates a
`_pyspark_connect_temp_view_<uuid>` and rewrites the query string; Elixir has no f-strings, and
the generated name is an artefact of Python's API. Neither client creates a server-side view, so
nothing needs cleaning up. The third argument reads as bindings *or* options: a list binds `?`,
a map binds `:name`, a keyword list is options (`views:`, `args:`) — `Keyword.keyword?/1` is the
whole dispatch.

## 2026-09-01 — A write command has nowhere to hoist a reference

A command is not a relation, so there is no `WithRelations` to put a subquery's references in;
the carrier would have gone into a repeated `Expression` field and failed at encode. `grounded/2`
refuses it where the argument still has a name and says to collect the value and pass a literal.
Whether the server could resolve a subquery in `write_v2`'s `condition:` if the references rode
on the input relation is unmeasured.

## 2026-09-01 — M10 is not the Geni port, and the AnalyzePlan surface is why

Geni wrapped live JVM objects, so its only way to test `cube` was to run it and count rows
against a 13,580-row Kaggle parquet. Latu's golden fixtures already prove its plan is
byte-identical to PySpark's, and identical plans get identical answers, so porting those
assertions tests *Spark*. What the fixtures cannot see became the milestone: **a fixture proves
the bytes match, not that the server accepts the plan** (→ the resolve sweep, M10.3); Geni's
dominant assertion is `columns`/`dtypes` and Latu had no AnalyzePlan surface beyond
`spark_version` (→ M10.1, M10.2); Geni's suite runs on data with nulls (→ `na`/`stat` with a
nulls-bearing fixture, M10.4). The idiom layers (`clojure_idioms`, `numpy`, `pandas`,
`:kebab-columns`, `cut`/`qcut`) are a feature proposal, not tests, and lose to the naming
precedence. Geni's `docs_test.clj` — every public var has a docstring — was taken.

## 2026-09-01 — M10.1: a schema is reported as data, with Spark's own name for each type

The mirror of M8.1: a schema Latu **reports** is `[%{name:, type:, nullable:}]` where `type` is
Spark's `simpleString`, nested types rendering into the string (`array<int>`,
`struct<a:int,b:string>`). No `StructType`, no client-side type model. `simple_string/1` is
transcribed from PySpark's `sql/types.py`, and an integration test cross-checks every column
against Spark's own `typeof` — the server is the oracle for the renderer. `char`/`varchar`
never reach a query's output schema (Catalyst rewrites them to string and keeps the spelling in
metadata) but do come back from a DDL parse.

**`print_schema/2` uses the `tree_string` arm**; PySpark renders client-side from its type model,
which Latu has not. Same string, one round trip. A `level` must be positive (PySpark's `level=0`
silently means unset). All fifteen AnalyzePlan arms share request and response arm names and a
one-field response, so `Client.analyzed/2` is one generic unwrapper over a table.

## 2026-09-01 — M10.2: the rest of AnalyzePlan, and why `cache!` is the one that pipes

**`persist`, `cache` and `unpersist` return the frame from their `!` forms.** Classic Spark's
`cache()` is a driver-local call that cannot fail, which is why it returns `this`; over Connect
it is an `AnalyzePlan` round trip and can. So the tuple stays: `persist/2` → `{:ok, df}`,
`persist!/2` → `df`. The round trip is analysis, not execution — nothing is materialised, pinned
by a test that persists a frame whose execution would raise.

A storage level is one of Spark's ten names, `:memory_and_disk_deser` by default;
`storage_level/1` reports the five flags plus Spark's name where one fits. `Latu.parse_ddl/2` is
public where PySpark's is private: Latu sends schemas as strings, so "does this DDL mean what I
think" is a real question. `is_empty/1` is a one-row limit, counted — PySpark's empty projection
returns a zero-column Arrow batch the decode path is not known to survive. Wire facts:
`explain()` sends `EXPLAIN_MODE_SIMPLE` explicitly; `unpersist`'s `blocking` is always sent;
`persist`, `unpersist` and `get_storage_level` carry a bare `Relation` where every other arm
carries a `Plan`.

## 2026-09-01 — M10.3: the resolve sweep, and documentation as a test

**`dev/probe_functions.exs` asks the server to analyse a call to every wrapped function**
(`Latu.schema/1`, not `collect`), trying type profiles until one analyses and retrying window
functions inside an `OVER`. The report is the deliverable; `dev/function_args.exs` is the
checked-in record, and `test/latu/function_sweep_test.exs` asserts offline that the record still
covers the exported surface exactly — so a Spark bump that adds or removes a function fails in a
second. Deliberately no 578-round-trip integration test; re-run the probe at a version bump.

Findings: **no bug in the function surface** — every non-excluded name/arity pair resolves.
`product`, `timestamp_add` and `timestamp_diff` resolve despite being absent from `SHOW
FUNCTIONS`; only `unwrap_udt` fails, for want of a UDT column. **The TIME family is unusable on
4.2**, functions and literals alike. `posexplode` and `inline` cannot sweep because a generator
expanding to several columns cannot sit in one aliased projection. Exclusions are keyed by name
*or* `{name, arity}`, because `make_timestamp` resolves at 6 and 7 and only its short forms are
refused. Each exclusion group carries its reason in the probe.

**Every public function in a documented module carries a `@doc`**, as a test. The first run
found 168 gaps, 141 of them the second arity of every `optional_tail` row — in a generated
library a gap is a shape, not a forgotten function.

## 2026-09-01 — M10.4.1: the `na` family, and a fixture with holes in it

`fill_na/3`, `drop_na/2`, `replace/3`. **`how: :any` sends nothing**: `min_non_nulls` has
presence and absent already means "every column", `how: :all` sends `1`, an explicit threshold
overrides both. `min_non_nulls:` rather than PySpark's `thresh:` — the wire field's own name. A
per-column fill with a `:subset` is refused: PySpark silently overwrites the subset, and pairs
already name their columns.

**`lit/1`'s rule is not universal.** `NAFill` takes bool, int, float and
string only and sends an integer as a **`long`** (the server refuses an int32 by name); `replace`
converts every non-boolean integer to a **double** to keep both sides of a pair the same width.
**Check the literal rule at every new relation that carries a literal.** The type is the filter,
not an error: `fill_na` fills only columns whose type matches the value, so filling a string
column with a number does nothing, quietly — Spark's rule, said in the docstring.
`fixtures/measurements.parquet` carries a row that is null all the way across, so `how: :all`
drops something.

## 2026-09-01 — M10.4.2: the `stat` family

Five lazy verbs (`summary`, `describe`, `crosstab`, `freq_items`, `sample_by`) and three actions
(`cov`, `corr`, `approx_quantile`) — PySpark's own split, since those three collect internally.
`Latu.Plan` validates `approx_quantile`'s ranges, because a hand-built plan deserves the same
message. A stratum keeps `lit/1`'s magnitude rule (PySpark builds it with `F.lit`) — the third
literal site in the area, third answer — and strata are values, so an atom is refused with a
message saying why. `freq_items`' support and `corr`'s method are always sent (PySpark fills them
client-side). `approx_quantile/4` keeps PySpark's asymmetric return: one name gives a flat list,
a list of names a list per column.

**`cov` and `corr` read a null as zero; `approx_quantile` ignores it.** Spark's
`StatFunctions.calculateCovImpl` and `calculateCorrImpl` wrap each column in
`when(isnull(col(c)), lit(0.0))`, so `Latu.cov/3` and `F.covar_samp/2` give different answers on
the same data and neither is wrong. Stated in all three docstrings and pinned by a test asserting
the disagreement. The plan was right; the server semantics were not guessable.

## 2026-09-02 — Spark ML is a separate library, and what Latu owes it

`latu_ml` ships separately, and the reason is **verification**, not protocol: ML rides the same
twelve RPCs (`MlCommand` is `Command` field 17, `MlRelation` is `Relation` field 300), but
Latu's central instrument is the golden fixture and ML has no such oracle — `Fetch` is remote
method invocation against a ~32-entry server-side allowlist rather than a plan; the surface is 42
estimators and 61 transformers addressed by Java class name; `MlParams` is
`map<string, Literal>` with no machine-readable param metadata anywhere in the protocol, so 103
operators' parameters would be hand-transcribed and nothing would notice drift. A package whose
claim is "checked against PySpark's own plans" cannot contain a large surface checked against
nobody. *(This entry originally argued from resource ownership — `fit` allocates a server-side
`ObjectRef`. That premise was falsified by `checkpoint/2`; see the M13.5 entry.)*

**What Latu owes `latu_ml` is a seam, not ML code**, and both landed at M11.1:
`Latu.Plan.plan_id/0` is public (one allocator for plan ids), and `Client.execute/2` returns a
typed `%Latu.Client.Execution{}` rather than dropping every command arm but SQL's. `latu_ml`
depends on Latu as an ordinary library user.

**Package `latu_ml`, namespace `Latu.ML`** — Spark nests (`pyspark.ml`) and Elixir's first-party
companion convention nests (`phoenix_live_view` → `Phoenix.LiveView`); the flat `LatuML` shape
is for third-party extensions. **`latu` must never define any module under `Latu.ML`** — two
applications defining one module is a compile-time conflict.

## 2026-09-02 — How `observe` reports its metrics, and the scope that follows (M11.1)

**Latu returns observed metrics from the action; PySpark mutates a handle.** PySpark fills an
`Observation` object from inside the response loop and stashes `executionInfo` on the DataFrame
— both mutable state, unavailable to inert frames and a library holding no processes (an Agent or
ETS-backed `Observation` was refused for the same reason). So the metrics ride back from
`*_with_metrics` twins — `collect`, `count`, `to_explorer`, `write`, `save_as_table`,
`insert_into`, `write_v2`, and `merge` (M11.3) — each with a `!` form. Rejected: a twin per
action (thirty-odd functions, growing with every new server output), and one `Latu.run/2`
returning the whole struct (invents a concept Spark has no name for).

**Naming: `*_with_metrics`, not `*_observed`.** Spark compounds with `With` throughout
(`zipWithIndex`, `sortWithinPartitions`), and `collect_observed` parses as "collect the observed
rows". The write path decided the shape: `observe` on a write is the canonical use, and a write
has no rows to return. **A plain write raises on an observed plan**, naming the twin (narrowed
to writes at M12.6; originally every un-twinned action raised).

**The seam**: `Client.execute/2` returns `{:ok, batches, execution}`, the finished
`%Latu.Client.Execution{}` carrying schema, session, command result and observed metrics. No
new module — `Latu.Client.Result` beside `Latu.Result` would be two names for one idea. Later
results (`CheckpointCommandResult`, `Metrics`, `ExecutionProgress`, the retry policy) became
fields rather than new return shapes. Metric keys are atoms with no `keys:` option: the names
came out of the caller's own source.

## 2026-09-02 — `to/2` takes a type message, not a string (M11.2)

`ToSchema.schema` is a `DataType` message with no string alternative
(`DataTypeProtoConverter.toCatalystType` has no `UNPARSED` case on 4.2.0). Latu has no type model
by two prior decisions, so `to/2` takes what the server parsed: `Latu.parse_ddl_type/2` sends DDL
through the `ddl_parse` arm and hands the `DataType` back opaque. The verb stays a pure lazy
builder; **this is the one generated protobuf struct in Latu's public surface**, and `Latu.Plan`
names it `data_type()` so the layers above never spell `Latu.Protocol`. `parse_ddl/2` keeps
reporting the same parse as data — reporting a schema and naming one are different jobs.

## 2026-09-02 — A checkpoint is a resource, and Latu makes you say when it ends (M11.3)

`checkpoint` is the first thing in Latu that allocates: server-side memory or checkpoint files
held until something frees them. PySpark frees it in `CachedRemoteRelation.__del__` — a block
its own source labels `!!HACK ALERT!!`, and one that holds a session reference inside a plan
node, which Latu's layering forbids; an inert `%Latu.DataFrame{}` has no process to attach a
finalizer to anyway.

**Both forms ship.** `with_checkpoint/3` is the bracket (release in an `after`; a failing
release is logged, never raised, so it cannot replace the caller's exception). `checkpoint/2` plus
`release/1` is the REPL form, because the whole value of a checkpoint at a prompt is that it
outlives the expression that made it. `release/1` refuses a frame that was not checkpointed. The
session bounds a leak: the server drops cached relations when it ends.

Server facts, both the opposite of the guess: releasing twice succeeds while querying a released
frame fails; and `storage_level` is read only inside the `local` branch, so a level on a reliable
checkpoint would be dropped silently — Latu refuses the combination, which PySpark cannot express
anyway. One verb with `local:` rather than PySpark's two, because the wire has one command.

## 2026-09-02 — A merge is one call per clause, not a builder per clause (M11.3)

PySpark assembles a merge with a two-level fluent builder (three classes, fifteen methods). Latu
has one call per clause carrying match and action — `merge_into/4`, `when_matched/3`,
`when_not_matched/3`, `when_not_matched_by_source/3`, then `merge/2` — over an inert
`%Latu.MergeInto{}`, `GroupedData`'s pattern. The clause decides what the action may be (a
table, `@merge_clauses`, read off PySpark's class shapes), and a wrong pairing is refused by name
client-side. An assignment key travels as an `ExpressionString`, as PySpark's does; Spark
resolves it against the target alone. An empty merge is refused (`NO_MERGE_ACTION_SPECIFIED` is
fixed). Spark's "only the last clause may omit its condition" is **not** enforced — it is thrown
from `AstBuilder`, a rule about SQL text the DataFrame path never applies. **A rule that lives in
the parser is not a rule about the plan.** `withSchemaEvolution()` is `schema_evolution: true`.

**A merge cannot run on the test server**: `RewriteMergeIntoTable` handles only a v2
`SupportsRowLevelOperations` target, so Iceberg or Delta is needed. The integration test asserts
the plan is refused for the *right* reason by contrasting it with a merge into a table that does
not exist; the cookbook recipe carries the `> **Not executed.**` marker.

## 2026-09-02 — Structured streaming is a separate package, like ML

A `StreamingQuery` is a running computation that progresses without you, fails asynchronously,
and cannot be used without being observed over time — a lifecycle rather than a value, and a
lifecycle wants an owner. `WriteStreamOperationStart`, the query commands and the listener
events stay unbuilt, as `MlCommand` does; the seams both packages need exist since M11.1. *(The
original reasoning — "PySpark manages it with process-owned state" — was corrected at M13.5: the
streaming core is handle-based RPCs. The conclusion stands on the bracket test there.)*

## 2026-09-02 — `disconnect/2` does not release the server session by default

Latu has two verbs where PySpark's `stop()` has one, and keeps them apart on purpose: a clone
shares its parent's channel, and a second `connect/2` can join an existing `session_id:`, which
is how the interrupt idiom in the cookbook and `control_test.exs` works — the worker's
`disconnect/2` must not end the session it borrowed. A default release would make Latu's own
recommended pattern override Latu's own default, and would turn `disconnect/2` into a network
call with a 5 s budget at the one moment the server may already be gone. Sessions time out on
their own; `release_session/2` is the explicit call. Settled at M14 (2026-09-03) on that shape
argument; nothing was measured against `ReleaseSession`, and nothing needed to be. A
`docs/deviations.md` row makes it visible where users look.

## 2026-09-02 — A cloned session shares the channel

`CloneSession` returns a `%Latu.Session{}` with the new ids over the *same* channel: a Spark
Connect session is server-side state keyed by an id, the connection is not part of its identity,
and a second TCP connection buys nothing. Consequence, documented rather than designed around:
`disconnect/2` on either session closes the transport for both, and `release_session/2` ends one
alone — which is why it is public.

## 2026-09-02 — Spark's structured error detail arrives free; `FetchErrorDetails` is explicit (M12.3)

elixir-grpc already decodes `grpc-status-details-bin` into `GRPC.RPCError.details`, and
`Google.Rpc.ErrorInfo` ships in `deps/googleapis`, so the error class, SQLSTATE, JVM class
hierarchy, message parameters, stack trace and `errorId` cost no dependency, no proto work and no
round trip. **`FetchErrorDetails` is an explicit call**, `Latu.error_details/2`, not automatic:
Latu returns `{:error, _}` for expected refusals too, and a round trip on each would buy only the
cause chain. An error with no `error_id` comes back unchanged. The server invalidates an error's
detail as it answers, so it can be fetched exactly once. The stack trace is on the struct, not in
`message/1`; `message/1` prefixes the class only when Spark did not already.

`FetchErrorDetails` is the one handler that never echoes the session id, so `Session.confirm/3`
treats an absent id as no information, not a mismatch — the rule it already applied to
`server_session_id`.

## 2026-09-02 — SQL metrics fold into the `*_with_metrics` twins, as `%Latu.ExecutionInfo{}` (M12.3)

Spark's per-node SQL metrics (`ExecutePlanResponse.metrics`) had nowhere to go; PySpark hangs
them off `df.executionInfo`. The twins now return `{value, %Latu.ExecutionInfo{observed:,
metrics:}}` — completing the family rather than adding a second one, at the cost of a breaking
change three weeks after the twins shipped. **`observed` is atom-keyed, `metrics` is
string-keyed**: observation names came from the caller's source, metric names come off the wire
and must not grow the atom table.

## 2026-09-02 — `ExecutionProgress` is a per-action `:progress` option, not a session handler (M12.3)

A session-level default is hidden state firing on every action; a Latu session's appeal is that
it does nothing you did not ask for. Progress is wanted on the one query you are waiting for, so
it is one option at the call site. The handler runs in the caller's own process between batches —
a slow one slows the query, a raising one fails it — stated on `Latu.Progress` and pinned by a
test. Cost: the option is threaded through every action (`count/1` grew an arity) and joins the
writer's reserved keys so it never reaches Spark as a writer option.

**The server reports on a timer, `spark.connect.progress.reportInterval`, default two seconds**,
so a query that finishes inside it reports nothing; `docker-compose.yml` sets 100 ms for the
integration tests.

## 2026-09-02 — The `!`-twin convention is a test (M12.3)

`test/latu/twins_test.exs` reads specs off `Latu`, `Latu.DataFrame`, `Latu.Catalog` and
`Latu.Session` and asserts anything that can return `{:error, _}` has a `!` twin at the same
arity, and every twin has a base. `@doc false` functions are outside the convention by
definition; `Latu.Session.confirm/3` is the one exemption — a pure check the transport runs, not
an action. `Code.ensure_loaded!/1` is load-bearing: `function_exported?/3` is false for a
compiled-but-unloaded module, which made the first version seed-dependent.

## 2026-09-02 — `usage-rules.md` ships in M12.3, not M13

The `usage_rules` convention (ash-project): a short rules file at the library root that a
consuming project syncs into an agent's context. It is different content from the generated
cheatsheet — the things not guessable from function names — and was written while the M12.3 audit
had the reasoning in hand. Deliberately short; it is read into a context budget.

## 2026-09-02 — Session tags, `interrupt/2`, and what `GetStatus` needs (M12.2)

Tags are what make interrupt usable: the process running a query is blocked in it, so the
interrupting call comes from a `Task` or another shell, by tag. Three server facts, none
guessable: **`GetStatus` with no `operation_status` field returns nothing** — an empty message
asks for every operation, an absent one for none; **a session does not exist on the server until
it runs something**, and the session RPCs split three ways on one that does not (`Interrupt`
creates it, `ReleaseSession` no-ops, `GetStatus`/`CloneSession` refuse); `:terminating` covers
finished, failed and cancelled alike. **Killing a task does not stop a Latu query** — an
execution is reattachable, so a killed client leaves it running until the detached timeout.
`spark-reattach` holds abandoned executions by design; recreate **both** containers when the
environment is a suspect.

## 2026-09-02 — The config family maps one Latu function per read arm, because the three reads differ (M12.4)

Read from `SparkConnectConfigHandler`, `RuntimeConfig` and `SQLConf`, because the proto does not
say it:

| arm | key is set | registered, unset | not registered |
|---|---|---|---|
| `Get` | its value | Spark's own default | error, `SQL_CONF_NOT_FOUND` |
| `GetOption` | its value | Spark's own default | nil |
| `GetWithDefault(d)` | its value | **`d`** — Spark's default ignored, `d` type-checked | `d` |

So `GetWithDefault` is not `GetOption` plus `||`: for a registered-but-unset conf the `||` never
fires. Three functions: `conf/2` is `GetOption` (`Map.get`), `fetch_conf/2` is `Get`
(`Map.fetch`, PySpark's `spark.conf.get`), `conf/3` is `GetWithDefault` (renamed at M12.6; the
first shape had `Get` as the default read). Plus `confs/2`, `set_conf/3`, `set_confs/2`,
`unset_conf/2`, `is_modifiable/2`. **`GetAll` returns only what the session has set**, so a conf
at its default is absent from `confs/1` while `conf/2` answers for it. **`is_modifiable/2` false
does not predict a refusal** — it is false for every unregistered key, which `set_conf/3` stores
happily; what it reliably catches is a static conf. `Client.get_configs/2` moved from `Get` to
`GetOption`, making its docstring true.

## 2026-09-02 — `confs/2` puts the prefix back on, because the server takes it off (M12.4)

`GetAll` with a prefix strips it from every key it returns (PySpark never sends one). Latu
re-attaches it: a map whose keys are not conf keys cannot be handed back to `conf/2`, and the
trap is silent.

## 2026-09-02 — `ConfigResponse.warnings` is logged on a write, ignored on a read (M12.4)

The server attaches warnings for deprecated and unsupported confs on every config operation; Latu
had dropped the field. PySpark re-emits them on `set`/`unset` and ignores them on `get`; Latu
matches with `Logger.warning/1`, keeping the setters' return shape.

## 2026-09-02 — The transport constants live on the session, because there is nowhere else (M12.4)

`window_size`, `keepalive`, `keepalive_tolerance` and `retry` are `%Latu.Session{}` fields and
`Latu.connect/2` options. Latu holds no processes, so there is no GenServer state to hang a
default on; application config would be global and unable to differ between two sessions in one
VM. The retry policy is a `%Latu.Retry{}` struct with defaults byte-for-byte PySpark's
`DefaultPolicy` (15 attempts, 50 ms ×4 to a 60 s cap, jitter past 2 s); **which** errors are
retried stays non-configurable — a correctness property of reattach, not a preference.
`max_empty_reattaches` stays a module attribute: it catches a misconfigured server, not a
preference. *(`to_explorer_limit` was a fifth field here and went at 2026-09-04.)*

## 2026-09-02 — `range`'s `num_partitions` is a keyword, not Spark's fifth argument (M12.4)

`Latu.range(session, 0, 10, 2, 4)` gives a reader no way to tell the step from the partitions,
so `num_partitions:` is a trailing keyword on every arity. The field is `optional`, so every
existing fixture is byte-identical, and the session-wide knob is a conf
(`spark.sql.leafNodeDefaultParallelism`).

## 2026-09-02 — Java UDF: Latu calls one, it does not ship the jar (M12.5)

**Calling a user-defined function already works with no Latu code.** `Latu.Column.fun/3` sends an
`UnresolvedFunction` by name and the session's function registry does not record what put the
entry there — a SQL UDF, a Hive UDF, an admin's `--jars` class and `CREATE FUNCTION … USING JAR`
through `Latu.sql/3` all resolve identically. Proven by `test/integration/udf_test.exs` with a
SQL UDF and no jar.

**Shipping a jar from the client is deferred on testability.** It needs `AddArtifacts` under a
`jars/` prefix (which routes to `sparkContext.addJar`, so the file lands on the shared cluster)
plus `RegisterFunction` carrying a `JavaUDF`, and it cannot be exercised end to end without a jar
in the repo or a JDK in the compose image; plan-pinning it would repeat the merge situation. The
ML/streaming argument does not transfer — `registerJava` is fire and forget into session state —
so this is a scope call that flips the day someone with a jar asks. Re-uploading a changed jar
under the same name in one session throws `ARTIFACT_ALREADY_EXISTS`.

## 2026-09-02 — Telemetry events follow SparkEx's names, and one is Latu's own (M12.5)

All five of SparkEx's events are mirrored with `:latu` for `:spark_ex` — `[:rpc, :start|:stop]`,
`[:retry, :attempt]`, `[:reattach, :attempt]`, `[:result, :batch]`, `[:result, :progress]` — so
a reporter written for one needs only a prefix; a partial match is worse than none. **The one
addition is `[:latu, :execute, :start|:stop]`**: the rpc span measures *opening* the stream, and
a result is lazy, so a dashboard on it would show a thirty-second query as two milliseconds. The
execute span closes in `Stream.resource`'s after_fun and reports `:ok`, `:error` or
**`:abandoned`** — an abandoned reattachable execution keeps running on the server. Emitted from
`Latu.Client`, never from the IO-free state machine. **Metadata is ids only, never the session**,
which carries a token; pinned by a test connecting with a token. `:telemetry` is a hard
dependency; `dev/standins/telemetry.ex` is a working mini implementation so offline tests are
not vacuous.

## 2026-09-02 — JDBC needed no code, and now something says so (M12.5)

`read/2` and `write/2` pass any format and every unrecognised option through, and a write with
neither `:path` nor `:table` was already legal — JDBC's shape. `test/integration/jdbc_test.exs`
runs it against embedded Derby, which ships inside the Spark assembly, with no driver jar. It
works only because `local[1]` puts the executor in the driver's JVM, so it tests Latu's option
passing and is not a recipe to copy.

## 2026-09-02 — M12.6 — the warts

A pre-M13 review read the public surface as a first-time user would, before documentation was
written around it. Every item was approved by the maintainer as a scope decision.

- **The observe guard is narrowed to writes** (`write`, `save_as_table`, `insert_into`,
  `write_v2`, `merge`). Every un-twinned action had raised, so an upstream `observe/3` made a
  frame unshowable, unjoinable, un-checkpointable and a Livebook renderer crash; two refusals
  were false (`create_temp_view/3` runs no query) and the guard was not airtight anyway. PySpark
  drops the metrics on `show()` too. A write consumes the frame, so its metrics would be produced
  and dropped with nothing to show for it.
- **An observation failure lands on `info.observed[name]` as `{:error, _}`**; the action
  succeeds. Spark reports a metric failure inside the metrics message, not by failing the query.
- **`:progress` reaches every action**, and `take`/`tail`/`first`/`head` take `keys:` too.
- **`conf/2` is `Map.get` (`GetOption`), `fetch_conf/2` is `Map.fetch` (`Get`)**;
  `conf_option/2` is gone. Setters return `:ok` — the struct does not change, so there is nothing
  to rebind. `silent:` is gone from `set_confs/2`: a refusal Latu cannot return is not one it
  offers.
- **`Latu.Column.as/2` is gone.** Every projecting verb takes a keyword list, and its name was
  the one export `Latu` and `Latu.Column` shared. One story: call `Latu` qualified, import
  `Latu.Column`.
- **`repartition_by_range/3` takes `num_partitions:`**, no longer the reverse of
  `repartition/3`.
- **`Latu.Client` is `@moduledoc false`, `Session.pin/2` is `@doc false`, `Session.tags/1` and
  `clear_tags/1` are gone.** Hiding is free where unhiding later is not; `session.tags` is the
  Elixir spelling.
- **`:protocol` is a new `Latu.Error` kind** for Latu-side invariant failures (a stream ending
  without `ResultComplete`, a batch out of order, a reply out of shape); `:rpc` means only that
  the server refused or failed the call.
- Smaller: `count!/2` on a `GroupedData`; `W.partition_by/1` wraps a single column; `over/2`
  warns on `partitions: nil` and is quiet on the explicit `partition_by([])`;
  `create_dataframe/3` refuses a non-list column and names the bracket fix; `read(format: :csv)`
  is refused by name; `sql(s, q, min: 30)` says named bindings are a map. *(`to_explorer/2` also
  learned to refuse past its bound here; the bound went at 2026-09-04.)*

**The same fact stated in three places will disagree.** `import Latu` had three stories, the
function count four, the observe twins were "seven" in three files and eight in the code. One
home per fact — the moduledoc — and the rest point at it.

## 2026-09-02 — Three ways an Elixir name can be unavailable (M13.2)

| kind | example | can you `def` it? | qualified call? | what actually breaks |
|---|---|---|---|---|
| special form | `alias`, `quote` | yes | yes | `import` of the module exporting it |
| operator | `not`, `and`, `or`, `when` | **no — syntax error** | impossible | the name, entirely |
| `Kernel` function or macro | `inspect`, `abs`, `round` | yes | yes | an unqualified call after `import` |

`Latu.as/2` exists because a module exporting `alias/2` cannot be imported; `not_/1` and
`when_/2` exist because the name cannot be defined at all. Four documents had called all of these
"special forms". `docs/deviations.md` and the two docstrings each state their own mechanism.

## 2026-09-02 — A docstring carries its reason; the pointer is for source readers (M13.2)

This file does not ship — not an ExDoc extra, not in Hex's default `:files` — so a docstring
whose only answer is "see `docs/decisions.md`" answers nothing for the person asking. An
`@doc`/`@moduledoc` states the reason in a sentence and may keep a pointer as a tail; a `#`
comment may point and nothing else, since whoever reads it has the repo.

## 2026-09-02 — `usage-rules.md` restates rather than points, and that is the exception (M13.2)

Everywhere else one home per fact and the copies point at it. `usage-rules.md` is synced into an
agent's context, where a link to a moduledoc it does not have is worse than a repeated sentence,
so it stays self-contained; the drift risk is paid by `examples_test.exs` checking every call and
reference in it. Do not "fix" it by replacing content with pointers.

## 2026-09-03 — What Latu owns, and why `checkpoint` is allowed where streaming is not (M13.5)

"Latu holds no processes" was wrong twice: the gRPC channel is a process the adapter starts, and
`checkpoint/2` allocates server-side storage nothing frees for you. **What is true and
checkable: Latu defines no GenServer, Agent, Supervisor, Registry or pool, declares no
application callback module, and hands out resources without keeping them between calls** — the
channel lives in the struct `connect/2` returns and `disconnect/2` closes; the checkpoint id lives
in the frame `release/1` takes.

The ML and streaming entries had argued from ownership, and `checkpoint` falsifies that premise;
the streaming core is handle-based RPCs (`StreamingQueryCommand` keyed by an id, `active` a
server call) and needs no client process either. **The rule that survives: hand out resources,
never keep them, and only ones you can honestly bracket.** A checkpoint is data at rest — one
operation, scopable by `with_checkpoint/3` the way `File.open/3` scopes a file. A streaming
query is a running computation that progresses without you; `with_streaming_query/3` cannot
exist, because start-await-stop is a batch query with extra steps. A resource that outlives every
bracket you could write is a lifecycle, and a lifecycle wants an owner. (The listener bus is the
one genuinely process-shaped part, and its payload is Python-only.) ML is a weak case on this
axis — a fitted model brackets fine — and is settled by verification instead (its entry).

Removing `checkpoint` was considered and refused: `cache/1` is a hint and leaves the lineage
intact, where a checkpoint truncates it, and the workaround (write a table, read it back) is
more ownership, not less. It is the most awkward corner of the API and the docs say so.

Three independent reasons — ownership, leverage, verification — had been compressed into one
rule and reused for two decisions, and the first counterexample separating them broke it. A
single rule about to justify two decisions needs both reasons written out.

## 2026-09-03 — The data fixtures are generated, and the suite refuses by name

`test/wire/*.bin` — the golden plans — are committed, because a frozen reference is the point.
`fixtures/*` — the data files the integration suite reads — are gitignored and written by
`dev/make_data_fixtures.py`: small, deterministic, no third-party licence. `mix fixtures` wraps
the generator (with an absolute path — `System.cmd/3` searches PATH, not the cwd), and
`test/support/fixtures.exs` refuses an integration run whose files are missing, naming them and
the command, gated on `:integration` so `mix check` still runs on a fresh clone.

## 2026-09-03 — `Latu.Functions.Docs` and `Groups` refuse to compile without their harvest

Both read a checked-in `priv/*.exs` at compile time, formerly behind a `File.exists?` guard
"so the repo compiles before anyone has run the harvest". Both files are checked in, so the
guard's only remaining effect was to turn a missing file into 671 one-line docstrings or an
ungrouped reference, silently — and a Hex tarball lacking `priv/` would have published quietly.
Now each raises at compile time naming the regenerating script, and `functions_test.exs` asserts
a floor on the harvest sizes so a truncated file fails too.

## 2026-09-04 — Spark bounds a result in the plan, so Latu does not bound it in the action

**Maintainer's call, reversing M7.** `to_explorer/2`'s `limit:` option, the session's
`:to_explorer_limit` (100,000) and the refusal machinery are all gone. The Spark way to take part
of a result is `df.limit(10000).collect()` — `collect` and `toPandas` take no row argument — so
`df |> Latu.limit(10_000) |> Latu.to_explorer()` is the idiom and `Latu.take/3` is the same for
rows; the option was a third, action-shaped spelling Spark does not have. The Elixir rung agrees:
`Ecto.Repo.all/2` and `Explorer.DataFrame.collect/1` have no default bound either.

Three entries had noticed the seam: M7 justified the bound with "an accidental `collect`" and
exempted `collect/2` on the next line; M12.4 called `to_explorer_limit` "the odd one" on the
session; M12.6 made the bound refuse instead of truncate, fixing the symptom of a default that
was the defect. And of the three actions that materialise a whole result, the bounded one was the
cheapest — `collect/2` holds the decoded frame *plus* the rows again as BEAM-heap maps;
`to_explorer/2` holds only the frame, outside the BEAM heap. A guard on one of three doors is a
speed bump on the safe path. `to_arrow/2` stays the escape hatch past the decoder and the guard;
`fetch_frame/2` still refuses zero batches, since even an empty result arrives as one.

## 2026-09-04 — `deviations.md`'s entries are headings, because ExDoc's sidebar is h2-only

Each entry is an `###` rather than a bold line plus paragraph, which rendered as an
undifferentiated wall. Safe because measured: ExDoc's extras sidebar lists h2 only, so 127 `###`
headings add nothing to the nav. Every deviation now has an anchor. Tables were rejected again —
an entry is a paragraph of argument, and a table cell is not.

## 2026-09-04 — `mix docs` warnings are a gate, so the exemptions are named

`mix docs` runs with `warnings_as_errors`, because a warning channel nobody reads is where the
next real defect goes — and one was: six public functions declared `[Latu.Result.Schema.field()]`
while that module was hidden. The fix was to move the **type** (`field/0`) to `Latu.Result`,
already the public decode boundary, rather than promote the whole module; **ask which part of an
internal module is public before promoting it.**

ExDoc's two knobs are not interchangeable, read from its source: prose references consult
`skip_code_autolink_to` by *term*; typespecs consult only `skip_undefined_reference_warnings_on`
by *module*. So `mix.exs` names terms for prose and two module ids (`Latu.Plan`, `Latu.Result`)
for specs, with the cost stated there: an undefined type in a spec on those modules is reported by
the compiler, not by ExDoc. **No page is exempted**; the four repo-only links
(`docs/decisions.md`, `CLAUDE.md`, `LICENSE`) are plain code spans, because a link that 404s
for every reader of the published docs is not a link. ExDoc resolves an extra link by basename
alone, so `docs_test.exs` checks both halves of every relative link.

## 2026-09-04 — The warehouse stays inside the container (M14.2)

`docker-compose.yml` bind-mounted `./warehouse` over Spark's warehouse directory. Nothing on the
host read it; its one effect was that tables outlived a recreate, which the tests never rely on.
On Linux Docker it was a defect: a missing bind-mount source is created root-owned, and the
`apache/spark` image runs as uid 185, so `save_as_table` failed with a permission error — on
CI first, and for any Linux contributor next. Docker Desktop maps ownership, which is why the
maintainer's Mac never saw it. The mount is gone; the warehouse lives at
`/opt/spark/work-dir/spark-warehouse` in the container, writable on every platform, and is
discarded with it. `fixtures/` stays a bind mount because the host writes it and the container
only reads it — and `fixtures/.gitkeep` is tracked so a fresh clone has the directory before
compose can create it as root.

## 2026-09-04 — `Latu.to_ddl/2` is dropped rather than shipped hidden (M14.3)

It was `@doc false` with one caller, a test. A hidden function in a 0.1.0 tree is a promise
nobody made and everybody can rely on; `JsonToDDL` stays reachable as an `analyze` arm through
`Latu.Client`, which is where the integration test now exercises it. `parse_ddl/2` remains the
public direction — `docs/deviations.md` says why only that one is.

## 2026-09-04 — `mix test` runs with `--warnings-as-errors` (M14.3)

`elixirc_options: [warnings_as_errors: true]` covers `lib/`; `mix test` compiles `test/` itself
and was the one path a warning could take through CI. Both aliases now pass the flag. The cost
is the same one `lib/` already pays: an unused variable in a test is a red build.

## 2026-09-04 — What the Hex package carries, and what it does not (M14.3)

`files` is Hex's default list plus `usage-rules.md`, which tooling reads out of `deps/`. Not
`assets/`: nothing in the package reads an image, hexdocs are built from the working tree at
publish time rather than from the tarball, and 196 KB of logos would ride along on every
`deps.get` for no reader. The README's lockup uses absolute `raw.githubusercontent.com` URLs
because the same file renders on hex.pm, where a relative `assets/...` path resolves to nothing;
the guide links stay relative because ExDoc rewrites those to the rendered pages, which is
worth more than a working link on hex.pm (reversed the same day — next entry). `CHANGELOG.md`
is an extra, so hex.pm's Changelog
link points at hexdocs. `source_ref` is `"v#{@version}"`: the tag moves only when the version
does.

## 2026-09-04 — The README's links are absolute, because hex.pm renders it too (0.1.1)

Measured after publishing: hex.pm rewrites a relative link in the README to
`repo.hex.pm/preview/latu/<version>/<path>`, and the guides, `docs/deviations.md` and
`CONTRIBUTING.md` are not in the tarball, so seven links 404'd on the package page. The README is
the one extra that renders in three places — GitHub, hexdocs, hex.pm — and only an absolute
`hexdocs.pm/latu/<page>.html` URL works in all three; the other extras render on hexdocs alone
and keep relative links, which `docs_test.exs` checks (it skips absolute ones). The hex.pm README
is frozen per version, hence 0.1.1 with no code change.
