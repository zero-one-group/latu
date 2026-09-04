# dev/ — tooling, not library code

Nothing here ships with Latu. It gives you (a) a Spark Connect server to talk to and (b) a way
to make PySpark print the protobuf plans Latu must produce.

## The Spark servers

`docker-compose.yml` has two profiles, both on `apache/spark:4.2.0`.

`spark-connect` (:15002, UI :4040) is the normal server. `spark-reattach` (:15003, UI :4041)
sets `senderMaxStreamDuration=5s` and `senderMaxStreamSize=1m`, so the server ends every
ExecutePlan stream early *without* a `ResultComplete` and any real query exercises the
`ReattachExecute` path. The integration suite needs both — reattach bugs are invisible
otherwise, and they corrupt results rather than raising.

Two things about that file: `--packages org.apache.spark:spark-connect_2.13:...` is **not**
needed (the Connect server ships in the Spark 4.x assembly), and
`spark.connect.grpc.binding.address=0.0.0.0` is essential, or the server binds loopback inside
the container and the published port is dead. A conf change needs
`docker compose up -d --force-recreate`, for both containers.

## One-time setup

```bash
docker compose up -d                      # both servers
docker compose logs -f spark-connect      # wait for "Spark Connect server started"

python3 -m venv dev/.venv                 # PySpark, as the oracle
. dev/.venv/bin/activate
pip install -r dev/requirements.txt
export SPARK_REMOTE='sc://localhost:15002'

python -c "from pyspark.sql import SparkSession as S; S.builder.getOrCreate().range(5).show()"
mix fixtures                              # the integration suite's data files
```

## The example

`example.exs` is Latu's own smoke test. Keep it current as the API grows; the *documented*
snippets live in `docs/guides/*.md`, which `mix check.all` runs.

```bash
mix run dev/example.exs        # runs and exits
iex -S mix                     # or keep the bindings
iex> import_file "dev/example.exs"
```

It leaves the session connected, so the REPL form gives you a live `session`.

## The oracle

```bash
python dev/pyspark_oracle.py 'spark.range(10).filter(F.col("id") > 3)'
python dev/pyspark_oracle.py --generate
```

In scope for a fixture expression: `spark`, `F` (functions), `W` (Window); the command helpers
`save`, `save_as_table`, `insert_into`, `v2`, `sql_cmd`, `view_cmd`, `merge_cmd` — PySpark's
terminal methods minus the execution, since writes, SQL, views and merges are *Commands*;
`catalog_plan`/`node_plan` for a LogicalPlan whose DataFrame method executes rather than
returns (catalog operations, `cov`, `corr`, `approxQuantile`); and `analyze_arm(method, df,
**kwargs)`, `_analyze`'s branch minus the RPC — an analyze fixture is the request arm
submessage, not a `Plan`, and its golden test is `assert_wire_message/2`.

Run `--generate` twice and expect an empty diff: a fixture that changes between runs is a coin
flip, not a golden test. `python dev/fixture_coverage.py` (`--uncovered`) says how much of the
API has a fixture; needs no server.

### What `--generate` writes

Two files per fixture. `<name>.bin` is the serialised `Plan`, which the Elixir suite decodes and
compares against Latu's — decoded structs rather than bytes, because protobuf serialisation is
not canonical. `<name>.txtpb` is the same plan in text format and **no test reads it**: it is
for learning a plan's shape, and for making regeneration reviewable when protos are re-vendored.
Both are committed, so `mix test` stays hermetic and CI never runs Python. Regenerating is a
deliberate act, tied to a Spark version bump.

### Three things the oracle rewrites before writing

**plan_ids are normalised**, renumbered depth-first from 0, because PySpark allocates them from
a process-global counter and so does Latu (`:erlang.unique_integer/1`). `--raw-ids` opts out.
A reference to a relation outside its tree normalises to `-1` on both sides.

**Lambda variable names are normalised**, renumbered from 0 by creation order, keeping the
`x`/`y`/`z` Spark assigns by position. PySpark's `fresh_var_name` also draws on a process-global
counter, so without this every new higher-order fixture would rewrite the earlier ones. The
suffix cannot be dropped: a nested lambda reuses the position letter and Spark resolves lambda
variables by name.

**Origins are stripped.** PySpark stamps every expression with its Python call site
(`common { origin { python_origin { ... } } }`); Latu never emits one. Empty `ExpressionCommon`
shells left behind are removed too — a present-but-empty submessage is not equal to an absent
one.

`Latu.Plan.normalize_ids/1` mirrors the first two. Keep them in step.

## The function library

Three scripts, and none of them invents anything.

`extract_functions.py` derives Latu's registry from **PySpark's Connect client** — the copy in
`dev/.venv`, which `requirements.txt` pins. Not the classic client, which names Catalyst
functions that Connect spells differently (`count_distinct` there is `count` with `is_distinct`
on the wire). Needs no server.

```bash
python dev/extract_functions.py           # shape summary + everything it will not generate
python dev/extract_functions.py --write   # rewrite the registry, then mix format
python dev/extract_functions.py --check   # exit 1 if that file is stale
```

Every run fails if a public PySpark function is neither a row nor in `EXCLUDE`, which is the
check that matters at a Spark version bump. Five shapes are generated; the rest are hand-written,
because "optional argument" is at least five behaviours in Spark (`docs/decisions.md`), and the
script names every function it refuses. The subtle shape is a default Spark always sends, whose
constant is in the function *body*, not the signature — `mask`'s four `None` parameters send
`"X"`, `"x"`, `"n"` and NULL.

`harvest_docs.py` runs `DESCRIBE FUNCTION EXTENDED` over a Connect session and writes
`priv/function_docs.exs`; `harvest_function_groups.py` fetches PySpark's own API reference
(`functions.rst` at the targeted tag) and writes `priv/function_groups.exs`, which `mix.exs`
reads through ExDoc's `:default_group_for_doc`. Both outputs are **checked in**, and compiling
refuses to go on without them. Rerun both after a Spark version bump and commit the diff;
`test/latu/functions_test.exs` goes red when an export has no group, and the `EXTRA` map in the
groups harvest is where a name Spark's reference omits gets its category.

```bash
docker compose up -d spark-connect
. dev/.venv/bin/activate
python dev/harvest_docs.py
python dev/harvest_function_groups.py --write
```

## The reattach server

`:15003` caps `senderMaxStreamDuration` at `5s`, `senderMaxStreamSize` at `1m` and
`observerRetryBufferSize` at `10m`. **Size is the useful trigger; duration is a backstop**: a
result of a given size produces the same number of cutoffs on any machine, while duration races
JVM warm-up. The duration has a practical floor — below the time a sender needs to deliver one
response the client can never advance — and `observerRetryBufferSize` must stay comfortably above
`senderMaxStreamSize`, or a reattach cannot replay what it needs. PySpark handles all of this,
so it is the baseline: the same `range(5_000_000_000).count()` gives the same answer against
both ports.

## No server?

Most operators serialise with no session at all:

```python
from pyspark.sql.connect.plan import Project, Range
import pyspark.sql.connect.functions as F

plan = Project(Range(start=0, end=10, step=1), [(F.col("id") + F.lit(1)).alias("x")])
print(plan.to_proto(None))
```

Operators that resolve schemas or read configs (`SQL`, some joins, UDFs) fail with `None`, which
is why the oracle itself always opens a session.

## Does every function actually resolve?

```bash
docker compose up -d spark-connect
mix run dev/probe_functions.exs            # report only
mix run dev/probe_functions.exs --write    # + write dev/function_args.exs, then mix format
mix run dev/probe_functions.exs --only lag,upper
```

The one question a golden fixture cannot answer: it proves Latu sends the bytes PySpark sends,
not that the server *accepts* them. The probe asks the server to **analyse** a call to every
wrapped function (`Latu.schema/1`, not `collect`), trying type profiles until one analyses and
retrying window functions inside an `OVER`. Whatever never resolves needs explicit arguments or
an exclusion with a reason, and the exit code is non-zero while any remain.

`dev/function_args.exs` is the checked-in record; `test/latu/function_sweep_test.exs` asserts
offline that it still covers the exported surface, so a Spark bump that adds or removes a
function fails in a second. **Re-run the probe at any Spark bump or registry regeneration, and
diff**: an unchanged file means no wrapper's shape moved. `dev/probe_dtypes.exs` is the same
instrument for the Arrow types the schema guard refuses.
