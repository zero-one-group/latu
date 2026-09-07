# Probe: what `Latu.to_nx/2` is actually worth, against the routes that existed before it.
#
#     docker compose up -d spark-connect
#     mix run dev/probe_nx_copies.exs
#
# The measurement ML0 promised and 0.3.0 still owed. `dev/probe_copies.exs` established the
# Explorer baseline and refuted `to_nx/2`'s *performance* case before it was written; this asks
# the question the other way round, now that the verb exists: on the same bytes, what does it
# cost, and where is it the only route rather than the fast one.
#
# **Which column to read depends on what you are comparing**, and getting this wrong is the
# easiest way to misread the table.
#
#   * **Explorer against Nx**: `d RSS`. Explorer's frames and tensors are native allocations
#     *outside* the BEAM heap and invisible to `:erlang.memory/0`, so the BEAM columns cannot
#     see them at all. This is why `probe_copies.exs`'s table could not simply grow a `to_nx`
#     row.
#   * **Two BEAM-heap routes against each other**: `peak BEAM`. It is an absolute reading, so
#     it is the honest one where both routes materialise on the BEAM.
#   * **Never `d RSS` between rows of one block.** RSS does not fall back, and the rows run in
#     order, so a later row shows a smaller delta for free — `to_nx` is last in every block
#     here and its `d RSS` flatters it. `RSS abs` climbing down a block is that effect made
#     visible. Fixing it properly needs a fresh VM per row, which is more than a probe.
#
# Two parts, and they answer different questions.
#
#   1. **A plain `double` column**, where every route works. This is cost, apples to apples:
#      `to_arrow` as the floor, the Explorer route, and `to_nx`.
#   2. **A `Vector` column**, where they do not. `collect/2` and `to_explorer/2` refuse one, so
#      the comparison is `to_nx/2` against `vector_to_array` plus a rebuild — the route a user
#      had before 0.3.0, and the one PySpark takes. Every route is cross-checked to produce the
#      same tensor first, because three timings for three different answers are worth nothing.
#
# The Vector column is built with `array_to_vector`, which reaches Spark from Latu alone — no
# `latu_ml` needed, and the same trick `test/integration/nx_test.exs` uses. It is not a SQL
# routine (`docs/decisions.md`, 2026-09-06), so it goes over the wire as a function call.
#
# The 100 MB rows can OOM the compose server: `local[1]` takes Spark's default 1 g driver heap,
# and 100 MB through `DirectTaskResult` serialisation does not fit. A failed row reports and the
# table continues.

Code.require_file("support/probe_memory.exs", __DIR__)

import Latu.Column

session = Latu.connect!(System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

unless Code.ensure_loaded?(Nx) do
  IO.puts("Nx is not loaded, and every row here needs it. `mix deps.get` with :nx present.")
  System.halt(1)
end

IO.puts("Spark #{Latu.spark_version!(session)} — to_nx/2 against the routes before it")

# The first query carries JVM warm-up and planning, which would otherwise land entirely on
# whichever row is measured first.
{:ok, _warm} =
  session |> Latu.range(50_000) |> Latu.select(a: expr("cast(id as double)")) |> Latu.to_arrow()

# =============================================
# 1. A plain double column: cost, apples to apples
# =============================================
#
# Three f64 columns is 24 bytes a row on the wire, so the row counts are the target sizes — the
# same shape `probe_copies.exs` uses, so its rows and these are directly comparable.

doubles = fn rows ->
  session
  |> Latu.range(rows)
  |> Latu.select(
    a: expr("cast(id as double)"),
    b: expr("cast(id as double) * 2"),
    c: expr("cast(id as double) * 3")
  )
end

for mb <- [10, 50, 100] do
  rows = div(mb * 1_048_576, 24)
  df = doubles.(rows)

  IO.puts("\n#{mb} MB of doubles — #{rows} rows x 3 f64")
  ProbeMemory.header()

  # The floor: raw IPC blobs, no decoder, no guard, no tensor.
  ProbeMemory.measure("to_arrow", fn -> {:ok, _blobs} = Latu.to_arrow(df) end)

  # Today's route before 0.3.0, both halves in one span so the frame and the tensors are alive
  # together if the route really does hold them together.
  ProbeMemory.measure("to_explorer |> to_tensor, one span", fn ->
    frame = Latu.to_explorer!(df)

    for name <- ["a", "b", "c"] do
      frame |> Explorer.DataFrame.pull(name) |> Explorer.Series.to_tensor()
    end
  end)

  # The new route. One pass, and the Arrow buffer is the tensor's binary.
  ProbeMemory.measure("to_nx", fn -> Latu.to_nx!(df) end)
end

# =============================================
# 2. A Vector column: where the routes differ in kind
# =============================================
#
# Eight doubles a row is 64 bytes of payload, plus whatever `VectorUDT`'s struct costs on the
# wire — a type byte, a null size and indices, and the offsets for the values list. So the
# stream is larger than the payload and the labels below name the payload, not the transfer.

width = 8
elements = Enum.map_join(1..width, ", ", fn i -> "cast(id as double) * #{i}" end)

vectors = fn rows ->
  session
  |> Latu.range(rows)
  |> Latu.select(a: expr("array(#{elements})"))
  |> then(&Latu.select(&1, features: fun("array_to_vector", [:a])))
end

# Do the routes agree? Three timings for three different tensors would be worthless, so this
# runs first, small, and stops the probe if they disagree.
IO.puts("\ncross-check, 1000 rows — every route must give the same tensor")

check = vectors.(1000)

by_to_nx = Latu.to_nx!(check, columns: ["features"])["features"]

by_rebuild =
  check
  |> Latu.select(v: fun("vector_to_array", [:features, "float64"]))
  |> Latu.to_explorer!()
  |> Explorer.DataFrame.pull("v")
  |> Explorer.Series.to_list()
  |> Nx.tensor(type: :f64)

if Nx.shape(by_to_nx) == Nx.shape(by_rebuild) and
     Nx.to_number(Nx.all_close(by_to_nx, by_rebuild, atol: 0.0, rtol: 0.0)) == 1 do
  IO.puts("  agree — #{inspect(Nx.shape(by_to_nx))} #{inspect(Nx.type(by_to_nx))}")
else
  IO.puts(
    "  DISAGREE — to_nx #{inspect(Nx.shape(by_to_nx))} " <>
      "vs rebuild #{inspect(Nx.shape(by_rebuild))}"
  )

  IO.puts("  Everything below would be comparing different answers. Stopping.")
  Latu.disconnect(session)
  System.halt(1)
end

for mb <- [10, 50, 100] do
  rows = div(mb * 1_048_576, width * 8)
  df = vectors.(rows)

  IO.puts("\n#{mb} MB of Vector payload — #{rows} rows x #{width} f64")
  ProbeMemory.header()

  # `to_explorer/2` refuses a Vector column — but **not for free**. `DataFrame.fetch/2` executes
  # first and checks the schema second, so the whole result crosses the wire and is then thrown
  # away. That is worth a row of its own: it is the cost of the route that does not work.
  ProbeMemory.measure("to_explorer (refused, after transfer)", fn ->
    {:error, _refused} = Latu.to_explorer(df)
  end)

  ProbeMemory.measure("to_arrow", fn -> {:ok, _blobs} = Latu.to_arrow(df) end)

  # The route before 0.3.0: convert server-side, decode as a list column, rebuild per row.
  ProbeMemory.measure("vector_to_array |> to_explorer", fn ->
    df
    |> Latu.select(v: fun("vector_to_array", [:features, "float64"]))
    |> Latu.to_explorer!()
  end)

  ProbeMemory.measure("vector_to_array |> rebuild {n, d}", fn ->
    df
    |> Latu.select(v: fun("vector_to_array", [:features, "float64"]))
    |> Latu.to_explorer!()
    |> Explorer.DataFrame.pull("v")
    |> Explorer.Series.to_list()
    |> Nx.tensor(type: :f64)
  end)

  # The other pre-0.3.0 route: no Explorer at all, straight from collected rows.
  ProbeMemory.measure("vector_to_array |> collect |> Nx", fn ->
    {:ok, collected} =
      df |> Latu.select(v: fun("vector_to_array", [:features, "float64"])) |> Latu.collect()

    collected |> Enum.map(& &1.v) |> Nx.tensor(type: :f64)
  end)

  # One slice and one reshape over a contiguous buffer.
  ProbeMemory.measure("to_nx", fn -> Latu.to_nx!(df, columns: ["features"]) end)
end

IO.puts("""

What the run of 2026-09-07 said, so a re-run has something to disagree with:

  * **Part 1: to_nx is not a memory win on primitives, and above one batch it costs more.** At
    100 MB it peaked at 201.9 MB of BEAM binary against the Explorer route's 168.9 — because
    several batches are concatenated into one buffer a column, so the Arrow blobs and the
    tensor are both live. Zero-copy is the *single-batch* case. Wall time was comparable and
    slightly better at scale (649 ms against 773). Read `peak BEAM` here, not `d RSS`.
  * **Part 2: on a Vector column it is a different regime, not a saving.** At 100 MB of
    payload, peak BEAM was 135.5 MB against 1682.4 for the Explorer rebuild and 1978.6 for the
    collect route — 12 to 15 times less — and about twice as fast. Both older routes
    materialise every row as a list of boxed floats before `Nx.tensor` walks it.
  * **The Explorer decode is not what costs; the rebuild is.** `vector_to_array |> to_explorer`
    stopped at 118.9 MB. So a *frame* is a perfectly good destination for that route; only
    building a tensor through it is ruinous. The guide splits its advice on exactly this.
  * **Refusing costs the whole transfer.** `to_explorer` on a Vector column spent 1733 ms and
    125.8 MB before the schema check threw the result away.
""")

Latu.disconnect(session)
