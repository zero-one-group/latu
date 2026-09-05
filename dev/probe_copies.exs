# Probe: what bringing a sample down actually costs today, per verb.
#
#     docker compose up -d spark-connect
#     mix run dev/probe_copies.exs
#
# The baseline `Latu.to_nx/2` has to beat (M15 plan, 0.3.0). `latu-ml-roadmap.md` §4 claims the
# Explorer route copies the sample about four times and holds it three times over at peak; that
# is a guess until this table has numbers, and the guide paragraph should quote measurements
# rather than the guess. Peak is sampled, not read after the fact: the copies that matter are
# transient, and a before/after reading misses every one of them.
#
# Explorer's frames live in Rust and are invisible to `:erlang.memory/0` (design §9.1), so RSS
# is the only measure that sees them — and RSS never falls back, so read the *deltas* down a
# column, not the absolutes. Run it on an otherwise idle machine.

defmodule ProbeCopies do
  @sample_ms 5

  def measure(label, fun) do
    :erlang.garbage_collect()
    Process.sleep(50)

    base = reading()
    sampler = spawn_sampler(self())
    {us, result} = :timer.tc(fun)
    peak = stop_sampler(sampler)

    report(label, div(us, 1000), base, peak)
    result
  end

  defp report(label, ms, base, peak) do
    IO.puts(
      "  " <>
        String.pad_trailing(label, 40) <>
        String.pad_leading("#{ms} ms", 10) <>
        String.pad_leading(mb(peak.binary - base.binary), 14) <>
        String.pad_leading(mb(peak.total - base.total), 12) <>
        String.pad_leading(mb(peak.rss - base.rss), 12)
    )
  end

  def header do
    IO.puts(
      "  " <>
        String.pad_trailing("verb", 40) <>
        String.pad_leading("wall", 10) <>
        String.pad_leading("peak binary", 14) <>
        String.pad_leading("peak BEAM", 12) <>
        String.pad_leading("peak RSS", 12)
    )
  end

  defp mb(bytes) when bytes < 0, do: "-"
  defp mb(bytes), do: :erlang.float_to_binary(bytes / 1_048_576, decimals: 1) <> " MB"

  defp reading do
    %{binary: :erlang.memory(:binary), total: :erlang.memory(:total), rss: rss()}
  end

  # RSS is the only number that sees Polars' allocations. `ps` is portable enough for macOS and
  # the CI image; a failure reports 0 rather than stopping the probe.
  defp rss do
    case Integer.parse(String.trim(to_string(:os.cmd(~c"ps -o rss= -p #{:os.getpid()}")))) do
      {kb, _rest} -> kb * 1024
      :error -> 0
    end
  end

  defp spawn_sampler(parent) do
    spawn(fn -> sample(parent, reading()) end)
  end

  defp sample(parent, peak) do
    receive do
      {:stop, from} -> send(from, {:peak, peak})
    after
      @sample_ms ->
        now = reading()

        sample(parent, %{
          binary: max(peak.binary, now.binary),
          total: max(peak.total, now.total),
          rss: max(peak.rss, now.rss)
        })
    end
  end

  defp stop_sampler(sampler) do
    send(sampler, {:stop, self()})

    receive do
      {:peak, peak} -> peak
    after
      2_000 -> %{binary: 0, total: 0, rss: 0}
    end
  end
end

import Latu.Column

session = Latu.connect!(System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
nx? = Code.ensure_loaded?(Nx)

IO.puts("Spark #{Latu.spark_version!(session)} — the cost of bringing a sample down")

unless nx? do
  IO.puts("""

  Nx is not loaded, so the tensor rows are skipped — and those are the baseline 0.3.0 has to
  beat. Add it as a dev dependency and re-run:

      {:nx, "~> <the current line>", only: :dev}

  `mix hex.info nx` gives the version; 0.3.0 is where it becomes `optional: true` for real.
  """)
end

# Three f64 columns is 24 bytes a row on the wire, so the row counts are the target sizes.
for mb <- [10, 50, 100] do
  rows = div(mb * 1_048_576, 24)

  df =
    session
    |> Latu.range(rows)
    |> Latu.select(
      a: expr("cast(id as double)"),
      b: expr("cast(id as double) * 2"),
      c: expr("cast(id as double) * 3")
    )

  IO.puts("\n#{mb} MB of Arrow — #{rows} rows x 3 doubles")
  ProbeCopies.header()

  # The floor: raw IPC blobs, no decoder and no guard.
  ProbeCopies.measure("to_arrow", fn -> {:ok, _blobs} = Latu.to_arrow(df) end)

  # The first half of today's Nx route.
  frame = ProbeCopies.measure("to_explorer", fn -> Latu.to_explorer!(df) end)

  if nx? do
    # The second half, per column — what `to_nx/2` replaces with a slice and a reshape.
    ProbeCopies.measure("to_explorer |> to_tensor (3 cols)", fn ->
      for name <- ["a", "b", "c"] do
        frame |> Explorer.DataFrame.pull(name) |> Explorer.Series.to_tensor()
      end
    end)
  end

  # Held for comparison only while the tensors are built above; released here.
  _ = frame
end

IO.puts("")
Latu.disconnect(session)
