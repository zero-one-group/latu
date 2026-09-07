# The measurement harness the memory probes share. Not a probe itself.
#
#     Code.require_file("support/probe_memory.exs", __DIR__)
#
# Peak is **sampled**, not read after the fact: the copies that matter are transient, and a
# before/after reading misses every one of them.
#
# Explorer's frames live in Rust and are invisible to `:erlang.memory/0` (design §9.1), so RSS
# is the only measure that sees them — and `Nx.from_binary/2` is the opposite, putting its
# tensor *in* the BEAM binary heap. The two regimes are why every row reports binary, BEAM
# total and RSS, and why **no single column compares them**. RSS never falls back, so a delta
# after a large step reads near zero; both the delta and the absolute are printed for that
# reason, and neither is trustworthy alone. Run on an otherwise idle machine.

defmodule ProbeMemory do
  @moduledoc false

  @sample_ms 5

  @doc """
  Run `fun`, report what it cost, and hand back its value.

  A raise is caught and reported as a failed row rather than stopping the probe: the 100 MB
  rows can OOM the compose server, and the rows above them are still worth having.
  """
  def measure(label, fun) do
    :erlang.garbage_collect()
    Process.sleep(50)

    base = reading()
    sampler = spawn_sampler(self())

    {us, result} =
      :timer.tc(fn ->
        try do
          {:ok, fun.()}
        rescue
          error -> {:failed, Exception.message(error)}
        end
      end)

    peak = stop_sampler(sampler)

    case result do
      {:ok, value} ->
        report(label, div(us, 1000), base, peak)
        value

      {:failed, message} ->
        reason = String.slice(message, 0, 88)
        IO.puts("  " <> String.pad_trailing(label, 40) <> "FAILED — " <> reason)
        nil
    end
  end

  defp report(label, ms, base, peak) do
    IO.puts(
      "  " <>
        String.pad_trailing(label, 40) <>
        String.pad_leading("#{ms} ms", 10) <>
        String.pad_leading(mb(peak.binary - base.binary), 14) <>
        String.pad_leading(mb(peak.total - base.total), 12) <>
        String.pad_leading(mb(peak.rss - base.rss), 12) <>
        String.pad_leading(mb(peak.rss), 12)
    )
  end

  def header do
    IO.puts(
      "  " <>
        String.pad_trailing("verb", 40) <>
        String.pad_leading("wall", 10) <>
        String.pad_leading("peak binary", 14) <>
        String.pad_leading("peak BEAM", 12) <>
        String.pad_leading("d RSS", 12) <>
        String.pad_leading("RSS abs", 12)
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
