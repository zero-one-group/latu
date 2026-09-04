# Probe: how long a write command takes on this server, and what a stall correlates with.
# Verdict: docs/decisions.md, "The write-stall verdict" — concurrent write tasks in the Docker
# Desktop VM; `--master local[1]` eliminates it. Re-run if the stall ever reappears.
#
#     docker compose up -d spark-connect
#     mix run dev/probe_writes.exs

defmodule ProbeWrites do
  def time(label, fun) do
    {us, result} = :timer.tc(fun)
    ms = us |> div(1000) |> Integer.to_string()
    status = if result == :ok, do: "ok", else: inspect(result, printable_limit: 120)

    IO.puts(
      String.pad_trailing(label, 44) <> String.pad_leading(ms <> " ms", 10) <> "  " <> status
    )

    result
  end
end

session = Latu.connect!("sc://localhost:15002")
IO.puts("Spark #{Latu.spark_version!(session)} — one write command per row, wall time\n")

df = Latu.range(session, 5)
base = "/tmp/latu_probe/#{System.unique_integer([:positive])}"

ProbeWrites.time("collect (baseline query, same session)", fn ->
  {:ok, _} = Latu.collect(df)
  :ok
end)

for n <- 1..3 do
  ProbeWrites.time("write parquet ##{n} to /tmp, same session", fn ->
    Latu.write(df, format: "parquet", path: "#{base}/p#{n}")
  end)
end

ProbeWrites.time("write csv to /tmp, same session", fn ->
  Latu.write(df, format: "csv", path: "#{base}/c1", header: true)
end)

session2 = Latu.connect!("sc://localhost:15002")
df2 = Latu.range(session2, 5)

for n <- 1..2 do
  ProbeWrites.time("write parquet ##{n}, FRESH session", fn ->
    Latu.write(df2, format: "parquet", path: "#{base}/s2_p#{n}")
  end)
end

ProbeWrites.time("save_as_table (warehouse bind mount)", fn ->
  Latu.save_as_table(df2, "latu_probe_#{System.unique_integer([:positive])}")
end)

Latu.disconnect(session)
Latu.disconnect(session2)

IO.puts("""

Reading the rows:
  only write #1 of each session stalls  ->  per-session setup (the artifact class loader)
  every write stalls                    ->  per-job, or the filesystem
  nothing stalls                        ->  needs the suite's churn; re-run during check.all
""")
