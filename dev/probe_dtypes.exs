# Measure what Polars actually does with the Spark types the schema guard refuses.
#
#     docker compose up -d spark-connect
#     mix run dev/probe_dtypes.exs
#
# Refusal is the guard's safe default — a wrong refusal costs the user a cast, a wrong pass
# costs a NIF panic with no diagnostic — so a kind only moves to the allowed set on this
# script's say-so. It fetches each refused kind raw over `to_arrow` (which bypasses the
# guard) and hands the bytes to Explorer directly, reporting decode, error, or panic. Act on
# the report in `Latu.Result.Schema`, and update `test/integration/dtypes_test.exs` in the
# same commit. Interval outcomes can differ with POLARS_IMPORT_INTERVAL_AS_STRUCT=1; probe
# both ways before deciding.

import Latu.Column

session = Latu.connect!(System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

probes = [
  day_time_interval: "INTERVAL '1 02:03:04' DAY TO SECOND",
  year_month_interval: "INTERVAL '1-2' YEAR TO MONTH",
  calendar_interval: "make_interval(1, 2, 0, 3, 4, 5, 6.7)",
  variant: ~s|parse_json('{"a": 1}')|,
  time: "cast('12:34:56' as time)"
]

for {kind, sql} <- probes do
  df = session |> Latu.range(1) |> Latu.select(x: expr(sql))

  case Latu.to_arrow(df) do
    {:error, error} ->
      IO.puts("#{kind}: the server refused — #{String.slice(error.message, 0, 140)}")

    {:ok, blobs} ->
      try do
        dtypes =
          blobs
          |> Enum.map(&Explorer.DataFrame.load_ipc_stream!/1)
          |> hd()
          |> Explorer.DataFrame.dtypes()

        IO.puts("#{kind}: DECODES as #{inspect(dtypes)} — candidate to leave the refused set")
      rescue
        e ->
          IO.puts(
            "#{kind}: raises #{inspect(e.__struct__)} — " <>
              String.slice(Exception.message(e), 0, 140)
          )
      end
  end
end

Latu.disconnect(session)
