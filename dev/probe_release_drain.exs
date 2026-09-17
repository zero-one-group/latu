# Probe: can Latu reap an abandoned result stream by draining it, and does ReleaseExecute bound
# that drain? Settles the finding-2 fix before it is written (review of 2026-09-17).
#
#     docker compose up -d spark-connect          # the :15002 server, senderMaxStreamDuration 2m
#     mix run dev/probe_release_drain.exs
#
# Background. A Gun response process (`GRPC.Client.Adapters.Gun.StreamResponseProcess`) is linked
# to the connection, not to whoever reads the stream, and holds its buffered messages until its
# terminal one is read. A consumer that halts early (`Enum.take`), raises, or is killed leaves it
# alive with its buffer; `disconnect/2` does not reap it. The candidate fix drains the abandoned
# stream to EOF in `responses/2`'s `after_fun`, so the terminal message is read and the process
# exits. That is only cheap if `ReleaseExecute(ReleaseAll)`, which the after_fun already sends,
# stops the server's sender — otherwise the drain reads the rest of the result.
#
# This drives the raw ExecutePlan / ReleaseExecute / drain by hand, below Latu's public stream,
# the way the fix would, and measures three things:
#
#   1. today's behaviour leaks: consume a few batches, release, do NOT drain — the process stays.
#   2. the fix: release then drain — the process is reaped, and the drain should be a bounded tail.
#   3. contrast: drain without releasing — the whole remainder, i.e. the cost if release does not
#      bound the sender.
#
# What to read. The verdict compares the release-then-drain of section 2 against the unreleased
# drain of section 3. **Drain wall-clock time is the honest signal**: a drain that only consumes
# already-buffered bytes is fast (CPU), while one that waits on the server to keep producing is
# slow (network). If section 2's drain is far quicker and lighter than section 3's, ReleaseExecute
# bounds it and the after_fun drain is safe. Run against :15002 (the 2m sender) on purpose: it is
# the worst case, where one ExecutePlan segment carries the whole result rather than
# self-terminating at senderMaxStreamDuration.
#
# Tuning. Raise LATU_PROBE_ROWS until section 3's drain clearly exceeds a few hundred ms — that is
# the regime where the server is still producing when you abandon, which is where release either
# bounds it or does not. Gun buffers what it receives with application flow `infinity`, so a very
# large result can hold that many bytes in this probe's BEAM; raise gradually. LATU_PROBE_TAKE is
# the number of batches read before abandoning (default 3).

alias Latu.Protocol.Spark.Connect, as: Proto
alias Latu.Protocol.Spark.Connect.SparkConnectService.Stub

session = Latu.connect!(System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
rows = String.to_integer(System.get_env("LATU_PROBE_ROWS", "20000000"))
take = String.to_integer(System.get_env("LATU_PROBE_TAKE", "3"))
plan = Latu.Plan.new(Latu.range(session, rows).plan)

# Count of live Gun per-stream response processes, the thing that leaks.
resp_procs = fn ->
  Enum.count(Process.list(), fn pid ->
    with {:dictionary, dict} <- Process.info(pid, :dictionary),
         {mod, _fun, _arity} <- Keyword.get(dict, :"$initial_call", nil) do
      mod == GRPC.Client.Adapters.Gun.StreamResponseProcess
    else
      _ -> false
    end
  end)
end

# Open a raw, reattachable ExecutePlan, mirroring Latu.Client.open/1. Returns {operation_id, pull}
# where pull yields one ExecutePlanResponse per call, the way Latu.Client.start/read do.
open = fn ->
  op = Latu.Internal.UUID.v4()

  request = %Proto.ExecutePlanRequest{
    session_id: session.session_id,
    client_observed_server_side_session_id: session.server_session_id,
    user_context: %Proto.UserContext{user_id: session.user_id, user_name: session.user_name},
    operation_id: op,
    client_type: session.client_type,
    tags: session.tags,
    plan: plan,
    request_options: [
      %Proto.ExecutePlanRequest.RequestOption{
        request_option: {:reattach_options, %Proto.ReattachOptions{reattachable: true}}
      }
    ]
  }

  {:ok, stream} = Stub.execute_plan(session.channel, request)
  suspend = fn item, _acc -> {:suspend, item} end
  {op, fn command -> Enumerable.reduce(stream, command, suspend) end}
end

release = fn op ->
  request = %Proto.ReleaseExecuteRequest{
    session_id: session.session_id,
    client_observed_server_side_session_id: session.server_session_id,
    user_context: %Proto.UserContext{user_id: session.user_id, user_name: session.user_name},
    operation_id: op,
    client_type: session.client_type,
    release: {:release_all, %Proto.ReleaseExecuteRequest.ReleaseAll{}}
  }

  Stub.release_execute(session.channel, request)
end

read = fn pull ->
  case pull.({:cont, nil}) do
    {:suspended, {:ok, %Proto.ExecutePlanResponse{} = resp}, pull} -> {:resp, resp, pull}
    {:suspended, {:error, _e} = err, pull} -> {:term, err, pull}
    {:suspended, other, pull} -> {:other, other, pull}
    {status, _acc} when status in [:done, :halted] -> :eof
  end
end

arrow = fn %Proto.ExecutePlanResponse{response_type: rt} ->
  case rt do
    {:arrow_batch, %{data: d, row_count: n}} -> {byte_size(d), n}
    _ -> {0, 0}
  end
end

# Read until `k` arrow batches have been seen; return the continuation and what was consumed.
consume = fn pull0, k ->
  Enum.reduce_while(Stream.repeatedly(fn -> nil end), {pull0, 0, 0}, fn _,
                                                                        {pull, batches, bytes} ->
    case read.(pull) do
      {:resp, resp, pull} ->
        {b, _n} = arrow.(resp)
        batches = batches + if b > 0, do: 1, else: 0
        acc = {pull, batches, bytes + b}
        if batches >= k, do: {:halt, acc}, else: {:cont, acc}

      {:term, _err, pull} ->
        {:halt, {pull, batches, bytes}}

      {:other, _o, pull} ->
        {:cont, {pull, batches, bytes}}

      :eof ->
        {:halt, {nil, batches, bytes}}
    end
  end)
end

# Read the rest of a stream to EOF, timing it; returns %{ms, batches, bytes}.
drain = fn pull0 ->
  t0 = System.monotonic_time(:millisecond)

  {_pull, batches, bytes} =
    Enum.reduce_while(Stream.repeatedly(fn -> nil end), {pull0, 0, 0}, fn _,
                                                                          {pull, batches, bytes} ->
      case read.(pull) do
        {:resp, resp, pull} ->
          {b, _n} = arrow.(resp)
          {:cont, {pull, batches + if(b > 0, do: 1, else: 0), bytes + b}}

        {:term, _err, pull} ->
          {:cont, {pull, batches, bytes}}

        {:other, _o, pull} ->
          {:cont, {pull, batches, bytes}}

        :eof ->
          {:halt, {pull, batches, bytes}}
      end
    end)

  %{ms: System.monotonic_time(:millisecond) - t0, batches: batches, bytes: bytes}
end

mb = fn bytes -> Float.round(bytes / 1_048_576, 1) end

base = resp_procs.()
IO.puts("Spark #{Latu.spark_version!(session)} — finding-2 drain probe")
IO.puts("range(#{rows}), reading #{take} batches before abandoning")
IO.puts("baseline StreamResponseProcess count: #{base}\n")

# --- 1. Today: release without draining leaks the local process ---
{op1, pull1} = open.()
{pull1, b1, by1} = consume.(pull1, take)
release.(op1)
Process.sleep(1500)
leaked = resp_procs.() - base

IO.puts(
  "1. release, no drain   consumed #{b1} batches / #{mb.(by1)} MB; " <>
    "processes above baseline after 1.5 s: #{leaked}  (expect 1 = the leak)"
)

if pull1, do: drain.(pull1)

# --- 2. The fix: release then drain ---
{op2, pull2} = open.()
{pull2, _b, _by} = consume.(pull2, take)
release.(op2)
d2 = if pull2, do: drain.(pull2), else: %{ms: 0, batches: 0, bytes: 0}
after2 = resp_procs.() - base

IO.puts(
  "2. release then drain  tail #{d2.batches} batches / #{mb.(d2.bytes)} MB in #{d2.ms} ms; " <>
    "processes above baseline after: #{after2}  (expect 0 = reaped)"
)

# --- 3. Contrast: drain without releasing (the whole remainder) ---
{_op3, pull3} = open.()
{pull3, _b, _by} = consume.(pull3, take)
d3 = if pull3, do: drain.(pull3), else: %{ms: 0, batches: 0, bytes: 0}
IO.puts("3. drain, no release   tail #{d3.batches} batches / #{mb.(d3.bytes)} MB in #{d3.ms} ms")

IO.puts("")

pct = fn a, b -> if b > 0, do: Float.round(a / b * 100, 1), else: 0.0 end

cond do
  d3.bytes == 0 or d3.ms < 200 ->
    IO.puts(
      "INCONCLUSIVE: the unreleased drain (section 3) was too small or too fast to judge. " <>
        "Raise LATU_PROBE_ROWS until it clearly exceeds a few hundred ms, then re-run."
    )

  d2.bytes <= div(d3.bytes, 5) and d2.ms <= div(d3.ms, 5) ->
    IO.puts(
      "BOUNDED: ReleaseExecute cut the drain to #{pct.(d2.bytes, d3.bytes)}% of the bytes " <>
        "and #{pct.(d2.ms, d3.ms)}% of the time. The after_fun drain is safe."
    )

  true ->
    IO.puts(
      "UNBOUNDED: release left #{pct.(d2.bytes, d3.bytes)}% of the bytes and " <>
        "#{pct.(d2.ms, d3.ms)}% of the time to read. The after_fun drain would pull most of " <>
        "the result; do not add it — document finding 2 as a limitation instead."
    )
end

Latu.disconnect(session, release: true)
