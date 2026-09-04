defmodule Latu.Telemetry do
  @moduledoc """
  The `:telemetry` events Latu emits, and the only place their names are written down.

  `:telemetry.execute/3` is a plain function call, so this fits an architecture that holds no
  processes: a handler runs in whichever process is talking to Spark, which is the caller's.
  A slow handler slows the query, exactly as a `:progress` function does.

  | Event | Measurements | When |
  |---|---|---|
  | `[:latu, :rpc, :start]` | `system_time` | every unary RPC, before the call |
  | `[:latu, :rpc, :stop]` | `duration` | the same call, after it |
  | `[:latu, :rpc, :exception]` | `duration` | the call raised rather than returning |
  | `[:latu, :execute, :start]` | `system_time` | a result stream is first enumerated |
  | `[:latu, :execute, :stop]` | `duration` | that stream ended, however it ended |
  | `[:latu, :retry, :attempt]` | `backoff`, `attempt` | a transient error the transport retries |
  | `[:latu, :reattach, :attempt]` | `backoff`, `attempt` | an ordinary mid-stream reattach |
  | `[:latu, :result, :batch]` | `rows`, `bytes` | each Arrow batch |
  | `[:latu, :result, :progress]` | `percent` | each progress message from the server |

  Metadata is `rpc`, `outcome` and `error_class` on the RPC events, and `session_id` plus
  `operation_id` on everything that belongs to an execution. The RPC events carry no
  `operation_id`; `[:latu, :execute, :stop]` carries an `outcome` of `:ok`, `:error` or
  `:abandoned`. Durations are in native time units, as `:telemetry.span/3`'s are.

  **`[:latu, :rpc, :*]` covers every gRPC call, including `ExecutePlan` — but for that one and
  `ReattachExecute` it measures *opening* the stream, not draining it**, because a Latu result
  is a lazy stream and there is no function whose return is the end of the query. So a dashboard
  built on the rpc duration would show a 30-second query as two milliseconds. That is what
  `[:latu, :execute, :*]` is for: it opens when the stream is first enumerated and closes when
  it ends, whether that is a finish, a failure, or a consumer that stopped reading — the
  `:abandoned` outcome, worth alerting on, since an abandoned reattachable execution goes on
  running on the server (see `Latu.interrupt/2`).

  Names follow [SparkEx](https://github.com/lukaszsamson/spark_ex)'s, with `:latu` for its
  `:spark_ex`, so a metrics reporter written for one needs nothing but a prefix for the other.

  ## What is never in metadata

  The session's `:token` and its `:headers`. Every event is built from named fields —
  `session_id`, `operation_id` — and never from the `%Latu.Session{}` itself, so a handler that
  logs its metadata cannot leak a credential. `test/integration/telemetry_test.exs` asserts it
  against a session that has a token.

  Nor is the plan, the schema or any row. A measurement is a number and metadata is an id.

  ## Attaching

      :telemetry.attach_many(
        "latu-log",
        [[:latu, :rpc, :stop], [:latu, :retry, :attempt]],
        fn event, measurements, metadata, _config ->
          Logger.info("\#{inspect(event)} \#{inspect(measurements)} \#{inspect(metadata)}")
        end,
        nil
      )
  """

  alias Latu.Error

  @doc false
  @spec rpc(String.t(), String.t(), (-> result)) :: result when result: var
  def rpc(name, session_id, call) when is_function(call, 0) do
    metadata = %{rpc: name, session_id: session_id}

    :telemetry.span([:latu, :rpc], metadata, fn ->
      result = call.()

      {result, Map.merge(metadata, outcome(result))}
    end)
  end

  @doc false
  @spec execution_started(map()) :: :ok
  def execution_started(ids) do
    :telemetry.execute([:latu, :execute, :start], %{system_time: System.system_time()}, ids)
  end

  @doc false
  @spec execution_finished(integer(), map()) :: :ok
  def execution_finished(duration, metadata) do
    :telemetry.execute([:latu, :execute, :stop], %{duration: duration}, metadata)
  end

  @doc false
  @spec attempt(:retry | :reattach, non_neg_integer(), non_neg_integer(), map()) :: :ok
  def attempt(kind, backoff, attempt, ids) when kind in [:retry, :reattach] do
    :telemetry.execute([:latu, kind, :attempt], %{backoff: backoff, attempt: attempt}, ids)
  end

  @doc false
  @spec batch(non_neg_integer(), non_neg_integer(), map()) :: :ok
  def batch(rows, bytes, ids) do
    :telemetry.execute([:latu, :result, :batch], %{rows: rows, bytes: bytes}, ids)
  end

  @doc false
  @spec progress(non_neg_integer(), map()) :: :ok
  def progress(percent, ids) do
    :telemetry.execute([:latu, :result, :progress], %{percent: percent}, ids)
  end

  # `Latu.Client.rpc/3` returns exactly these two shapes, so this is total. An error class is
  # reported as nil rather than omitted, because a handler should not have to check whether a
  # key is there.
  defp outcome({:error, %Error{} = error}) do
    %{outcome: :error, error_class: error.error_class}
  end

  defp outcome(_result), do: %{outcome: :ok, error_class: nil}
end
