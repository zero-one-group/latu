defmodule Latu.StreamingQuery do
  @moduledoc """
  A running structured streaming query: a server-side object, addressed by id.

  `Latu.write_stream/2` hands one back. The verbs here are one `StreamingQueryCommand` each,
  keyed by the id and run id the struct carries, plus the four `spark.streams` verbs, which
  take a session as `Latu.Catalog`'s do. Inert data, like everything Latu hands out: no
  process watches the query, and nothing stops it for you.

      {:ok, query} =
        Latu.write_stream(rollups,
          format: "parquet", path: "/data/rollups", trigger: :available_now,
          checkpoint_location: "/data/ckpt")

      {:ok, true} = Latu.StreamingQuery.await_termination(query)
      {:ok, progress} = Latu.StreamingQuery.last_progress(query)
      :ok = Latu.StreamingQuery.stop(query)

  Three things a server measured that the types cannot say:

    * **A query outlives its client.** Stop it, or it runs until the session ends; a lost handle
      comes back through `get/2` or `active/1`. `Latu.with_stream/3` stops in an `after`.
    * **Failure is asynchronous.** A query that dies at 3am raises nothing here. Whoever is in
      `await_termination/2` gets the failure as `{:error, _}`; anyone else asks `exception/1`,
      or watches `events/1` for a `:terminated`.
    * **`Latu.interrupt/2` with no scope stops streaming queries too.** By tag it reaches the
      ones started under that tag, which is the session-wide kill and a hazard for anyone
      interrupting a batch query in a session that also streams.

  Progress and status are snake-cased maps of Spark's own JSON, which over Connect is lossy
  against the driver's report: no top-level row counts, and a `start_offset` that is a string.
  Sum `:num_input_rows` over `:sources` for the total. `docs/deviations.md` has the shape.

  Times are milliseconds, as everywhere in Latu.
  """

  alias Latu.Client
  alias Latu.Error
  alias Latu.Plan
  alias Latu.Session

  @enforce_keys [:session, :id, :run_id]
  defstruct [:session, :id, :run_id, :name]

  @typedoc """
  The handle. `name` is `nil` when the query was started without one; `run_id` changes when a
  query is restarted from its checkpoint, and the server refuses a command carrying the old one.
  """
  @type t :: %__MODULE__{
          session: Session.t(),
          id: String.t(),
          run_id: String.t(),
          name: String.t() | nil
        }

  @typedoc "One progress report, Spark's JSON with snake-cased keys. See `last_progress/1`."
  @type progress :: %{optional(atom()) => term()}

  @typedoc """
  Which of Spark's three listener events this is, or `:unknown` for a fourth a newer server
  invented. See `t:event/0` for what each one carries.
  """
  @type event_type :: :progress | :idle | :terminated | :unknown

  @typedoc """
  One event off the listener bus: `:type` plus Spark's own JSON, snake-cased.

    * `:progress` — carries `:progress`, exactly what `last_progress/1` returns.
    * `:idle` — `:id`, `:run_id`, `:timestamp`.
    * `:terminated` — `:id`, `:run_id`, `:exception`, `:error_class_on_exception`.
    * `:unknown` — a type this client has not met, with `:event_type` and the raw `:json`
      undecoded. A newer server can add one, and dropping the bus over it would be worse than
      handing it on.
  """
  @type event :: %{required(:type) => event_type(), optional(atom()) => term()}

  @typedoc "What `status/1` answers."
  @type status :: %{
          message: String.t(),
          is_data_available: boolean(),
          is_trigger_active: boolean(),
          is_active: boolean()
        }

  # How long one server-side wait lasts before `await_termination/2` asks again. See there.
  @default_interval 10_000

  # Keys whose *values* are left exactly as Spark sent them: an offset is a string for one
  # source and a JSON object for another, and observed metrics are keyed by the caller's names.
  @verbatim_keys ~w(startOffset endOffset latestOffset observedMetrics)

  @doc false
  # Built by `Latu.DataFrame.write_stream/2` from the `WriteStreamOperationStartResult`.
  @spec started(Session.t(), map()) :: t()
  def started(%Session{} = session, %{query_id: %{id: id, run_id: run_id}, name: name}) do
    %__MODULE__{session: session, id: id, run_id: run_id, name: named(name)}
  end

  # =============================================
  # Status
  # =============================================

  @doc "Whether the query is still running."
  @spec is_active(t()) :: {:ok, boolean()} | {:error, Error.t()}
  def is_active(%__MODULE__{} = query) do
    with {:ok, status} <- status(query), do: {:ok, status.is_active}
  end

  @doc "Like `is_active/1`, raising on failure."
  @spec is_active!(t()) :: boolean()
  def is_active!(%__MODULE__{} = query), do: unwrap!(is_active(query))

  @doc """
  The query's status: its message, whether data is waiting, whether a trigger is running, and
  whether it is active at all.

      {:ok, %{message: "Waiting for data to arrive", is_active: true}} =
        Latu.StreamingQuery.status(query)
  """
  @spec status(t()) :: {:ok, status()} | {:error, Error.t()}
  def status(%__MODULE__{} = query) do
    with {:ok, {:status, status}, _session} <- answer(query, :status, :status) do
      {:ok,
       %{
         message: status.status_message,
         is_data_available: status.is_data_available,
         is_trigger_active: status.is_trigger_active,
         is_active: status.is_active
       }}
    end
  end

  @doc "Like `status/1`, raising on failure."
  @spec status!(t()) :: status()
  def status!(%__MODULE__{} = query), do: unwrap!(status(query))

  @doc """
  The most recent progress report, or `nil` before the first batch.

      {:ok, %{batch_id: 3, sources: [%{num_input_rows: 2}]}} =
        Latu.StreamingQuery.last_progress(query)

  Spark's own JSON, keys snake-cased into atoms. The server keeps reports for a while after a
  query terminates, so this answers on a stopped query too.
  """
  @spec last_progress(t()) :: {:ok, progress() | nil} | {:error, Error.t()}
  def last_progress(%__MODULE__{} = query) do
    with {:ok, reports} <- progress(query, :last_progress), do: {:ok, List.last(reports)}
  end

  @doc "Like `last_progress/1`, raising on failure."
  @spec last_progress!(t()) :: progress() | nil
  def last_progress!(%__MODULE__{} = query), do: unwrap!(last_progress(query))

  @doc "The recent progress reports, oldest first, as `last_progress/1` decodes them."
  @spec recent_progress(t()) :: {:ok, [progress()]} | {:error, Error.t()}
  def recent_progress(%__MODULE__{} = query), do: progress(query, :recent_progress)

  @doc "Like `recent_progress/1`, raising on failure."
  @spec recent_progress!(t()) :: [progress()]
  def recent_progress!(%__MODULE__{} = query), do: unwrap!(recent_progress(query))

  defp progress(%__MODULE__{} = query, arm) do
    with {:ok, {:recent_progress, %{recent_progress_json: reports}}, _session} <-
           answer(query, arm, :recent_progress) do
      {:ok, Enum.map(reports, &decode_progress/1)}
    end
  end

  @doc """
  Decode one progress report from the JSON the server sends, as `last_progress/1` does.

  Every key becomes a snake-cased atom, except under `start_offset`, `end_offset`,
  `latest_offset` and `observed_metrics`, whose values are left exactly as Spark wrote them:
  an offset is a string for one source and a JSON object for another, and observed metrics
  are keyed by the caller's own names. Spark's own key set is small and fixed, so the atoms
  are bounded.

      iex> Latu.StreamingQuery.decode_progress(~s({"batchId": 3, "durationMs": {"addBatch": 12},
      ...>   "sources": [{"startOffset": "21", "numInputRows": 2}],
      ...>   "observedMetrics": {"myMetric": {"rowCount": 2}}}))
      %{
        batch_id: 3,
        duration_ms: %{add_batch: 12},
        sources: [%{start_offset: "21", num_input_rows: 2}],
        observed_metrics: %{"myMetric" => %{"rowCount" => 2}}
      }
  """
  @spec decode_progress(String.t()) :: progress()
  def decode_progress(json) when is_binary(json), do: json |> JSON.decode!() |> snake()

  @doc """
  Why the query stopped, if it failed: `{:ok, nil}` while it runs or after a clean stop, or
  `{:ok, %Latu.Error{kind: :query}}` carrying Spark's error class and the server's stack trace.

  The poll for a query nobody is waiting on. `await_termination/2` gets the same failure as its
  own `{:error, _}`.
  """
  @spec exception(t()) :: {:ok, Error.t() | nil} | {:error, Error.t()}
  def exception(%__MODULE__{} = query) do
    with {:ok, result, _session} <- command(query, :exception) do
      case result do
        nil -> {:ok, nil}
        {:exception, failure} -> {:ok, failed(failure)}
        other -> unexpected(:exception, other)
      end
    end
  end

  @doc "Like `exception/1`, raising on failure."
  @spec exception!(t()) :: Error.t() | nil
  def exception!(%__MODULE__{} = query), do: unwrap!(exception(query))

  @doc """
  Print the query's physical plan, as `Latu.explain/2` does for a frame.

  ## Options

    * `:mode` — `:simple` (the default) or `:extended`. The streaming explain has two levels
      where the frame's has five; the spelling is the same so the two read alike.
  """
  @spec explain(t(), keyword()) :: :ok | {:error, Error.t()}
  def explain(%__MODULE__{} = query, opts \\ []) do
    with {:ok, plan} <- explain_string(query, opts), do: IO.puts(String.trim_trailing(plan))
  end

  @doc "Like `explain/2`, raising on failure."
  @spec explain!(t(), keyword()) :: :ok
  def explain!(%__MODULE__{} = query, opts \\ []), do: unwrap!(explain(query, opts))

  @doc """
  The query's physical plan as a string, where `explain/2` prints it.

  ## Options

  `explain/2`'s: `:mode`.
  """
  @spec explain_string(t(), keyword()) :: {:ok, String.t()} | {:error, Error.t()}
  def explain_string(%__MODULE__{} = query, opts \\ []) do
    opts = Keyword.validate!(opts, mode: :simple)

    with {:ok, {:explain, %{result: plan}}, _session} <-
           answer(query, {:explain, extended?(opts[:mode])}, :explain) do
      {:ok, plan}
    end
  end

  @doc "Like `explain_string/2`, raising on failure."
  @spec explain_string!(t(), keyword()) :: String.t()
  def explain_string!(%__MODULE__{} = query, opts \\ []) do
    unwrap!(explain_string(query, opts))
  end

  defp extended?(:simple), do: false
  defp extended?(:extended), do: true

  defp extended?(mode) do
    raise ArgumentError,
          "unknown explain mode #{inspect(mode)}, expected :simple or :extended"
  end

  # =============================================
  # Control
  # =============================================

  @doc """
  Stop the query. Idempotent on the server: stopping a query that has already terminated is a
  no-op, which is what lets `Latu.with_stream/3` stop in an `after` whatever happened.
  """
  @spec stop(t()) :: :ok | {:error, Error.t()}
  def stop(%__MODULE__{} = query) do
    with {:ok, _result, _session} <- command(query, :stop), do: :ok
  end

  @doc "Like `stop/1`, raising on failure."
  @spec stop!(t()) :: :ok
  def stop!(%__MODULE__{} = query), do: unwrap!(stop(query))

  @doc """
  Block until every row the source has now has been processed.

  A bounded-source verb: over a file directory it returns when the directory is drained, and
  over a rate or Kafka source it never returns, because there is always more. One server call
  with no timeout, so a long drain runs into the transport's empty-reattach ceiling
  (`Latu.Client`) rather than being looped as `await_termination/2` is. The deterministic test
  shape is `trigger: :available_now` and `await_termination/2` instead.
  """
  @spec process_all_available(t()) :: :ok | {:error, Error.t()}
  def process_all_available(%__MODULE__{} = query) do
    with {:ok, _result, _session} <- command(query, :process_all_available), do: :ok
  end

  @doc "Like `process_all_available/1`, raising on failure."
  @spec process_all_available!(t()) :: :ok
  def process_all_available!(%__MODULE__{} = query), do: unwrap!(process_all_available(query))

  @doc """
  Wait for the query to end.

      {:ok, true} = Latu.StreamingQuery.await_termination(query)
      {:ok, false} = Latu.StreamingQuery.await_termination(query, timeout: 30_000)

  `{:ok, true}` once it has stopped, by `stop/1` or by an `:available_now` trigger running out
  of data; `{:ok, false}` when the timeout passes first. **A query that failed comes back as
  `{:error, %Latu.Error{}}` carrying its own exception**, the server's `awaitTermination`
  rethrowing it, so waiting is how a failure finds you.

  The wait is a loop of bounded server waits, `awaitTermination(interval)` until it answers
  true. A streaming command sends nothing while it waits, and a Connect server ends any silent
  response stream every `senderMaxStreamDuration`, so one unbounded wait would hit Latu's
  empty-reattach ceiling on a healthy query. Spark's semantics are kept; only the RPC is cut
  into slices. `docs/decisions.md` has the measurement.

  ## Options

    * `:timeout` — how long to wait in total, or `:infinity`. Defaults to `:infinity`.
    * `:interval` — how long each server-side wait lasts. Defaults to `10_000`. Shorter
      means more round trips; longer means a single `ExecutePlan` idles longer, and only
      matters when it exceeds the server's `senderMaxStreamDuration`.
  """
  @spec await_termination(t(), keyword()) :: {:ok, boolean()} | {:error, Error.t()}
  def await_termination(%__MODULE__{} = query, opts \\ []) do
    {timeout, interval} = wait_options!(opts)

    wait(timeout, interval, fn slice ->
      with {:ok, {:await_termination, %{terminated: terminated?}}, _session} <-
             answer(query, {:await_termination, slice}, :await_termination) do
        {:ok, terminated?}
      end
    end)
  end

  @doc "Like `await_termination/2`, raising on failure."
  @spec await_termination!(t(), keyword()) :: boolean()
  def await_termination!(%__MODULE__{} = query, opts \\ []) do
    unwrap!(await_termination(query, opts))
  end

  # =============================================
  # The session's queries
  # =============================================

  @doc """
  Every query running on the session, started by this client or not.

  The recovery route: a query outlives the process that started it, and this is how the next
  process finds it.
  """
  @spec active(Session.t()) :: {:ok, [t()]} | {:error, Error.t()}
  def active(%Session{} = session) do
    with {:ok, {:active, %{active_queries: queries}}, session} <-
           manager(session, :active, :active) do
      {:ok, Enum.map(queries, &instance(session, &1))}
    end
  end

  @doc "Like `active/1`, raising on failure."
  @spec active!(Session.t()) :: [t()]
  def active!(%Session{} = session), do: unwrap!(active(session))

  @doc """
  One active query by id, or `nil`. The answer carries the current run id, which is what
  makes a handle recovered here usable where a remembered one may not be.
  """
  @spec get(Session.t(), String.t()) :: {:ok, t() | nil} | {:error, Error.t()}
  def get(%Session{} = session, id) when is_binary(id) do
    with {:ok, result, session} <- manager(session, {:get_query, id}) do
      case result do
        nil -> {:ok, nil}
        {:query, query} -> {:ok, instance(session, query)}
        other -> unexpected(:get_query, other)
      end
    end
  end

  @doc "Like `get/2`, raising on failure."
  @spec get!(Session.t(), String.t()) :: t() | nil
  def get!(%Session{} = session, id), do: unwrap!(get(session, id))

  @doc """
  Wait for any query on the session to end. `await_termination/2`'s loop and options, over
  the session.

  Sticky, as Spark's is: once any query has terminated since the session started or since
  `reset_terminated/1`, it answers `{:ok, true}` at once. A query that failed comes back as
  `{:error, _}` carrying its exception, again as Spark's does.

  ## Options

  `await_termination/2`'s: `:timeout`, `:interval`.
  """
  @spec await_any_termination(Session.t(), keyword()) :: {:ok, boolean()} | {:error, Error.t()}
  def await_any_termination(%Session{} = session, opts \\ []) do
    {timeout, interval} = wait_options!(opts)

    wait(timeout, interval, fn slice ->
      with {:ok, {:await_any_termination, %{terminated: terminated?}}, _session} <-
             manager(session, {:await_any_termination, slice}, :await_any_termination) do
        {:ok, terminated?}
      end
    end)
  end

  @doc "Like `await_any_termination/2`, raising on failure."
  @spec await_any_termination!(Session.t(), keyword()) :: boolean()
  def await_any_termination!(%Session{} = session, opts \\ []) do
    unwrap!(await_any_termination(session, opts))
  end

  @doc "Forget the terminated queries `await_any_termination/2` would otherwise answer for."
  @spec reset_terminated(Session.t()) :: :ok | {:error, Error.t()}
  def reset_terminated(%Session{} = session) do
    with {:ok, _result, _session} <- manager(session, :reset_terminated), do: :ok
  end

  @doc "Like `reset_terminated/1`, raising on failure."
  @spec reset_terminated!(Session.t()) :: :ok
  def reset_terminated!(%Session{} = session), do: unwrap!(reset_terminated(session))

  # =============================================
  # The listener bus
  # =============================================

  @doc """
  Every streaming event on the session, as a lazy `Stream`.

      session
      |> Latu.StreamingQuery.events()
      |> Stream.filter(&(&1.type == :progress))
      |> Enum.take(3)

  PySpark registers a listener object and calls back on a thread it owns; Latu hands you the
  events and lets you decide where they run, as `Latu.Progress` does for a batch query. Nothing
  is spawned: the bus opens when the stream is first enumerated, and closes when the
  enumeration ends, however it ends.

  Each element is a map with a `:type` — see `t:event/0`. A progress event's `:progress` is
  what `last_progress/1` answers, so `decode_progress/1` is the whole decoder for both.

  Four things to know, all of them the server's doing:

    * **One bus per session.** The server answers a second `add` with a log line and nothing on
      the wire, so a second `events/1` on the same session hangs until the reattach guard gives
      up and then names this as the likely cause. Enumerate one bus per session at a time.
    * **Closing is a command, not a release.** Halting the stream sends
      `remove_listener_bus_listener`; if that fails it is logged and the bus stays registered
      for the life of the session. A `Latu.disconnect/2` clears it either way.
    * **Events are at-least-once.** A reattach replays the responses the server still holds,
      and neither Latu nor PySpark acknowledges an event, so a duplicate is possible. Match on
      `:run_id` and `:batch_id` if that matters.
    * **A silent bus is the normal state**, so this is the one execution in Latu where an idle
      stream is not eventually an error. Before the first response it still is.

  Raises `Latu.Error` on failure, as `Latu.stream/2` does, since an enumeration has no way to
  return one.
  """
  @spec events(Session.t()) :: Enumerable.t()
  def events(%Session{} = session) do
    session
    |> Client.responses(Plan.new(Plan.streaming_query_listener_bus_command(:add)),
      silence: :expected,
      close: Plan.new(Plan.streaming_query_listener_bus_command(:remove))
    )
    |> Stream.flat_map(fn
      {:events, events} -> Enum.map(events, &decode_event/1)
      {:listener_bus, :open} -> []
      # The bus's own ExecutePlan reports progress like any other, and on a server with a
      # short `progress.reportInterval` it does so constantly. That is the *execution's*
      # progress, not a query's, so it is dropped here as `Latu.stream/2` drops it.
      {:progress, %Latu.Progress{}} -> []
      {:done, _execution} -> []
      {:error, error} -> raise error
    end)
  end

  @doc false
  # Public so the offline tests can pin the three shapes without a server, as
  # `Latu.DataFrame.write_stream_command/2` is. Not API: `events/1` is.
  def decode_event(%{event_type: :QUERY_PROGRESS_EVENT, event_json: json}) do
    decoded(:progress, json)
  end

  def decode_event(%{event_type: :QUERY_IDLE_EVENT, event_json: json}) do
    decoded(:idle, json)
  end

  def decode_event(%{event_type: :QUERY_TERMINATED_EVENT, event_json: json}) do
    decoded(:terminated, json)
  end

  def decode_event(%{event_type: type, event_json: json}) do
    %{type: :unknown, event_type: type, json: json}
  end

  defp decoded(type, json), do: json |> decode_progress() |> Map.put(:type, type)

  # =============================================
  # Waiting
  # =============================================

  defp wait_options!(opts) do
    opts = Keyword.validate!(opts, timeout: :infinity, interval: @default_interval)
    {timeout!(opts[:timeout]), interval!(opts[:interval])}
  end

  defp timeout!(:infinity), do: :infinity
  defp timeout!(timeout) when is_integer(timeout) and timeout > 0, do: timeout

  defp timeout!(timeout) do
    raise ArgumentError, ":timeout is a positive integer or :infinity, not #{inspect(timeout)}"
  end

  defp interval!(interval) when is_integer(interval) and interval > 0, do: interval

  defp interval!(interval) do
    raise ArgumentError, ":interval is a positive integer, not #{inspect(interval)}"
  end

  # One bounded server wait at a time, until the server says terminated or the caller's budget
  # runs out. `ask` takes the milliseconds this slice may last and answers `{:ok, boolean}`.
  defp wait(timeout, interval, ask), do: loop(deadline(timeout), interval, ask)

  defp loop(deadline, interval, ask) do
    case slice(deadline, interval) do
      :expired ->
        {:ok, false}

      slice ->
        case ask.(slice) do
          {:ok, true} -> {:ok, true}
          {:ok, false} -> loop(deadline, interval, ask)
          {:error, _} = error -> error
        end
    end
  end

  defp deadline(:infinity), do: :infinity
  defp deadline(timeout), do: System.monotonic_time(:millisecond) + timeout

  defp slice(:infinity, interval), do: interval

  defp slice(deadline, interval) do
    case deadline - System.monotonic_time(:millisecond) do
      left when left <= 0 -> :expired
      left -> min(left, interval)
    end
  end

  # =============================================
  # The wire
  # =============================================

  # One StreamingQueryCommand, answering with the result's arm — `nil` when the server set none,
  # which for `stop`, `process_all_available` and an `exception` with nothing to report is the
  # whole answer.
  defp command(%__MODULE__{} = query, arm) do
    plan = Plan.new(Plan.streaming_query_command(query.id, query.run_id, arm))

    case Client.execute_command(query.session, plan) do
      {:ok, %{streaming_query_command_result: nil}} ->
        {:error, Error.new(:protocol, "the server answered #{inspect(arm)} with no result")}

      {:ok, %{streaming_query_command_result: result, session: session}} ->
        {:ok, result.result_type, session}

      {:error, _} = error ->
        error
    end
  end

  # The same command, insisting on the arm the question implies.
  defp answer(%__MODULE__{} = query, arm, expected) do
    with {:ok, result, session} <- command(query, arm) do
      case result do
        {^expected, _payload} -> {:ok, result, session}
        other -> unexpected(arm, other)
      end
    end
  end

  defp manager(%Session{} = session, arm) do
    plan = Plan.new(Plan.streaming_query_manager_command(arm))

    case Client.execute_command(session, plan) do
      {:ok, %{streaming_query_manager_command_result: nil}} ->
        {:error, Error.new(:protocol, "the server answered #{inspect(arm)} with no result")}

      {:ok, %{streaming_query_manager_command_result: result, session: session}} ->
        {:ok, result.result_type, session}

      {:error, _} = error ->
        error
    end
  end

  defp manager(%Session{} = session, arm, expected) do
    with {:ok, result, session} <- manager(session, arm) do
      case result do
        {^expected, _payload} -> {:ok, result, session}
        other -> unexpected(arm, other)
      end
    end
  end

  defp unexpected(arm, nil) do
    {:error, Error.new(:protocol, "asked #{inspect(arm)} and the server answered with no arm")}
  end

  defp unexpected(arm, {answered, _payload}) do
    message = "asked #{inspect(arm)} and the server answered with #{inspect(answered)}"
    {:error, Error.new(:protocol, message)}
  end

  defp instance(%Session{} = session, %{id: %{id: id, run_id: run_id}, name: name}) do
    %__MODULE__{session: session, id: id, run_id: run_id, name: named(name)}
  end

  # A query started without a name comes back as `""` from the start command and as an absent
  # optional from the manager. One spelling here.
  defp named(nil), do: nil
  defp named(""), do: nil
  defp named(name), do: name

  # `exception_message` is the JVM exception's `toString`: the class name and a colon ahead of
  # Spark's own `[CLASS] message`. PySpark drops the prefix the same way.
  defp failed(%{exception_message: message, error_class: class, stack_trace: trace}) do
    Error.new(:query, without_class(message), error_class: class, stacktrace: trace)
  end

  defp without_class(nil), do: "the query failed, and the server sent no message"

  defp without_class(message) do
    case Regex.run(~r/^[\w.$]+: (.+)$/s, message) do
      [_all, rest] -> rest
      nil -> message
    end
  end

  defp snake(map) when is_map(map) do
    Map.new(map, fn
      {key, value} when key in @verbatim_keys -> {atom_key(key), value}
      {key, value} -> {atom_key(key), snake(value)}
    end)
  end

  defp snake(list) when is_list(list), do: Enum.map(list, &snake/1)
  defp snake(value), do: value

  defp atom_key(key), do: key |> Macro.underscore() |> String.to_atom()

  defp unwrap!({:ok, value}), do: value
  defp unwrap!(:ok), do: :ok
  defp unwrap!({:error, error}), do: raise(error)
end
