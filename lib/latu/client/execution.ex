defmodule Latu.Client.Execution do
  @moduledoc false
  # One ExecutePlan, as a state machine: events in, actions out, no IO.
  #
  # No IO, so every branch is testable with no server — the same reason Latu.Session.confirm/3
  # lives on Session. Reattach is the one part of Latu where being wrong yields silently wrong
  # data rather than an error, and every decision that could make it wrong lives here. Backoff
  # jitter is the only nondeterminism, and it is bounded.

  alias Latu.Error
  alias Latu.Protocol.Spark.Connect, as: Proto
  alias Latu.Retry
  alias Latu.Session

  # A sender that delivers nothing still costs a whole stream duration, so a slow query
  # legitimately produces empty reattaches and this must not be tight. It catches only a server
  # configured so that no sender can ever deliver a response, which would reattach forever.
  # The retry policy proper is a separate concern. See dev/README.md.
  @max_empty_reattaches 100

  # Spark reports a lost execution or session in the message, with no status code of its own to
  # match on. PySpark detects it the same way.
  @lost_handle ["INVALID_HANDLE.OPERATION_NOT_FOUND", "INVALID_HANDLE.SESSION_NOT_FOUND"]

  @enforce_keys [:session, :operation_id]
  defstruct [
    :session,
    :operation_id,
    :last_response_id,
    :schema,
    :command_result,
    :ml_command_result,
    :write_stream_operation_start_result,
    :streaming_query_command_result,
    :streaming_query_manager_command_result,
    :listener_bus,
    :checkpointed,
    :metrics,
    :progress,
    silence: :fatal,
    observed: %{},
    rows: 0,
    events: 0,
    complete?: false,
    empty: 0,
    retries: 0
  ]

  @typedoc """
  One `observe` name's metrics, exactly as the server sent them.

  Opaque cargo, like `command_result`: this module must not know what a literal means.
  `Latu.DataFrame` decodes them through `Latu.Result.Literal`.
  """
  @type observed :: Proto.ExecutePlanResponse.ObservedMetrics.t()

  @typedoc "One Arrow IPC stream, complete in itself: schema, one record batch, end marker."
  @type batch :: %{
          data: binary(),
          row_count: non_neg_integer(),
          start_offset: non_neg_integer() | nil
        }

  @typedoc """
  Whether a stream that sends nothing is a broken server or the normal state.

  `:fatal` for a result or a command: the caller asked for something, so a server that never
  delivers it is misconfigured and `@max_empty_reattaches` empty streams say so. `:expected`
  for the streaming listener bus, which is silent whenever no query is doing anything and must
  stay open for hours — there, silence is only fatal *before the first response*, which is what
  still catches a server that never answers at all. See `Latu.StreamingQuery.events/1`.
  """
  @type silence :: :fatal | :expected

  @type t :: %__MODULE__{
          session: Session.t(),
          operation_id: String.t(),
          last_response_id: String.t() | nil,
          silence: silence(),
          schema: Proto.DataType.t() | nil,
          command_result: Proto.Relation.t() | nil,
          ml_command_result: Proto.MlCommandResult.t() | nil,
          write_stream_operation_start_result: Proto.WriteStreamOperationStartResult.t() | nil,
          streaming_query_command_result: Proto.StreamingQueryCommandResult.t() | nil,
          streaming_query_manager_command_result:
            Proto.StreamingQueryManagerCommandResult.t() | nil,
          listener_bus: :open | nil,
          checkpointed: String.t() | nil,
          metrics: Proto.ExecutePlanResponse.Metrics.t() | nil,
          progress: Proto.ExecutePlanResponse.ExecutionProgress.t() | nil,
          observed: %{optional(String.t()) => observed()},
          rows: non_neg_integer(),
          events: non_neg_integer(),
          complete?: boolean(),
          empty: non_neg_integer(),
          retries: non_neg_integer()
        }

  @typedoc "What the transport saw."
  @type event :: {:response, Proto.ExecutePlanResponse.t()} | :eof | {:error, Error.t()}

  # `event/0` above is the transport's. A streaming query's own events are a different thing
  # entirely, so they keep the protocol's full name rather than shadowing it.
  @typedoc "One streaming listener event, as the server sent it: JSON plus a type enum."
  @type listener_event :: Proto.StreamingQueryListenerEvent.t()

  @typedoc "What the transport should do about it."
  @type action ::
          {:emit, batch()}
          | {:emit_events, [listener_event()]}
          | :pull
          | {:reattach, non_neg_integer()}
          | {:restart, non_neg_integer()}
          | {:done, t()}
          | {:fail, Error.t()}

  @doc """
  `operation_id` is the caller's, generated before the first call — see `Latu.Client`.

  `:silence` says whether a stream that delivers nothing is a broken server. Defaults to
  `:fatal`; see `t:silence/0`.
  """
  @spec new(Session.t(), String.t(), keyword()) :: t()
  def new(%Session{} = session, operation_id, opts \\ []) when is_binary(operation_id) do
    opts = Keyword.validate!(opts, silence: :fatal)

    %__MODULE__{
      session: session,
      operation_id: operation_id,
      silence: silence!(opts[:silence])
    }
  end

  defp silence!(silence) when silence in [:fatal, :expected], do: silence

  defp silence!(silence) do
    raise ArgumentError, ":silence is :fatal or :expected, not #{inspect(silence)}"
  end

  @doc "Fold one event in and say what to do next."
  @spec step(t(), event()) :: {action(), t()}
  def step(%__MODULE__{} = execution, {:response, %Proto.ExecutePlanResponse{} = response}) do
    with {:ok, session} <-
           Session.confirm(
             execution.session,
             response.session_id,
             response.server_side_session_id
           ),
         :ok <- check_operation(execution, response.operation_id) do
      %{execution | session: session, empty: 0, retries: 0}
      |> latch_schema(response.schema)
      |> latch_metrics(response.metrics)
      |> observe(response.observed_metrics)
      |> track(response.response_id)
      |> take(response.response_type)
    else
      {:error, error} -> {{:fail, error}, execution}
    end
  end

  # A reattachable stream that ends without ResultComplete means there is more data. Taking it
  # for the end is exactly how a client silently truncates a long result.
  def step(%__MODULE__{complete?: true} = execution, :eof) do
    {{:done, execution}, execution}
  end

  def step(%__MODULE__{} = execution, :eof) do
    if silent_too_long?(execution) do
      {{:fail, went_quiet(execution)}, execution}
    else
      {{:reattach, 0}, %{execution | empty: execution.empty + 1}}
    end
  end

  def step(%__MODULE__{retries: retries} = execution, {:error, %Error{} = error}) do
    retry = policy(execution)
    budget = retries < retry.max_retries

    cond do
      # The server has no record of this execution. If nothing had arrived yet, the original
      # ExecutePlan never landed and re-sending it under the same operation_id is safe.
      lost_handle?(error) and is_nil(execution.last_response_id) and budget ->
        wait = Retry.wait(retry, retries, error.retry_delay)
        {{:restart, wait}, %{execution | retries: retries + 1}}

      # If responses had already arrived, they are gone and re-sending would duplicate the ones
      # that did. PySpark raises RESPONSE_ALREADY_RECEIVED here for the same reason.
      lost_handle?(error) ->
        {{:fail, unrecoverable(execution, error)}, execution}

      budget and Retry.retryable?(error) ->
        wait = Retry.wait(retry, retries, error.retry_delay)
        {{:reattach, wait}, %{execution | retries: retries + 1}}

      true ->
        {{:fail, give_up(execution, error)}, execution}
    end
  end

  # =============================================
  # Silence
  # =============================================

  # An `:expected` execution is the listener bus, which is silent whenever no query is doing
  # anything and has to stay open for hours — so `@max_empty_reattaches` cannot apply to it
  # once it is running. Before the first response it still does, and that is the case worth
  # keeping: a redundant `add_listener_bus_listener` is answered by the server with a log line
  # and nothing on the wire, so without this bound the stream would reattach forever.
  defp silent_too_long?(%__MODULE__{silence: :expected, last_response_id: nil} = execution) do
    execution.empty >= @max_empty_reattaches
  end

  defp silent_too_long?(%__MODULE__{silence: :expected}), do: false

  defp silent_too_long?(%__MODULE__{empty: empty}), do: empty >= @max_empty_reattaches

  defp went_quiet(%__MODULE__{silence: :expected, empty: empty}) do
    Error.new(
      :protocol,
      "the server ended #{empty} response streams in a row without ever answering; a " <>
        "listener bus is not open until the server acknowledges it, and it answers a second " <>
        "one on the same session with silence — check whether this session already has one"
    )
  end

  defp went_quiet(%__MODULE__{empty: empty}) do
    Error.new(
      :protocol,
      "the server ended #{empty} response streams in a row without sending anything; " <>
        "senderMaxStreamDuration is likely too short for it to make progress"
    )
  end

  # =============================================
  # Retries
  # =============================================

  # Which errors are retryable, how often and how long is the session's `Latu.Retry`; the one
  # case of its own here is a lost handle, which only an execution can have.
  defp policy(%__MODULE__{session: %Session{retry: %Retry{} = retry}}), do: retry

  defp lost_handle?(%Error{message: message}) do
    Enum.any?(@lost_handle, &String.contains?(message, &1))
  end

  # Nothing had arrived, so the only way here is a spent budget.
  defp unrecoverable(%__MODULE__{last_response_id: nil} = execution, %Error{} = error) do
    give_up(execution, error)
  end

  defp unrecoverable(%__MODULE__{}, %Error{} = error) do
    %{
      error
      | message:
          "the server lost this execution after sending responses, so the result cannot be " <>
            "recovered: #{error.message}"
    }
  end

  defp give_up(%__MODULE__{retries: retries} = execution, %Error{} = error) do
    if retries >= policy(execution).max_retries do
      %{error | message: "#{error.message} (gave up after #{retries} retries)"}
    else
      error
    end
  end

  # =============================================
  # Identity
  # =============================================

  # The server echoes the operation_id Latu generated. An absent one is no information.
  defp check_operation(%__MODULE__{}, id) when id in [nil, ""], do: :ok
  defp check_operation(%__MODULE__{operation_id: id}, id), do: :ok

  defp check_operation(%__MODULE__{operation_id: expected}, got) do
    {:error, Error.new(:session, "server answered for operation #{got}, expected #{expected}")}
  end

  # The result's DataType rides outside the response_type oneof, on a response of its own
  # before any batch; a replay after a reattach may resend it, so the first one wins. Held as
  # opaque cargo — interpreting it is Latu.Result.Schema's job, not the state machine's.
  defp latch_schema(%__MODULE__{schema: nil} = execution, schema) when not is_nil(schema) do
    %{execution | schema: schema}
  end

  defp latch_schema(execution, _schema), do: execution

  # Spark's own per-node SQL metrics, field 4 — also outside the oneof, so `take/2` never sees
  # it and it is not the catch-all that was dropping this. Last one wins rather than first: the
  # server sends progressively more complete metrics as a query runs, and the final message is
  # the finished picture. A replay after a reattach resends the same values.
  defp latch_metrics(%__MODULE__{} = execution, metrics) when not is_nil(metrics) do
    %{execution | metrics: metrics}
  end

  defp latch_metrics(execution, _metrics), do: execution

  # Observed metrics also ride outside the oneof, and unlike the schema they are keyed: one
  # entry per `observe` name. Last one wins, which is PySpark's rule — it calls `dict.update`
  # on the observation's result — and a replay after a reattach resends the same values from
  # the same execution, so which one wins does not change the answer. Opaque cargo, as the
  # schema is.
  defp observe(%__MODULE__{} = execution, [_ | _] = metrics) do
    %{execution | observed: Map.merge(execution.observed, Map.new(metrics, &{&1.name, &1}))}
  end

  defp observe(%__MODULE__{} = execution, _none), do: execution

  # Every response carries a response id, batch or not, and it is the cursor a reattach resumes
  # from — so it is tracked for schema and metrics responses too.
  defp track(execution, id) when is_binary(id) and id != "" do
    %{execution | last_response_id: id}
  end

  defp track(execution, _id), do: execution

  # =============================================
  # Responses
  # =============================================

  defp take(execution, {:result_complete, _}), do: {:pull, %{execution | complete?: true}}

  # Progress *is* in the oneof, unlike the schema and the SQL metrics, so it arrives here. Held
  # as the latest one seen and read by the transport, which is the only layer allowed to call
  # the caller's handler — this module does no IO by contract.
  defp take(execution, {:execution_progress, progress}) do
    {:pull, %{execution | progress: progress}}
  end

  defp take(execution, {:arrow_batch, batch}) do
    cond do
      # Latu never asks for result chunking, so a chunked batch is a partial Arrow stream that
      # would decode as if it were whole. Refuse rather than truncate.
      is_integer(batch.chunk_index) ->
        {{:fail,
          Error.new(:protocol, "server chunked a result batch, which Latu did not request")},
         execution}

      # Also the assertion that a reattach neither dropped nor replayed a batch.
      is_integer(batch.start_offset) and batch.start_offset != execution.rows ->
        message = "batch starts at row #{batch.start_offset}, expected #{execution.rows}"
        {{:fail, Error.new(:protocol, message)}, execution}

      true ->
        taken = %{data: batch.data, row_count: batch.row_count, start_offset: batch.start_offset}
        {{:emit, taken}, %{execution | rows: execution.rows + batch.row_count}}
    end
  end

  # A CheckpointCommand answers with the id of a relation the server is now holding. Latched
  # like the schema and the SQL result, and for the same reason: a replay after a reattach must
  # not clobber it. The *id* rather than the message, because that is the whole of it and it is
  # what `remove_cached_relation/1` needs.
  defp take(%__MODULE__{checkpointed: nil} = execution, {:checkpoint_command_result, result})
       when not is_nil(result.relation) do
    {:pull, %{execution | checkpointed: result.relation.relation_id}}
  end

  # A SqlCommand answers with the relation to keep querying — the root `Latu.sql/3`'s
  # DataFrame wraps. Latched like the schema: first one wins, so a replay after a reattach
  # cannot clobber it. Opaque cargo here; `Latu.Plan.adopt/1` is what interprets it.
  defp take(%__MODULE__{command_result: nil} = execution, {:sql_command_result, result})
       when not is_nil(result.relation) do
    {:pull, %{execution | command_result: result.relation}}
  end

  # An MlCommand answers with a fitted model's handle, one fetched attribute, or a summary.
  # Latched like the SQL result, and for the same reason. The whole message rather than one
  # field, because which arm is set is the answer; opaque cargo either way, since this module
  # must not know what an `operator_info` means. `latu_ml` is what interprets it.
  defp take(%__MODULE__{ml_command_result: nil} = execution, {:ml_command_result, result})
       when not is_nil(result.result_type) do
    {:pull, %{execution | ml_command_result: result}}
  end

  # The three streaming commands each answer once, and are latched as the ML result is: first
  # one wins, the whole message, opaque. Unlike the ML arm none is guarded on a set `result_type`,
  # because for these an empty message *is* an answer — `stop` and `reset_terminated` carry no
  # payload, `exception` with nothing to report sets no arm, and `get_query` on an unknown id
  # sets none either. `Latu.StreamingQuery` is what interprets them.
  defp take(
         %__MODULE__{write_stream_operation_start_result: nil} = execution,
         {:write_stream_operation_start_result, result}
       ) do
    {:pull, %{execution | write_stream_operation_start_result: result}}
  end

  defp take(
         %__MODULE__{streaming_query_command_result: nil} = execution,
         {:streaming_query_command_result, result}
       ) do
    {:pull, %{execution | streaming_query_command_result: result}}
  end

  defp take(
         %__MODULE__{streaming_query_manager_command_result: nil} = execution,
         {:streaming_query_manager_command_result, result}
       ) do
    {:pull, %{execution | streaming_query_manager_command_result: result}}
  end

  # The listener bus, and the one arm that arrives many times on one ExecutePlan. It is NOT
  # latched: events are emitted and forgotten, exactly as a batch is, so a channel held open for
  # hours holds nothing. `events` is a count for the same reason `rows` is. The bus's opening
  # ack is a separate response and rides the latch, so `Latu.Client` sees it as one element.
  #
  # A replayed response after a reattach would re-emit its events. The server's own client has
  # the same property — it acknowledges nothing and asks for no offsets — so an event is
  # at-least-once and the docs say so rather than pretending otherwise.
  defp take(%__MODULE__{} = execution, {:streaming_query_listener_events_result, result}) do
    execution = open_bus(execution, result.listener_bus_listener_added)

    case result.events do
      [] ->
        {:pull, execution}

      events ->
        {{:emit_events, events}, %{execution | events: execution.events + length(events)}}
    end
  end

  # `schema`, `metrics` and `observed_metrics` sit outside the `response_type` oneof and are
  # handled in `step/2`, so a response may set no arm at all. The server will also grow arms
  # Latu has not met. Skip what we do not handle rather than rejecting it.
  defp take(execution, _response_type), do: {:pull, execution}

  # Below the last `take/2` clause on purpose: a helper between them would split the run, and
  # Elixir fails the build on non-contiguous clauses of one name.
  defp open_bus(%__MODULE__{listener_bus: nil} = execution, true) do
    %{execution | listener_bus: :open}
  end

  defp open_bus(execution, _added), do: execution
end
