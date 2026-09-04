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

  # gRPC status codes, spelled out because this module must not reference GRPC — see the
  # layering test.
  @unavailable 14
  @internal 13

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
    :checkpointed,
    :metrics,
    :progress,
    observed: %{},
    rows: 0,
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

  @type t :: %__MODULE__{
          session: Session.t(),
          operation_id: String.t(),
          last_response_id: String.t() | nil,
          schema: Proto.DataType.t() | nil,
          command_result: Proto.Relation.t() | nil,
          checkpointed: String.t() | nil,
          metrics: Proto.ExecutePlanResponse.Metrics.t() | nil,
          progress: Proto.ExecutePlanResponse.ExecutionProgress.t() | nil,
          observed: %{optional(String.t()) => observed()},
          rows: non_neg_integer(),
          complete?: boolean(),
          empty: non_neg_integer(),
          retries: non_neg_integer()
        }

  @typedoc "What the transport saw."
  @type event :: {:response, Proto.ExecutePlanResponse.t()} | :eof | {:error, Error.t()}

  @typedoc "What the transport should do about it."
  @type action ::
          {:emit, batch()}
          | :pull
          | {:reattach, non_neg_integer()}
          | {:restart, non_neg_integer()}
          | {:done, t()}
          | {:fail, Error.t()}

  @doc "`operation_id` is the caller's, generated before the first call — see `Latu.Client`."
  @spec new(Session.t(), String.t()) :: t()
  def new(%Session{} = session, operation_id) when is_binary(operation_id) do
    %__MODULE__{session: session, operation_id: operation_id}
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

  def step(%__MODULE__{empty: empty} = execution, :eof) when empty >= @max_empty_reattaches do
    {{:fail,
      Error.new(
        :protocol,
        "the server ended #{empty} response streams in a row without sending anything; " <>
          "senderMaxStreamDuration is likely too short for it to make progress"
      )}, execution}
  end

  def step(%__MODULE__{} = execution, :eof) do
    {{:reattach, 0}, %{execution | empty: execution.empty + 1}}
  end

  def step(%__MODULE__{retries: retries} = execution, {:error, %Error{} = error}) do
    retry = policy(execution)
    budget = retries < retry.max_retries

    cond do
      # The server has no record of this execution. If nothing had arrived yet, the original
      # ExecutePlan never landed and re-sending it under the same operation_id is safe.
      lost_handle?(error) and is_nil(execution.last_response_id) and budget ->
        {{:restart, Retry.wait(retry, retries)}, %{execution | retries: retries + 1}}

      # If responses had already arrived, they are gone and re-sending would duplicate the ones
      # that did. PySpark raises RESPONSE_ALREADY_RECEIVED here for the same reason.
      lost_handle?(error) ->
        {{:fail, unrecoverable(execution, error)}, execution}

      budget and retryable?(error) ->
        {{:reattach, Retry.wait(retry, retries)}, %{execution | retries: retries + 1}}

      true ->
        {{:fail, give_up(execution, error)}, execution}
    end
  end

  # =============================================
  # Retries
  # =============================================

  # Which errors are retryable is PySpark's DefaultPolicy; how often and how long is the
  # session's `Latu.Retry`. PySpark's third case — any error carrying RetryInfo metadata — is
  # not covered: elixir-grpc does not decode that detail.
  defp policy(%__MODULE__{session: %Session{retry: %Retry{} = retry}}), do: retry

  defp lost_handle?(%Error{message: message}) do
    Enum.any?(@lost_handle, &String.contains?(message, &1))
  end

  defp retryable?(%Error{status: @unavailable}), do: true

  defp retryable?(%Error{status: @internal, message: message}) do
    message =~ "INVALID_CURSOR.DISCONNECTED"
  end

  defp retryable?(%Error{}), do: false

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

  # `schema`, `metrics` and `observed_metrics` sit outside the `response_type` oneof and are
  # handled in `step/2`, so a response may set no arm at all. The server will also grow arms
  # Latu has not met. Skip what we do not handle rather than rejecting it.
  defp take(execution, _response_type), do: {:pull, execution}
end
