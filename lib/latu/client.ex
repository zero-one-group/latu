defmodule Latu.Client do
  # The gRPC transport: channels in, channels out. Nothing above this layer touches `GRPC`
  # directly. Internal for v0.1 — `Latu.stream/2` and `Latu.to_arrow/2` are the escape hatches;
  # docs/decisions.md (M12.6).
  @moduledoc false

  require Logger

  alias Latu.Client.Execution
  alias Latu.Error
  alias Latu.Internal.UUID
  alias Latu.Progress
  alias Latu.Protocol.Spark.Connect, as: Proto
  alias Latu.Protocol.Spark.Connect.SparkConnectService.Stub
  alias Latu.Session
  alias Latu.Telemetry

  @loopback ~w(localhost 127.0.0.1 ::1 0:0:0:0:0:0:0:1)

  # The one field each AnalyzePlan response arm carries. Read out of PySpark's
  # `AnalyzeResult.fromProto`; `persist` and `unpersist` carry none.
  @analyze_fields %{
    schema: :schema,
    explain: :explain_string,
    tree_string: :tree_string,
    is_local: :is_local,
    is_streaming: :is_streaming,
    input_files: :files,
    spark_version: :version,
    ddl_parse: :parsed,
    same_semantics: :result,
    semantic_hash: :result,
    get_storage_level: :storage_level,
    json_to_ddl: :ddl_string
  }

  @type batch :: Execution.batch()

  @doc """
  Open a channel and put it on the session.

  Refuses to send a bearer token in cleartext to anything but a loopback host.
  """
  @spec connect(Session.t()) :: {:ok, Session.t()} | {:error, Error.t()}
  def connect(%Session{channel: nil} = session) do
    with :ok <- check_token_transport(session),
         {:ok, opts} <- connect_opts(session),
         {:ok, channel} <- open(target(session), opts) do
      {:ok, %{session | channel: channel}}
    end
  end

  def connect(%Session{}) do
    {:error, Error.new(:connect, "session is already connected; disconnect/1 first")}
  end

  # Bounded on purpose, and much shorter than the session's own timeout: this is a courtesy
  # call on the way out, and a courtesy call that can block a shutdown for a minute is worse
  # than one that gives up. Set here rather than beside its use because the docstring below
  # reads it, and an attribute read before it is set is nil.
  @release_on_disconnect_timeout 5_000

  @doc """
  Close the channel. Idempotent.

  `release: true` also ends the session on the server first — every temp view, cached frame and
  artifact with it — instead of leaving Spark to time it out. **It is not the default**, and
  that is a deliberate retreat: see `docs/decisions.md`. `release_session/2` is the direct call
  and the one to reach for when you mean it.

  When asked for, the release is given #{@release_on_disconnect_timeout}ms rather than the
  session's own timeout — it is a courtesy on the way out, and one that can block a shutdown for
  a minute is worse than one that gives up — and **a failed release does not stop the close**,
  because ending a session the server has already forgotten is not a failure the caller can act
  on. It is logged at debug level. `release_session/2` reports it.
  """
  @spec disconnect(Session.t(), keyword()) :: {:ok, Session.t()} | {:error, Error.t()}
  def disconnect(session, opts \\ [])

  def disconnect(%Session{channel: nil} = session, _opts), do: {:ok, session}

  def disconnect(%Session{} = session, opts) do
    opts = Keyword.validate!(opts, release: false)
    session = if opts[:release], do: released(session), else: session

    case GRPC.Stub.disconnect(session.channel) do
      {:ok, _channel} -> {:ok, %{session | channel: nil}}
      {:error, reason} -> {:error, Error.new(:connect, "disconnect failed: #{inspect(reason)}")}
    end
  end

  defp released(%Session{} = session) do
    case release_session(session, timeout: @release_on_disconnect_timeout) do
      {:ok, session} ->
        session

      {:error, error} ->
        Logger.debug("ReleaseSession failed, closing the channel anyway: #{error.message}")
        session
    end
  end

  @doc """
  The Spark version the server reports.

  `AnalyzePlan` with the `spark_version` arm needs no `Plan`, which makes it the cheapest
  round-trip there is and the only capability negotiation the protocol offers.
  """
  @spec spark_version(Session.t()) :: {:ok, String.t()} | {:error, Error.t()}
  def spark_version(%Session{} = session) do
    with {:ok, version, _session} <-
           analyzed(session, {:spark_version, %Proto.AnalyzePlanRequest.SparkVersion{}}) do
      {:ok, version}
    end
  end

  @doc """
  One `AnalyzePlan` round-trip, returning the arm's single value.

  Every arm answers in a message holding exactly one field, and the request arm and the
  response arm share a name — so this table is the whole of the unwrapping. `persist` and
  `unpersist` answer with an empty message and go through `analyze/2` instead.
  """
  @spec analyzed(Session.t(), {atom(), struct()}) ::
          {:ok, term(), Session.t()} | {:error, Error.t()}
  def analyzed(%Session{} = session, {name, _message} = arm)
      when is_map_key(@analyze_fields, name) do
    with {:ok, response, session} <- analyze(session, arm) do
      case response.result do
        {^name, result} ->
          {:ok, Map.fetch!(result, @analyze_fields[name]), session}

        other ->
          {:error, Error.new(:protocol, "asked AnalyzePlan for #{name}, got: #{inspect(other)}")}
      end
    end
  end

  @doc """
  One `AnalyzePlan` round-trip, returning the response and a session pinned to the server's id.

  Callers that care about detecting a server restart across calls thread the returned session;
  see `Latu.Session.pin/2`.
  """
  @spec analyze(Session.t(), tuple()) ::
          {:ok, struct(), Session.t()} | {:error, Error.t()}
  def analyze(%Session{channel: nil}, _arm) do
    {:error, Error.new(:connect, "session is not connected; call Latu.connect/1 first")}
  end

  def analyze(%Session{} = session, arm) do
    request = %Proto.AnalyzePlanRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_session_id,
      user_context: user_context(session),
      client_type: session.client_type,
      analyze: arm
    }

    call = fn -> Stub.analyze_plan(session.channel, request, timeout: session.timeout) end

    with {:ok, response} <- rpc("AnalyzePlan", session, call),
         {:ok, session} <-
           Session.confirm(session, response.session_id, response.server_side_session_id) do
      {:ok, response, session}
    end
  end

  @doc """
  Run a plan, collect every Arrow batch it produces, and hand back the finished execution.

  Batches come out separately because they stream; everything else the run produced is on the
  `Latu.Client.Execution` struct — `schema` (the `DataType` the server sends before the first
  batch, `nil` when it sent none, and what `Latu.Result.Schema` checks before any bytes reach
  the decoder), `observed` (per-`observe`-name metrics), `command_result`, `session` and the
  row count. New things the server learns to send become fields there rather than a wider
  return tuple; `docs/decisions.md` has why.

  Reattaches when the server ends a response stream early, which it does deliberately after
  `senderMaxStreamDuration` or `senderMaxStreamSize`. A clean EOF with no `ResultComplete`
  means there is more data, not that the result is finished.

  No deadline is set, matching PySpark: a query may legitimately run for hours. A dead
  connection is caught by the channel's keepalive instead. To bound a *query*, run it in a task
  and **interrupt it on the server** — which works because Latu holds no processes of its own:

      session = Latu.Session.add_tag(session, "report")
      task = Task.async(fn -> Latu.Client.execute(session, plan) end)

      case Task.yield(task, 30_000) do
        {:ok, result} -> result
        nil ->
          Latu.interrupt(session, tag: "report")
          Task.await(task)
      end

  **Killing the task instead is not the same thing, and it is worse.** A Latu execution is
  reattachable, which is precisely the promise that a client may vanish and come back — so a
  killed client leaves the execution *running on the server*, holding its cluster resources
  until the detached timeout expires. Latu releases the execution when the stream ends
  normally, including on an error; `Task.shutdown/2` and a killed process skip that. Interrupt
  first, then let the task finish.
  """
  @spec execute(Session.t(), Proto.Plan.t(), keyword()) ::
          {:ok, [batch()], Execution.t()} | {:error, Error.t()}
  def execute(session, plan, opts \\ [])

  def execute(%Session{channel: nil}, _plan, _opts) do
    {:error, Error.new(:connect, "session is not connected; call Latu.connect/1 first")}
  end

  def execute(%Session{} = session, %Proto.Plan{} = plan, opts) do
    session
    |> responses(plan)
    |> watched(opts)
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, batch}, {:ok, batches} -> {:cont, {:ok, [batch | batches]}}
      {:done, execution}, {:ok, batches} -> {:halt, {:ok, Enum.reverse(batches), execution}}
      {:error, error}, _acc -> {:halt, {:error, error}}
      _tagged, acc -> {:cont, acc}
    end)
    |> case do
      {:ok, _batches} ->
        {:error, Error.new(:protocol, "the response stream stopped unaccountably")}

      result ->
        result
    end
  end

  @doc """
  Execute a command plan and hand back the finished execution.

  A `SqlCommand` answers with `sql_command_result`, which lands on `command_result` — the
  relation `Latu.sql/3` keeps querying. Commands with nothing to say (writes, views) leave it
  `nil` and run equally well through `execute/2`; this exists for the ones that answer, and for
  the metrics an observed write reports. PySpark's `execute_command`.
  """
  @spec execute_command(Session.t(), Proto.Plan.t(), keyword()) ::
          {:ok, Execution.t()} | {:error, Error.t()}
  def execute_command(session, plan, opts \\ [])

  def execute_command(%Session{channel: nil}, _plan, _opts) do
    {:error, Error.new(:connect, "session is not connected; call Latu.connect/1 first")}
  end

  def execute_command(%Session{} = session, %Proto.Plan{} = plan, opts) do
    session
    |> responses(plan)
    |> watched(opts)
    |> Enum.reduce_while(:running, fn
      {:done, execution}, _acc -> {:halt, {:ok, execution}}
      {:error, error}, _acc -> {:halt, {:error, error}}
      _tagged, acc -> {:cont, acc}
    end)
    |> case do
      :running -> {:error, Error.new(:protocol, "the response stream stopped unaccountably")}
      result -> result
    end
  end

  # =============================================
  # Configuration
  # =============================================

  @typedoc """
  One `ConfigRequest.Operation`, as a tagged tuple. What each arm actually does — and the three
  ways `get`, `get_option` and `get_with_default` differ — is on `Latu.conf/2` and friends.
  """
  @type config_op ::
          {:get, [String.t()]}
          | {:get_option, [String.t()]}
          | {:get_with_default, [{String.t(), String.t() | nil}]}
          | {:get_all, String.t() | nil}
          | {:set, [{String.t(), String.t()}]}
          | {:unset, [String.t()]}
          | {:is_modifiable, [String.t()]}

  @doc """
  One `Config` round-trip.

  Hands back the response's pairs in the server's own order; a value may be nil, because
  `KeyValue.value` is optional and three of the read arms use that to mean "not set".
  """
  @spec config(Session.t(), config_op()) ::
          {:ok, [{String.t(), String.t() | nil}], Session.t()} | {:error, Error.t()}
  def config(%Session{channel: nil}, _op) do
    {:error, Error.new(:connect, "session is not connected; call Latu.connect/1 first")}
  end

  def config(%Session{} = session, op) when is_tuple(op) do
    request = %Proto.ConfigRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_session_id,
      user_context: user_context(session),
      client_type: session.client_type,
      operation: %Proto.ConfigRequest.Operation{op_type: config_op(op)}
    }

    call = fn -> Stub.config(session.channel, request, timeout: session.timeout) end

    with {:ok, response} <- rpc("Config", session, call),
         {:ok, session} <-
           Session.confirm(session, response.session_id, response.server_side_session_id) do
      warn(op, response.warnings)
      {:ok, Enum.map(response.pairs, &{&1.key, &1.value}), session}
    end
  end

  defp config_op({:get, keys}), do: {:get, %Proto.ConfigRequest.Get{keys: keys}}

  defp config_op({:get_option, keys}) do
    {:get_option, %Proto.ConfigRequest.GetOption{keys: keys}}
  end

  defp config_op({:get_with_default, pairs}) do
    {:get_with_default, %Proto.ConfigRequest.GetWithDefault{pairs: key_values(pairs)}}
  end

  defp config_op({:get_all, prefix}), do: {:get_all, %Proto.ConfigRequest.GetAll{prefix: prefix}}

  # `silent` stays false: it turns a refused conf into a server-side warning and nothing else,
  # and a refusal Latu cannot return is not one it offers. docs/decisions.md (M12.6).
  defp config_op({:set, pairs}) do
    {:set, %Proto.ConfigRequest.Set{pairs: key_values(pairs), silent: false}}
  end

  defp config_op({:unset, keys}), do: {:unset, %Proto.ConfigRequest.Unset{keys: keys}}

  defp config_op({:is_modifiable, keys}) do
    {:is_modifiable, %Proto.ConfigRequest.IsModifiable{keys: keys}}
  end

  defp key_values(pairs) do
    Enum.map(pairs, fn {key, value} -> %Proto.KeyValue{key: key, value: value} end)
  end

  # The server warns about deprecated confs, and about the two it does not support under
  # Connect at all. It sends them for a read as well; PySpark surfaces them only on a write,
  # and Latu follows — reading a deprecated conf in a loop should not narrate.
  defp warn({:set, _pairs}, warnings), do: log(warnings)
  defp warn({:unset, _}, warnings), do: log(warnings)
  defp warn(_op, _warnings), do: :ok

  defp log(warnings), do: Enum.each(warnings, fn warning -> Logger.warning(warning) end)

  @doc """
  Read session configuration values in one `Config` round-trip.

  `GetOption`, so a key the server does not define comes back nil. `Get` would refuse it, which
  is what `Latu.conf/2` wants and an internal batch read does not.
  """
  @spec get_configs(Session.t(), [String.t()]) ::
          {:ok, %{String.t() => String.t() | nil}, Session.t()} | {:error, Error.t()}
  def get_configs(%Session{} = session, keys) when is_list(keys) do
    with {:ok, pairs, session} <- config(session, {:get_option, keys}) do
      {:ok, Map.new(pairs), session}
    end
  end

  # =============================================
  # Session control
  # =============================================

  # The server's own spelling, downcased. An unrecognised state is passed through as its raw
  # atom rather than guessed at — the enum will grow.
  @operation_states %{
    OPERATION_STATE_UNSPECIFIED: :unspecified,
    OPERATION_STATE_UNKNOWN: :unknown,
    OPERATION_STATE_RUNNING: :running,
    OPERATION_STATE_TERMINATING: :terminating,
    OPERATION_STATE_SUCCEEDED: :succeeded,
    OPERATION_STATE_FAILED: :failed,
    OPERATION_STATE_CANCELLED: :cancelled
  }

  @doc """
  Cancel executions on the server. See `Latu.interrupt/2`.
  """
  @spec interrupt(Session.t(), keyword()) ::
          {:ok, [String.t()], Session.t()} | {:error, Error.t()}
  def interrupt(%Session{channel: nil}, _opts) do
    {:error, Error.new(:connect, "session is not connected; call Latu.connect/1 first")}
  end

  def interrupt(%Session{} = session, opts) when is_list(opts) do
    request = %Proto.InterruptRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_session_id,
      user_context: user_context(session),
      client_type: session.client_type
    }

    request = struct!(request, interrupt_scope(opts))
    call = fn -> Stub.interrupt(session.channel, request, timeout: session.timeout) end

    with {:ok, response} <- rpc("Interrupt", session, call),
         {:ok, session} <-
           Session.confirm(session, response.session_id, response.server_side_session_id) do
      {:ok, response.interrupted_ids, session}
    end
  end

  defp interrupt_scope(opts) do
    case Keyword.validate!(opts, [:tag, :operation_id]) do
      [] ->
        [interrupt_type: :INTERRUPT_TYPE_ALL]

      [tag: tag] ->
        [interrupt_type: :INTERRUPT_TYPE_TAG, interrupt: {:operation_tag, to_string(tag)}]

      [operation_id: id] ->
        [interrupt_type: :INTERRUPT_TYPE_OPERATION_ID, interrupt: {:operation_id, to_string(id)}]

      both ->
        raise ArgumentError,
              "interrupt takes :tag or :operation_id, not both; got #{inspect(both)}"
    end
  end

  @doc """
  What the server is running for this session. See `Latu.status/2`.
  """
  @spec status(Session.t(), [String.t()]) ::
          {:ok, [%{operation_id: String.t(), state: atom()}], Session.t()} | {:error, Error.t()}
  def status(%Session{channel: nil}, _operation_ids) do
    {:error, Error.new(:connect, "session is not connected; call Latu.connect/1 first")}
  end

  def status(%Session{} = session, operation_ids) when is_list(operation_ids) do
    request = %Proto.GetStatusRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_session_id,
      user_context: user_context(session),
      client_type: session.client_type,
      operation_status: operation_filter(operation_ids)
    }

    call = fn -> Stub.get_status(session.channel, request, timeout: session.timeout) end

    with {:ok, response} <- rpc("GetStatus", session, call) |> explain_missing_session(session),
         {:ok, session} <-
           Session.confirm(session, response.session_id, response.server_side_session_id) do
      {:ok, Enum.map(response.operation_statuses, &operation_status/1), session}
    end
  end

  # GetStatus and CloneSession look the session up rather than creating it, so both refuse a
  # session the server has never had to make — which is any session that has not run anything
  # yet. The gRPC message names the handle and not the reason; this says the reason.
  defp explain_missing_session({:error, %Error{} = error} = failure, %Session{} = session) do
    if String.contains?(error.message, "INVALID_HANDLE.SESSION_NOT_FOUND") do
      {:error,
       %{
         error
         | message:
             "the server has no session #{session.session_id}: a session becomes real on the " <>
               "server when it first runs something, and is gone once released — #{error.message}"
       }}
    else
      failure
    end
  end

  defp explain_missing_session(result, %Session{}), do: result

  # The field's *presence* is what asks for operation statuses at all: absent and the server
  # sends none, empty and it sends every operation it knows about
  # (`SparkConnectGetStatusHandler`). So it is always set.
  defp operation_filter(ids) do
    %Proto.GetStatusRequest.OperationStatusRequest{operation_ids: Enum.map(ids, &to_string/1)}
  end

  defp operation_status(%Proto.GetStatusResponse.OperationStatus{} = status) do
    %{
      operation_id: status.operation_id,
      state: Map.get(@operation_states, status.state, status.state)
    }
  end

  @doc """
  Fork the session on the server. See `Latu.clone_session/2`.
  """
  @spec clone_session(Session.t(), keyword()) ::
          {:ok, Session.t(), Session.t()} | {:error, Error.t()}
  def clone_session(%Session{channel: nil}, _opts) do
    {:error, Error.new(:connect, "session is not connected; call Latu.connect/1 first")}
  end

  def clone_session(%Session{} = session, opts) when is_list(opts) do
    opts = Keyword.validate!(opts, [:session_id])
    wanted = opts[:session_id]

    if wanted && not UUID.valid?(wanted) do
      raise ArgumentError, "session_id must be a UUID, got #{inspect(wanted)}"
    end

    request = %Proto.CloneSessionRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_session_id,
      user_context: user_context(session),
      client_type: session.client_type,
      new_session_id: wanted
    }

    call = fn -> Stub.clone_session(session.channel, request, timeout: session.timeout) end

    with {:ok, response} <-
           rpc("CloneSession", session, call) |> explain_missing_session(session),
         {:ok, session} <-
           Session.confirm(session, response.session_id, response.server_side_session_id),
         {:ok, clone} <- cloned(session, response, wanted) do
      {:ok, clone, session}
    end
  end

  # The response carries both pairs: the original session's ids, which Session.confirm/3 checks
  # as on any other call, and the clone's. The clone shares the channel — a session is
  # server-side state keyed by an id, not a connection.
  defp cloned(_session, %{new_session_id: id}, _wanted) when id in [nil, ""] do
    {:error, Error.new(:session, "CloneSession answered with no new session id")}
  end

  defp cloned(_session, %{new_session_id: id}, wanted)
       when is_binary(wanted) and id != wanted do
    {:error, Error.new(:session, "asked the server to clone into #{wanted}, it used #{id}")}
  end

  defp cloned(%Session{} = session, response, _wanted) do
    {:ok,
     %{
       session
       | session_id: response.new_session_id,
         server_session_id: blank_to_nil(response.new_server_side_session_id)
     }}
  end

  defp blank_to_nil(value) when value in [nil, ""], do: nil
  defp blank_to_nil(value), do: value

  @doc """
  End the session on the server. See `Latu.release_session/2`.
  """
  @spec release_session(Session.t(), keyword()) :: {:ok, Session.t()} | {:error, Error.t()}
  def release_session(%Session{channel: nil}, _opts) do
    {:error, Error.new(:connect, "session is not connected; call Latu.connect/1 first")}
  end

  def release_session(%Session{} = session, opts) when is_list(opts) do
    opts = Keyword.validate!(opts, allow_reconnect: false, timeout: session.timeout)

    request = %Proto.ReleaseSessionRequest{
      session_id: session.session_id,
      user_context: user_context(session),
      client_type: session.client_type,
      allow_reconnect: opts[:allow_reconnect]
    }

    call = fn -> Stub.release_session(session.channel, request, timeout: opts[:timeout]) end

    with {:ok, response} <- rpc("ReleaseSession", session, call) do
      Session.confirm(session, response.session_id, response.server_side_session_id)
    end
  end

  @doc """
  Cache binary blobs as session artifacts, returning one sha-256 hex hash per blob, in order.

  PySpark's `cache_artifacts`: one `ArtifactStatus` round-trip finds which hashes the server
  already holds, and one `AddArtifacts` client-streaming call uploads the rest — a blob at or
  under 32 KiB rides in a batch, a larger one is split into 32 KiB chunks behind a
  `BeginChunkedArtifact`. The hashes are what `ChunkedCachedLocalRelation` references.
  """
  @spec cache_artifacts(Session.t(), [binary()]) ::
          {:ok, [String.t()], Session.t()} | {:error, Error.t()}
  def cache_artifacts(%Session{channel: nil}, _blobs) do
    {:error, Error.new(:connect, "session is not connected; call Latu.connect/1 first")}
  end

  def cache_artifacts(%Session{} = session, blobs) when is_list(blobs) do
    hashes = Enum.map(blobs, &sha256/1)

    with {:ok, cached, session} <- artifact_statuses(session, Enum.uniq(hashes)),
         missing = missing_artifacts(hashes, blobs, cached),
         {:ok, session} <- add_artifacts(session, missing) do
      {:ok, hashes, session}
    end
  end

  defp sha256(blob), do: :crypto.hash(:sha256, blob) |> Base.encode16(case: :lower)

  # One artifact per hash the server lacks, first blob wins on duplicates.
  defp missing_artifacts(hashes, blobs, cached) do
    hashes
    |> Enum.zip(blobs)
    |> Enum.uniq_by(fn {hash, _blob} -> hash end)
    |> Enum.reject(fn {hash, _blob} -> MapSet.member?(cached, hash) end)
  end

  defp artifact_statuses(%Session{} = session, hashes) do
    request = %Proto.ArtifactStatusesRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_session_id,
      user_context: user_context(session),
      client_type: session.client_type,
      names: Enum.map(hashes, &("cache/" <> &1))
    }

    call = fn -> Stub.artifact_status(session.channel, request, timeout: session.timeout) end

    with {:ok, response} <- rpc("ArtifactStatus", session, call),
         {:ok, session} <-
           Session.confirm(session, response.session_id, response.server_side_session_id) do
      cached =
        for hash <- hashes,
            status = response.statuses["cache/" <> hash],
            status != nil and status.exists,
            into: MapSet.new(),
            do: hash

      {:ok, cached, session}
    end
  end

  defp add_artifacts(%Session{} = session, []), do: {:ok, session}

  defp add_artifacts(%Session{} = session, artifacts) do
    requests = artifact_requests(session, artifacts)

    call = fn ->
      stream = Stub.add_artifacts(session.channel, timeout: session.timeout)

      requests
      |> Enum.reduce(stream, &GRPC.Stub.send_request(&2, &1))
      |> GRPC.Stub.end_stream()
      |> GRPC.Stub.recv(timeout: session.timeout)
    end

    with {:ok, response} <- rpc("AddArtifacts", session, call),
         {:ok, session} <-
           Session.confirm(session, response.session_id, response.server_side_session_id) do
      case Enum.reject(response.artifacts, & &1.is_crc_successful) do
        [] -> {:ok, session}
        bad -> {:error, Error.new(:protocol, "artifact upload corrupted: #{summary_names(bad)}")}
      end
    end
  end

  defp summary_names(summaries), do: Enum.map_join(summaries, ", ", & &1.name)

  # 32 KiB, PySpark's ArtifactManager.CHUNK_SIZE — both the chunk size for a large artifact
  # and the packing bound for a batch of small ones.
  @artifact_chunk_size 32 * 1024

  @doc false
  # The AddArtifactsRequest sequence for `{hash, blob}` pairs — pure, so the wire shape is
  # testable with no server. Small blobs pack into Batch requests up to the chunk size; a
  # larger blob flushes the batch and streams as BeginChunkedArtifact + 32 KiB chunks,
  # mirroring PySpark's _add_artifacts.
  def artifact_requests(%Session{} = session, artifacts) do
    {requests, batch, _size} =
      Enum.reduce(artifacts, {[], [], 0}, fn {hash, blob}, {requests, batch, size} ->
        cond do
          byte_size(blob) > @artifact_chunk_size ->
            {requests ++ flush_batch(session, batch) ++ chunked_requests(session, hash, blob), [],
             0}

          size + byte_size(blob) > @artifact_chunk_size ->
            {requests ++ flush_batch(session, batch), [single(hash, blob)], byte_size(blob)}

          true ->
            {requests, batch ++ [single(hash, blob)], size + byte_size(blob)}
        end
      end)

    requests ++ flush_batch(session, batch)
  end

  defp single(hash, blob) do
    %Proto.AddArtifactsRequest.SingleChunkArtifact{
      name: "cache/" <> hash,
      data: %Proto.AddArtifactsRequest.ArtifactChunk{data: blob, crc: :erlang.crc32(blob)}
    }
  end

  defp flush_batch(_session, []), do: []

  defp flush_batch(%Session{} = session, singles) do
    [
      artifact_request(session, {:batch, %Proto.AddArtifactsRequest.Batch{artifacts: singles}})
    ]
  end

  defp chunked_requests(%Session{} = session, hash, blob) do
    [first | rest] = chunk_binary(blob)

    begin = %Proto.AddArtifactsRequest.BeginChunkedArtifact{
      name: "cache/" <> hash,
      total_bytes: byte_size(blob),
      num_chunks: 1 + length(rest),
      initial_chunk: %Proto.AddArtifactsRequest.ArtifactChunk{
        data: first,
        crc: :erlang.crc32(first)
      }
    }

    [
      artifact_request(session, {:begin_chunk, begin})
      | Enum.map(rest, fn chunk ->
          artifact_request(
            session,
            {:chunk,
             %Proto.AddArtifactsRequest.ArtifactChunk{data: chunk, crc: :erlang.crc32(chunk)}}
          )
        end)
    ]
  end

  defp artifact_request(%Session{} = session, payload) do
    %Proto.AddArtifactsRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_session_id,
      user_context: user_context(session),
      client_type: session.client_type,
      payload: payload
    }
  end

  defp chunk_binary(blob) when byte_size(blob) <= @artifact_chunk_size, do: [blob]

  defp chunk_binary(blob) do
    <<chunk::binary-size(@artifact_chunk_size), rest::binary>> = blob
    [chunk | chunk_binary(rest)]
  end

  # =============================================
  # The reattach stream
  # =============================================

  @doc false
  # The tagged element stream every consumer folds: `{:schema, data_type}` at most once and
  # always before any batch (the server sends it on a response of its own first, and
  # `handle/2` puts it first even if one ever rode in with a batch), `{:command_result,
  # relation}` at most once (a SqlCommand answering), `{:ok, batch}` per batch, then exactly
  # one `{:done, execution}` or `{:error, error}`. Raises at the call site when the session is
  # not connected, since an enumeration cannot return an error. Internal contract, consumed by
  # `execute/2` and `Latu.DataFrame.stream/2`.
  def responses(%Session{channel: nil}, _plan) do
    raise Error.new(:connect, "session is not connected; call Latu.connect/1 first")
  end

  def responses(%Session{} = session, %Proto.Plan{} = plan) do
    Stream.resource(
      fn ->
        # Generated before the first call, not read off a response: a reattach may be needed
        # before anything arrives, and it has to name the operation already in flight.
        state = %{
          execution: Execution.new(session, UUID.v4()),
          plan: plan,
          pull: nil,
          started?: false,
          done?: false,
          started_at: System.monotonic_time(),
          outcome: :abandoned
        }

        Telemetry.execution_started(ids(state.execution))

        state
      end,
      &next/1,
      &finished/1
    )
  end

  @doc """
  Call an action's `:progress` function on each progress message. Internal contract.

  The one place a caller's own function runs inside the transport, and it runs in the process
  consuming the stream — the caller's — because Latu starts none of its own. `Latu.Progress`
  says so out loud.
  """
  @spec watched(Enumerable.t(), keyword()) :: Enumerable.t()
  def watched(responses, opts) do
    case Keyword.get(opts, :progress) do
      nil ->
        responses

      handler when is_function(handler, 1) ->
        Stream.each(responses, fn
          {:progress, progress} -> handler.(progress)
          _other -> :ok
        end)

      other ->
        raise ArgumentError, ":progress is a one-argument function, not #{inspect(other)}"
    end
  end

  defp next(%{done?: true} = state), do: {:halt, state}

  # A failure to open goes through the state machine too, so a retryable one is retried. When
  # the very first ExecutePlan never opened, `started?` is still false and the retry re-sends it
  # rather than reattaching to an operation the server may never have created.
  defp next(%{pull: nil} = state) do
    case open(state) do
      {:ok, state} -> {[], state}
      {:error, error} -> handle(state, {:error, error})
    end
  end

  defp next(%{pull: pull} = state) do
    {event, pull} = read(pull)
    handle(%{state | pull: pull}, event)
  end

  defp handle(state, event) do
    seen_schema = state.execution.schema
    seen_result = state.execution.command_result
    seen_progress = state.execution.progress
    seen_retries = state.execution.retries
    {action, execution} = Execution.step(state.execution, event)
    attempted(action, seen_retries, execution)
    {elements, state} = act(action, %{state | execution: execution})

    elements =
      case execution.command_result do
        ^seen_result -> elements
        relation -> [{:command_result, relation} | elements]
      end

    elements =
      case execution.progress do
        ^seen_progress ->
          elements

        progress ->
          progress = Progress.new(progress)
          Telemetry.progress(Progress.percent(progress), ids(execution))

          [{:progress, progress} | elements]
      end

    case execution.schema do
      ^seen_schema -> {elements, state}
      schema -> {[{:schema, schema} | elements], state}
    end
  end

  defp act({:emit, batch}, state) do
    Telemetry.batch(batch.row_count, byte_size(batch.data), ids(state.execution))

    {[{:ok, batch}], state}
  end

  defp act(:pull, state), do: {[], state}

  defp act({:done, execution}, state) do
    {[{:done, execution}], %{state | done?: true, outcome: :ok}}
  end

  defp act({:fail, error}, state) do
    {[{:error, error}], %{state | done?: true, outcome: :error}}
  end

  # Backing off blocks the process consuming the stream, which is the caller's. Latu starts no
  # process to do it somewhere else.
  defp act({:reattach, backoff}, state) do
    release_until(state)
    if backoff > 0, do: Process.sleep(backoff)

    {[], %{state | pull: nil}}
  end

  # The server has no record of the execution, so there is nothing to release and nothing to
  # reattach to. Clearing `started?` sends ExecutePlan again, under the same operation_id.
  defp act({:restart, backoff}, state) do
    if backoff > 0, do: Process.sleep(backoff)

    {[], %{state | pull: nil, started?: false}}
  end

  # `Latu.Telemetry` keeps SparkEx's split between a *retry* and a *reattach*, and the state
  # machine is what distinguishes them: an :eof reattach is the ordinary mid-stream case and
  # spends no budget, where an error it decided to retry increments `retries`. Emitted here
  # rather than in `Latu.Client.Execution`, which does no IO by design — the same rule that
  # puts a `:progress` handler's call in this module.
  defp attempted({:restart, backoff}, _seen, execution) do
    Telemetry.attempt(:retry, backoff, execution.retries, ids(execution))
  end

  defp attempted({:reattach, backoff}, seen, %{retries: retries} = execution)
       when retries > seen do
    Telemetry.attempt(:retry, backoff, retries, ids(execution))
  end

  defp attempted({:reattach, backoff}, _seen, execution) do
    Telemetry.attempt(:reattach, backoff, execution.empty, ids(execution))
  end

  defp attempted(_action, _seen, _execution), do: :ok

  # Named fields only. Never the session, which carries a token.
  defp ids(execution) do
    %{session_id: execution.session.session_id, operation_id: execution.operation_id}
  end

  # One element per call. The stream is once-through and destructive: only ever advance the
  # continuation it hands back, never re-enumerate from the top.
  defp read(pull) do
    case pull.({:cont, nil}) do
      {:suspended, {:ok, %Proto.ExecutePlanResponse{} = response}, pull} ->
        {{:response, response}, pull}

      {:suspended, {:error, %GRPC.RPCError{} = error}, pull} ->
        {{:error, rpc_error(error)}, pull}

      {:suspended, other, pull} ->
        {{:error, Error.new(:protocol, "unexpected ExecutePlan message: #{inspect(other)}")},
         pull}

      {status, _acc} when status in [:done, :halted] ->
        {:eof, nil}
    end
  end

  defp open(%{started?: false, execution: execution} = state) do
    session = execution.session

    request = %Proto.ExecutePlanRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_session_id,
      user_context: user_context(session),
      operation_id: execution.operation_id,
      client_type: session.client_type,
      tags: session.tags,
      plan: state.plan,
      request_options: [reattachable()]
    }

    start(state, "ExecutePlan", fn -> Stub.execute_plan(session.channel, request) end)
  end

  defp open(%{execution: execution} = state) do
    session = execution.session

    request = %Proto.ReattachExecuteRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_session_id,
      user_context: user_context(session),
      operation_id: execution.operation_id,
      client_type: session.client_type,
      last_response_id: execution.last_response_id
    }

    start(state, "ReattachExecute", fn -> Stub.reattach_execute(session.channel, request) end)
  end

  defp start(state, name, call) do
    with {:ok, stream} <- rpc(name, state.execution.session, call) do
      suspend = fn item, _acc -> {:suspend, item} end
      pull = fn command -> Enumerable.reduce(stream, command, suspend) end

      {:ok, %{state | pull: pull, started?: true}}
    end
  end

  # Without this the server sends no ResultComplete, and a stream it cut short is
  # indistinguishable from a finished one. See Latu.Client.Execution.
  defp reattachable do
    %Proto.ExecutePlanRequest.RequestOption{
      request_option: {:reattach_options, %Proto.ReattachOptions{reattachable: true}}
    }
  end

  # `Stream.resource`'s after_fun runs on a normal finish, on an early halt and on a raise, so
  # this is the one place an execution's end can be reported exactly once — including the
  # `:abandoned` case, where the consumer stopped reading. The `[:latu, :rpc, :*]` span around
  # ExecutePlan measures *opening* the stream; this measures draining it, which is what anyone
  # asking "how long did the query take" means.
  defp finished(state) do
    release_all(state)

    duration = System.monotonic_time() - state.started_at
    metadata = Map.put(ids(state.execution), :outcome, state.outcome)

    Telemetry.execution_finished(duration, metadata)
  end

  # Best effort, and inline: without it the server holds the execution and its response buffer
  # until detachedTimeout. A failure here is not worth surfacing — the server reaps anyway.
  defp release_all(%{started?: false}), do: :ok
  defp release_all(%{execution: %{session: %Session{channel: nil}}}), do: :ok

  defp release_all(%{execution: execution}) do
    release(execution, {:release_all, %Proto.ReleaseExecuteRequest.ReleaseAll{}})
  end

  # Frees the responses the server no longer has to keep for a replay. Sent at a reattach
  # rather than after every response as PySpark does: PySpark hands each release to a thread
  # pool, and Latu starts no process to do that, so per-response would mean a synchronous
  # round-trip per batch. The server's buffer is bounded by observerRetryBufferSize and trims
  # itself regardless, so this only lowers the high-water mark. docs/decisions.md.
  defp release_until(%{started?: false}), do: :ok
  defp release_until(%{execution: %{last_response_id: nil}}), do: :ok
  defp release_until(%{execution: %{session: %Session{channel: nil}}}), do: :ok

  defp release_until(%{execution: execution}) do
    until = %Proto.ReleaseExecuteRequest.ReleaseUntil{response_id: execution.last_response_id}
    release(execution, {:release_until, until})
  end

  defp release(%Execution{} = execution, arm) do
    session = execution.session

    request = %Proto.ReleaseExecuteRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_session_id,
      user_context: user_context(session),
      operation_id: execution.operation_id,
      client_type: session.client_type,
      release: arm
    }

    rpc("ReleaseExecute", session, fn -> Stub.release_execute(session.channel, request) end)
    :ok
  end

  # =============================================
  # RPC
  # =============================================

  defp user_context(%Session{} = session) do
    %Proto.UserContext{user_id: session.user_id, user_name: session.user_name}
  end

  # Every unary RPC funnels through here, which is why the telemetry span is here and not at
  # ten call sites. Only the session *id* is handed over — see `Latu.Telemetry`.
  defp rpc(name, %Session{} = session, call) do
    Telemetry.rpc(name, session.session_id, fn ->
      case call.() do
        {:ok, result} -> {:ok, result}
        {:error, %GRPC.RPCError{} = error} -> {:error, rpc_error(error)}
        {:error, reason} -> {:error, Error.new(:rpc, "#{name} failed: #{inspect(reason)}")}
      end
    end)
  end

  # =============================================
  # Error detail
  # =============================================

  # Spark puts structured detail in `grpc-status-details-bin`, and elixir-grpc has already
  # decoded it by the time we see the error: `GRPC.RPCError.details` is the `Google.Rpc.Status`
  # message's own `details`, a list of `Google.Protobuf.Any`. `Google.Rpc.ErrorInfo` comes from
  # `deps/googleapis`, which `grpc_core` depends on — so this needs no dependency, no proto
  # generation and no round trip.
  @error_info_url "type.googleapis.com/google.rpc.ErrorInfo"

  defp rpc_error(%GRPC.RPCError{} = error) do
    Error.new(:rpc, error.message, [status: error.status, details: error.details] ++ info(error))
  end

  defp info(%GRPC.RPCError{details: details}) when is_list(details) do
    case Enum.find(details, &(&1.type_url == @error_info_url)) do
      nil -> []
      any -> from_error_info(Google.Rpc.ErrorInfo.decode(any.value))
    end
  rescue
    # Inspecting an error must never be the thing that fails. A trailer Latu cannot read leaves
    # the fields nil, which is exactly what a server that sent none does.
    _ -> []
  end

  defp info(%GRPC.RPCError{}), do: []

  # The metadata keys are PySpark's — `connect.py`'s `convert_exception` reads the same six.
  defp from_error_info(%Google.Rpc.ErrorInfo{metadata: metadata}) do
    [
      error_class: metadata["errorClass"],
      sql_state: metadata["sqlState"],
      stacktrace: metadata["stackTrace"],
      error_id: metadata["errorId"],
      classes: json_list(metadata["classes"]),
      parameters: json_map(metadata["messageParameters"])
    ]
  end

  defp json_list(nil), do: []

  defp json_list(encoded) do
    case JSON.decode(encoded) do
      {:ok, list} when is_list(list) -> list
      _ -> []
    end
  end

  defp json_map(nil), do: %{}

  defp json_map(encoded) do
    case JSON.decode(encoded) do
      {:ok, map} when is_map(map) -> map
      _ -> %{}
    end
  end

  @doc """
  The full server-side cause chain for an error. See `Latu.error_details/2`.
  """
  @spec error_details(Session.t(), Error.t()) :: {:ok, Error.t()} | {:error, Error.t()}
  def error_details(%Session{channel: nil}, %Error{}) do
    {:error, Error.new(:connect, "session is not connected; call Latu.connect/1 first")}
  end

  def error_details(%Session{}, %Error{error_id: nil} = error) do
    {:ok, error}
  end

  def error_details(%Session{} = session, %Error{error_id: id} = error) do
    request = %Proto.FetchErrorDetailsRequest{
      session_id: session.session_id,
      client_observed_server_side_session_id: session.server_session_id,
      user_context: user_context(session),
      client_type: session.client_type,
      error_id: id
    }

    call = fn -> Stub.fetch_error_details(session.channel, request, timeout: session.timeout) end

    with {:ok, response} <- rpc("FetchErrorDetails", session, call),
         {:ok, _session} <-
           Session.confirm(session, response.session_id, response.server_side_session_id) do
      {:ok, filled(error, Enum.map(causes(response), &cause/1))}
    end
  end

  # The server discards an error's detail once it has been fetched — `errorIdToError.invalidate`
  # in its own handler — so a second call answers with an empty message. Keeping what we already
  # have makes the call idempotent for the caller even though the server is not.
  defp filled(%Error{} = error, []), do: error
  defp filled(%Error{} = error, causes), do: %{error | causes: causes}

  # The response is a flat list of exceptions plus `cause_idx` links, root first. Walking the
  # chain rather than returning the list is the whole value: the root cause is what you want,
  # and it is not at a fixed position.
  defp causes(%Proto.FetchErrorDetailsResponse{root_error_idx: nil}), do: []

  defp causes(%Proto.FetchErrorDetailsResponse{} = response) do
    chain(response.errors, response.root_error_idx, [])
  end

  defp chain(errors, index, seen) when is_integer(index) and index >= 0 do
    case Enum.at(errors, index) do
      # A cycle cannot happen in a well-formed chain, but a malformed one must not loop here.
      nil ->
        Enum.reverse(seen)

      error ->
        if index in Enum.map(seen, & &1.index) do
          Enum.reverse(seen)
        else
          chain(errors, error.cause_idx, [%{index: index, error: error} | seen])
        end
    end
  end

  defp chain(_errors, _index, seen), do: Enum.reverse(seen)

  defp cause(%{error: error}) do
    %{
      message: error.message,
      classes: error.error_type_hierarchy,
      error_class: error.spark_throwable && error.spark_throwable.error_class,
      stacktrace: Enum.map(error.stack_trace, &frame/1)
    }
  end

  defp frame(%{declaring_class: class, method_name: method} = element) do
    where =
      case element.file_name do
        nil -> "Unknown Source"
        "" -> "Unknown Source"
        file -> "#{file}:#{element.line_number}"
      end

    "#{class}.#{method}(#{where})"
  end

  # =============================================
  # Channel
  # =============================================

  # "host:port" puts elixir-grpc in compatibility mode, which rewrites to `ipv4:host:port`
  # and resolves through Gun rather than the DNS resolver. That is what we want: a Spark
  # Connect session is server-side state pinned to one server, so re-resolving or balancing
  # across A records would silently move us to a server that has never heard of it.
  defp target(%Session{host: host, port: port}), do: "#{host}:#{port}"

  defp open(target, opts) do
    case GRPC.Stub.connect(target, opts) do
      {:ok, channel} ->
        {:ok, channel}

      {:error, reason} ->
        {:error, Error.new(:connect, "cannot reach #{target}: #{inspect(reason)}")}
    end
  end

  defp connect_opts(session) do
    with {:ok, cred} <- credential(session) do
      # Two separate timeouts govern establishment: elixir-grpc's own wait, and Gun's
      # `await_timeout` (which defaults to 5s and fires first). Set both from one number,
      # or raising `connect_timeout` alone would do nothing.
      # Never set `max_frame_size_received`: it is 40x slower. Why the window and keepalive
      # defaults are what they are, and why they live on the session: docs/decisions.md.
      opts = [
        connect_timeout: session.connect_timeout,
        headers: headers(session),
        adapter_opts: [
          await_timeout: session.connect_timeout,
          http2_opts: %{
            initial_connection_window_size: session.window_size,
            initial_stream_window_size: session.window_size,
            keepalive: session.keepalive,
            keepalive_tolerance: session.keepalive_tolerance
          }
        ]
      ]

      {:ok, if(cred, do: Keyword.put(opts, :cred, cred), else: opts)}
    end
  end

  # Always explicit. Left to itself elixir-grpc reaches for CAStore and raises if it is
  # absent; the OS trust store is already there.
  defp credential(%Session{use_ssl: false}), do: {:ok, nil}

  defp credential(%Session{use_ssl: true}) do
    case cacerts() do
      {:ok, cacerts} ->
        {:ok, GRPC.Credential.new(ssl: [verify: :verify_peer, depth: 99, cacerts: cacerts])}

      :error ->
        {:error,
         Error.new(
           :connect,
           "no OS trust store available for TLS; add :castore to your deps or pass your own " <>
             "ssl options"
         )}
    end
  end

  defp cacerts do
    {:ok, :public_key.cacerts_get()}
  rescue
    _ -> :error
  catch
    _, _ -> :error
  end

  # gRPC metadata keys must be lowercase; Spark passes URL params through verbatim.
  defp headers(%Session{} = session) do
    extra = Enum.map(session.headers, fn {k, v} -> {String.downcase(k), v} end)
    if session.token, do: [{"authorization", "Bearer " <> session.token} | extra], else: extra
  end

  # =============================================
  # Policy
  # =============================================

  defp check_token_transport(%Session{token: nil}), do: :ok
  defp check_token_transport(%Session{use_ssl: true}), do: :ok

  defp check_token_transport(%Session{host: host}) when host in @loopback, do: :ok

  defp check_token_transport(%Session{host: host}) do
    {:error,
     Error.new(
       :connect,
       "refusing to send a bearer token in cleartext to #{host}; add ;use_ssl=true to the URL"
     )}
  end
end
