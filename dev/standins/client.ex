defmodule Latu.Client.Execution do
  @moduledoc """
  Stand-in for `lib/latu/client/execution.ex`, folded in here because the runner treats the
  state machine as part of the transport. Only the fields the layers above read — and it comes
  first because a struct cannot be expanded before its module is defined in the same file.
  """

  defstruct [
    :session,
    :schema,
    :command_result,
    :checkpointed,
    :metrics,
    :progress,
    observed: %{},
    rows: 0
  ]
end

defmodule Latu.Client do
  @moduledoc """
  Stand-in for `lib/latu/client.ex`, for `dev/check_offline.exs` only.

  The real one is the whole gRPC layer. Nothing above it may be exercised here — these return
  shapes, not results, so a caller's pattern match is checked and nothing else.
  """

  def connect(session), do: {:ok, session}
  def disconnect(session, _opts \\ []), do: {:ok, session}
  def interrupt(session, _opts \\ []), do: {:ok, [], session}
  def status(session, _operation_ids \\ []), do: {:ok, [], session}
  def clone_session(session, _opts \\ []), do: {:ok, session, session}
  def release_session(session, _opts \\ []), do: {:ok, session}
  def error_details(_session, error), do: {:ok, error}
  def spark_version(_session), do: {:ok, "4.2.0"}
  def analyze(_session, _request), do: {:error, :stand_in}
  def analyzed(_session, _arm), do: {:error, :stand_in}

  def execute(session, _plan, _opts \\ []),
    do: {:ok, [], %Latu.Client.Execution{session: session}}

  def execute_command(session, _plan, _opts \\ []) do
    {:ok, %Latu.Client.Execution{session: session}}
  end

  def watched(responses, _opts), do: responses
  def get_configs(session, _keys), do: {:ok, %{}, session}
  def config(session, _op), do: {:ok, [], session}
  def cache_artifacts(session, _blobs), do: {:ok, [], session}
  def artifact_requests(_session, _artifacts), do: []
  def responses(_session, _plan), do: []
end
