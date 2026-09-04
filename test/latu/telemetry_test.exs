defmodule Latu.TelemetryTest do
  use ExUnit.Case, async: true

  alias Latu.Error
  alias Latu.Telemetry

  # Runs against the real `:telemetry` under mix, and against `dev/standins/telemetry.ex`
  # offline — a working mini implementation rather than a no-op, so nothing here passes without
  # an event being emitted.
  #
  # A `:telemetry` handler is attached globally, so this module's handler receives every
  # `[:latu, ...]` event the whole suite emits, including from a concurrent integration test.
  # Each test owns a session id and `forward/4` forwards only its own events; `an event from
  # another session` is the control. The stand-in dispatches only to same-process handlers and
  # so is *more* forgiving than the real thing.
  #
  # What only a server can show — that `Latu.Client` emits these for a real query, and that no
  # metadata value is ever the session's token — is test/integration/telemetry_test.exs.
  @moduletag :capture_log

  @events [
    [:latu, :rpc, :start],
    [:latu, :rpc, :stop],
    [:latu, :execute, :start],
    [:latu, :execute, :stop],
    [:latu, :retry, :attempt],
    [:latu, :reattach, :attempt],
    [:latu, :result, :batch],
    [:latu, :result, :progress]
  ]

  # A remote capture rather than an `fn`: `:telemetry.attach/4` warns about a local or
  # anonymous handler, and the test process rides along in the config instead — now beside the
  # session id, which is what tells this test's events from the rest of the suite's.
  def forward(event, measurements, metadata, {test, session_id}) do
    if metadata[:session_id] == session_id do
      send(test, {:event, event, measurements, metadata})
    end
  end

  setup context do
    session_id = "s-#{System.unique_integer([:positive])}"
    ids = %{session_id: session_id, operation_id: "op-#{System.unique_integer([:positive])}"}

    id = "latu-telemetry-#{inspect(context.test)}"
    :telemetry.attach_many(id, @events, &__MODULE__.forward/4, {self(), session_id})
    on_exit(fn -> :telemetry.detach(id) end)

    %{session_id: session_id, ids: ids}
  end

  describe "rpc/3" do
    test "brackets the call, and hands back exactly what the call returned",
         %{session_id: session_id} do
      assert {:ok, :answer} = Telemetry.rpc("AnalyzePlan", session_id, fn -> {:ok, :answer} end)

      assert_received {:event, [:latu, :rpc, :start], _measurements, start}
      assert start.rpc == "AnalyzePlan"
      assert start.session_id == session_id

      assert_received {:event, [:latu, :rpc, :stop], measurements, stop}
      assert is_integer(measurements.duration)
      assert stop.rpc == "AnalyzePlan"
      assert stop.outcome == :ok
      assert stop.error_class == nil
    end

    test "reports a failure as an outcome, without turning it into a raise",
         %{session_id: session_id} do
      error = Error.new(:rpc, "nope", error_class: "UNRESOLVED_COLUMN")

      assert {:error, ^error} =
               Telemetry.rpc("ExecutePlan", session_id, fn -> {:error, error} end)

      assert_received {:event, [:latu, :rpc, :stop], _measurements, stop}
      assert stop.outcome == :error
      assert stop.error_class == "UNRESOLVED_COLUMN"
    end

    test "error_class is nil rather than absent, so a handler need not check for the key",
         %{session_id: session_id} do
      Telemetry.rpc("Config", session_id, fn -> {:error, Error.new(:rpc, "nope")} end)

      assert_received {:event, [:latu, :rpc, :stop], _measurements, stop}
      assert Map.has_key?(stop, :error_class)
      assert stop.error_class == nil
    end

    # **The control for the filter in `forward/4`.** Remove that filter and this goes red.
    #
    # The foreign event is emitted from *this* process on purpose: the offline stand-in
    # dispatches only to same-process handlers, so an event from a spawned process would be
    # dropped by the stand-in rather than by the filter, and the test would be green here for
    # a reason that does not hold under mix.
    test "an event from another session does not reach this test's handler",
         %{session_id: session_id} do
      Telemetry.rpc("AnalyzePlan", "a-session-this-test-does-not-own", fn -> {:ok, :answer} end)

      refute_received {:event, [:latu, :rpc, :start], _measurements, _metadata}

      Telemetry.rpc("AnalyzePlan", session_id, fn -> {:ok, :answer} end)

      assert_received {:event, [:latu, :rpc, :start], _measurements, _metadata}
    end
  end

  describe "an execution's own span" do
    test "start carries the ids and stop carries the outcome", %{ids: ids} do
      Telemetry.execution_started(ids)
      Telemetry.execution_finished(1_234, Map.put(ids, :outcome, :abandoned))

      assert_received {:event, [:latu, :execute, :start], measurements, ^ids}
      assert is_integer(measurements.system_time)

      assert_received {:event, [:latu, :execute, :stop], %{duration: 1_234}, stop}
      assert stop.outcome == :abandoned
      assert stop.operation_id == ids.operation_id
    end
  end

  describe "the stream events" do
    test "a retry and a reattach are separate events with the same shape", %{ids: ids} do
      Telemetry.attempt(:retry, 200, 2, ids)
      Telemetry.attempt(:reattach, 0, 7, ids)

      assert_received {:event, [:latu, :retry, :attempt], %{backoff: 200, attempt: 2}, ^ids}
      assert_received {:event, [:latu, :reattach, :attempt], %{backoff: 0, attempt: 7}, ^ids}
    end

    test "a batch reports its rows and its bytes", %{ids: ids} do
      Telemetry.batch(1_000, 4_096, ids)

      assert_received {:event, [:latu, :result, :batch], %{rows: 1_000, bytes: 4_096}, ^ids}
    end

    test "progress reports a percentage", %{ids: ids} do
      Telemetry.progress(42, ids)

      assert_received {:event, [:latu, :result, :progress], %{percent: 42}, ^ids}
    end
  end

  describe "what never reaches a handler" do
    test "the whole surface takes ids, so a session cannot be handed over by accident" do
      # Structural rather than defensive: the metadata a handler sees is built from named
      # fields, and nothing in this module has an arity that accepts a `%Latu.Session{}`. A
      # new one would have to be added here deliberately.
      assert Enum.sort(Telemetry.__info__(:functions)) ==
               Enum.sort(
                 rpc: 3,
                 execution_started: 1,
                 execution_finished: 2,
                 attempt: 4,
                 batch: 3,
                 progress: 2
               )
    end
  end
end
