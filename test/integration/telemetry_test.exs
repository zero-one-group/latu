defmodule Latu.Integration.TelemetryTest do
  # `async: false`: a `:telemetry` handler is attached globally, in ETS, so it receives events
  # from every process — including a concurrent async test's query.
  use ExUnit.Case, async: false

  alias Latu.Error

  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  #
  # The event *shapes* are asserted with no server in test/latu/telemetry_test.exs. What only a
  # server can show is here: that `Latu.Client` actually emits them, which of them a real query
  # produces, and that no metadata value is ever the session's token.
  @moduletag :integration
  @moduletag :capture_log

  @token "s3cr3t-never-in-metadata"

  @events [
    [:latu, :rpc, :start],
    [:latu, :rpc, :stop],
    [:latu, :execute, :start],
    [:latu, :execute, :stop],
    [:latu, :result, :batch]
  ]

  def forward(event, measurements, metadata, test) do
    send(test, {:event, event, measurements, metadata})
  end

  setup context do
    id = "latu-integration-#{inspect(context.test)}"
    :telemetry.attach_many(id, @events, &__MODULE__.forward/4, self())
    on_exit(fn -> :telemetry.detach(id) end)

    # A token is refused in cleartext to anything but a loopback host, and this is loopback.
    # The server has no auth interceptor, so it is ignored — the point is that Latu carries it
    # and telemetry must not.
    session = Latu.connect!("sc://localhost:15002/;token=#{@token}")
    on_exit(fn -> Latu.disconnect(session) end)

    %{session: session}
  end

  defp drain(acc \\ []) do
    receive do
      {:event, event, measurements, metadata} -> drain([{event, measurements, metadata} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp of(events, name), do: Enum.filter(events, fn {event, _m, _md} -> event == name end)

  test "a unary RPC is bracketed, and names itself", %{session: session} do
    Latu.schema!(Latu.range(session, 3))

    events = drain()

    assert [{_event, _measurements, start} | _] = of(events, [:latu, :rpc, :start])
    assert start.rpc == "AnalyzePlan"

    assert [{_event, measurements, stop} | _] = of(events, [:latu, :rpc, :stop])
    assert stop.rpc == "AnalyzePlan"
    assert stop.outcome == :ok
    assert is_integer(measurements.duration)
  end

  test "a collect is one execution, with batches inside it", %{session: session} do
    Latu.collect!(Latu.range(session, 5))

    events = drain()

    assert length(of(events, [:latu, :execute, :start])) == 1
    assert [{_event, measurements, stop}] = of(events, [:latu, :execute, :stop])
    assert stop.outcome == :ok
    assert is_integer(measurements.duration)
    assert is_binary(stop.operation_id)

    assert [{_event, batch, _md} | _] = of(events, [:latu, :result, :batch])
    assert batch.rows > 0
    assert batch.bytes > 0
  end

  test "a failing RPC reports its outcome and its error class", %{session: session} do
    assert {:error, %Error{}} = Latu.fetch_conf(session, "latu.no.such.conf")

    assert [{_event, _measurements, stop} | _] =
             drain() |> of([:latu, :rpc, :stop]) |> Enum.filter(&(elem(&1, 2).rpc == "Config"))

    assert stop.outcome == :error
    assert stop.error_class == "SQL_CONF_NOT_FOUND"
  end

  test "a stream the caller stops reading ends as :abandoned", %{session: session} do
    session |> Latu.range(1_000) |> Latu.stream() |> Enum.take(1)

    assert [{_event, _measurements, stop}] = drain() |> of([:latu, :execute, :stop])
    assert stop.outcome == :abandoned
  end

  test "no metadata value is ever the token, or the session", %{session: session} do
    Latu.collect!(Latu.range(session, 3))
    Latu.schema!(Latu.range(session, 3))

    events = drain()
    assert events != []

    values = for {_event, _measurements, metadata} <- events, {_key, value} <- metadata, do: value

    refute Enum.any?(values, &(is_binary(&1) and String.contains?(&1, @token)))
    refute Enum.any?(values, &is_struct(&1, Latu.Session))
  end
end
