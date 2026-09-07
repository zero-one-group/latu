defmodule Latu.ClientTest do
  use ExUnit.Case, async: true

  alias Latu.Client
  alias Latu.Error
  alias Latu.Plan
  alias Latu.Session

  # Everything that reaches the network lives in test/integration/.

  test "execute/2 on an unconnected session says so, rather than crashing" do
    assert {:error, %Error{kind: :connect, message: message}} =
             Client.execute(Session.from_url!("sc://h"), plan())

    assert message =~ "not connected"
  end

  describe "retrying/3" do
    setup do
      retry = [initial_backoff: 1, jitter: 0, max_retries: 3]
      %{session: Session.from_url!("sc://h", retry: retry)}
    end

    test "tries a retryable failure again, on the session's schedule", %{session: session} do
      {calls, answer} = flaky(2, {:ok, :answer})

      assert Client.retrying("Test", session, answer) == {:ok, :answer}
      assert Agent.get(calls, & &1) == 3
    end

    test "hands the failure back once the budget is spent", %{session: session} do
      {calls, never} = flaky(99, {:ok, :never})

      assert {:error, %Error{status: 14}} = Client.retrying("Test", session, never)
      assert Agent.get(calls, & &1) == 4
    end

    test "does not try again what is not worth it", %{session: session} do
      refused = fn -> {:error, %GRPC.RPCError{status: 3, message: "INVALID_ARGUMENT"}} end

      assert {:error, %Error{status: 3}} = Client.retrying("Test", session, refused)
    end

    test "each attempt is a retry event naming the RPC", %{session: session} do
      # A remote capture, as telemetry_test does: `:telemetry.attach/4` logs about a local
      # one. The handler is global, so it forwards this session's events alone.
      handler = "retrying-#{inspect(make_ref())}"
      config = {self(), session.session_id}
      :telemetry.attach(handler, [:latu, :retry, :attempt], &__MODULE__.forward/4, config)
      on_exit(fn -> :telemetry.detach(handler) end)

      {_calls, answer} = flaky(1, {:ok, :answer})
      assert {:ok, :answer} = Client.retrying("Test", session, answer)

      assert_received {%{attempt: 1, backoff: 1}, %{rpc: "Test"}}
    end

    def forward(_event, measurements, metadata, {test, session_id}) do
      if metadata[:session_id] == session_id, do: send(test, {measurements, metadata})
    end

    # A call that fails `failures` times with UNAVAILABLE, then answers — returning what a
    # `Stub` call returns, since `rpc/3` is what turns that into a `%Latu.Error{}`.
    defp flaky(failures, then) do
      {:ok, calls} = Agent.start_link(fn -> 0 end)

      call = fn ->
        if Agent.get_and_update(calls, &{&1 + 1, &1 + 1}) <= failures,
          do: {:error, %GRPC.RPCError{status: 14, message: "UNAVAILABLE: connection reset"}},
          else: then
      end

      {calls, call}
    end
  end

  defp plan, do: Plan.new(Plan.range(0, 5, 1))
end
