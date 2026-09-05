defmodule Latu.Client.ExecutionTest do
  use ExUnit.Case, async: true

  alias Latu.Client.Execution
  alias Latu.Error
  alias Latu.Protocol.Spark.Connect.ExecutePlanResponse, as: Response
  alias Latu.Protocol.Spark.Connect.MlCommandResult
  alias Latu.Session

  @operation "op-1"

  setup do
    session = Session.from_url!("sc://h")
    %{session: session, execution: Execution.new(session, @operation)}
  end

  describe "arrow batches" do
    test "are emitted as they arrive", %{session: session, execution: execution} do
      assert {{:emit, first}, execution} = step(execution, session, batch(3, 0, "AAA"))
      assert {{:emit, second}, _execution} = step(execution, session, batch(2, 3, "BB"))

      assert first == %{data: "AAA", row_count: 3, start_offset: 0}
      assert second == %{data: "BB", row_count: 2, start_offset: 3}
    end

    test "a gap in the row offsets is refused", %{session: session, execution: execution} do
      {{:emit, _}, execution} = step(execution, session, batch(3, 0, "AAA"))

      assert {{:fail, %Error{kind: :protocol, message: message}}, _} =
               step(execution, session, batch(2, 99, "BB"))

      assert message == "batch starts at row 99, expected 3"
    end

    test "an absent offset is not checked", %{session: session, execution: execution} do
      {{:emit, _}, execution} = step(execution, session, batch(3, nil, "AAA"))

      assert {{:emit, _}, _} = step(execution, session, batch(2, nil, "BB"))
    end

    test "a chunked batch is refused, not decoded", %{session: session, execution: execution} do
      chunked = {:arrow_batch, %Response.ArrowBatch{row_count: 3, data: "A", chunk_index: 0}}

      assert {{:fail, %Error{kind: :protocol, message: message}}, _} =
               step(execution, session, chunked)

      assert message =~ "chunked"
    end
  end

  describe "responses that carry no batch" do
    test "schema and metrics ask for the next one", %{session: session, execution: execution} do
      schema = %Response{session_id: session.session_id, schema: :a_data_type}
      metrics = %Response{session_id: session.session_id, metrics: :some_metrics}

      assert {:pull, execution} = Execution.step(execution, {:response, schema})
      assert {:pull, _execution} = Execution.step(execution, {:response, metrics})
    end

    test "an arm Latu does not handle is skipped", %{session: session, execution: execution} do
      progress = {:execution_progress, %Response.ExecutionProgress{}}

      assert {:pull, _execution} = step(execution, session, progress)
    end
  end

  describe "end of stream" do
    test "after ResultComplete it is done, and pins the session", %{session: s, execution: ex} do
      {:pull, ex} = step(ex, s, result_complete(), server: "srv-1")

      assert {{:done, %{session: session}}, _execution} = Execution.step(ex, :eof)
      assert session.server_session_id == "srv-1"
      assert s.server_session_id == nil
    end

    test "without ResultComplete it reattaches at once", %{execution: execution} do
      assert {{:reattach, 0}, _execution} = Execution.step(execution, :eof)
    end

    test "batches alone do not complete a result", %{session: session, execution: execution} do
      {{:emit, _}, execution} = step(execution, session, batch(3, 0, "AAA"))

      assert {{:reattach, 0}, _execution} = Execution.step(execution, :eof)
    end

    test "a server that never sends anything is given up on", %{session: s, execution: ex} do
      empty = Enum.reduce(1..100, ex, fn _, ex -> elem(Execution.step(ex, :eof), 1) end)

      assert {{:fail, %Error{kind: :protocol, message: message}}, _} = Execution.step(empty, :eof)
      assert message =~ "without sending anything"
      assert message =~ "senderMaxStreamDuration"

      # One response is enough to say the server is making progress.
      {{:emit, _}, alive} = step(empty, s, batch(1, 0, "A"))
      assert {{:reattach, 0}, _} = Execution.step(alive, :eof)
    end
  end

  describe "the result schema" do
    test "is latched from the response that carries it", %{session: session, execution: ex} do
      schema = %Response{session_id: session.session_id, schema: :a_data_type}

      assert ex.schema == nil
      assert {:pull, ex} = Execution.step(ex, {:response, schema})
      assert ex.schema == :a_data_type
    end

    test "the first one wins — a replay after a reattach does not clobber it", %{
      session: session,
      execution: ex
    } do
      first = %Response{session_id: session.session_id, schema: :a_data_type}
      replayed = %Response{session_id: session.session_id, schema: :another}

      {:pull, ex} = Execution.step(ex, {:response, first})
      {:pull, ex} = Execution.step(ex, {:response, replayed})

      assert ex.schema == :a_data_type
    end

    test "a batch riding on the same response is still emitted", %{session: s, execution: ex} do
      response = %Response{
        session_id: s.session_id,
        schema: :a_data_type,
        response_type: {:arrow_batch, %Response.ArrowBatch{row_count: 1, data: "A"}}
      }

      assert {{:emit, %{data: "A"}}, ex} = Execution.step(ex, {:response, response})
      assert ex.schema == :a_data_type
    end
  end

  describe "a SqlCommand's result relation" do
    test "is latched, first one wins", %{session: session, execution: ex} do
      assert ex.command_result == nil

      assert {:pull, ex} = Execution.step(ex, {:response, command_result(session, :a_relation)})
      assert ex.command_result == :a_relation

      assert {:pull, ex} = Execution.step(ex, {:response, command_result(session, :another)})
      assert ex.command_result == :a_relation
    end

    test "an empty result carries no relation and latches nothing", %{
      session: session,
      execution: ex
    } do
      assert {:pull, ex} = Execution.step(ex, {:response, command_result(session, nil)})
      assert ex.command_result == nil
    end
  end

  # Placeholders where a `Literal` belongs, as the SQL tests use `:a_relation`: what the arm
  # carries is `latu_ml`'s business, and passing an atom through proves this module never looks.
  describe "an MlCommand's result" do
    test "is latched, first one wins", %{session: session, execution: ex} do
      assert ex.ml_command_result == nil

      assert {:pull, ex} = Execution.step(ex, {:response, ml_result(session, {:param, :a_lit})})
      assert ex.ml_command_result.result_type == {:param, :a_lit}

      assert {:pull, ex} = Execution.step(ex, {:response, ml_result(session, {:summary, "s"})})
      assert ex.ml_command_result.result_type == {:param, :a_lit}
    end

    test "a result with no arm set latches nothing", %{session: session, execution: ex} do
      assert {:pull, ex} = Execution.step(ex, {:response, ml_result(session, nil)})
      assert ex.ml_command_result == nil
    end
  end

  describe "the reattach cursor" do
    test "follows the latest response, batch or not", %{session: session, execution: execution} do
      {{:emit, _}, execution} = step(execution, session, batch(1, 0, "A"), id: "r1")
      assert execution.last_response_id == "r1"

      schema = %Response{session_id: session.session_id, response_id: "r2", schema: :a_data_type}
      {:pull, execution} = Execution.step(execution, {:response, schema})

      assert execution.last_response_id == "r2"
    end

    test "is not clobbered by a response that carries none", %{session: session, execution: ex} do
      {{:emit, _}, ex} = step(ex, session, batch(1, 0, "A"), id: "r1")
      {{:emit, _}, ex} = step(ex, session, batch(1, 1, "B"), id: "")

      assert ex.last_response_id == "r1"
    end
  end

  describe "identity" do
    test "another session's answer is refused", %{execution: execution} do
      stray = %Response{session_id: "someone-else", response_type: batch(1, 0, "A")}

      assert {{:fail, %Error{kind: :session}}, _} = Execution.step(execution, {:response, stray})
    end

    test "another operation's answer is refused", %{session: session, execution: execution} do
      assert {{:fail, %Error{kind: :session, message: message}}, _} =
               step(execution, session, batch(1, 0, "A"), operation: "op-2")

      assert message == "server answered for operation op-2, expected op-1"
    end

    test "an absent operation id is no information", %{session: session, execution: execution} do
      assert {{:emit, _}, _} = step(execution, session, batch(1, 0, "A"), operation: "")
    end

    test "the server restarting mid-execution is caught", %{session: session, execution: ex} do
      {{:emit, _}, ex} = step(ex, session, batch(1, 0, "A"), server: "srv-1")

      assert {{:fail, %Error{kind: :session, message: message}}, _} =
               step(ex, session, batch(1, 1, "B"), server: "srv-2")

      assert message =~ "restarted"
    end
  end

  describe "transport errors" do
    test "UNAVAILABLE is retried", %{execution: execution} do
      assert {{:reattach, 50}, _} = Execution.step(execution, unavailable())
    end

    test "INTERNAL is retried only for a disconnected cursor", %{execution: execution} do
      cursor = grpc_error(13, "INTERNAL: INVALID_CURSOR.DISCONNECTED: reattach")

      assert {{:reattach, 50}, _} = Execution.step(execution, cursor)
      assert {{:fail, _}, _} = Execution.step(execution, grpc_error(13, "INTERNAL: bad plan"))
    end

    test "anything else is terminal", %{execution: execution} do
      for status <- [3, 4, 8, 10, 16] do
        assert {{:fail, _}, _} = Execution.step(execution, grpc_error(status, "nope"))
      end

      no_status = {:error, Error.new(:rpc, "ExecutePlan failed: :closed")}
      assert {{:fail, _}, _} = Execution.step(execution, no_status)
    end

    test "back off on PySpark's schedule", %{execution: execution} do
      {delays, _} =
        Enum.map_reduce(1..7, execution, fn _, execution ->
          {{:reattach, backoff}, execution} = Execution.step(execution, unavailable())
          {backoff, execution}
        end)

      # Below the jitter threshold the wait is exact; above it, jitter is added.
      assert Enum.take(delays, 3) == [50, 200, 800]

      for {delay, base} <- Enum.zip(Enum.drop(delays, 3), [3200, 12_800, 51_200, 60_000]) do
        assert delay >= base and delay < base + 500
      end
    end

    test "give up after fifteen in a row", %{execution: execution} do
      spent = spend_budget(execution)

      assert {{:fail, %Error{message: message}}, _} = Execution.step(spent, unavailable())
      assert message =~ "gave up after 15 retries"
    end

    test "a response starts a fresh budget", %{session: session, execution: execution} do
      {{:emit, _}, recovered} = step(spend_budget(execution), session, batch(1, 0, "A"))

      assert {{:reattach, 50}, _} = Execution.step(recovered, unavailable())
    end

    test "the budget is the session's, not this module's", %{execution: execution} do
      none = Execution.new(Session.from_url!("sc://h", retry: [max_retries: 0]), @operation)

      assert {{:fail, %Error{message: message}}, _} = Execution.step(none, unavailable())
      assert message =~ "gave up after 0 retries"
      # The default policy retries the same error, so it is the policy doing the work.
      assert {{:reattach, 50}, _} = Execution.step(execution, unavailable())
    end

    test "so is the schedule" do
      brisk = Session.from_url!("sc://h", retry: [initial_backoff: 10, jitter: 0])
      execution = Execution.new(brisk, @operation)

      assert {{:reattach, 10}, execution} = Execution.step(execution, unavailable())
      assert {{:reattach, 40}, _} = Execution.step(execution, unavailable())
    end
  end

  describe "an execution the server has lost" do
    test "is re-sent when nothing had arrived yet", %{execution: execution} do
      for handle <- ["INVALID_HANDLE.OPERATION_NOT_FOUND", "INVALID_HANDLE.SESSION_NOT_FOUND"] do
        assert {{:restart, 50}, _} = Execution.step(execution, lost(handle))
      end
    end

    test "is not re-sent once responses have arrived", %{session: s, execution: execution} do
      {{:emit, _}, execution} = step(execution, s, batch(1, 0, "A"), id: "r1")

      assert {{:fail, %Error{message: message}}, _} =
               Execution.step(execution, lost("INVALID_HANDLE.OPERATION_NOT_FOUND"))

      assert message =~ "cannot be recovered"
    end

    test "re-sending draws on the same budget", %{execution: execution} do
      handle = lost("INVALID_HANDLE.OPERATION_NOT_FOUND")

      spent =
        Enum.reduce(1..15, execution, fn _, execution ->
          {{:restart, _}, execution} = Execution.step(execution, handle)
          execution
        end)

      assert {{:fail, %Error{message: message}}, _} = Execution.step(spent, handle)
      assert message =~ "gave up after 15 retries"
    end

    test "the check is on the message, not the status", %{execution: execution} do
      # Spark reports it as an error class inside the message; there is no code for it.
      invalid = {:error, Error.new(:rpc, "INVALID_HANDLE.OPERATION_NOT_FOUND: gone", status: 3)}

      assert {{:restart, _}, _} = Execution.step(execution, invalid)
      assert {{:fail, _}, _} = Execution.step(execution, grpc_error(3, "INVALID_ARGUMENT"))
    end
  end

  defp step(execution, session, response_type, opts \\ []) do
    response = %Response{
      session_id: session.session_id,
      server_side_session_id: opts[:server],
      operation_id: Keyword.get(opts, :operation, @operation),
      response_id: opts[:id],
      response_type: response_type
    }

    Execution.step(execution, {:response, response})
  end

  defp batch(row_count, start_offset, data) do
    {:arrow_batch,
     %Response.ArrowBatch{row_count: row_count, data: data, start_offset: start_offset}}
  end

  defp result_complete, do: {:result_complete, %Response.ResultComplete{}}

  defp command_result(session, relation) do
    %Response{
      session_id: session.session_id,
      response_type: {:sql_command_result, %Response.SqlCommandResult{relation: relation}}
    }
  end

  defp ml_result(session, result_type) do
    %Response{
      session_id: session.session_id,
      response_type: {:ml_command_result, %MlCommandResult{result_type: result_type}}
    }
  end

  defp unavailable, do: grpc_error(14, "UNAVAILABLE: connection reset")

  defp lost(handle), do: grpc_error(5, "#{handle}: gone")

  defp grpc_error(status, message), do: {:error, Error.new(:rpc, message, status: status)}

  defp spend_budget(execution) do
    Enum.reduce(1..15, execution, fn _, execution ->
      {{:reattach, _}, execution} = Execution.step(execution, unavailable())
      execution
    end)
  end
end
