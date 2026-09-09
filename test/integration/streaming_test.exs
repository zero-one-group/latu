defmodule Latu.Integration.StreamingTest do
  use ExUnit.Case, async: false

  alias Latu.StreamingQuery

  # The offline tests pin every streaming plan against PySpark's bytes; this file asserts what
  # only a server can: that a query starts and answers by id, that `AvailableNow` over a
  # bounded file source drains it and terminates by itself, that the manager finds a query and
  # hands back a handle that works, and that the three result latches hold when a blocking wait
  # spans the sender duration.
  #
  # The **reattach** server, on purpose: its `senderMaxStreamDuration=5s` is what cuts a silent
  # `awaitTermination` mid-wait, which is the case `await_termination/2`'s loop exists for.
  # `local[*]` also gives a streaming query a thread without starving the batch writes.
  #
  # Only `:available_now` runs in the gate (docs/decisions.md, S-D7): a processing-time trigger
  # is a sleep, so everything with one sits behind `--include streaming`.
  #
  # async: false — a streaming query holds a task slot for its lifetime, and these share the
  # server's /tmp.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15003"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session, release: true) end)
    %{session: session}
  end

  # The server's /tmp outlives the BEAM; see write_test.exs.
  @run System.system_time(:millisecond)

  defp tmp_path, do: "/tmp/latu_test/#{@run}_#{System.unique_integer([:positive])}"

  # Three one-file appends, so `maxFilesPerTrigger=1` makes three batches of two rows.
  defp bounded_source(session) do
    path = tmp_path()

    for _ <- 1..3 do
      session
      |> Latu.range(2)
      |> Latu.repartition(1)
      |> Latu.write!(format: "parquet", path: path, mode: :append)
    end

    path
  end

  defp stream(session, path) do
    Latu.read(session,
      format: "parquet",
      schema: "id BIGINT",
      path: path,
      max_files_per_trigger: 1,
      is_streaming: true
    )
  end

  defp sink_opts(trigger) do
    [
      format: "parquet",
      path: tmp_path(),
      trigger: trigger,
      checkpoint_location: tmp_path(),
      query_name: "latu_#{System.unique_integer([:positive])}"
    ]
  end

  describe "an :available_now query over a bounded file source" do
    test "drains the source and terminates by itself", %{session: session} do
      frame = stream(session, bounded_source(session))
      assert Latu.is_streaming!(frame)

      opts = sink_opts(:available_now)
      {:ok, query} = Latu.write_stream(frame, opts)

      assert %StreamingQuery{name: name, id: id, run_id: run_id} = query
      assert name == opts[:query_name]
      assert id =~ ~r/^[0-9a-f]{8}-[0-9a-f]{4}-/
      assert run_id =~ ~r/^[0-9a-f]{8}-[0-9a-f]{4}-/

      # An interval past the 5s sender, so a wait that lasts reattaches at least once and the
      # await_termination latch is exercised across the cut rather than only within one stream.
      assert {:ok, true} =
               StreamingQuery.await_termination(query, timeout: 90_000, interval: 7_000)

      assert {:ok, false} = StreamingQuery.is_active(query)
      assert {:ok, %{is_active: false, message: message}} = StreamingQuery.status(query)
      assert is_binary(message)
      assert {:ok, nil} = StreamingQuery.exception(query)

      # Progress survives termination (probe §5), and the aggregates live on the sources.
      assert {:ok, reports} = StreamingQuery.recent_progress(query)
      assert length(reports) == 3, "expected one batch per file, got #{length(reports)}"
      assert input_rows(reports) == 6
      assert Enum.map(reports, & &1.batch_id) == [0, 1, 2]
      assert %{name: ^name, run_id: ^run_id} = List.last(reports)
      assert {:ok, %{batch_id: 2}} = StreamingQuery.last_progress(query)

      assert Latu.count!(Latu.read(session, format: "parquet", path: opts[:path])) == 6

      # Idempotent on the server, which is what `with_stream/3`'s `after` relies on.
      assert :ok = StreamingQuery.stop(query)
    end

    test "with_stream/3 hands the function's result back and stops on the way out", %{
      session: session
    } do
      frame = stream(session, bounded_source(session))

      assert {:ok, {query, %{batch_id: 2}}} =
               Latu.with_stream(frame, sink_opts(:available_now), fn query ->
                 {:ok, true} = StreamingQuery.await_termination(query, timeout: 90_000)
                 {query, StreamingQuery.last_progress!(query)}
               end)

      assert {:ok, false} = StreamingQuery.is_active(query)
    end

    test "explain answers with the last batch's plan", %{session: session} do
      frame = stream(session, bounded_source(session))

      Latu.with_stream!(frame, sink_opts(:available_now), fn query ->
        # Before a batch has run there is no plan to show, and Spark says so in prose.
        {:ok, true} = StreamingQuery.await_termination(query, timeout: 90_000)

        assert {:ok, plan} = StreamingQuery.explain_string(query)
        assert plan =~ "Physical Plan"
        assert {:ok, extended} = StreamingQuery.explain_string(query, mode: :extended)
        assert byte_size(extended) > byte_size(plan)
      end)
    end
  end

  describe "the session's queries" do
    @describetag :streaming

    test "active/1 and get/2 find a running query, and the handle works", %{session: session} do
      frame = stream(session, bounded_source(session))
      {:ok, query} = Latu.write_stream(frame, sink_opts({:processing_time, "1 second"}))

      try do
        assert {:ok, active} = StreamingQuery.active(session)
        assert Enum.any?(active, &(&1.id == query.id and &1.run_id == query.run_id))

        assert {:ok, %StreamingQuery{} = found} = StreamingQuery.get(session, query.id)
        assert found.run_id == query.run_id
        assert found.name == query.name
        assert {:ok, true} = StreamingQuery.is_active(found)

        # A bounded-source verb: the directory drains and it returns.
        assert :ok = StreamingQuery.process_all_available(found)
        assert input_rows(StreamingQuery.recent_progress!(found)) == 6

        # The cache is keyed by id *and* run id, so a stale run id on a running query misses it
        # and falls through to `streams.get(id)`, which knows the real one.
        stale = %{query | run_id: "00000000-0000-0000-0000-000000000000"}
        assert {:error, %Latu.Error{error_class: class}} = StreamingQuery.status(stale)
        assert class == "CONNECT_INVALID_PLAN.STREAMING_QUERY_RUN_ID_MISMATCH"
      after
        :ok = StreamingQuery.stop(query)
      end

      assert {:ok, false} = StreamingQuery.is_active(query)
      assert {:ok, nil} = StreamingQuery.get(session, query.id)
      refute Enum.any?(StreamingQuery.active!(session), &(&1.id == query.id))
    end

    test "await_any_termination is sticky until reset", %{session: session} do
      frame = stream(session, bounded_source(session))
      {:ok, query} = Latu.write_stream(frame, sink_opts(:available_now))
      assert {:ok, true} = StreamingQuery.await_termination(query, timeout: 90_000)

      # Something terminated since the session began, so this answers at once.
      assert {:ok, true} = StreamingQuery.await_any_termination(session, timeout: 30_000)
      assert :ok = StreamingQuery.reset_terminated(session)
      assert {:ok, false} = StreamingQuery.await_any_termination(session, timeout: 1_000)
    end
  end

  describe "a handle the server does not know" do
    test "is refused by class", %{session: session} do
      frame = stream(session, bounded_source(session))
      {:ok, query} = Latu.write_stream(frame, sink_opts(:available_now))
      assert {:ok, true} = StreamingQuery.await_termination(query, timeout: 90_000)

      unknown = %{query | id: "00000000-0000-0000-0000-000000000000"}
      assert {:error, %Latu.Error{error_class: class}} = StreamingQuery.status(unknown)
      assert class == "CONNECT_INVALID_PLAN.STREAMING_QUERY_NOT_FOUND"
    end
  end

  # The top-level row counts never arrive over Connect (docs/decisions.md); the sources' do.
  defp input_rows(reports) do
    Enum.sum(for report <- reports, source <- report.sources, do: source.num_input_rows)
  end
end
