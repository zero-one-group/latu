defmodule Latu.StreamingTest do
  use ExUnit.Case, async: true

  import Latu.Wire

  alias Latu.DataFrame
  alias Latu.Plan
  alias Latu.Session
  alias Latu.StreamingQuery

  # The progress decoder's example is a doctest: the one piece of streaming that is pure, and
  # the one surface no proto guards.
  doctest Latu.StreamingQuery

  # Golden plans come from PySpark: python dev/pyspark_oracle.py --generate

  setup do
    %{session: Session.from_url!("sc://h")}
  end

  describe "streaming sources against PySpark" do
    test "read/2 with is_streaming sets Read's own flag, beside the source", %{session: s} do
      s
      |> Latu.read(
        format: "parquet",
        schema: "id BIGINT",
        path: "/tmp/latu/src",
        max_files_per_trigger: 1,
        is_streaming: true
      )
      |> assert_wire("read_stream")
    end

    test "table/3 with is_streaming", %{session: session} do
      session
      |> Latu.table("events", is_streaming: true)
      |> assert_wire("read_stream_table")
    end

    test "the flag defaults off, so the batch fixtures are unchanged", %{session: session} do
      assert_wire(Latu.table(session, "people"), "read_table")
      assert_wire(Latu.read(session, format: "json", path: "/fixtures/people.json"), "read_json")
    end
  end

  describe "with_watermark/3 and distinct/3 against PySpark" do
    test "with_watermark is a relation of its own", %{session: session} do
      session
      |> rate()
      |> Latu.with_watermark(:timestamp, "10 seconds")
      |> assert_wire("with_watermark")
    end

    test "distinct within the watermark, by columns", %{session: session} do
      session
      |> rate()
      |> Latu.with_watermark(:timestamp, "10 seconds")
      |> Latu.distinct([:value], within_watermark: true)
      |> assert_wire("distinct_within_watermark")
    end

    test "and by every column", %{session: session} do
      session
      |> rate()
      |> Latu.with_watermark(:timestamp, "10 seconds")
      |> Latu.distinct([], within_watermark: true)
      |> assert_wire("distinct_within_watermark_all")
    end

    test "a delay that is not a string is refused", %{session: session} do
      assert_raise ArgumentError, ~r/interval string/, fn ->
        Latu.with_watermark(rate(session), :timestamp, 10)
      end
    end

    test "options in the columns' place are refused by name", %{session: session} do
      assert_raise ArgumentError, ~r/columns before the options/, fn ->
        Latu.distinct(rate(session), within_watermark: true)
      end
    end
  end

  describe "write_stream_command/2 against PySpark" do
    test "a file sink: format, mode, name, trigger, partitioning, checkpoint", %{session: s} do
      s
      |> rate()
      |> DataFrame.write_stream_command(
        format: "parquet",
        path: "/tmp/latu/sink",
        output_mode: :append,
        query_name: "rollups",
        trigger: :available_now,
        partition_by: [:value],
        checkpoint_location: "/tmp/latu/ckpt"
      )
      |> assert_wire_command("write_stream_available_now")
    end

    test "a table sink with a processing-time trigger and clustering", %{session: session} do
      session
      |> rate()
      |> DataFrame.write_stream_command(
        table: "sink_tbl",
        trigger: {:processing_time, "10 seconds"},
        cluster_by: [:value]
      )
      |> assert_wire_command("write_stream_processing_time_table")
    end

    test "no destination at all, a once trigger, and a writer option", %{session: session} do
      session
      |> rate()
      |> DataFrame.write_stream_command(format: "console", trigger: :once, num_rows: 5)
      |> assert_wire_command("write_stream_once_console")
    end

    test "a continuous trigger and update mode", %{session: session} do
      session
      |> rate()
      |> DataFrame.write_stream_command(
        format: "kafka",
        trigger: {:continuous, "1 second"},
        output_mode: :update
      )
      |> assert_wire_command("write_stream_continuous")
    end

    test "nothing set: no trigger arm, empty strings where PySpark leaves them", %{session: s} do
      s
      |> rate()
      |> DataFrame.write_stream_command([])
      |> assert_wire_command("write_stream_defaults")
    end

    test "path and table together are refused", %{session: session} do
      assert_raise ArgumentError, ~r/never both/, fn ->
        DataFrame.write_stream_command(rate(session), path: "/a", table: "t")
      end
    end

    test "an unknown trigger shape names the four", %{session: session} do
      assert_raise ArgumentError, ~r/:available_now, :once/, fn ->
        DataFrame.write_stream_command(rate(session), trigger: :every_second)
      end

      assert_raise ArgumentError, ~r/interval a string/, fn ->
        DataFrame.write_stream_command(rate(session), trigger: {:processing_time, 10})
      end
    end

    test "an unknown output mode names the three", %{session: session} do
      assert_raise ArgumentError, ~r/:append, :complete, :update/, fn ->
        DataFrame.write_stream_command(rate(session), output_mode: :upsert)
      end
    end
  end

  describe "StreamingQueryCommand against PySpark" do
    # One assertion per fixture rather than a loop over pairs: `dev/fixture_coverage.py` reads
    # the name out of the `assert_wire_command` call, so a name reached through a variable is a
    # golden nothing counts.
    test "the flag arms" do
      assert_wire_command(arm(:status), "stream_status")
      assert_wire_command(arm(:last_progress), "stream_last_progress")
      assert_wire_command(arm(:recent_progress), "stream_recent_progress")
      assert_wire_command(arm(:stop), "stream_stop")
      assert_wire_command(arm(:process_all_available), "stream_process_all_available")
      assert_wire_command(arm(:exception), "stream_exception")
    end

    test "explain, plain and extended" do
      "q-1"
      |> Plan.streaming_query_command("r-1", {:explain, false})
      |> assert_wire_command("stream_explain")

      "q-1"
      |> Plan.streaming_query_command("r-1", {:explain, true})
      |> assert_wire_command("stream_explain_extended")
    end

    test "await_termination, bounded and not: an absent timeout is absent, not zero" do
      "q-1"
      |> Plan.streaming_query_command("r-1", {:await_termination, 5_000})
      |> assert_wire_command("stream_await_termination")

      "q-1"
      |> Plan.streaming_query_command("r-1", {:await_termination, nil})
      |> assert_wire_command("stream_await_termination_unbounded")
    end

    test "an unknown arm names the set" do
      assert_raise ArgumentError, ~r/:status, :last_progress/, fn ->
        Plan.streaming_query_command("q-1", "r-1", :pause)
      end
    end
  end

  describe "StreamingQueryManagerCommand against PySpark" do
    test "the four arms PySpark sends" do
      assert_wire_command(Plan.streaming_query_manager_command(:active), "streams_active")

      {:get_query, "q-1"}
      |> Plan.streaming_query_manager_command()
      |> assert_wire_command("streams_get")

      :reset_terminated
      |> Plan.streaming_query_manager_command()
      |> assert_wire_command("streams_reset_terminated")

      {:await_any_termination, 5_000}
      |> Plan.streaming_query_manager_command()
      |> assert_wire_command("streams_await_any_termination")

      {:await_any_termination, nil}
      |> Plan.streaming_query_manager_command()
      |> assert_wire_command("streams_await_any_termination_unbounded")
    end

    test "the listener arms are not built" do
      assert_raise ArgumentError, ~r/expected :active/, fn ->
        Plan.streaming_query_manager_command(:list_listeners)
      end
    end
  end

  describe "the handle" do
    test "is built from the start result, with an empty name read as none", %{session: s} do
      started = %{query_id: %{id: "q-1", run_id: "r-1"}, name: ""}

      assert %StreamingQuery{session: ^s, id: "q-1", run_id: "r-1", name: nil} =
               StreamingQuery.started(s, started)

      assert %StreamingQuery{name: "rollups"} =
               StreamingQuery.started(s, %{started | name: "rollups"})
    end

    test "wait options are validated before anything is sent", %{session: session} do
      query = StreamingQuery.started(session, %{query_id: %{id: "q", run_id: "r"}, name: ""})

      assert_raise ArgumentError, ~r/:timeout is a positive integer or :infinity/, fn ->
        StreamingQuery.await_termination(query, timeout: 0)
      end

      assert_raise ArgumentError, ~r/:interval is a positive integer/, fn ->
        StreamingQuery.await_termination(query, interval: -1)
      end

      assert_raise ArgumentError, ~r/:simple or :extended/, fn ->
        StreamingQuery.explain_string(query, mode: :codegen)
      end
    end

    test "an unconnected session is refused, not dialled", %{session: session} do
      query = StreamingQuery.started(session, %{query_id: %{id: "q", run_id: "r"}, name: ""})

      assert {:error, %Latu.Error{kind: :connect}} = StreamingQuery.status(query)
      assert {:error, %Latu.Error{kind: :connect}} = StreamingQuery.active(session)
    end
  end

  defp rate(session), do: Latu.read(session, format: "rate", is_streaming: true)

  # The ids are the ones `query()` carries in the oracle, where the server never sees them.
  defp arm(arm), do: Plan.streaming_query_command("q-1", "r-1", arm)
end
