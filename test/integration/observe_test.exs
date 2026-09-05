defmodule Latu.Integration.ObserveTest do
  use ExUnit.Case, async: false

  # observe/3 against a real server. The plan is proven offline by the golden fixtures; what only
  # the server answers is when observed_metrics arrives on the response stream, whether a *write*
  # reports any at all, and what Spark accepts as a metric expression.
  #
  # Reads fixtures/measurements.parquet (dev/make_data_fixtures.py) — nulls matter here, because
  # half of what you would observe in earnest is a null count.
  #
  # async: false because the write cases share the server's /tmp, as write_test does.
  #
  # Needs a Spark Connect server on :15002 and the fixtures generated — `mix fixtures`.
  import Latu.Column

  alias Latu.Functions, as: F

  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  @run System.system_time(:millisecond)

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session, release: true) end)

    df = Latu.read(session, format: "parquet", path: "/fixtures/measurements.parquet")

    %{session: session, df: df}
  end

  defp tmp_path, do: "/tmp/latu_test/#{@run}_#{System.unique_integer([:positive])}"

  describe "collect_with_metrics/2" do
    test "the metrics come back beside the rows", %{df: df} do
      {:ok, rows, info} =
        df
        |> Latu.observe(:checks, total: F.count(lit(1)))
        |> Latu.collect_with_metrics()

      assert %{checks: %{total: 8}} = info.observed
      assert length(rows) == 8
    end

    test "an aggregate that counts nulls, which is what people observe for", %{df: df} do
      {:ok, _rows, info} =
        df
        |> Latu.observe(:quality, rows: F.count(lit(1)), scored: F.count(:score))
        |> Latu.collect_with_metrics()

      # `count(col)` skips nulls and `count(1)` does not, so the gap is the null count: the
      # fixture's eight rows carry three null scores.
      assert %{quality: %{rows: 8, scored: 5}} = info.observed
    end

    test "two observations in one plan both report", %{df: df} do
      {:ok, _rows, info} =
        df
        |> Latu.observe(:before, all: F.count(lit(1)))
        |> Latu.filter(is_not_null(:score))
        |> Latu.observe(:after, kept: F.count(lit(1)))
        |> Latu.collect_with_metrics()

      assert %{before: %{all: 8}, after: %{kept: 5}} = info.observed
    end

    test "a non-integer metric decodes through Latu.Result.Literal", %{df: df} do
      {:ok, _rows, info} =
        df
        |> Latu.observe(:spread, lowest: F.min(:score), mean: F.avg(:score))
        |> Latu.collect_with_metrics()

      # Doubles on the wire, floats here — the fixture's five non-null scores average 34.0.
      assert %{spread: %{lowest: 10.0, mean: 34.0}} = info.observed
    end

    test "the frame itself is unchanged by observing it", %{df: df} do
      {:ok, rows, _info} =
        df |> Latu.observe(:checks, n: F.count(lit(1))) |> Latu.collect_with_metrics()

      assert rows == Latu.collect!(df)
    end

    # The other half of an ExecutionInfo. Spark decides what to report, so this asserts
    # the shape rather than any particular metric — the claim is that Latu no longer drops the
    # arm, not that a given node exists.
    test "Spark's own SQL metrics come back beside the observed ones", %{df: df} do
      {:ok, _rows, info} = Latu.collect_with_metrics(Latu.observe(df, :n, c: F.count(lit(1))))

      # Non-empty deliberately: `is_list/1` followed by a loop passes on an empty list having
      # asserted nothing. A `range` scan reports "number of output rows", so a red here means
      # Spark sends no SQL metrics and `Latu.ExecutionInfo`'s docs need correcting.
      assert info.metrics != []
      assert Enum.any?(info.metrics, &(&1.metrics != %{}))

      for node <- info.metrics do
        assert is_binary(node.name)
        assert is_integer(node.plan_id)

        for {name, metric} <- node.metrics do
          assert is_binary(name)
          assert is_integer(metric.value)
          assert is_binary(metric.type)
        end
      end
    end

    test "the server refuses a bare column, which no offline check can see", %{df: df} do
      # Spark requires aggregate expressions in a CollectMetrics node. The plan encodes fine.
      assert {:error, %Latu.Error{kind: :rpc}} =
               df |> Latu.observe(:bad, raw: col(:id)) |> Latu.collect_with_metrics()
    end
  end

  describe "count_with_metrics/1 and to_explorer_with_metrics/2" do
    test "count reports both its own answer and the metrics", %{df: df} do
      {:ok, count, info} =
        df |> Latu.observe(:checks, total: F.count(lit(1))) |> Latu.count_with_metrics()

      assert %{checks: %{total: ^count}} = info.observed
    end

    test "to_explorer reports beside the frame", %{df: df} do
      {:ok, frame, info} =
        df |> Latu.observe(:checks, total: F.count(lit(1))) |> Latu.to_explorer_with_metrics()

      assert %{checks: %{total: total}} = info.observed
      assert Explorer.DataFrame.n_rows(frame) == total
    end
  end

  describe "write_with_metrics/2 — the case observe exists for" do
    test "a write reports the metrics its input observed", %{df: df} do
      path = tmp_path()

      assert {:ok, info} =
               df
               |> Latu.observe(:quality, rows: F.count(lit(1)), scored: F.count(:score))
               |> Latu.write_with_metrics(format: "parquet", path: path)

      assert %{quality: %{rows: 8, scored: 5}} = info.observed

      # And the write actually happened, rather than the metrics arriving from thin air.
      assert df.session
             |> Latu.read(format: "parquet", path: path)
             |> Latu.count!() == 8
    end

    test "a write with nothing observed reports an empty map, not an error", %{df: df} do
      assert {:ok, %Latu.ExecutionInfo{observed: observed}} =
               Latu.write_with_metrics(df, format: "parquet", path: tmp_path())

      assert observed == %{}
    end

    test "save_as_table reports too", %{df: df} do
      table = "observed_#{@run}_#{System.unique_integer([:positive])}"

      assert {:ok, %{observed: %{quality: %{rows: 8}}}} =
               df
               |> Latu.observe(:quality, rows: F.count(lit(1)))
               |> Latu.save_as_table_with_metrics(table, mode: :overwrite)
    end

    test "a twin takes :progress, and it reaches the transport", %{df: df} do
      # A handler that is never called cannot be told apart from one the server had no report for
      # — the query is too short for the 100ms interval — so the assertion is that the option
      # is accepted and the write still happens, rather than that a report arrived.
      owner = self()

      assert {:ok, %Latu.ExecutionInfo{}} =
               df
               |> Latu.observe(:quality, rows: F.count(lit(1)))
               |> Latu.write_with_metrics(
                 format: "parquet",
                 path: tmp_path(),
                 progress: fn progress -> send(owner, {:progress, progress}) end
               )
    end
  end

  describe "an observed frame is still a frame" do
    # The guard is on writes only: everything else runs and does not report, as PySpark does
    # when nobody calls Observation.get. The server accepts a CollectMetrics node under a read
    # and under a join exactly as it does under a write — which is the half a unit test cannot
    # see.
    test "reads run and do not report", %{df: df} do
      observed = Latu.observe(df, :checks, total: F.count(lit(1)))

      assert ExUnit.CaptureIO.capture_io(fn ->
               assert :ok = observed |> Latu.limit(2) |> Latu.show()
             end) =~ "| id|"

      assert {:ok, 8} = Latu.count(observed)
      assert {:ok, [_ | _]} = Latu.collect(observed)
    end

    test "an observed frame composes into a join and a view", %{df: df, session: session} do
      observed = Latu.observe(df, :checks, total: F.count(lit(1)))

      # Seven, not eight: the fixture's null id matches nothing, not even itself.
      assert {:ok, 7} = observed |> Latu.join(df, on: :id) |> Latu.count()

      view = "observed_view_#{@run}_#{System.unique_integer([:positive])}"
      assert :ok = Latu.create_temp_view(observed, view)
      assert {:ok, 8} = session |> Latu.table(view) |> Latu.count()
    end
  end
end
