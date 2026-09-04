defmodule Latu.WindowTest do
  use ExUnit.Case, async: true

  @moduletag :capture_log

  import ExUnit.CaptureLog
  import Latu.Column
  import Latu.Wire

  alias Latu.Functions, as: F
  alias Latu.Plan
  alias Latu.Protocol.Spark.Connect, as: Proto
  alias Latu.Session
  alias Latu.Window, as: W

  setup do
    {:ok, session: Session.from_url!("sc://localhost:15002")}
  end

  # =============================================
  # Golden
  # =============================================

  describe "against PySpark" do
    test "partition, order, no frame", %{session: session} do
      window = W.partition_by([:id]) |> W.order_by([desc(:id)])

      session
      |> Latu.range(10)
      |> Latu.with_columns(rn: over(F.row_number(), window))
      |> assert_wire("window_row_number")
    end

    test "a row frame with unbounded and current-row bounds", %{session: session} do
      window =
        W.partition_by([:id])
        |> W.order_by([:id])
        |> W.rows_between(:unbounded_preceding, :current_row)

      session
      |> Latu.range(10)
      |> Latu.with_columns(s: over(F.sum(:id), window))
      |> assert_wire("window_rows_frame")
    end

    test "a row frame offset is a 32-bit integer", %{session: session} do
      window = W.partition_by([:id]) |> W.order_by([:id]) |> W.rows_between(-1, 1)

      session
      |> Latu.range(10)
      |> Latu.with_columns(s: over(F.sum(:id), window))
      |> assert_wire("window_rows_offsets")
    end

    test "a range frame offset is a 64-bit long, for the same number", %{session: session} do
      window = W.partition_by([:id]) |> W.order_by([:id]) |> W.range_between(-1, 1)

      session
      |> Latu.range(10)
      |> Latu.with_columns(s: over(F.sum(:id), window))
      |> assert_wire("window_range_offsets")
    end

    test "no partition at all", %{session: session} do
      session
      |> Latu.range(10)
      |> Latu.with_columns(rn: over(F.row_number(), W.order_by([:id])))
      |> assert_wire("window_no_partition")
    end
  end

  # =============================================
  # The spec
  # =============================================

  describe "boundaries" do
    test "zero is the current row, because Spark has no other reading of it" do
      base = W.partition_by([:id])

      assert plan(base |> W.rows_between(0, 2)) == plan(base |> W.rows_between(:current_row, 2))
    end

    test "a direction in the wrong position is refused" do
      base = W.partition_by([:id])

      assert_raise ArgumentError, ~r/upper bound/, fn ->
        W.rows_between(base, :unbounded_following, 1)
      end

      assert_raise ArgumentError, ~r/lower bound/, fn ->
        W.rows_between(base, -1, :unbounded_preceding)
      end
    end

    test "an unknown boundary names the alternatives" do
      assert_raise ArgumentError, ~r/:current_row/, fn ->
        W.rows_between(W.partition_by([:id]), :yesterday, 1)
      end
    end

    test "a row offset must fit in 32 bits; a range offset need not" do
      base = W.partition_by([:id])
      big = 3_000_000_000

      assert_raise ArgumentError, ~r/32-bit/, fn -> plan(W.rows_between(base, -big, 0)) end
      assert %Proto.Expression{} = plan(W.range_between(base, -big, 0))
    end

    test "an absent frame sends no frame_spec" do
      assert %Proto.Expression{expr_type: {:window, spec}} = plan(W.partition_by([:id]))
      assert spec.frame_spec == nil
    end
  end

  describe "over/2" do
    test "warns when the window has no partition" do
      log = capture_log(fn -> over(F.row_number(), W.order_by([:id])) end)

      assert log =~ "no partition_by"
    end

    test "says nothing when it has one" do
      log = capture_log(fn -> over(F.row_number(), W.partition_by([:id])) end)

      refute log =~ "no partition_by"
    end

    test "says nothing about an explicitly global window" do
      log = capture_log(fn -> over(F.row_number(), W.partition_by([])) end)

      refute log =~ "no partition_by"
    end

    test "a single column needs no list" do
      assert W.partition_by(:id) == W.partition_by([:id])
      assert W.order_by(:id) == W.order_by([:id])
    end
  end

  defp plan(window), do: Plan.over(F.row_number(), window)
end
