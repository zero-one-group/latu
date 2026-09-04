defmodule Latu.LocalDataTest do
  use ExUnit.Case, async: true

  import Latu.Wire

  alias Explorer.DataFrame, as: Frame
  alias Latu.DataFrame
  alias Latu.Protocol.Spark.Connect, as: Proto
  alias Latu.Result
  alias Latu.Session

  setup do
    %{session: Session.from_url!("sc://h")}
  end

  describe "the schema-only local relation against PySpark" do
    test "an empty frame is the schema alone, no data field", %{session: session} do
      # PySpark ddl-parses "id INT" and sends StructType().json(); Latu sends the user's
      # string verbatim — the proto takes either — so this feeds the fixture's own JSON back
      # through create_dataframe. That pins the message shape (the local_relation arm, the
      # absent data field); the schema content is verbatim-in-verbatim-out by construction.
      expected = Proto.Plan.decode(File.read!("test/wire/local_relation_schema_only.bin"))
      {:root, %{rel_type: {:local_relation, %{schema: json}}}} = expected.op_type

      {:ok, df} = DataFrame.create_dataframe(session, [], schema: json)
      assert_wire(df.plan, "local_relation_schema_only")
    end
  end

  describe "create_dataframe/3 refusals, all before any RPC" do
    test "empty data with no schema", %{session: session} do
      assert_raise ArgumentError, ~r/nothing to infer|no types to infer/, fn ->
        DataFrame.create_dataframe(session, [])
      end
    end

    test "a schema that is not a string", %{session: session} do
      assert_raise ArgumentError, ~r/DDL or Spark's JSON/, fn ->
        DataFrame.create_dataframe(session, [%{id: 1}], schema: :id)
      end
    end

    test "data in no shape Latu reads", %{session: session} do
      # apply/3 keeps the deliberately-wrong literal out of type inference: the 1.20 checker
      # proves a direct call with a binary always raises — which is the point — and
      # warnings-as-errors would fail the build on its own correct conclusion.
      assert_raise ArgumentError, ~r/Explorer.DataFrame, a list of row maps/, fn ->
        apply(DataFrame, :create_dataframe, [session, "not data"])
      end
    end

    test "a row missing a column", %{session: _session} do
      assert_raise KeyError, fn ->
        DataFrame.columns_for([%{a: 1, b: 2}, %{a: 3}])
      end
    end
  end

  describe "column coercion" do
    test "row maps sort their keys — PySpark's rule for dicts" do
      columns = DataFrame.columns_for([%{b: 2, a: 1}, %{b: 4, a: 3}])

      assert columns == [{"a", [1, 3]}, {"b", [2, 4]}]
    end

    test "a keyword list keeps its declared order" do
      assert DataFrame.columns_for(b: [2], a: [1]) == [{"b", [2]}, {"a", [1]}]
    end

    test "a map of columns sorts its keys" do
      assert DataFrame.columns_for(%{"b" => [2], "a" => [1]}) == [{"a", [1]}, {"b", [2]}]
    end

    test "an Explorer frame passes through, empty means empty" do
      frame = Frame.new(a: [1])

      assert DataFrame.columns_for(frame) == frame
      assert DataFrame.columns_for(Frame.new(a: [])) == :empty
      assert DataFrame.columns_for([]) == :empty
      assert DataFrame.columns_for(%{}) == :empty
    end
  end

  describe "the Arrow round trip" do
    test "from_columns |> to_ipc decodes back to the same rows" do
      frame = Result.from_columns([{"id", [1, 2, 3]}, {"name", ["a", "b", "c"]}])
      ipc = Result.to_ipc(frame)

      {:ok, decoded} = Result.decode([%{data: ipc, row_count: 3, start_offset: 0}])

      assert Result.rows(decoded, :atoms) == [
               %{id: 1, name: "a"},
               %{id: 2, name: "b"},
               %{id: 3, name: "c"}
             ]
    end

    test "chunks each decode independently and cover every row" do
      frame = Result.from_columns([{"id", Enum.to_list(1..10)}])
      chunks = Result.to_ipc_chunks(frame, 4)

      assert length(chunks) == 3

      rows =
        chunks
        |> Enum.zip([4, 4, 2])
        |> Enum.flat_map(fn {chunk, count} ->
          {:ok, decoded} = Result.decode([%{data: chunk, row_count: count, start_offset: nil}])
          Result.rows(decoded, :atoms)
        end)

      assert Enum.map(rows, & &1.id) == Enum.to_list(1..10)
    end
  end

  describe "chunk arithmetic" do
    test "rows per chunk respects both bounds" do
      # 100 rows, 1000 bytes → 10 bytes/row; 55-byte chunks → 5 rows.
      assert DataFrame.rows_per_chunk(100, 1000, 1000, 55) == 5
      # The row bound wins when it is tighter.
      assert DataFrame.rows_per_chunk(100, 1000, 3, 55) == 3
      # Never zero, even when one row is over the byte bound.
      assert DataFrame.rows_per_chunk(10, 1000, 1000, 50) == 1
    end

    test "blobs group into batches by size, oversized ones travel alone" do
      blobs = ["aaaa", "bbbb", "cccccccccccc", "dd"]

      assert DataFrame.batch_blobs(blobs, 10) == [
               ["aaaa", "bbbb"],
               ["cccccccccccc"],
               ["dd"]
             ]

      assert DataFrame.batch_blobs([], 10) == []
    end
  end
end
