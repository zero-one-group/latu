defmodule Latu.ResultTest do
  use ExUnit.Case, async: true

  alias Explorer.DataFrame
  alias Latu.Error
  alias Latu.Result

  # Explorer writes the same Arrow IPC streaming format Spark sends, so the decode path is
  # testable with no server: dump a frame, hand the bytes back as a batch.

  describe "decode/2" do
    test "one batch" do
      assert {:ok, frame} = Result.decode([batch(%{id: [1, 2, 3]})])

      assert DataFrame.to_columns(frame, atom_keys: true) == %{id: [1, 2, 3]}
    end

    test "several batches concatenate, in order" do
      batches = [batch(%{id: [1, 2]}), batch(%{id: [3]}), batch(%{id: [4, 5]})]

      assert {:ok, frame} = Result.decode(batches)
      assert DataFrame.to_columns(frame, atom_keys: true) == %{id: [1, 2, 3, 4, 5]}
    end

    test "prunes columns at load time" do
      assert {:ok, frame} = Result.decode([batch(%{id: [1], name: ["a"]})], columns: ["id"])

      assert DataFrame.names(frame) == ["id"]
    end

    test "catches a row count that disagrees with the server's" do
      [one] = [batch(%{id: [1]})]

      assert {:error, %Error{kind: :decode, message: message}} =
               Result.decode([%{one | row_count: 99}])

      assert message == "decoded 1 rows, the server reported 99"
    end

    test "catches byte-concatenated batches, which decode as the first one alone" do
      [%{data: first}, %{data: second}] = [batch(%{id: [1, 2]}), batch(%{id: [3, 4]})]
      glued = %{data: first <> second, row_count: 4, start_offset: 0}

      assert {:error, %Error{kind: :decode, message: message}} = Result.decode([glued])
      assert message == "decoded 2 rows, the server reported 4"
    end

    test "no batches at all is an error, not an empty frame" do
      assert {:error, %Error{kind: :decode}} = Result.decode([])
    end

    test "rejects an unknown option" do
      assert_raise ArgumentError, fn -> Result.decode([batch(%{id: [1]})], limit: 1) end
    end
  end

  describe "rows/2" do
    test "maps with atom keys, in row order" do
      {:ok, frame} = Result.decode([batch(%{id: [1, 2]}), batch(%{id: [3]})])

      assert Result.rows(frame, :atoms) == [%{id: 1}, %{id: 2}, %{id: 3}]
    end

    test "or string keys" do
      {:ok, frame} = Result.decode([batch(%{id: [1]})])

      assert Result.rows(frame, :strings) == [%{"id" => 1}]
    end

    test "nils survive the trip" do
      {:ok, frame} = Result.decode([batch(%{id: [1, nil]})])

      assert Result.rows(frame, :atoms) == [%{id: 1}, %{id: nil}]
    end
  end

  describe "only/1" do
    test "pulls the single cell positionally, whatever the column is called" do
      {:ok, frame} = Result.decode([batch(%{"count(1)" => [42]})])

      assert Result.only(frame) == {:ok, 42}
    end

    test "refuses more than one column" do
      {:ok, frame} = Result.decode([batch(%{a: [1], b: [2]})])

      assert {:error, %Error{kind: :decode, message: message}} = Result.only(frame)
      assert message == "expected one column, got 2"
    end

    test "refuses more than one row, through only/2" do
      {:ok, frame} = Result.decode([batch(%{a: [1, 2]})])

      assert {:error, %Error{kind: :decode}} = Result.only(frame)
    end
  end

  describe "only/2" do
    test "pulls the single cell out" do
      assert {:ok, frame} = Result.decode([batch(%{show_string: ["+---+\n"]})])
      assert Result.only(frame, "show_string") == {:ok, "+---+\n"}
    end

    test "refuses anything that is not one row" do
      assert {:ok, frame} = Result.decode([batch(%{show_string: ["a", "b"]})])

      assert {:error, %Error{kind: :decode, message: message}} = Result.only(frame, "show_string")

      assert message == "expected one row of show_string, got 2"
    end
  end

  defp batch(columns) do
    frame = DataFrame.new(columns)

    %{
      data: DataFrame.dump_ipc_stream!(frame),
      row_count: DataFrame.n_rows(frame),
      start_offset: 0
    }
  end
end
