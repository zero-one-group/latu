defmodule Latu.Integration.NxTest do
  use ExUnit.Case, async: true

  import Latu.Column

  # Needs a Spark Connect server on :15003 — docker compose up -d spark-reattach.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15003"

  # `test/latu/result/arrow_test.exs` checks the reader against **pyarrow's** bytes, which is
  # the right oracle for the format and the wrong one for Spark. This is the other half: the
  # same reader, on bytes a server wrote. The `Vector` case is the one that needs it — the unit
  # fixture is built from Spark's declared `VectorUDT.sqlType`, and whether Spark serialises a
  # UDT that way is a claim only a server can settle.

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session, release: true) end)

    arrays =
      Latu.sql!(session, """
      SELECT CAST(a AS ARRAY<DOUBLE>) AS a FROM VALUES
        (array(1.5, 2.5)), (array(0.5, 3.5)), (array(0.0, 0.0))
      AS t(a)
      """)

    # `array_to_vector` is not a SQL routine — Spark keeps it in an internal function registry
    # the parser never searches (`docs/decisions.md`, 2026-09-06) — so it is reached the way
    # PySpark reaches it, as a plain function call over the wire.
    vectors = Latu.select(arrays, v: fun("array_to_vector", [:a]))

    %{session: session, arrays: arrays, vectors: vectors}
  end

  describe "against a real server" do
    test "a numeric column is a 1-D tensor", %{session: session} do
      df = Latu.sql!(session, "SELECT CAST(id AS DOUBLE) AS id FROM RANGE(4)")

      assert {:ok, %{"id" => t}} = Latu.to_nx(df)
      assert Nx.type(t) == {:f, 64}
      assert Nx.shape(t) == {4}
      assert Nx.to_flat_list(t) == [0.0, 1.0, 2.0, 3.0]
    end

    test "an array column is one {rows, width} tensor", %{arrays: arrays} do
      assert {:ok, %{"a" => t}} = Latu.to_nx(arrays)
      assert Nx.type(t) == {:f, 64}
      assert Nx.shape(t) == {3, 2}
      assert Nx.to_flat_list(t) == [1.5, 2.5, 0.5, 3.5, 0.0, 0.0]
    end

    # The pairing that justifies the verb existing at all.
    test "a Vector column reads here and nowhere else", %{vectors: vectors} do
      assert {:error, error} = Latu.collect(vectors)
      assert error.message =~ "UDT that does not say its SQL type"

      assert {:error, error} = Latu.to_explorer(vectors)
      assert error.message =~ "UDT that does not say its SQL type"

      assert {:ok, %{"v" => t}} = Latu.to_nx(vectors)
      assert Nx.type(t) == {:f, 64}
      assert Nx.shape(t) == {3, 2}
      assert Nx.to_flat_list(t) == [1.5, 2.5, 0.5, 3.5, 0.0, 0.0]
    end

    # If Spark ever reorders or renames those children, this goes red and `Latu.Result.Nx`'s
    # `@vector_udt` is what has to change. Nothing else in the suite would notice.
    test "Spark writes a Vector as the struct the reader expects", %{vectors: vectors} do
      assert {:ok, [batch | _]} = Latu.to_arrow(vectors)
      assert {:ok, [{"v", :struct}]} = Latu.Result.Arrow.schema(batch)

      assert {:ok, [%{columns: [column]}]} = Latu.Result.Arrow.read(batch)
      assert Enum.map(column.children, & &1.name) == ["type", "size", "indices", "values"]

      values = Enum.find(column.children, &(&1.name == "values"))
      assert [item] = values.children
      assert item.type == {:float, 64}
    end

    test "columns: prunes what the server sent", %{session: session} do
      df = Latu.sql!(session, "SELECT CAST(id AS DOUBLE) AS a, id AS b FROM RANGE(3)")

      assert {:ok, both} = Latu.to_nx(df)
      assert Map.keys(both) |> Enum.sort() == ["a", "b"]

      assert {:ok, one} = Latu.to_nx(df, columns: ["b"])
      assert Map.keys(one) == ["b"]
      assert Nx.to_flat_list(one["b"]) == [0, 1, 2]
    end

    test "stream_nx yields a map per batch", %{arrays: arrays} do
      tensors = arrays |> Latu.stream_nx() |> Enum.to_list()

      assert length(tensors) >= 1
      assert Enum.all?(tensors, &match?(%{"a" => _}, &1))

      rows = tensors |> Enum.map(&elem(Nx.shape(&1["a"]), 0)) |> Enum.sum()
      assert rows == 3
    end

    test "a string column is refused, naming the column", %{session: session} do
      df = Latu.sql!(session, "SELECT 'a' AS s")

      assert {:error, error} = Latu.to_nx(df)
      assert error.message =~ "column s is a string column"
    end
  end
end
