defmodule Latu.Result.ArrowTest do
  use ExUnit.Case, async: true

  # The fixtures are written by pyarrow (`dev/make_arrow_fixtures.py`), so a pass here means the
  # reader agrees with Arrow's own encoder rather than with itself. Nothing reaches a server:
  # these are bytes.

  alias Latu.Result.Arrow

  @dir Path.expand("../../arrow", __DIR__)

  defp stream(name), do: File.read!(Path.join(@dir, "#{name}.arrow"))

  defp column(name, wanted) do
    {:ok, [batch | _]} = Arrow.read(stream(name))
    Enum.find(batch.columns, &(&1.name == wanted))
  end

  defp doubles(binary), do: for(<<v::little-float-64 <- binary>>, do: v)
  defp i64s(binary), do: for(<<v::little-signed-64 <- binary>>, do: v)
  defp i32s(binary), do: for(<<v::little-signed-32 <- binary>>, do: v)

  describe "primitives" do
    test "a double column is its own buffer, and no validity bitmap where there are no nulls" do
      c = column("doubles", "v")

      assert c.type == {:float, 64}
      assert c.length == 4
      assert c.null_count == 0
      assert [nil, data] = c.buffers
      assert doubles(data) == [1.5, 2.5, 0.0, -3.25]
    end

    test "integer width and signedness come off the schema, not off the buffer size" do
      assert column("int64s", "v").type == {:int, 64, :signed}
      assert column("int32s", "v").type == {:int, 32, :signed}
      assert column("float32s", "v").type == {:float, 32}
    end

    test "columns keep the schema's order and are read independently" do
      {:ok, [batch]} = Arrow.read(stream("two_columns"))

      assert Enum.map(batch.columns, & &1.name) == ["a", "b"]
      assert batch.rows == 3
      assert [_, b] = batch.columns
      assert i64s(Enum.at(b.buffers, 1)) == [10, 20, 30]
    end
  end

  describe "nesting" do
    test "a list column is offsets plus one child holding every value" do
      c = column("list_uniform", "v")

      assert c.type == :list
      assert c.length == 3
      assert [_, offsets] = c.buffers
      assert i32s(offsets) == [0, 2, 4, 6]

      assert [item] = c.children
      assert item.type == {:float, 64}
      assert item.length == 6
      assert doubles(Enum.at(item.buffers, 1)) == [1.5, 2.5, 0.5, 3.5, 0.0, 0.0]
    end

    # This is the shape the whole of `to_nx/2` turns on: Spark's proto schema says a `features`
    # column is a UDT and refuses to say more, while Arrow's own schema spells it out.
    test "a Vector column is a struct of type, size, indices and values" do
      c = column("vector_dense", "features")

      assert c.type == :struct
      assert Enum.map(c.children, & &1.name) == ["type", "size", "indices", "values"]

      kinds = Enum.find(c.children, &(&1.name == "type"))
      assert :binary.bin_to_list(Enum.at(kinds.buffers, 1)) == [1, 1, 1, 1]

      values = Enum.find(c.children, &(&1.name == "values"))
      assert i32s(Enum.at(values.buffers, 1)) == [0, 2, 4, 6, 8]
      assert [item] = values.children
      assert doubles(Enum.at(item.buffers, 1)) == [1.5, 2.5, 0.5, 3.5, 0.0, 0.0, 4.0, 4.5]
    end

    test "a sparse Vector says so in its type byte, which is the discriminator" do
      c = column("vector_sparse", "features")
      kinds = Enum.find(c.children, &(&1.name == "type"))

      assert :binary.bin_to_list(Enum.at(kinds.buffers, 1)) == [0, 0]
    end
  end

  describe "what the reader reports rather than interprets" do
    test "nulls arrive as a count and a validity bitmap" do
      c = column("doubles_with_null", "v")

      assert c.null_count == 1
      assert Enum.at(c.buffers, 0) != nil
    end

    test "a string column has three buffers" do
      c = column("strings", "v")

      assert c.type == :utf8
      assert length(c.buffers) == 3
    end

    test "booleans are a bitmap: three rows in one byte" do
      c = column("booleans", "v")

      assert c.type == :bool
      assert byte_size(Enum.at(c.buffers, 1)) == 1
    end
  end

  describe "edges" do
    test "a stream with a schema and no batch reads as no batches, and still has a schema" do
      assert {:ok, []} = Arrow.read(stream("empty"))
      assert {:ok, [{"v", {:float, 64}}]} = Arrow.schema(stream("empty"))
    end

    test "malformed input is refused rather than raised" do
      assert {:error, message} = Arrow.read("")
      assert message =~ "no Arrow messages"

      assert {:error, message} = Arrow.read("hello there, not arrow at all")
      assert message =~ "not an Arrow IPC stream"

      truncated = binary_part(stream("doubles"), 0, 40)
      assert {:error, _} = Arrow.read(truncated)
    end
  end
end
