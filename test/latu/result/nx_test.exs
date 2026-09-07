defmodule Latu.Result.NxTest do
  use ExUnit.Case, async: true

  # Same fixtures as `Latu.Result.ArrowTest`, one layer up: which bytes become which tensor, and
  # which columns are refused with a message that names the fix. No server.

  alias Latu.Result.Nx, as: Tensors

  @dir Path.expand("../../arrow", __DIR__)

  defp stream(name), do: File.read!(Path.join(@dir, "#{name}.arrow"))

  describe "the shapes that decode" do
    test "a numeric column is a 1-D tensor of its own type" do
      assert {:ok, %{"v" => t}} = Tensors.decode([stream("doubles")])

      assert Nx.type(t) == {:f, 64}
      assert Nx.shape(t) == {4}
      assert Nx.to_flat_list(t) == [1.5, 2.5, 0.0, -3.25]
    end

    test "integers keep their width" do
      assert {:ok, %{"v" => t}} = Tensors.decode([stream("int64s")])

      assert Nx.type(t) == {:s, 64}
      assert Nx.to_flat_list(t) == [1, -2, 3, 4]
    end

    test "equal-length lists become one {rows, width} tensor" do
      assert {:ok, %{"v" => t}} = Tensors.decode([stream("list_uniform")])

      assert Nx.shape(t) == {3, 2}
      assert Nx.to_flat_list(t) == [1.5, 2.5, 0.5, 3.5, 0.0, 0.0]
    end

    # The reason the verb exists: `collect/2` and `to_explorer/2` both refuse this column.
    test "a dense Vector column becomes one {rows, width} tensor" do
      assert {:ok, %{"features" => t}} = Tensors.decode([stream("vector_dense")])

      assert Nx.type(t) == {:f, 64}
      assert Nx.shape(t) == {4, 2}
      assert Nx.to_flat_list(t) == [1.5, 2.5, 0.5, 3.5, 0.0, 0.0, 4.0, 4.5]
    end

    test "several batches concatenate, in order" do
      one = stream("doubles")

      assert {:ok, %{"v" => t}} = Tensors.decode([one, one])
      assert Nx.shape(t) == {8}
      assert Nx.to_flat_list(t) == [1.5, 2.5, 0.0, -3.25, 1.5, 2.5, 0.0, -3.25]

      wide = stream("vector_dense")
      assert {:ok, %{"features" => f}} = Tensors.decode([wide, wide])
      assert Nx.shape(f) == {8, 2}
    end
  end

  describe "columns:" do
    test "prunes, and names what is not there" do
      assert {:ok, both} = Tensors.decode([stream("two_columns")])
      assert Map.keys(both) |> Enum.sort() == ["a", "b"]

      assert {:ok, one} = Tensors.decode([stream("two_columns")], columns: ["b"])
      assert Map.keys(one) == ["b"]
      assert Nx.to_flat_list(one["b"]) == [10, 20, 30]

      assert {:error, message} = Tensors.decode([stream("two_columns")], columns: ["nope"])
      assert message =~ "no column named nope"
      assert message =~ "the result has a, b"
    end

    # An Arrow buffer is a slice of the whole batch and holds it alive, so pruning has to copy
    # or it would keep every column it was asked to drop. Needs a batch over 64 bytes: below
    # that the BEAM copies into the process heap however the buffer was made.
    test "pruning releases the rest of the batch, and taking everything does not copy" do
      whole = stream("big_two")

      assert {:ok, all} = Tensors.decode([whole])
      assert {:ok, only_b} = Tensors.decode([whole], columns: ["b"])

      shared = Nx.to_binary(all["b"])
      copied = Nx.to_binary(only_b["b"])

      assert byte_size(shared) == byte_size(copied)
      assert :binary.referenced_byte_size(shared) == byte_size(whole)
      assert :binary.referenced_byte_size(copied) == byte_size(copied)
    end
  end

  describe "refusals name the fix" do
    test "nulls" do
      assert {:error, message} = Tensors.decode([stream("doubles_with_null")])
      assert message =~ "column v has 1 null"
      assert message =~ "drop or fill"
    end

    test "strings" do
      assert {:error, message} = Tensors.decode([stream("strings")])
      assert message =~ "column v is a string column"
    end

    test "booleans, which Arrow packs as a bitmap" do
      assert {:error, message} = Tensors.decode([stream("booleans")])
      assert message =~ "bitmap"
      assert message =~ "cast it to an integer"
    end

    test "ragged lists" do
      assert {:error, message} = Tensors.decode([stream("list_ragged")])
      assert message =~ "rows of different length"
    end

    test "sparse Vectors" do
      assert {:error, message} = Tensors.decode([stream("vector_sparse")])
      assert message =~ "column features holds sparse Vectors"
      assert message =~ "densify"
    end

    test "a stream carrying no batch at all" do
      assert {:error, message} = Tensors.decode([stream("empty")])
      assert message =~ "no batches at all"
    end
  end
end
