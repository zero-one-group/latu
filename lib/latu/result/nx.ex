# Tensors from Arrow bytes, behind an optional dependency: without :nx this file compiles to
# nothing and a caller who never wanted tensors pays nothing for it. Same shape as
# `lib/latu/kino.ex`.
if Code.ensure_loaded?(Nx) do
  defmodule Latu.Result.Nx do
    @moduledoc """
    `Latu.Result.Arrow`'s buffers as `Nx` tensors.

    Two shapes, and nothing else:

      * a **numeric column with no nulls** becomes a 1-D tensor of its own type. The Arrow
        buffer is already the tensor's binary, so a single batch costs no copy at all.
      * a **column of equal-length numeric lists**, and a **column of dense `Vector`s**, become
        one `{rows, width}` tensor. Both are one contiguous buffer in Arrow with an offsets
        buffer beside it, so this is a check that the offsets are regular and then a reshape.

    Everything else is refused by name, because a tensor has one type and one shape and there
    is no honest default for a column that has neither: nulls, strings, booleans (Arrow packs
    them as a bitmap, not a byte per value), ragged lists, sparse vectors, and anything nested
    beyond the two shapes above.

    A `Vector` column is the reason this exists. Spark describes it as a UDT with no SQL type,
    so `Latu.collect/2` and `Latu.to_explorer/2` both refuse it (`docs/deviations.md`) — but
    the Arrow stream carries its own schema, and in there it is an ordinary struct whose
    `values` child is a list of doubles.
    """

    alias Latu.Result.Arrow

    @vector_udt ["type", "size", "indices", "values"]

    @typedoc "Every requested column, keyed by name."
    @type tensors :: %{String.t() => Nx.Tensor.t()}

    @doc """
    Decode Arrow IPC streams — `Latu.to_arrow/2`'s batches — into one tensor per column.

    Options: `:columns`, a list of names to keep. Pruning **copies**: an Arrow buffer is a
    sub-binary of the whole batch and holds it alive, so keeping one column of a wide result
    without copying would retain every other column's bytes too. Taking the whole batch does
    not copy, because there is nothing left to release.
    """
    @spec decode([binary()], keyword()) :: {:ok, tensors()} | {:error, String.t()}
    def decode(streams, opts \\ []) when is_list(streams) do
      wanted = Keyword.get(opts, :columns)

      with {:ok, batches} <- read_all(streams, []),
           {:ok, by_name} <- group(batches, wanted) do
        by_name
        |> Enum.reduce_while({:ok, %{}}, fn {name, columns}, {:ok, acc} ->
          case tensor(name, columns, wanted != nil) do
            {:ok, tensor} -> {:cont, {:ok, Map.put(acc, name, tensor)}}
            {:error, _} = error -> {:halt, error}
          end
        end)
      end
    end

    defp read_all([], acc), do: {:ok, acc |> Enum.reverse() |> List.flatten()}

    defp read_all([stream | rest], acc) do
      with {:ok, batches} <- Arrow.read(stream), do: read_all(rest, [batches | acc])
    end

    # One entry per column name, holding that column from every batch in order.
    # Spark sends the schema even for an empty result, so no batch at all is a protocol
    # surprise rather than an empty frame — the same reading `Latu.DataFrame` takes.
    defp group([], _wanted), do: {:error, "the server sent no batches at all"}

    defp group(batches, wanted) do
      names = batches |> hd() |> Map.fetch!(:columns) |> Enum.map(& &1.name)
      missing = if wanted, do: wanted -- names, else: []
      repeated = Enum.find(names, &(Enum.count(names, fn n -> n == &1 end) > 1))

      cond do
        Enum.any?(batches, &(Enum.map(&1.columns, fn c -> c.name end) != names)) ->
          {:error, "the batches do not all carry the same columns"}

        # Tensors are keyed by name, so the second column would vanish without a word.
        repeated ->
          {:error,
           "the result has more than one column named #{repeated}, and tensors are keyed by " <>
             "name; alias them apart in select/2, or rename/2 every column"}

        missing != [] ->
          {:error,
           "no column named #{Enum.join(missing, ", ")}; " <>
             "the result has #{Enum.join(names, ", ")}"}

        true ->
          keep = wanted || names

          {:ok,
           Enum.map(keep, fn name ->
             {name, Enum.map(batches, fn b -> Enum.find(b.columns, &(&1.name == name)) end)}
           end)}
      end
    end

    # =============================================
    # One column, across every batch
    # =============================================

    defp tensor(name, columns, pruned?) do
      with {:ok, pieces} <- Enum.reduce_while(columns, {:ok, []}, &piece(name, &1, &2)) do
        build(name, pieces, pruned?)
      end
    end

    defp piece(name, column, {:ok, acc}) do
      case shape_of(name, column) do
        {:ok, piece} -> {:cont, {:ok, [piece | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end

    # `{type, width_or_nil, rows, binary}` — width nil for a 1-D column.
    defp shape_of(name, %{type: :struct, children: children} = column) do
      case Enum.map(children, & &1.name) do
        @vector_udt -> vector(name, column, children)
        _ -> {:error, "column #{name} is a struct, and to_nx/2 reads numbers, lists and Vectors"}
      end
    end

    defp shape_of(name, %{type: :list} = column), do: list(name, column)

    defp shape_of(name, %{type: type} = column) do
      case nx_type(type) do
        {:ok, nx} ->
          with :ok <- no_nulls(name, column) do
            {:ok, {nx, nil, column.length, data(column)}}
          end

        :error ->
          {:error, "column #{name} is #{describe(type)}"}
      end
    end

    # A dense Vector is the `values` child and nothing else: the type byte is 1 for every row,
    # and `size` and `indices` are null throughout. A sparse one has no fixed width to reshape.
    defp vector(name, column, children) do
      kinds = children |> Enum.find(&(&1.name == "type")) |> data()

      cond do
        column.null_count > 0 ->
          {:error, "column #{name} has #{column.null_count} null Vector(s); to_nx/2 reads none"}

        kinds == nil or :binary.bin_to_list(kinds) |> Enum.any?(&(&1 != 1)) ->
          {:error,
           "column #{name} holds sparse Vectors, which have no one width to reshape; " <>
             "densify them server-side first"}

        true ->
          children |> Enum.find(&(&1.name == "values")) |> then(&list(name, &1))
      end
    end

    # A list column is `{validity, offsets}` plus one child. Regular offsets — 0, d, 2d, … —
    # are what make the child's buffer an `{n, d}` tensor rather than n separate rows.
    defp list(name, %{children: [child]} = column) do
      with :ok <- no_nulls(name, column),
           :ok <- no_nulls(name, child),
           {:ok, nx} <- child_type(name, child),
           {:ok, width} <- width(name, column, child) do
        {:ok, {nx, width, column.length, data(child)}}
      end
    end

    defp list(name, _column) do
      {:error, "column #{name} is a list of something to_nx/2 does not read"}
    end

    defp child_type(name, child) do
      case nx_type(child.type) do
        {:ok, nx} -> {:ok, nx}
        :error -> {:error, "column #{name} holds #{describe(child.type)}, not numbers"}
      end
    end

    defp width(name, column, child) do
      offsets = Enum.at(column.buffers, 1)

      cond do
        column.length == 0 ->
          {:ok, 0}

        offsets == nil ->
          {:error, "column #{name} carries no offsets"}

        true ->
          stride = div(child.length, column.length)

          if child.length == stride * column.length and regular?(offsets, 0, stride) do
            {:ok, stride}
          else
            {:error,
             "column #{name} has rows of different length; a tensor needs one width, so " <>
               "pad or filter them first"}
          end
      end
    end

    defp regular?(<<>>, _expected, _stride), do: true

    defp regular?(<<value::little-signed-32, rest::binary>>, expected, stride)
         when value == expected do
      regular?(rest, expected + stride, stride)
    end

    defp regular?(_other, _expected, _stride), do: false

    defp no_nulls(_name, %{null_count: 0}), do: :ok

    defp no_nulls(name, %{null_count: count}) do
      {:error,
       "column #{name} has #{count} null(s), and a tensor has no null; drop or fill them first"}
    end

    # The data buffer is the second, for every type that has one.
    defp data(%{buffers: buffers}), do: Enum.at(buffers, 1)

    # =============================================
    # Building it
    # =============================================

    defp build(name, pieces, pruned?) do
      {types, widths, rows, binaries} = unzip(pieces)

      cond do
        Enum.uniq(types) |> length() > 1 ->
          {:error, "column #{name} is not the same type in every batch"}

        Enum.uniq(widths) |> length() > 1 ->
          widths = Enum.uniq(widths) |> Enum.map_join(" then ", &inspect/1)
          {:error, "column #{name} is #{widths} wide in different batches"}

        true ->
          type = hd(types)
          width = hd(widths)
          total = Enum.sum(rows)
          binary = flatten(binaries, pruned?)

          shape = if width, do: {total, width}, else: {total}
          {:ok, binary |> Nx.from_binary(type) |> Nx.reshape(shape)}
      end
    end

    # `piece/3` prepends, so `pieces` is newest-first and the fold's own prepending is the one
    # reversal that puts `binaries` back in batch order — `flatten/2` concatenates blind.
    defp unzip(pieces) do
      Enum.reduce(pieces, {[], [], [], []}, fn {t, w, r, b}, {ts, ws, rs, bs} ->
        {[t | ts], [w | ws], [r | rs], [b | bs]}
      end)
    end

    # A missing data buffer is a zero-row column, which is a legal empty tensor.
    defp flatten(binaries, pruned?) do
      case Enum.map(binaries, &(&1 || "")) do
        [one] -> if pruned?, do: :binary.copy(one), else: one
        many -> IO.iodata_to_binary(many)
      end
    end

    defp nx_type({:float, 16}), do: {:ok, {:f, 16}}
    defp nx_type({:float, 32}), do: {:ok, {:f, 32}}
    defp nx_type({:float, 64}), do: {:ok, {:f, 64}}
    defp nx_type({:int, width, :signed}) when width in [8, 16, 32, 64], do: {:ok, {:s, width}}
    defp nx_type({:int, width, :unsigned}) when width in [8, 16, 32, 64], do: {:ok, {:u, width}}
    defp nx_type(_other), do: :error

    defp describe(:bool) do
      "boolean, which Arrow packs as a bitmap rather than a byte a value; cast it to an " <>
        "integer first"
    end

    defp describe(:utf8), do: "a string column, and to_nx/2 reads numbers"
    defp describe(:null), do: "all null, and a tensor has no null"
    defp describe(:list), do: "a nested list, and to_nx/2 reads one level"
    defp describe(:struct), do: "a struct that is not a Vector"
    defp describe({:int, width, sign}), do: "#{sign} #{width}-bit, which Nx has no type for"

    defp describe({:float, width}) do
      "#{width}-bit floating point, which Nx has no type for"
    end

    defp describe({:other, tag}), do: "an Arrow type Latu does not read (##{tag})"
  end
end
