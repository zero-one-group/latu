defmodule Latu.Functions.Docs do
  @moduledoc false

  # Spark documents itself: `DESCRIBE FUNCTION EXTENDED <name>` returns usage and examples for
  # every builtin. `dev/harvest_docs.py` runs that over a Connect session and writes
  # `priv/function_docs.exs`; this module turns one entry into an `@doc`. See dev/README.md.
  #
  # The file is checked in, and compiling refuses to go on without it: a silent fallback would
  # ship one-line docstrings and no error.

  @path Path.expand("../../../priv/function_docs.exs", __DIR__)
  @external_resource @path

  if not File.exists?(@path) do
    raise "priv/function_docs.exs is missing. It is checked in; `python dev/harvest_docs.py` " <>
            "regenerates it (dev/README.md)."
  end

  @harvested @path |> Code.eval_file() |> elem(0)

  @doc "How many wire names the harvest covers. `test/latu/functions_test.exs` holds the floor."
  def harvested, do: map_size(@harvested)

  @doc """
  The `@doc` for one wire name, falling back to a one-liner when the harvest has no entry.

  Called from a module body at compile time, so it lives here rather than in `Latu.Functions`.
  """
  def doc_for(wire, aside \\ nil) do
    entry = @harvested[wire]

    [header(wire, aside), section(entry, "usage"), examples(entry), note(entry)]
    |> Enum.reject(&is_nil/1)
    |> Enum.join("\n\n")
  end

  defp header(wire, nil), do: "Spark's `#{wire}`."
  defp header(wire, aside), do: "Spark's `#{wire}`. #{aside}"

  # A pre-`note` harvest has no such key, so every lookup tolerates a missing one.
  defp section(entry, key) when is_map(entry) do
    case Map.get(entry, key) do
      text when is_binary(text) and text != "" -> text
      _ -> nil
    end
  end

  defp section(_entry, _key), do: nil

  defp examples(entry) do
    case section(entry, "examples") do
      nil -> nil
      text -> "## Examples (Spark SQL)\n\n```sql\n#{text}\n```"
    end
  end

  # Prose, so it must stay outside the fence — Spark's own ordering puts it last.
  defp note(entry) do
    case section(entry, "note") do
      nil -> nil
      text -> "> **Note:** " <> String.replace(text, "\n", "\n> ")
    end
  end
end
