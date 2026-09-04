defmodule Latu.Functions.Groups do
  @moduledoc false

  # Spark's own category for each function. `Latu.Functions` is 671 name/arity pairs, and one
  # flat list of them is not a reference anyone can use; the registry carries no category, so
  # `dev/harvest_function_groups.py` takes one from PySpark's API reference at the tag Latu
  # targets and writes `priv/function_groups.exs`. See dev/README.md.
  #
  # Read by `mix.exs`'s `:default_group_for_doc` at docs time rather than emitted as
  # `@doc group:` in the generator: ExDoc hands that function the `:module`, `:name` and
  # `:arity` of every entry, so one config function does what fifty attributes would.
  #
  # The file is checked in, and compiling refuses to go on without it: a silent fallback would
  # render the whole reference ungrouped.

  @path Path.expand("../../../priv/function_groups.exs", __DIR__)
  @external_resource @path

  if not File.exists?(@path) do
    raise "priv/function_groups.exs is missing. It is checked in; " <>
            "`python dev/harvest_function_groups.py --write` regenerates it (dev/README.md)."
  end

  @harvested @path |> Code.eval_file() |> elem(0)

  @doc "How many names the harvest covers. `test/latu/functions_test.exs` holds the floor."
  def harvested, do: map_size(@harvested)

  @doc "Spark's category for one of Latu's function names, or nil when the harvest has none."
  def of(name), do: @harvested[Kernel.to_string(name)]
end
