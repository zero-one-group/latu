defmodule Latu.FunctionSweepTest do
  use ExUnit.Case, async: true

  # `dev/function_args.exs` is the measured record of every wrapped function resolving against a
  # live server. This test needs no server: it asserts the record still describes the
  # surface, so a Spark bump that adds or removes a function fails here — offline, in a second —
  # rather than waiting for someone to remember the probe.
  #
  # When it fails: docker compose up -d spark-connect && mix run dev/probe_functions.exs --write

  @record "dev/function_args.exs"
  @external_resource @record

  @sweep (case File.read(@record) do
            {:ok, contents} -> contents |> Code.eval_string() |> elem(0)
            {:error, _reason} -> nil
          end)

  setup do
    assert @sweep, "#{@record} is missing — run mix run dev/probe_functions.exs --write"

    exported = MapSet.new(Latu.Functions.registered() ++ Latu.Functions.handwritten())

    %{exported: exported}
  end

  test "every exported function is either swept or excluded", %{exported: exported} do
    accounted = MapSet.union(swept(), excluded(exported))

    assert exported |> MapSet.difference(accounted) |> Enum.sort() == []
  end

  test "and nothing is swept that is no longer exported", %{exported: exported} do
    assert swept() |> MapSet.difference(exported) |> Enum.sort() == []
  end

  test "every exclusion says why", %{exported: _} do
    for {key, reason} <- @sweep.excluded do
      assert is_binary(reason) and String.length(reason) > 20,
             "#{inspect(key)} is excluded without saying why"
    end
  end

  defp swept do
    MapSet.new(@sweep.resolved, fn {name, arity, _profile, _over?} -> {name, arity} end)
  end

  defp excluded(exported) do
    for {key, _reason} <- @sweep.excluded,
        pair <- pairs(key, exported),
        into: MapSet.new(),
        do: pair
  end

  # An exclusion key is a name — every arity of it — or a {name, arity} for just the one.
  defp pairs({name, arity}, _exported), do: [{name, arity}]

  defp pairs(name, exported) when is_atom(name) do
    for {^name, arity} <- exported, do: {name, arity}
  end
end
