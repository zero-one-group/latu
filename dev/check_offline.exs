# Compile as much of Latu as this container can, and check the stand-ins have not drifted.
#
#     elixir dev/check_offline.exs
#
# The offline container has Elixir 1.14 and no hex, so `:protobuf`, `:grpc` and `:explorer` are
# unavailable and `mix` cannot run. Three modules are therefore replaced by `dev/standins/`, and
# everything else in `lib/` is compiled for real with warnings as errors.
#
# This is a pre-flight, not a gate. `mix check.all` on a real toolchain is the authority.

# Real modules replaced by a stand-in, and why each cannot compile here.
stubbed = %{
  "lib/latu/plan.ex" => "dev/standins/plan.ex",
  "lib/latu/plan/inspect.ex" => "dev/standins/plan.ex",
  "lib/latu/client.ex" => "dev/standins/client.ex",
  "lib/latu/client/execution.ex" => "dev/standins/client.ex",
  "lib/latu/result.ex" => "dev/standins/result.ex"
}

# Every `defimpl` in lib/, so a new one cannot appear without someone deciding whether the
# stand-ins need it too. Update this list deliberately.
known_impls = [
  {"Inspect", "Latu.Session"},
  {"Inspect", "Latu.Protocol.Spark.Connect.Expression"},
  {"Inspect", "Latu.Protocol.Spark.Connect.Expression.SortOrder"},
  {"Inspect", "Latu.GroupedData"},
  {"Inspect", "Latu.Subquery"},
  {"Inspect", "Latu.DataFrame"},
  # Behind `Code.ensure_loaded?(Kino.Render)`, so it is indented and never compiles here —
  # :kino is not a dependency this container can fetch. Listed so it is still accounted for.
  {"Kino.Render", "Latu.DataFrame"}
]

# A stand-in type must carry the protocol implementations its real counterpart does. Module
# names rather than struct literals, because none of these exist until the compile below.
mirrored_impls = [
  {Inspect, Latu.Plan.Expression, "Latu.Protocol.Spark.Connect.Expression"},
  {Inspect, Latu.Plan.SortOrder, "Latu.Protocol.Spark.Connect.Expression.SortOrder"}
]

defmodule Grouping do
  @moduledoc false
  # Elixir warns — and under --warnings-as-errors fails — when clauses of the same name and
  # arity are not contiguous. `client.ex`, `plan.ex`, `plan/inspect.ex` and `result.ex` are
  # stand-in-replaced below and therefore never compiled here, so that whole class of error
  # has no other check here. This needs no compiler: it reads the AST.

  def ungrouped(path) do
    {:ok, ast} = path |> File.read!() |> Code.string_to_quoted()

    ast |> bodies() |> Enum.flat_map(&runs(&1, path))
  end

  # Each module-ish body is its own scope: a `def` inside a `defimpl` does not group with one
  # outside it.
  defp bodies({form, _meta, args}) when form in [:defmodule, :defimpl, :defprotocol] do
    case List.last(args) do
      [do: {:__block__, _, children}] -> [children | Enum.flat_map(children, &bodies/1)]
      [do: single] -> [[single]]
      _other -> []
    end
  end

  defp bodies({_form, _meta, args}) when is_list(args), do: Enum.flat_map(args, &bodies/1)
  defp bodies(list) when is_list(list), do: Enum.flat_map(list, &bodies/1)
  defp bodies({left, right}), do: bodies(left) ++ bodies(right)
  defp bodies(_other), do: []

  defp runs(children, path) do
    children
    |> Enum.flat_map(&signature/1)
    |> Enum.chunk_by(& &1)
    |> Enum.map(&hd/1)
    |> Enum.frequencies()
    |> Enum.filter(fn {_signature, runs} -> runs > 1 end)
    |> Enum.map(fn {{name, arity}, runs} ->
      "#{path}: #{name}/#{arity} is defined in #{runs} separate runs of clauses"
    end)
  end

  defp signature({form, _meta, [head | _]}) when form in [:def, :defp] do
    case unguard(head) do
      {name, _meta, args} when is_atom(name) and is_list(args) -> [{name, length(args)}]
      _other -> []
    end
  end

  defp signature(_other), do: []

  defp unguard({:when, _meta, [head | _]}), do: head
  defp unguard(head), do: head
end

defmodule Check do
  @moduledoc false

  def fail(message) do
    IO.puts(:stderr, "FAIL  " <> message)
    Process.put(:failed, true)
  end

  def ok(message), do: IO.puts("ok    " <> message)

  def halt! do
    if Process.get(:failed), do: System.halt(1), else: IO.puts("\nall offline checks passed")
  end

  @doc """
  Every `{name, arity}` a source file defines with `def`, defaults expanded.

  `defimpl` blocks are pruned: a `def inspect/2` inside one belongs to the protocol
  implementation, not to the module the file is named for.
  """
  def exports(path) do
    {:ok, ast} = path |> File.read!() |> Code.string_to_quoted()

    ast |> defs() |> Enum.uniq() |> Enum.sort()
  end

  defp defs({:defimpl, _meta, _args}), do: []
  defp defs({:def, _meta, [head | _]}), do: signature(head)
  defp defs({_form, _meta, args}) when is_list(args), do: Enum.flat_map(args, &defs/1)
  defp defs({left, right}), do: defs(left) ++ defs(right)
  defp defs(list) when is_list(list), do: Enum.flat_map(list, &defs/1)
  defp defs(_other), do: []

  defp signature({:when, _meta, [head | _]}), do: signature(head)

  defp signature({name, _meta, args}) when is_atom(name) and is_list(args) do
    optional = Enum.count(args, &match?({:\\, _, _}, &1))

    for arity <- (length(args) - optional)..length(args), do: {name, arity}
  end

  defp signature({name, _meta, nil}) when is_atom(name), do: [{name, 0}]
  defp signature(_other), do: []
end

# `lib/latu/session.ex` reads the package version from Mix.Project at compile time, so Mix has
# to be running even though this is not a Mix task. The version itself does not matter here.
Mix.start()

defmodule OfflineProject do
  @moduledoc false
  def project, do: [app: :latu, version: "0.0.0-offline"]
end

Mix.Project.push(OfflineProject)

sources =
  "lib/**/*.ex"
  |> Path.wildcard()
  |> Enum.reject(&String.starts_with?(&1, "lib/latu/protocol/"))

{real, replaced} = Enum.split_with(sources, &(not Map.has_key?(stubbed, &1)))

# Dependencies this container cannot fetch, rather than layers it cannot compile. Only what a
# module body evaluates at compile time needs one; `:grpc` and `:explorer` are reached solely
# through the stubbed modules above.
dependency_stand_ins = [
  "dev/standins/decimal.ex",
  "dev/standins/proto_types.ex",
  "dev/standins/telemetry.ex"
]

stand_ins = ((stubbed |> Map.values()) ++ dependency_stand_ins) |> Enum.uniq() |> Enum.sort()
output = Path.join(System.tmp_dir!(), "latu-offline-#{System.unique_integer([:positive])}")
File.mkdir_p!(output)

Code.put_compiler_option(:warnings_as_errors, true)

case Kernel.ParallelCompiler.compile_to_path(stand_ins ++ real, output) do
  {:ok, modules, _warnings} ->
    Check.ok(
      "compiled #{length(modules)} modules — " <>
        "#{length(real)} real, #{length(stand_ins)} stand-in"
    )

  {:error, _errors, _warnings} ->
    Check.fail("compilation")
    System.halt(1)
end

Enum.each(replaced, fn source ->
  stand_in = Map.fetch!(stubbed, source)
  parts = source |> Path.rootname() |> Path.split() |> Enum.drop(2) |> Enum.map(&Macro.camelize/1)
  module = Module.concat(["Latu" | parts])

  wanted = Check.exports(source)
  have = if Code.ensure_loaded?(module), do: module.__info__(:functions), else: []
  missing = wanted -- have

  cond do
    source =~ "execution" ->
      Check.ok("#{source} is folded into #{stand_in}")

    missing == [] ->
      Check.ok("#{stand_in} covers every export of #{source} (#{length(wanted)})")

    true ->
      Check.fail("#{stand_in} is missing #{inspect(missing)} — #{source} has drifted ahead of it")
  end
end)

# Every file, not only the ones that compiled: this is the one structural check the
# stand-in-replaced modules get.
grouped = stand_ins ++ real ++ replaced

case Enum.flat_map(grouped, &Grouping.ungrouped/1) do
  [] ->
    Check.ok("clauses of the same name and arity are grouped, in all #{length(grouped)} files")

  problems ->
    Enum.each(problems, &Check.fail/1)
end

found_impls =
  "lib/**/*.ex"
  |> Path.wildcard()
  |> Enum.reject(&String.starts_with?(&1, "lib/latu/protocol/"))
  |> Enum.flat_map(fn path ->
    Regex.scan(~r/^\s*defimpl\s+([\w.]+),\s*for:\s*([\w.]+)/m, File.read!(path))
    |> Enum.map(fn [_line, protocol, type] -> {protocol, type} end)
  end)

new_impls = Enum.sort(found_impls) -- Enum.sort(known_impls)
gone_impls = Enum.sort(known_impls) -- Enum.sort(found_impls)

case {new_impls, gone_impls} do
  {[], []} -> Check.ok("the #{length(known_impls)} protocol impls in lib/ are the expected ones")
  {new, []} -> Check.fail("new protocol impls #{inspect(new)} — do the stand-ins need them too?")
  {_, gone} -> Check.fail("protocol impls #{inspect(gone)} are gone; update this script")
end

# `impl_for` alone would be vacuous: Elixir derives a fallback implementation for every struct,
# so it never returns nil. What matters is a *dedicated* one — the fallback prints the fields,
# which is the opposite of the property being mirrored. Both halves are checked, and both were
# confirmed to fail when the impl is removed.
Enum.each(mirrored_impls, fn {protocol, module, real_type} ->
  value = struct!(module)
  impl = protocol.impl_for(value)
  fallback = Module.concat(protocol, Any)
  fields = value |> Map.from_struct() |> Map.keys() |> Enum.map(&to_string/1)
  rendered = Kernel.inspect(value)

  cond do
    impl in [nil, fallback] ->
      Check.fail(
        "#{inspect(module)} has no #{inspect(protocol)} impl of its own; #{real_type} has one"
      )

    Enum.any?(fields, &String.contains?(rendered, &1)) ->
      Check.fail("#{inspect(module)} leaks its fields through #{inspect(protocol)}: #{rendered}")

    true ->
      Check.ok("#{inspect(module)} implements #{inspect(protocol)} itself, as #{real_type} does")
  end
end)

# A pipeline through the facade, so the layers are exercised rather than merely compiled.
#
# Evaluated from a string on purpose: this script is compiled in full before any of it runs, so
# an `import Latu.Column` up here would be resolved before the modules above exist.
smoke = """
alias Latu.Functions, as: F
alias Latu.Window, as: W
import Latu.Column

session = Latu.Session.from_url!("sc://localhost:15002")

session
|> Latu.range(10)
|> Latu.filter(all([greater(:id, 2), not_equal(:id, 5)]))
|> Latu.with_columns(bucket: remainder(:id, 3))
|> Latu.with_columns(rn: over(F.row_number(), W.partition_by([:bucket])))
|> Latu.select([:id, doubled: multiply(:id, 2), label: F.when_(greater(:id, 5), "big")])
|> Latu.group_by(:bucket)
|> Latu.agg(n: F.count_distinct(:id), xs: F.transform(F.array([1, 2]), fn x -> add(x, 1) end))
"""

case Code.eval_string(smoke) do
  {frame, _bindings} when is_struct(frame, Latu.DataFrame) ->
    Check.ok("a pipeline through the facade builds")

  {other, _bindings} ->
    Check.fail("the facade pipeline returned #{inspect(other)}")
end

# Every function is either a registry row or a declared hand-written one. The test suite asserts
# this too, but that needs a real toolchain; here it costs nothing and catches a forgotten
# `handwritten/0` entry before it becomes a round trip.
surface = """
alias Latu.Functions, as: F

exported =
  F.__info__(:functions)
  |> Enum.reject(&(&1 in [registered: 0, wire_name: 1, handwritten: 0]))
  |> Enum.sort()

{Kernel.length(exported), Enum.sort(F.registered() ++ F.handwritten()) -- exported,
 exported -- Enum.sort(F.registered() ++ F.handwritten())}
"""

case Code.eval_string(surface) do
  {{count, [], []}, _bindings} ->
    Check.ok("all #{count} functions are declared: registry + hand-written == exports")

  {{_count, declared, exported}, _bindings} ->
    Check.fail(
      "declared but absent #{inspect(declared)}; " <>
        "exported but undeclared #{inspect(exported)}"
    )
end

# `import Latu` beside `import Latu.Column` compiles only while their exports stay disjoint.
# Any shared name is a new collision someone has to resolve before it ships.
overlap =
  MapSet.intersection(
    MapSet.new(Latu.__info__(:functions)),
    MapSet.new(Latu.Column.__info__(:functions))
  )
  |> MapSet.to_list()
  |> Enum.sort()

if overlap == [] do
  Check.ok("Latu and Latu.Column share no export")
else
  Check.fail("Latu and Latu.Column export overlap: #{inspect(overlap)}")
end

File.rm_rf!(output)
Check.halt!()
