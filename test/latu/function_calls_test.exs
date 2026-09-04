defmodule Latu.FunctionCallsTest do
  use ExUnit.Case, async: true

  # Every `F.<name>(...)` call site in the repo, checked against `Latu.Functions`' real exports
  # — name **and** arity.
  #
  # `test/latu/function_sweep_test.exs` proves the registry and the module agree, and
  # `dev/check_offline.exs` proves the module's exports match the registry. Neither looks at
  # the *callers*. Two mistakes have shipped past a name-only grep: a function that did not
  # exist at all, and `F.array/2` where the real one is `array/1` over a list.
  #
  # Reads the AST rather than the text, so the arity is exact.

  @aliases [[:F], [:Latu, :Functions]]

  test "every F.<name>/<arity> call site matches Latu.Functions" do
    exports = MapSet.new(Latu.Functions.__info__(:functions))

    problems =
      for path <- paths(),
          {name, arity, line} <- calls(path),
          not MapSet.member?(exports, {name, arity}) do
        "#{path}:#{line}: F.#{name}/#{arity} — #{arities_of(exports, name)}"
      end

    assert problems == []
  end

  defp paths do
    ("{lib,test,dev}/**/*.{ex,exs}"
     |> Path.wildcard()
     |> Enum.reject(&String.starts_with?(&1, "dev/.venv"))
     |> Enum.reject(&String.starts_with?(&1, "lib/latu/protocol/"))
     |> Enum.reject(&String.contains?(&1, "functions"))) --
      [__ENV__.file |> Path.relative_to_cwd()]
  end

  defp calls(path) do
    {:ok, ast} = path |> File.read!() |> Code.string_to_quoted()
    {_ast, found} = ast |> depipe() |> Macro.prewalk([], &collect/2)

    found
  end

  # `Code.string_to_quoted/1` does not expand pipes, so `x |> F.otherwise(y)` leaves the call
  # node holding ONE argument. Rewriting `|>` first is what makes the arity real — without it
  # every piped call reads one short, a false positive on exactly what this test is for.
  defp depipe(ast) do
    Macro.prewalk(ast, fn
      {:|>, _meta, [left, {call, meta, args}]} when is_list(args) -> {call, meta, [left | args]}
      node -> node
    end)
  end

  defp collect({{:., _, [{:__aliases__, _, parts}, name]}, meta, args} = node, acc)
       when is_atom(name) and is_list(args) do
    if parts in @aliases, do: {node, [{name, length(args), meta[:line]} | acc]}, else: {node, acc}
  end

  defp collect(node, acc), do: {node, acc}

  # NOT `describe/2`: ExUnit.Case exports a macro by that name and the collision is a
  # compile error inside a test body, not a shadowing warning.
  defp arities_of(exports, name) do
    case exports |> Enum.filter(&(elem(&1, 0) == name)) |> Enum.map(&elem(&1, 1)) do
      [] -> "no such function"
      arities -> "the real arities are #{inspect(Enum.sort(arities))}"
    end
  end
end
