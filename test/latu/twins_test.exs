defmodule Latu.TwinsTest do
  use ExUnit.Case, async: true

  # Latu's convention is that an action returning `{:error, _}` has a `!` twin that raises, and
  # a guesser — a person or a model — pays for every place it is not true.
  #
  # So it is a test rather than a convention, in the shape of `registered/0 == exports` and
  # `function_calls_test.exs`: read the specs off the compiled module, not a list someone
  # remembers to update.
  @modules [Latu, Latu.DataFrame, Latu.Catalog, Latu.Session]

  # `@doc false` is outside the convention by definition — it says "not API", and the twins are
  # an API promise. That covers `Latu.to_ddl/2`, which has no caller.
  #
  # One documented exemption:
  #
  #   * `Latu.Session.confirm/3` returns an ok/error tuple but is not an *action* — it is the
  #     pure check `Latu.Client` runs over every response. There is nothing for a `confirm!/3`
  #     to be useful for: the caller that would raise is the transport, and it turns the error
  #     into a `%Latu.Error{}` the caller sees anyway.
  @exempt [{Latu.Session, :confirm, 3}]

  test "every function whose spec can return an error has a ! twin" do
    missing =
      for module <- @modules,
          {{name, arity}, clauses} <- specs(module),
          not bang?(name),
          not hidden?(module, name),
          {module, name, arity} not in @exempt,
          Enum.any?(clauses, &returns_error?/1),
          not function_exported?(module, :"#{name}!", arity),
          do: "#{inspect(module)}.#{name}/#{arity}"

    assert missing == [], """
    These can fail and have no ! twin:

    #{Enum.map_join(missing, "\n", &"  #{&1}")}

    Either add the twin or, if raising makes no sense for it, add it to @exempt with the reason.
    """
  end

  test "and every ! twin has the function it is a twin of" do
    orphans =
      for module <- @modules,
          {{name, arity}, _clauses} <- specs(module),
          bang?(name),
          not function_exported?(module, unbang(name), arity),
          do: "#{inspect(module)}.#{name}/#{arity}"

    assert orphans == []
  end

  # `Code.ensure_loaded!/1` is load-bearing, not defensive: `function_exported?/3` answers false
  # for a module that is merely *compiled*, and neither `fetch_specs/1` nor `fetch_docs/1` loads
  # one. Without it these two tests are **seed-dependent** — whichever ran first paid for
  # loading `Latu.Catalog` and the other passed on its coat-tails. That is how this first went
  # red: nineteen "orphans" that all exist.
  defp specs(module) do
    Code.ensure_loaded!(module)
    {:ok, specs} = Code.Typespec.fetch_specs(module)

    specs
  end

  defp bang?(name), do: String.ends_with?(Atom.to_string(name), "!")

  # A name is outside the convention if any of its arities is `@doc false`. Checked by name
  # rather than by {name, arity} because a function with default arguments has one doc entry,
  # at its widest arity, while its specs may cover several.
  defp hidden?(module, name) do
    {:docs_v1, _, _, _, _, _, docs} = Code.fetch_docs(module)

    Enum.any?(docs, fn
      {{:function, ^name, _arity}, _anno, _sig, :hidden, _meta} -> true
      _entry -> false
    end)
  end

  defp unbang(name), do: name |> Atom.to_string() |> String.trim_trailing("!") |> String.to_atom()

  # A spec's return type, through the erlang abstract format. `:bounded_fun` is a spec with a
  # `when` clause.
  defp returns_error?({:type, _, :bounded_fun, [fun, _constraints]}), do: returns_error?(fun)
  defp returns_error?({:type, _, :fun, [_args, return]}), do: error_tuple?(return)
  defp returns_error?(_clause), do: false

  defp error_tuple?({:type, _, :union, members}), do: Enum.any?(members, &error_tuple?/1)
  defp error_tuple?({:type, _, :tuple, [{:atom, _, :error} | _]}), do: true
  defp error_tuple?(_type), do: false
end
