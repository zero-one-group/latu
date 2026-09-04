defmodule Latu.CheatsheetTest do
  use ExUnit.Case, async: true

  # `docs/cheatsheet.cheatmd` is generated and checked in, so it can go stale the moment a verb
  # is added, renamed, or has its first docstring line rewritten. This regenerates the page and
  # diffs it, rather than re-deriving what the page ought to contain: one generator, two callers.
  #
  # `mix`-only, like `docs_test.exs`'s sidebar checks: it reads `Code.fetch_docs/1` off the
  # compiled modules, because `Latu.Column`'s operators, predicates and sort keys are generated
  # inside `for` comprehensions and no source parse can enumerate them.

  # `dev/` is not in elixirc_paths, so the generator is `Code.require_file`d in setup_all and
  # does not exist when this file compiles. Without this the compiler warns five times on
  # every *recompile* — invisible on a warm build, on every fresh checkout and CI run.
  @compile {:no_warn_undefined, Latu.Dev.Cheatsheet}

  @generator "dev/cheatsheet.exs"

  setup_all do
    Code.require_file(@generator)
    :ok
  end

  test "the checked-in cheatsheet is what the generator produces now" do
    path = Latu.Dev.Cheatsheet.output()

    assert File.exists?(path),
           "#{path} is missing — run `mix run #{@generator} --write`"

    assert File.read!(path) == Latu.Dev.Cheatsheet.render(),
           "#{path} is stale — run `mix run #{@generator} --write` and commit the result"
  end

  # The generator could produce an empty page and the diff above would still pass, because the
  # file would match it. A floor is what notices — the same guard `examples_test.exs` puts on
  # its corpus.
  test "and it still has a corpus" do
    page = Latu.Dev.Cheatsheet.render()

    entries = page |> String.split("\n") |> Enum.count(&String.starts_with?(&1, "| `"))
    cards = page |> String.split("\n") |> Enum.count(&String.starts_with?(&1, "### "))

    assert entries >= 180, "cheatsheet entries fell to #{entries}"
    assert cards >= 12, "cheatsheet cards fell to #{cards}"
  end

  # A cheatsheet nobody can reach is a file, not a page. Same rule as every guide, enforced in
  # `docs_test.exs` for those — this module owns the one extra that is not a guide.
  test "it is an ExDoc extra" do
    extras = Mix.Project.config() |> Keyword.fetch!(:docs) |> Keyword.fetch!(:extras)

    assert Latu.Dev.Cheatsheet.output() in extras
  end

  # Every row is a table cell, and a table cell cannot wrap — so the generator clamps each
  # summary to fit. If that ever stops working the page still renders, just badly, which is
  # exactly the kind of defect a gate should carry rather than a reader.
  test "no line is wider than the repo's 98 columns" do
    over =
      Latu.Dev.Cheatsheet.render()
      |> String.split("\n")
      |> Enum.with_index(1)
      |> Enum.filter(fn {line, _n} -> String.length(line) > 98 end)

    assert over == []
  end
end
