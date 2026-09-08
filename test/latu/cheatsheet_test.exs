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

  # The diff above compares the generator with its own output, so a bug in the generator sits on
  # both sides of that assertion and passes it. Two did: `row/3` appended the twin marker before
  # clamping, which then cut it off the 17 cells that had one, and `first_sentence/1` split on
  # ". ", which is what "e.g." ends with. Both are pure functions over a string, so these call
  # them directly, against expectations written out by hand rather than derived a second time.

  describe "first_sentence/1" do
    test "returns a summary with no sentence break as it stands" do
      text = "Every column of the frame, in schema order"

      assert Latu.Dev.Cheatsheet.first_sentence(text) == text
    end

    test "drops the second sentence and closes the cut with one full stop" do
      text = "The first one. The second one."

      assert Latu.Dev.Cheatsheet.first_sentence(text) == "The first one."
    end

    test "keeps an example in the first sentence, `e.g.` and all" do
      text = "The Spark version the server reports, e.g. `4.2.0`."

      assert Latu.Dev.Cheatsheet.first_sentence(text) == text
    end

    test "does not end the sentence at an abbreviation inside it" do
      text = "A batch, i.e. one Arrow message. The next one."

      assert Latu.Dev.Cheatsheet.first_sentence(text) == "A batch, i.e. one Arrow message."
    end

    test "holds the sentence together at every abbreviation the generator knows" do
      assert abbreviations() != []

      for abbreviation <- abbreviations() do
        text = "A summary, #{abbreviation}. this much of it. And no more."
        first = "A summary, #{abbreviation}. this much of it."

        assert Latu.Dev.Cheatsheet.first_sentence(text) == first
      end
    end
  end

  describe "row/3" do
    test "marks a verb that has a raising twin" do
      row = Latu.Dev.Cheatsheet.row("show/2", "Prints the first rows.", true)

      assert String.ends_with?(row, "| Prints the first rows. **+ !** |")
    end

    test "keeps the marker on a summary long enough to clamp" do
      row = Latu.Dev.Cheatsheet.row("show/2", long_summary(), true)

      assert row =~ "…"
      assert String.ends_with?(row, "**+ !** |")
    end

    test "clamps a summary with no twin and leaves it unmarked" do
      row = Latu.Dev.Cheatsheet.row("show/2", long_summary(), false)

      assert row =~ "…"
      refute row =~ "**+ !**"
    end

    test "fits the generator's width, marker and all" do
      for twin? <- [true, false], call <- ["show/2", "with_column_renamed/3"] do
        assert String.length(Latu.Dev.Cheatsheet.row(call, long_summary(), twin?)) <= 98
      end
    end
  end

  # Longer than any budget `row/3` computes, so it clamps for either call name and both twins.
  defp long_summary do
    "Returns a frame with the rows " <> String.duplicate("and more of them ", 8) <> "kept."
  end

  # Read off the generator rather than repeated here: a list this file assumed would be a list
  # the generator could quietly lose.
  defp abbreviations do
    [_line, list] = Regex.run(~r/@abbreviations ~w\(([^)]+)\)/, File.read!(@generator))

    String.split(list)
  end
end
