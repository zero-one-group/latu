defmodule Latu.Integration.GuidesTest do
  use ExUnit.Case, async: true

  # Tier 2 of the example gate: a guide is a Markdown page whose `elixir` fences are a script.
  # `Latu.Guides` is the parser and the runner; this file is the accounting around them.
  #
  # A guide asserts by matching — `[%{id: 5} | _] = rows` — and only `elixir` fences run.
  #
  # A fence the test server cannot run (a merge needs Iceberg or Delta) is preceded by a line
  # beginning `> **Not executed.**` and the runner skips it. A visible blockquote, not an HTML
  # comment: the page tells the reader every snippet is executed, so the exceptions have to be
  # legible on the page. A skipped fence is still block-checked by `test/latu/examples_test.exs`,
  # which deliberately does not read this marker — two definitions of "skipped" would drift.
  # `test/integration/s3_guide_test.exs` runs one guide's skipped pair against a stack
  # `mix check.all` does not start.
  #
  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  @moduletag :integration
  @moduletag :capture_log

  # **A guide is one test containing a dozen round trips, and ExUnit's 60s budget is for one
  # assertion.** `docs/guides/quick-start.md` runs fourteen sequential executions, one of them a
  # write, against a `--master local[1]` server shared with the rest of the suite.
  #
  # 180s rather than a round number: `docs/decisions.md`'s write-stall entry settled on
  # 150_000 for the same shape of problem before `local[1]` retired it, and a guide does more
  # per test than a write test did. **The budget is not a licence to grow a guide** — a page
  # that needs more than this is a page a reader will not finish either.
  @moduletag timeout: 180_000

  @guides "docs/guides/*.md" |> Path.wildcard() |> Enum.sort()

  # Every skipped fence, by guide and reason. An opt-out nobody counts is an opt-out that
  # spreads, so a new one fails here until somebody writes down why — the same rule as
  # `@excused` in `Latu.ExamplesTest` and `@not_a_facade_verb` in `Latu.OptionsTest`.
  @illustrative [
    {"docs/guides/cookbook.md", "JDBC needs a database the test server does not have."},
    {"docs/guides/cookbook.md",
     "A merge needs an Iceberg or Delta target, and the test server has neither."},
    {"docs/guides/object-storage.md",
     "Not up in `mix check.all`: the s3 profile. `mix test --include s3` runs them."},
    {"docs/guides/object-storage.md",
     "Not up in `mix check.all`: the s3 profile. `mix test --include s3` runs them."}
  ]

  # A wildcard that matched nothing would define no tests and pass in silence, which is the
  # one failure mode a runner like this cannot report on its own.
  #
  # Read again here rather than asserting on `@guides`: the attribute is a literal by the time
  # the assertion runs, so `@guides != []` is a comparison the type checker can prove constant
  # — an assertion that cannot fail, which is the shape to distrust. Reading the directory also
  # catches a guide added since this module was compiled, which would have no test of its own.
  test "every guide has a test" do
    guides = "docs/guides/*.md" |> Path.wildcard() |> Enum.sort()

    assert guides != []
    assert guides == @guides
  end

  # The other side of the opt-out. `run/1` filters skipped fences and a filter nobody watches is
  # where a fence quietly stops being tested — so the whole skipped set is named, reason and all.
  test "every fence that is not executed is named, with its reason" do
    skipped =
      for guide <- @guides,
          {_line, _code, {:skipped, reason}} <- Latu.Guides.fences(File.read!(guide)),
          do: {guide, reason}

    assert Enum.sort(skipped) == Enum.sort(@illustrative)
  end

  for guide <- @guides do
    test "#{guide}" do
      run(unquote(guide))
    end
  end

  # A guide must not leave state on the server: these run async beside every other integration
  # test. Anything registering a temp view or writing a table needs its own name per run, and
  # anything setting a conf belongs in a test rather than in a guide.
  defp run(guide) do
    fences = Latu.Guides.fences(File.read!(guide))

    assert fences != [], "#{guide} has no `elixir` fences to run"

    runnable = for {line, code, :runs} <- fences, do: {line, code}

    assert runnable != [],
           "#{guide}'s fences are all marked not-executed; it is prose, not a guide"

    Latu.Guides.run(guide, runnable)
  end
end
