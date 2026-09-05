# Conventions

Design docs, roadmap and progress notes live in the Latu project on claude.ai, not in the repo.
Per-decision rationale goes in `docs/decisions.md`; every place the public API departs from
PySpark goes in `docs/deviations.md`, with why.

`docs/decisions.md` does not ship — not an ExDoc extra, not in the Hex package. So a `@doc`
carries its own one-sentence reason and may point at the log as a tail; a `#` comment may
point and nothing else.

**Docs record decisions, not the story of reaching them.** An entry in `docs/decisions.md` is
the decision and its reason; how it was found, which gate went red and what the container could
compile stay in git history. When a decision reverses, the new entry carries the reasoning and
the old one gets one line saying so — never a correction stacked under the old text, never a
retelling in a third file. A comment names a rule, not the incident that produced it. Prune when
something grows; do not append around it.

## Writing style (docs, docstrings, comments)

- Terse. Minimum words to get the message across. Not formal.
- Module docs and docstrings: state what it does, not how it feels about it.
- Comment the non-obvious only. No decorative banners.
- Explain a gotcha once, in the place it belongs, and link to it elsewhere.
- **Every example is executed, one of three ways.** A doctest for anything pure; an `elixir`
  fence in `docs/guides/*.md`, run in order by `test/integration/guides_test.exs`, for
  anything needing a server; and for the rest, `test/latu/examples_test.exs` proves the block
  parses and every call it names resolves at that arity. A guide asserts by matching —
  `[%{id: 5} | _] = rows` is documentation and a test at once.
- **A doctest asserts data, not `Inspect` output.** Where the rendering *is* the documented
  feature — the plan spine — the example calls `inspect/1` itself, so the expected value is a
  quoted string. A `doctest` line must have examples to run; one over a moduledoc with no
  `iex>` generates nothing and looks like coverage.
- **Rendered output inside a code block is commented** (`#=>`), so the block still parses and
  can be pasted whole.
- **A checkable claim goes in a fence, not in a sentence.** Prose is for the reasons. A `Latu.`
  call written inline in prose — a table cell, a sentence — is checked too, at the arity shown.
- **`Mod.fun/2` is a reference and becomes a hexdocs link; `Mod.fun` is not.** Qualify a
  reference to another module's function, and drop the arity from a name written in order to
  say it does not exist — "there is no `Latu.inspect`".
- **A new documentation page joins the gate by hand.** `examples_test.exs` fails until it is in
  that file's `@extras`, a guide under `docs/guides/`, or excused there by name with a reason;
  `docs_test.exs` fails until a guide is also in `mix.exs`'s `:extras` and its Guides group.
- **A guide fence that cannot run against the test server is marked, not omitted**: a visible
  `> **Not executed.** <reason>` line above it. Only where the server genuinely cannot run the
  code — a merge needs Iceberg or Delta — never to quiet an example that is awkward to assert on.
- **Every option gets a line under `## Options`, with its default and what it does**, every
  value of a closed set spelled out, in the **facade's own docstring** — `h Latu.join` is where
  a contract gets read, not the module it delegates to. A verb that forwards wholesale says so
  by name ("`collect/2`'s: `:keys` and `:progress`"). Enforced by `test/latu/options_test.exs`,
  which reads every `Keyword.validate!` and `lookup(@const, ...)` in `lib/`.
- **Never write an option into a docstring without reading the `Keyword.validate!` that accepts
  it.** The source is the only authority for what a verb takes, and the facade's *signature* is
  the authority for whether a caller can pass it at all.

## Python (`dev/` tooling only)

- Max line width 95.
- Docstrings one line where possible; multi-line only for a real caveat.

## Elixir

**The standard: would José merge this PR?** Not the elixir-lang core bar — the one he applies
reviewing a library like Explorer or Nx. If a construction or name would draw a review comment,
reconsider it. That outranks any rule below.

- **Functions and values, not magic.** No macro DSLs, no operator shadowing, no compile-time
  cleverness where a plain function and a struct will do. This is why there is no `defrelation`,
  no Ecto-style `filter(df, price > 100)` and no `Latu.Ops`; where Spark models something as one
  node and every client fakes a chain over it, use an inert struct (`Latu.GroupedData`,
  `Latu.CaseWhen`) coerced at the point of use. Do not re-propose any of these.
- **Naming precedence: Spark > Elixir > Polars > dplyr.** Use Spark's own spelling; reach
  further down the list only where Spark has none, and record the deviation. Check every new
  name against `Kernel`'s auto-imports and the special forms first — `alias`, `inspect/2`,
  `div/2` and `rem/2` have all bitten.
- Max line width 98 (`.formatter.exs`), `mix format` clean.
- `@moduledoc` on public modules, `@moduledoc false` on internals.
- Lazy plan builders take a struct and return a struct; never a tuple. Actions return `{:ok, _}
  | {:error, %Latu.Error{}}` with `!` variants — enforced by `test/latu/twins_test.exs`, which
  reads the specs.
- **A refusal names the fix.** Every rejection of a closed set prints the set (`lookup/3`);
  every rejection of a value says which argument or option it was (`identifier/2`,
  `option_value/2`); every missing required thing is named (`required!/2`).
- `lib/latu/protocol/generated/` is generated. Never hand-edit; run `mix proto.generate`.

## Working agreement

- The device shell has no docker, elixir, mix, protoc or network, and cannot delete files.
  Claude writes files; the maintainer runs them.
- Don't run `git status` from that shell — it leaves a `.git/index.lock` it cannot remove.
- One roadmap milestone at a time; sanity check at each boundary.
- Keep `dev/example.exs` current: Latu's own smoke test, runnable with
  `mix run dev/example.exs`. The *documented* snippets live in `docs/guides/*.md`.

Width applies to prose. Table rows and code fences are exempt where wrapping them would break
copy-paste or readability.

## Source files

Source files (`.yml`, `.exs`, `.ex`, `.py`) carry as little prose as possible. Explanations,
rationale and gotchas go in Markdown; a source comment should be one line pointing at where the
explanation lives.

## Section breaks

```elixir
# =============================================
# Parsing
# =============================================
```

## Tests

Test the public contract, not the implementation. No asserting on `inspect/1` formatting,
private helper edge cases already covered through the public function, or the exact shape of
strings the server never interprets. A test that breaks on a harmless refactor is a liability.

A negative-input test whose bad argument is a literal must call through `apply/3`: Elixir
1.20's type checker can prove the direct call raises, and warnings-as-errors fails the build on
that conclusion. `test/latu/local_data_test.exs` has the pattern.

## Units

Milliseconds are the unnamed default, matching the Elixir stdlib. Name the unit only when it is
*not* milliseconds, as `Supervisor` does with `:max_seconds`. Document the unit in the
`@moduledoc` and spec it `timeout()`.

## Checks

`mix check` before committing; `mix check.all` when the servers are up (`docker compose up
-d` starts both — the reattach profile is not optional, see `dev/README.md`). Both halt on the
first failure and exit non-zero.

No Dialyzer: Elixir's own type checking covers most of its ground with better messages.
`@spec` is still written, for documentation.

**Never `head` a grep you are using to decide something is finished.** A grep that answers "is
that all of them?" gets `| cat`, or a count first — truncation and no-matches look identical.

**Never time the suite with `--slowest` or `--slowest-modules`.** Both set `--trace`, and trace
forces `--max-cases 1` whatever the command line says — the run is serial and the module times
add up to the wall. Time with `mix check.all`, and read per-module times from a separate run.

**`mix docs` runs with `warnings_as_errors` and is part of the release gate.** Its exemptions
are named in `mix.exs` with reasons; a new warning is a real reference to fix, not a line to add
to the list.
