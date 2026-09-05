# Contributing

Requires Elixir ~> 1.20, Docker, `protoc`, and Python 3 for the plan oracle.

```bash
mix deps.get
docker compose up -d --wait            # both spark servers; the reattach profile is not optional
python3 -m venv dev/.venv && \
    dev/.venv/bin/pip install \
    -r dev/requirements.txt            # the plan oracle
mix fixtures                           # the integration suite's data files, once per checkout

mix check                              # format + warnings-as-errors + offline tests
mix check.all                          # the same, plus the integration suite
```

Both halt on the first failure and exit non-zero. `docker compose up -d spark-connect` alone
starts the ordinary server on :15002; the second profile on :15003 runs with a five-second
`senderMaxStreamDuration`, which is what makes the reattach tests mean anything.

`mix fixtures` needs the oracle's Python venv, which `dev/README.md`'s one-time setup creates
(`python3 -m venv dev/.venv`, then `dev/.venv/bin/pip install -r dev/requirements.txt`). The
data files it writes are generated rather than committed, so the suite stays hermetic and
carries no third-party data licence. `mix check` needs none of them; `mix check.all` refuses to
start without them, naming this command.

Conventions — naming, the shapes a builder and an action return, how errors are worded, how a
verb is documented — live in `CLAUDE.md`. Per-decision rationale is in `docs/decisions.md`, a
dated log; every place the public API departs from PySpark is in
[`docs/deviations.md`](docs/deviations.md), with why.

## Tests

Unit tests mirror `lib/` under `test/`. Anything touching the network lives in
`test/integration/` behind an `:integration` tag, excluded by default.

Four of them are checks on the repo rather than on Latu, and are worth knowing about before you
trip one:

  * `test/latu/docs_test.exs` — every public function in a documented module has a `@doc`;
    every documented module has a `groups_for_modules` entry; every guide in `docs/guides/` is
    in `mix.exs`'s `:extras` and its Guides group; and every relative link in every extra
    resolves, to the file it names.
  * `test/latu/function_calls_test.exs` — every `F.<name>/<arity>` call site anywhere in
    `lib`, `test` or `dev` matches a real export, read from the AST so the arity is exact.
  * `test/latu/examples_test.exs` — every code block in every docstring and prose doc parses as
    Elixir and names only functions that exist at the arity shown, including inline `Latu.`
    calls in a sentence or a table cell. It also insists that every markdown file in the repo is
    checked, run as a guide, or excused there by name.
  * `test/latu/options_test.exs` — every option any verb accepts is explained under `## Options`
    in the facade's own docstring, with every value of a closed set spelled out. It reads the
    `Keyword.validate!` and `lookup(@const, ...)` calls in `lib/` rather than a list anyone
    maintains.

## Documentation is executed

An example that is not executed is a lie with a shelf life, so every example is run one of
three ways:

  * a **doctest**, for anything pure — no server, runs in `mix check`;
  * an `elixir` fence in **`docs/guides/*.md`**, for anything that needs a server, run in order
    by `test/integration/guides_test.exs` with one shared binding. A guide asserts by matching,
    so `[%{id: 5} | _] = rows` is documentation and a test at once;
  * otherwise `examples_test.exs` above, which cannot run the example but proves every function
    it names is real.

A fence that **cannot** run against the test server — a merge needs an Iceberg or Delta target —
is preceded by a visible line beginning

    > **Not executed.**

with the reason on the same line. The runner skips it and `guides_test.exs` asserts the whole
skipped set by guide and reason, so a new one fails until somebody writes down why. The marker is
a blockquote rather than an HTML comment because the page tells readers its snippets are
executed: the exceptions belong on the page, not in its source.

`dev/example.exs` is Latu's own smoke test — `mix run dev/example.exs` with a server up — not
the source of the README's snippets.

## The plan oracle and the golden fixtures

Latu's correctness rests on its plan being byte-identical to the one PySpark builds for the same
pipeline. `dev/pyspark_oracle.py` generates those expectations and `dev/README.md` explains the
workflow, the two servers, and how to read a plan.

Protobuf modules under `lib/latu/protocol/generated/` are checked in and generated from the
vendored Spark 4.2.0 protos in `priv/proto/`. Do not edit them by hand:

```bash
mix proto.generate
```

## Release checklist

Maintainer only. The nested notes are the traps each step exists for; the reasoning behind them
is in `docs/decisions.md`.

**Before the release commit**

- [ ] every branch belonging in this version is merged to `main`
  - a branch touching `lib/` is a release change however it is named:
    `git diff --stat main..<branch> -- lib/`
- [ ] no `## Unreleased` section left in `CHANGELOG.md` — fold it into this version's entry while
  the version is still unpublished
- [ ] `CHANGELOG.md`'s top entry is the new version and date, migration in one line
- [ ] `@version` in `mix.exs` matches it
- [ ] the README's install snippet names the new minor
  - `~> 0.1` still *installs* 0.2.0, so nothing breaks and nothing 404s — but a minor may rename
    or remove before 1.0, so the loose constraint floats a user across the next one; and the
    README is frozen per version on hex.pm, so a stale snippet stays on that page for good
- [ ] `docs/decisions.md` carries every decision this version made; a reversed one is a new entry
  plus one line on the old
- [ ] `mix check.all` green, both compose profiles up
  - `DOCKER_DEFAULT_PLATFORM` unset: an emulated JVM runs several times slower and reads as
    flaky tests rather than as a slow machine (`dev/README.md`)
- [ ] `git rev-parse main origin/main` match

**Release**

- [ ] `mix hex.build`, and read the file list it prints
  - `priv/function_docs.exs` and `priv/function_groups.exs` must be in it: the compile refuses
    without them, so a consumer's `deps.get` fails on something you cannot reproduce locally
- [ ] working tree clean — `mix hex.publish` builds the docs from the tree, not from the tarball
- [ ] `git tag v<version>` and `git push origin v<version>`, **before** publishing
  - `source_ref` follows `@version`, so every "view source" link on hexdocs points at that tag;
    publish first and all of them 404 until it lands
- [ ] `mix hex.publish`

**Within the hour**

`mix hex.publish --revert <version>` works for one hour on a version that is not the package's
first; after that, retire only. Check the versioned URLs — the unversioned pages sit behind a CDN
cache and go on showing the previous release for a while.

- [ ] `hex.pm/packages/latu/<version>` renders
- [ ] `readme.hex.pm/latu/<version>`
- [ ] `latu.hexdocs.pm/<version>/readme.html`
- [ ] a "view source" link on hexdocs lands at `v<version>`
- [ ] every README link works **on hex.pm**, not only on GitHub
  - the README is frozen per version there and the tarball carries no guides, so a relative link
    404s; 0.1.1 exists for exactly this
