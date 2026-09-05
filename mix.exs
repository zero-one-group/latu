defmodule Latu.MixProject do
  use Mix.Project

  @version "0.1.1"
  @source_url "https://github.com/zero-one-group/latu"

  def project do
    [
      app: :latu,
      version: @version,
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      # Enforced here rather than per-run with `--force --warnings-as-errors`: any file the
      # incremental compiler touches is held to it, and the checks stop paying to recompile
      # the ~7k generated proto lines every time. See docs/decisions.md.
      elixirc_options: elixirc_options(Mix.env()),
      # test/support is loaded by test_helper.exs, not by the test loader.
      test_ignore_filters: [&String.starts_with?(&1, "test/support/")],
      deps: deps(),
      aliases: aliases(),
      description: "A native Elixir DataFrame API for Apache Spark, over Spark Connect.",
      package: package(),
      docs: docs(),
      source_url: @source_url
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "https://hexdocs.pm/latu/changelog.html",
        "Spark Connect" => "https://spark.apache.org/docs/latest/spark-connect-overview.html"
      },
      # Hex's defaults plus `usage-rules.md`, which tooling reads from the dep. `priv/` carries
      # the harvested docs and groups the compile refuses without, and the vendored protos.
      # `assets/` is not shipped: nothing in the package reads it, and the README's images are
      # absolute URLs.
      files: ~w(lib priv .formatter.exs mix.exs README.md LICENSE CHANGELOG.md usage-rules.md)
    ]
  end

  defp docs do
    [
      name: "Latu",
      main: "readme",
      # The README opens with the lockup image rather than an `# Latu` heading, so ExDoc is told
      # the page title; the tile is the sidebar logo and the favicon. See assets/README.md.
      logo: "assets/latu-avatar.svg",
      favicon: "assets/latu-avatar.svg",
      assets: %{"assets" => "assets"},
      extras: [
        {"README.md", title: "Latu"},
        "CHANGELOG.md",
        "docs/guides/quick-start.md",
        "docs/guides/cookbook.md",
        "docs/guides/from-pyspark.md",
        "docs/guides/from-explorer.md",
        "docs/cheatsheet.cheatmd",
        "usage-rules.md",
        "docs/deviations.md",
        "CONTRIBUTING.md"
      ],
      groups_for_extras: [
        Guides: [
          "docs/guides/quick-start.md",
          "docs/guides/cookbook.md",
          "docs/guides/from-pyspark.md",
          "docs/guides/from-explorer.md"
        ],
        Reference: ["docs/cheatsheet.cheatmd", "usage-rules.md", "docs/deviations.md"]
      ],
      # `mix docs` runs with warnings as errors, so the exemptions are named here. It warns on a
      # reference to something `@moduledoc false`, and Latu has three kinds it will always have:
      # the generated protocol modules, the transport, and the internals `docs/deviations.md`
      # documents *because* they are internals. Anything else is a real defect and fails the
      # build.
      #
      # Two knobs, not interchangeable (ExDoc's source): prose references consult
      # `skip_code_autolink_to` with the **term**; typespecs consult only
      # `skip_undefined_reference_warnings_on`, by page, module or file — never the term — so a
      # spec naming a generated protocol type can only be silenced per *module*. No file appears
      # in either list: every relative link in the docs resolves to an extra, and
      # `test/latu/docs_test.exs` checks that more strictly than ExDoc does.
      skip_code_autolink_to: fn term ->
        String.starts_with?(term, "Latu.Protocol.") or
          String.starts_with?(term, "Latu.Client") or
          String.starts_with?(term, "Latu.Plan.Inspect") or
          String.starts_with?(term, "Latu.Functions.Registry") or
          term == "Latu.Session.pin/2"
      end,
      # `Latu.Plan`'s types *are* the protocol's — `@type relation :: Proto.Relation.t()`,
      # `Latu.Result.decode/2` takes `[Latu.Client.batch()]`, and `Latu.Result.Literal.value/1`
      # takes the protocol's own `Literal`. All three name modules that are `@moduledoc false`
      # on purpose, so ExDoc has no page to link and says so once per spec.
      #
      # What this costs, stated plainly: an undefined *type* in a spec on these three modules
      # would not be reported here. Two other checks cover it — the compiler, since `mix.exs`
      # sets `warnings_as_errors` for `lib/` and an unknown type in a `@spec` is a compile
      # warning; and `test/latu/examples_test.exs`, which resolves every `Mod.fun/arity` in
      # every docstring against real exports, including these three modules' prose.
      skip_undefined_reference_warnings_on: fn id ->
        id in ["Latu.Plan", "Latu.Result", "Latu.Result.Literal"]
      end,
      warnings_as_errors: true,
      groups_for_modules: [
        Verbs: [Latu],
        Expressions: [Latu.Column, Latu.Functions, Latu.Window],
        Session: [Latu.Session, Latu.Retry, Latu.Catalog],
        # Livebook rendering has no module of its own: `lib/latu/kino.ex` is a `Kino.Render`
        # impl for `Latu.DataFrame` behind an optional dep, and nothing else.
        "Results and observability": [
          Latu.Result,
          Latu.Result.Literal,
          Latu.Result.UDT,
          Latu.Error,
          Latu.ExecutionInfo,
          Latu.Progress,
          Latu.Telemetry
        ],
        # Inert data a verb takes or returns, plus the plan layer itself. You read these; you
        # rarely name one.
        "Plans and carriers": [
          Latu.Plan,
          Latu.DataFrame,
          Latu.GroupedData,
          Latu.MergeInto,
          Latu.CaseWhen,
          Latu.Subquery
        ]
      ],
      # `Latu.Functions` is 671 name/arity pairs, and ExDoc hands this function the `:module`,
      # `:name` and `:arity` of every entry -- so one config function groups the whole reference
      # by Spark's own categories, where `@doc group:` would have meant fifty attributes inside
      # a generated module. See `Latu.Functions.Groups`.
      default_group_for_doc: fn metadata ->
        if metadata[:module] == Latu.Functions, do: Latu.Functions.Groups.of(metadata[:name])
      end,
      # The release tag. Every "view source" link on hexdocs points at it, so it moves only when
      # the version does.
      source_ref: "v#{@version}",
      source_url: @source_url
    ]
  end

  defp elixirc_options(:prod), do: []
  defp elixirc_options(_env), do: [warnings_as_errors: true]

  # `check` and `check.all` run `mix test`, which refuses to run outside the test env.
  def cli do
    [preferred_envs: [check: :test, "check.all": :test]]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:decimal, "~> 2.0 or ~> 3.0"},
      {:explorer, "~> 0.12"},
      {:grpc, "~> 1.0"},
      {:gun, "~> 2.4"},
      {:kino, "~> 0.12", optional: true},
      {:protobuf, "~> 0.17"},
      {:telemetry, "~> 1.0"},
      # 0.36 is the floor: `:default_group_for_doc` and `:group` doc metadata land there.
      {:ex_doc, "~> 0.36", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      "proto.generate": &generate_proto/1,
      fixtures: &generate_fixtures/1,
      # `--warnings-as-errors` because `mix test` compiles test files itself, outside
      # `elixirc_options`; without it a warning in test/ is the one kind CI lets through.
      check: [
        "format --check-formatted",
        "compile",
        "test --warnings-as-errors"
      ],
      # `--max-cases 4` matches the dev server's one task slot; `docs/decisions.md`.
      "check.all": [
        "format --check-formatted",
        "compile",
        "test --include integration --warnings-as-errors --max-cases 4"
      ]
    ]
  end

  # The integration suite's data files. Generated rather than committed — dev/README.md.
  defp generate_fixtures(_) do
    # Absolute: System.cmd/3 sends a *relative* command to :os.find_executable/1, which
    # searches PATH rather than the cwd, so a relative path here fails with :enoent.
    python = Path.expand(Path.join(~w(dev .venv bin python)))

    if not File.exists?(python) do
      Mix.raise("#{python} not found; create it as dev/README.md's one-time setup says")
    end

    {output, status} = System.cmd(python, ["dev/make_data_fixtures.py"], stderr_to_stdout: true)

    if status != 0, do: Mix.raise("make_data_fixtures.py failed:\n#{output}")

    Mix.shell().info("Wrote the integration suite's data files to fixtures/")
  end

  defp generate_proto(_) do
    output_dir = "lib/latu/protocol/generated"
    tmp_dir = Path.join(System.tmp_dir!(), "latu-proto-#{System.unique_integer([:positive])}")

    File.rm_rf!(output_dir)
    File.mkdir_p!(output_dir)
    File.mkdir_p!(tmp_dir)

    try do
      proto_files = Path.wildcard("priv/proto/spark/connect/*.proto") |> Enum.sort()
      opts = "plugins=grpc,package_prefix=latu.protocol"
      args = ["-I", "priv/proto", "--elixir_out=#{opts}:#{tmp_dir}" | proto_files]
      {output, status} = System.cmd("protoc", args, stderr_to_stdout: true)

      if status != 0, do: Mix.raise("protoc failed:\n#{output}")

      # protoc-gen-elixir nests output twice over; flatten it. See dev/README.md.
      sources = Path.wildcard(Path.join(tmp_dir, "**/*.pb.ex")) |> Enum.sort()
      if sources == [], do: Mix.raise("protoc produced no .pb.ex files under #{tmp_dir}")

      generated =
        for source <- sources do
          dest = Path.join(output_dir, Path.basename(source))
          if File.exists?(dest), do: Mix.raise("name collision on #{Path.basename(source)}")
          File.cp!(source, dest)
          dest
        end

      Mix.shell().info("Generated #{length(generated)} modules:\n#{Enum.join(generated, "\n")}")
    after
      File.rm_rf!(tmp_dir)
    end
  end
end
