Code.require_file("support/wire.exs", __DIR__)
Code.require_file("support/fixtures.exs", __DIR__)
Code.require_file("support/sql_golden.exs", __DIR__)
Code.require_file("support/lifecycle.exs", __DIR__)

# GRPC.Client.Connection announces its own shutdown at :debug, from a handle_continue that runs
# after the test that triggered it — so `@moduletag :capture_log` never sees it.
Logger.put_module_level(GRPC.Client.Connection, :info)

# `:streaming` is the processing-time streaming tests, each a sleep by nature, so they sit
# behind `--include streaming` and outside `mix check.all` (docs/decisions.md, S-D7).
#
# `LATU_SERVER=sail` is the LakeSail run: integration on, the tagged gaps off. It cannot be
# `--include integration --exclude sail`, because an include overrides every exclude.
# docs/lakesail.md.
excluded =
  case System.get_env("LATU_SERVER") do
    nil -> [:integration, :golden, :streaming]
    "sail" -> [:golden, :streaming, :sail, :sail_crash]
    other -> raise "LATU_SERVER is unset or \"sail\", not #{inspect(other)}"
  end

ExUnit.start(exclude: excluded)

# An integration run with no data files fails here, once and by name, rather than as a
# file-not-found inside six test files. See test/support/fixtures.exs.
Latu.Fixtures.check!(ExUnit.configuration())
