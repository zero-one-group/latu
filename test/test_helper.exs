Code.require_file("support/wire.exs", __DIR__)
Code.require_file("support/fixtures.exs", __DIR__)

# GRPC.Client.Connection announces its own shutdown at :debug, from a handle_continue that runs
# after the test that triggered it — so `@moduletag :capture_log` never sees it.
Logger.put_module_level(GRPC.Client.Connection, :info)

ExUnit.start(exclude: [:integration])

# An integration run with no data files fails here, once and by name, rather than as a
# file-not-found inside six test files. See test/support/fixtures.exs.
Latu.Fixtures.check!(ExUnit.configuration())
