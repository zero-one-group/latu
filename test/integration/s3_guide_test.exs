defmodule Latu.Integration.S3GuideTest do
  use ExUnit.Case, async: true

  # The other half of `test/integration/guides_test.exs`: the fences that page marks
  # `> **Not executed.**` are the whole subject of `docs/guides/object-storage.md`, so leaving
  # them to the parse-only tier would make the one guide about object storage the least
  # verified page in the docs.
  #
  # Needs the opt-in stack — `docker compose --profile s3 up -d --wait` — which is why this is
  # `:s3` and outside `mix check.all`. CI runs it as its own job. dev/README.md, decisions.
  #
  # The confs these fences set would not belong in a guide run against the shared :15002
  # server. They are fine here: :15004 is a server of its own, and the session is this test's.
  @moduletag :s3
  @moduletag :capture_log
  @moduletag timeout: 180_000

  @guide "docs/guides/object-storage.md"

  test "#{@guide}'s S3 fences run against the stack" do
    fences = Latu.Guides.fences(File.read!(@guide))
    skipped = for {line, code, {:skipped, _reason}} <- fences, do: {line, code}

    # If the page stops marking them, `guides_test.exs` has started running them and this
    # module is dead weight rather than quietly passing on nothing.
    assert skipped != [], "#{@guide} marks no fence not-executed; this module has no work"

    Latu.Guides.run(@guide, skipped)
  end
end
