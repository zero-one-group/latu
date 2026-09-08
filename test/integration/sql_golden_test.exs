defmodule Latu.Integration.SqlGoldenTest do
  use ExUnit.Case, async: true

  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect. Tagged
  # :golden and *not* :integration, so `--include golden` selects exactly these: an ExUnit
  # include beats an exclude, so a test carrying both tags cannot be filtered out by either.
  @moduletag :golden
  @moduletag :capture_log

  setup do
    session = Latu.connect!("sc://localhost:15002")
    on_exit(fn -> Latu.disconnect(session, release: true) end)
    %{session: session}
  end

  for name <- Latu.SqlGolden.cases() do
    test "#{name} answers as its golden says", %{session: session} do
      assert Latu.SqlGolden.assert_answer(session, unquote(name)) == :ok
    end
  end
end
