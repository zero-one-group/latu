defmodule Latu.Integration.ArtifactTest do
  use ExUnit.Case, async: true

  alias Latu.Error
  alias Latu.Session

  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  #
  # A jar is a zip, so `:zip.create/2` builds a real one in memory: no JDK, and no fixture to
  # commit. That is the whole reason `add_jar/3` could be written at all, since M12.5 deferred
  # it on exactly that (`docs/decisions.md`, 2026-09-07). Nothing here asks the JVM to *load* a
  # class out of the jar — what is under test is Latu's upload and the server's accounting of
  # it, and a class that resolves is `Latu.Column.fun/3`'s business, already covered by
  # `udf_test.exs` with a SQL UDF.
  #
  # An artifact is session-scoped, and every test connects its own session, so these cannot
  # collide with each other or with anything else on the server. Hence async, and hence the
  # unique name per test: the name is the artifact's identity, and the server refuses a second
  # jar under one name unless the bytes match.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session, release: true) end)
    %{session: session}
  end

  defp jar(contents) do
    {:ok, {_name, bytes}} =
      :zip.create(~c"latu.jar", [{~c"latu/marker.txt", contents}], [:memory])

    bytes
  end

  defp name, do: "latu-#{System.unique_integer([:positive])}.jar"

  # `LIST JARS` answers a one-column frame named `Results`, each row the `spark://` URI the
  # file server hands executors. Measured; it is the first thing a Spark bump would move.
  defp listed(session) do
    session
    |> Latu.sql!("LIST JARS")
    |> Latu.collect!(keys: :strings)
    |> Enum.map(&Map.fetch!(&1, "Results"))
  end

  test "a jar lands on the session, and LIST JARS shows it", %{session: session} do
    jar_name = name()

    assert listed(session) == []
    assert :ok = Latu.add_jar(session, jar_name, jar("hello"))
    assert Enum.any?(listed(session), &String.ends_with?(&1, "/jars/" <> jar_name))
  end

  test "identical bytes under one name are a no-op", %{session: session} do
    jar_name = name()
    bytes = jar("hello")

    assert :ok = Latu.add_jar(session, jar_name, bytes)
    assert :ok = Latu.add_jar(session, jar_name, bytes)
    assert length(listed(session)) == 1
  end

  test "different bytes under one name are refused", %{session: session} do
    jar_name = name()

    assert :ok = Latu.add_jar(session, jar_name, jar("hello"))

    assert {:error, %Error{} = error} = Latu.add_jar(session, jar_name, jar("different"))
    assert error.kind == :rpc
    assert error.message =~ jar_name
  end

  test "a jar past one chunk uploads whole", %{session: session} do
    jar_name = name()
    big = jar(:crypto.strong_rand_bytes(96 * 1024))

    assert byte_size(big) > 32 * 1024
    assert :ok = Latu.add_jar(session, jar_name, big)
    assert Enum.any?(listed(session), &String.ends_with?(&1, "/jars/" <> jar_name))
  end

  test "an unconnected session is refused before the upload" do
    assert {:error, %Error{kind: :connect}} =
             Latu.add_jar(Session.from_url!(@url), "x.jar", jar("hello"))
  end
end
