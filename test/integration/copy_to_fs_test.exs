defmodule Latu.Integration.CopyToFsTest do
  use ExUnit.Case, async: true

  alias Latu.Error
  alias Latu.Session

  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  #
  # The compose server has no Hadoop configuration, so its default filesystem is the container's
  # own disk, which `uploadArtifactToFs` refuses unless the session or the server allows it. The
  # conf is session-scoped SQL conf, so each test sets it on its own session; the one test that
  # does not is the refusal. What lands in /tmp inside the container is read back through
  # Spark, never through this process, which is the whole point of the verb.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"
  @allow_local "spark.sql.artifact.copyFromLocalToFs.allowDestLocal"
  @dir "/tmp/latu_copy_to_fs"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session, release: true) end)
    %{session: session}
  end

  defp allow_local(session) do
    assert Latu.is_modifiable!(session, @allow_local), "#{@allow_local} is a static conf here"
    :ok = Latu.set_conf(session, @allow_local, "true")
  end

  defp path(ext), do: "#{@dir}/#{System.unique_integer([:positive])}.#{ext}"

  defp rows(session, path) do
    session
    |> Latu.read(format: "csv", path: path, header: true, infer_schema: true)
    |> Latu.sort(:id)
    |> Latu.collect!()
  end

  test "bytes land at the path and Spark reads them back", %{session: session} do
    allow_local(session)
    file = path("csv")

    assert :ok = Latu.copy_to_fs(session, file, "id,name\n1,Ada\n2,Bo\n")
    assert [%{id: 1, name: "Ada"}, %{id: 2, name: "Bo"}] = rows(session, file)
  end

  test "a second copy to the same path overwrites, unlike a jar", %{session: session} do
    allow_local(session)
    file = path("csv")

    assert :ok = Latu.copy_to_fs(session, file, "id,name\n1,Ada\n")
    assert :ok = Latu.copy_to_fs(session, file, "id,name\n1,Ada\n2,Bo\n3,Cy\n")
    assert [_, _, _] = rows(session, file)
  end

  test "a file past one chunk arrives whole", %{session: session} do
    allow_local(session)
    file = path("csv")
    body = Enum.map_join(1..20_000, "", &"#{&1},row-#{&1}\n")

    assert byte_size(body) > 32 * 1024
    assert :ok = Latu.copy_to_fs(session, file, "id,name\n" <> body)

    assert {:ok, 20_000} =
             session |> Latu.read(format: "csv", path: file, header: true) |> Latu.count()
  end

  test "a local destination is refused unless the session allows it", %{session: session} do
    assert {:error, %Error{kind: :rpc}} = Latu.copy_to_fs(session, path("csv"), "id\n1\n")
  end

  test "a relative path or a scheme is refused before anything is sent", %{session: session} do
    for bad <- ["data/x.csv", "s3a://bucket/x.csv", "file:///tmp/x.csv"] do
      assert_raise ArgumentError, ~r/absolute path with no scheme/, fn ->
        apply(Latu, :copy_to_fs, [session, bad, "x"])
      end
    end
  end

  test "an unconnected session is refused before the upload" do
    assert {:error, %Error{kind: :connect}} =
             Latu.copy_to_fs(Session.from_url!(@url), "/tmp/x.csv", "x")
  end
end
