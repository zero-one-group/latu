defmodule Latu.Client.ArtifactTest do
  use ExUnit.Case, async: true

  alias Latu.Client
  alias Latu.Protocol.Spark.Connect, as: Proto
  alias Latu.Session

  # PySpark's ArtifactManager.CHUNK_SIZE — the request shapes below mirror its _add_artifacts.
  @chunk 32 * 1024

  setup do
    %{session: Session.from_url!("sc://h")}
  end

  test "small blobs pack into one batch, named and crc'd", %{session: session} do
    artifacts = [{"h1", "aaa"}, {"h2", "bbb"}]

    assert [%Proto.AddArtifactsRequest{payload: {:batch, batch}} = request] =
             Client.artifact_requests(session, artifacts)

    assert request.session_id == session.session_id
    assert [first, second] = batch.artifacts
    assert first.name == "cache/h1"
    assert first.data.data == "aaa"
    assert first.data.crc == :erlang.crc32("aaa")
    assert second.name == "cache/h2"
  end

  test "a batch flushes at 32 KiB", %{session: session} do
    a = String.duplicate("a", @chunk - 10)
    b = String.duplicate("b", 100)

    assert [%{payload: {:batch, one}}, %{payload: {:batch, two}}] =
             Client.artifact_requests(session, [{"h1", a}, {"h2", b}])

    assert [%{name: "cache/h1"}] = one.artifacts
    assert [%{name: "cache/h2"}] = two.artifacts
  end

  test "a large blob streams as begin_chunk + chunks, 32 KiB apiece", %{session: session} do
    blob = :crypto.strong_rand_bytes(@chunk * 2 + 5)

    requests = Client.artifact_requests(session, [{"big", blob}])

    assert [%{payload: {:begin_chunk, begin_chunk}} | chunks] = requests
    assert begin_chunk.name == "cache/big"
    assert begin_chunk.total_bytes == byte_size(blob)
    assert begin_chunk.num_chunks == 3
    assert byte_size(begin_chunk.initial_chunk.data) == @chunk

    assert [%{payload: {:chunk, mid}}, %{payload: {:chunk, last}}] = chunks
    assert byte_size(mid.data) == @chunk
    assert byte_size(last.data) == 5
    assert begin_chunk.initial_chunk.data <> mid.data <> last.data == blob
  end

  test "a large blob flushes the pending batch first", %{session: session} do
    blob = :crypto.strong_rand_bytes(@chunk + 1)

    assert [%{payload: {:batch, _}}, %{payload: {:begin_chunk, _}}, %{payload: {:chunk, _}}] =
             Client.artifact_requests(session, [{"small", "x"}, {"big", blob}])
  end
end
