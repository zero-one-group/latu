defmodule Latu.Integration.CreateDataFrameTest do
  use ExUnit.Case, async: true

  alias Latu.Protocol.Spark.Connect, as: Proto

  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  @people [%{id: 1, name: "Ada"}, %{id: 2, name: "Grace"}, %{id: 3, name: "Linus"}]

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session) end)
    %{session: session}
  end

  describe "under the threshold: LocalRelation" do
    test "rows round-trip — collect's inverse", %{session: session} do
      {:ok, df} = Latu.create_dataframe(session, @people)

      assert df |> Latu.sort([:id]) |> Latu.collect!() == @people
    end

    test "column input keeps its declared order", %{session: session} do
      df = Latu.create_dataframe!(session, b: [1, 2], a: ["x", "y"])

      {:ok, frame} = Latu.to_explorer(df)
      assert Explorer.DataFrame.names(frame) == ["b", "a"]
      assert Latu.collect!(df) == [%{b: 1, a: "x"}, %{b: 2, a: "y"}]
    end

    test "an Explorer frame ships as-is", %{session: session} do
      frame = Explorer.DataFrame.new(id: [10, 20])

      assert session |> Latu.create_dataframe!(frame) |> Latu.count() == {:ok, 2}
    end

    test "a created frame composes like any other", %{session: session} do
      total =
        session
        |> Latu.create_dataframe!(@people)
        |> Latu.filter(Latu.Column.greater(:id, 1))
        |> Latu.count!()

      assert total == 2
    end

    test "the schema casts: int64 data, INT schema", %{session: session} do
      df = Latu.create_dataframe!(session, [%{a: 1}], schema: "a INT")

      {:ok, frame} = Latu.to_explorer(df)
      assert Explorer.DataFrame.dtypes(frame) == %{"a" => {:s, 32}}
    end

    test "empty data with a schema is a typed empty frame", %{session: session} do
      df = Latu.create_dataframe!(session, [], schema: "id INT, name STRING")

      {:ok, frame} = Latu.to_explorer(df)
      assert Explorer.DataFrame.n_rows(frame) == 0
      assert Explorer.DataFrame.names(frame) == ["id", "name"]
    end
  end

  describe "over the threshold: ChunkedCachedLocalRelation" do
    # The threshold and chunk bounds are session configs, so a lowered threshold makes a tiny
    # frame take the artifact path — the same code that 64 MiB would exercise, without moving
    # 64 MiB. Session-scoped, and every test connects fresh, so nothing leaks.
    test "data escalates to cached artifacts and reads back whole", %{session: session} do
      {:ok, _} = Latu.sql(session, "SET spark.sql.session.localRelationCacheThreshold=1024")
      {:ok, _} = Latu.sql(session, "SET spark.sql.session.localRelationChunkSizeRows=100")

      rows = Enum.map(1..500, &%{id: &1, label: "row-#{&1}"})
      {:ok, df} = Latu.create_dataframe(session, rows)

      assert %Proto.Relation{rel_type: {:chunked_cached_local_relation, chunked}} = df.plan
      assert length(chunked.dataHashes) > 1
      assert chunked.schemaHash == nil

      assert Latu.count(df) == {:ok, 500}
      assert df |> Latu.sort([:id]) |> Latu.take!(1) == [%{id: 1, label: "row-1"}]
    end

    test "a schema uploads as its own chunk and still casts", %{session: session} do
      {:ok, _} = Latu.sql(session, "SET spark.sql.session.localRelationCacheThreshold=64")

      {:ok, df} = Latu.create_dataframe(session, [%{a: 1}, %{a: 2}], schema: "a INT")

      assert %Proto.Relation{rel_type: {:chunked_cached_local_relation, chunked}} = df.plan
      assert is_binary(chunked.schemaHash)

      {:ok, frame} = Latu.to_explorer(df)
      assert Explorer.DataFrame.dtypes(frame) == %{"a" => {:s, 32}}
    end

    test "the same data twice: the second upload finds it cached", %{session: session} do
      {:ok, _} = Latu.sql(session, "SET spark.sql.session.localRelationCacheThreshold=64")

      rows = Enum.map(1..50, &%{id: &1})

      assert session |> Latu.create_dataframe!(rows) |> Latu.count() == {:ok, 50}
      assert session |> Latu.create_dataframe!(rows) |> Latu.count() == {:ok, 50}
    end
  end
end
