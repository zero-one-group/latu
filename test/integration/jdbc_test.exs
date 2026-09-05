defmodule Latu.Integration.JdbcTest do
  use ExUnit.Case, async: true

  import Latu.Column

  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  #
  # `read/2` and `write/2` pass any format and any unrecognised option through, and a write with
  # neither `:path` nor `:table` is legal — the shape a JDBC write needs. No driver jar is added:
  # Derby ships inside the Spark assembly, so an embedded in-memory database is reachable with
  # nothing installed.
  #
  # An embedded Derby database lives in the JVM that opened it, and `--master local[1]` puts the
  # executor in the driver's JVM, so the write and the read see the same one. On a real cluster
  # they would not: this tests Latu's option passing and is not a recipe to copy.
  #
  # The database name carries a run token: it lives in the server's JVM, which outlives the
  # BEAM, so `System.unique_integer/1` alone would collide on a second run against a
  # still-running server.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"
  @run System.system_time(:millisecond)

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session, release: true) end)
    %{session: session}
  end

  defp db, do: "latu_#{@run}_#{System.unique_integer([:positive])}"

  defp options(db, extra) do
    [
      format: "jdbc",
      url: "jdbc:derby:memory:#{db};create=true",
      driver: "org.apache.derby.jdbc.EmbeddedDriver"
    ] ++ extra
  end

  test "a frame written over JDBC reads back, with no Latu code and no extra jar", %{
    session: session
  } do
    name = db()

    assert :ok =
             session
             |> Latu.range(3)
             |> Latu.select(id: :id, label: cast(:id, "string"))
             |> Latu.write(options(name, dbtable: "people", mode: :overwrite))

    rows =
      session
      |> Latu.read(options(name, dbtable: "people"))
      |> Latu.collect!()
      |> Enum.sort_by(& &1.id)

    assert rows == [
             %{id: 0, label: "0"},
             %{id: 1, label: "1"},
             %{id: 2, label: "2"}
           ]
  end

  test "a query pushed down as a subquery reads back too", %{session: session} do
    name = db()

    session
    |> Latu.range(10)
    |> Latu.write!(options(name, dbtable: "numbers", mode: :overwrite))

    # `query` is Spark's own JDBC option and Latu passes it through unrecognised, like any
    # other. It is the one that shows the generic path is genuinely generic.
    #
    # **The column is quoted and the table is not, and that asymmetry is Spark's.**
    # `JdbcUtils.schemaString` runs every column through `dialect.quoteIdentifier`, and
    # `DerbyDialect` does not override it, so `CREATE TABLE` gets `"id"` — double-quoted, and
    # therefore a case-sensitive lowercase name in Derby. The *table* name is used verbatim
    # from `dbtable`, so unquoted `numbers` folds to `NUMBERS` on both the create and the read
    # and matches itself. An unquoted `id` here folds to `ID` and does not resolve, which
    # reads exactly like a Latu bug and is not one.
    count =
      session
      |> Latu.read(options(name, query: ~s(SELECT "id" FROM numbers WHERE "id" > 6)))
      |> Latu.count!()

    assert count == 3
  end

  test "the schema comes from the database, not from Latu", %{session: session} do
    name = db()

    session
    |> Latu.range(2)
    |> Latu.select(id: :id, label: cast(:id, "string"))
    |> Latu.write!(options(name, dbtable: "typed", mode: :overwrite))

    schema =
      session
      |> Latu.read(options(name, dbtable: "typed"))
      |> Latu.schema!()

    assert Enum.map(schema, & &1.name) == ["id", "label"]
    assert Enum.map(schema, & &1.type) == ["bigint", "string"]
  end
end
