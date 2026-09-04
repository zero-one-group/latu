defmodule Latu.Integration.UdfTest do
  use ExUnit.Case, async: true

  import Latu.Column

  alias Latu.Error

  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  #
  # Latu's half of "call a user-defined function" is an `UnresolvedFunction` carrying a name.
  # The analyzer resolves it out of the session's function registry, which does not record what
  # put the entry there — a SQL UDF, a Hive UDF and a Java class all resolve the same way. So
  # this proves Latu's contribution with no jar: `CREATE FUNCTION ... RETURN <expr>` (a SQL UDF;
  # this server has no Hive catalog, so no other jar-free route), call it through `fun/3`, check
  # the answer. `OR REPLACE` and a fixed name: a TEMPORARY function is session state, and every
  # test here gets its own session.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session) end)
    %{session: session}
  end

  test "fun/3 reaches a function the session registered, not only a builtin", %{
    session: session
  } do
    Latu.sql!(
      session,
      "CREATE OR REPLACE TEMPORARY FUNCTION latu_add_one(x INT) RETURNS INT RETURN x + 1"
    )

    rows =
      session
      |> Latu.range(3)
      |> Latu.select(id: :id, bumped: fun("latu_add_one", [:id]))
      |> Latu.collect!()
      |> Enum.sort_by(& &1.id)

    assert rows == [
             %{id: 0, bumped: 1},
             %{id: 1, bumped: 2},
             %{id: 2, bumped: 3}
           ]
  end

  test "a name nothing registered is refused by the analyzer, by error class", %{
    session: session
  } do
    df = Latu.select(Latu.range(session, 1), x: fun("latu_no_such_routine", [:id]))

    assert {:error, %Error{} = error} = Latu.collect(df)
    assert error.error_class =~ "UNRESOLVED_ROUTINE"
  end

  # An assertion about Spark rather than about Latu: `CloneSession` is documented as preserving
  # "configuration and state", and `SparkSession.cloneSession` copies the SessionCatalog, which
  # holds temp views and temp functions alike.
  test "a registered function is session state, so a clone inherits it", %{session: session} do
    create = "CREATE OR REPLACE TEMPORARY FUNCTION latu_twice(x INT) RETURNS INT RETURN x * 2"
    Latu.sql!(session, create)

    clone = Latu.clone_session!(session)
    on_exit(fn -> Latu.release_session(clone) end)

    doubled =
      clone
      |> Latu.range(4, 5)
      |> Latu.select(doubled: fun("latu_twice", [:id]))
      |> Latu.collect!()

    assert doubled == [%{doubled: 8}]
  end
end
