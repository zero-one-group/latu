defmodule Latu.Integration.SqlTest do
  use ExUnit.Case, async: true

  alias Latu.Catalog

  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session) end)
    %{session: session}
  end

  describe "sql/3" do
    test "a query, collected", %{session: session} do
      assert session |> Latu.sql!("SELECT 1 AS one") |> Latu.collect!() == [%{one: 1}]
    end

    test "the DataFrame queries the result, so it collects twice", %{session: session} do
      df = Latu.sql!(session, "SELECT 2 + 2 AS four")

      assert Latu.collect!(df) == [%{four: 4}]
      assert Latu.collect!(df) == [%{four: 4}]
    end

    test "a list binds ? positionally", %{session: session} do
      assert session |> Latu.sql!("SELECT ? AS n, ? AS s", [42, "x"]) |> Latu.collect!() ==
               [%{n: 42, s: "x"}]
    end

    test "a map binds :name", %{session: session} do
      {:ok, df} =
        Latu.sql(session, "SELECT id FROM range(10) WHERE id > :min AND id < :max", %{
          min: 2,
          max: 8
        })

      assert {:ok, 5} = Latu.count(df)
    end

    test "DDL runs when called, not at collect", %{session: session} do
      assert {:ok, _df} = Latu.sql(session, "DROP TABLE IF EXISTS latu_never_created")
    end

    test "a broken query errors rather than raising", %{session: session} do
      assert {:error, %Latu.Error{}} = Latu.sql(session, "SELECT FROM WHERE")
    end
  end

  describe "temp views" do
    test "create, query through sql, drop", %{session: session} do
      df = session |> Latu.range(5) |> Latu.filter(Latu.Column.greater(:id, 2))

      assert :ok = Latu.create_temp_view(df, "v_big")

      assert session |> Latu.sql!("SELECT count(*) AS n FROM v_big") |> Latu.collect!() ==
               [%{n: 2}]

      assert Catalog.drop_temp_view(session, "v_big") == {:ok, true}
      assert Catalog.table_exists(session, "v_big") == {:ok, false}
    end

    test "an existing name refuses unless replace: is set", %{session: session} do
      df = Latu.range(session, 3)

      assert :ok = Latu.create_temp_view(df, "v_twice")
      assert {:error, %Latu.Error{}} = Latu.create_temp_view(df, "v_twice")
      assert :ok = Latu.create_temp_view(df, "v_twice", replace: true)
    end

    test "views: names a DataFrame into one query and registers nothing", %{session: session} do
      orders = session |> Latu.range(5) |> Latu.filter(Latu.Column.greater(:id, 1))

      assert session
             |> Latu.sql!("SELECT count(*) AS n FROM orders", views: [orders: orders])
             |> Latu.collect!() == [%{n: 3}]

      assert Catalog.table_exists(session, "orders") == {:ok, false}
      assert {:error, %Latu.Error{}} = Latu.sql(session, "SELECT * FROM orders")
    end

    test "views: composes with parameter binding", %{session: session} do
      people = Latu.range(session, 10)

      assert session
             |> Latu.sql!("SELECT count(*) AS n FROM people WHERE id > :min",
               views: [people: people],
               args: %{min: 6}
             )
             |> Latu.collect!() == [%{n: 3}]
    end
  end

  describe "the catalog" do
    test "current database and existence", %{session: session} do
      assert Catalog.current_database(session) == {:ok, "default"}
      assert Catalog.database_exists(session, "default") == {:ok, true}
      assert Catalog.database_exists(session, "no_such_db") == {:ok, false}
    end

    test "a temp view shows up in the listing, marked temporary", %{session: session} do
      :ok = session |> Latu.range(3) |> Latu.create_temp_view("v_listed")

      {:ok, tables} = Catalog.list_tables(session, pattern: "v_listed")

      assert [%{name: "v_listed", isTemporary: true}] =
               Enum.map(tables, &Map.take(&1, [:name, :isTemporary]))
    end

    test "list_columns names the view's columns", %{session: session} do
      :ok = session |> Latu.range(3) |> Latu.create_temp_view("v_cols")

      {:ok, [column]} = Catalog.list_columns(session, "v_cols")

      assert %{name: "id", dataType: "bigint"} = Map.take(column, [:name, :dataType])
    end

    test "cache, check, uncache", %{session: session} do
      :ok = session |> Latu.range(3) |> Latu.create_temp_view("v_cached")

      assert Catalog.is_cached(session, "v_cached") == {:ok, false}
      assert Catalog.cache_table(session, "v_cached") == :ok
      assert Catalog.is_cached(session, "v_cached") == {:ok, true}
      assert Catalog.uncache_table(session, "v_cached") == :ok
      assert Catalog.is_cached(session, "v_cached") == {:ok, false}
    end

    test "drop_table: if_exists tolerates a missing table, bare does not", %{session: session} do
      assert Catalog.drop_table(session, "latu_never_created", if_exists: true) == :ok
      assert {:error, %Latu.Error{}} = Catalog.drop_table(session, "latu_never_created")
    end
  end
end
