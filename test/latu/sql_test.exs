defmodule Latu.SqlTest do
  use ExUnit.Case, async: true

  import Latu.Wire

  alias Latu.DataFrame
  alias Latu.Plan
  alias Latu.Session

  # Golden plans come from PySpark: python dev/pyspark_oracle.py --generate

  setup do
    %{session: Session.from_url!("sc://h")}
  end

  describe "sql_command/2 against PySpark" do
    test "a bare query — no args reach the wire" do
      "SELECT 1 AS one"
      |> Plan.sql_command()
      |> assert_wire_command("sql_command")
    end

    test "a list binds ? positionally, as literals" do
      "SELECT ? AS n, ? AS s"
      |> Plan.sql_command([42, "x"])
      |> assert_wire_command("sql_pos_args")
    end

    test "a map binds :name, atom keys coerced" do
      "SELECT * FROM range(10) WHERE id > :min AND id < :max"
      |> Plan.sql_command(%{min: 2, max: 8})
      |> assert_wire_command("sql_named_args")
    end

    test "a view is a SubqueryAlias hoisted beside the query", %{session: session} do
      orders = Latu.range(session, 5)

      "SELECT count(*) AS n FROM orders"
      |> Plan.sql_command([], orders: orders.plan)
      |> assert_wire_command("sql_views")
    end
  end

  describe "sql/3's third argument" do
    test "a keyword list is options, and views take DataFrames", %{session: session} do
      orders = Latu.range(session, 5)

      {:sql_command, command} =
        Plan.sql_command("SELECT * FROM orders", [], orders: orders.plan).command_type

      assert {:with_relations, wrapper} = command.input.rel_type
      assert [%{rel_type: {:subquery_alias, %{alias: "orders"}}}] = wrapper.references
    end

    test "a view that is not a DataFrame is refused", %{session: session} do
      assert_raise ArgumentError, ~r/views: :orders needs a DataFrame/, fn ->
        Latu.sql(session, "SELECT 1", views: [orders: :nope])
      end
    end

    test "an unknown option is refused", %{session: session} do
      assert_raise ArgumentError, fn -> Latu.sql(session, "SELECT 1", nope: 1) end
    end
  end

  describe "create_temp_view_command/3 against PySpark" do
    test "the flags default off", %{session: session} do
      session
      |> Latu.range(3)
      |> DataFrame.create_temp_view_command("v_people")
      |> assert_wire_command("create_temp_view")
    end

    test "global and replace flags reach the wire", %{session: session} do
      session
      |> Latu.range(3)
      |> DataFrame.create_temp_view_command("v_people", global: true, replace: true)
      |> assert_wire_command("create_view_global_replace")
    end
  end

  describe "catalog/2 against PySpark" do
    test "an operation with no fields is still present on the wire" do
      :current_database |> Plan.catalog() |> assert_wire("catalog_current_database")
    end

    test "db_name and pattern ride along" do
      :list_tables
      |> Plan.catalog(db_name: "default", pattern: "latu_*")
      |> assert_wire("catalog_list_tables")
    end

    test "table_exists carries its names" do
      :table_exists
      |> Plan.catalog(table_name: "people", db_name: "default")
      |> assert_wire("catalog_table_exists")
    end

    test "drop_table with if_exists; an unset purge vanishes" do
      :drop_table
      |> Plan.catalog(table_name: "scratch", if_exists: true)
      |> assert_wire("catalog_drop_table")
    end

    test "drop_temp_view" do
      :drop_temp_view
      |> Plan.catalog(view_name: "v_people")
      |> assert_wire("catalog_drop_temp_view")
    end

    test "cache_table without a storage level" do
      :cache_table |> Plan.catalog(table_name: "people") |> assert_wire("catalog_cache_table")
    end
  end

  describe "validation" do
    test "sql args take literals, not column atoms" do
      assert_raise ArgumentError, ~r/cannot make a Spark literal/, fn ->
        Plan.sql_command("SELECT ?", [:id])
      end
    end

    test "a named argument's key is a name" do
      assert_raise ArgumentError, ~r/argument name/, fn ->
        Plan.sql_command("SELECT :x", %{1 => 2})
      end
    end

    test "view flags are flags" do
      assert_raise ArgumentError, ~r/flag/, fn ->
        Plan.create_view(Plan.range(0, 1, 1), "v", replace: "yes")
      end
    end

    test "an unknown catalog operation is refused by name" do
      assert_raise ArgumentError, ~r/catalog operation/, fn ->
        Plan.catalog(:list_partitions)
      end
    end

    test "a nil field stays off the wire entirely" do
      assert Plan.normalize_ids(Plan.catalog(:list_tables)) ==
               Plan.normalize_ids(Plan.catalog(:list_tables, db_name: nil, pattern: nil))
    end
  end
end
