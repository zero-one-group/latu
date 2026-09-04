defmodule Latu.ReadTest do
  use ExUnit.Case, async: true

  import Latu.Wire

  alias Latu.Plan
  alias Latu.Session

  # Golden plans come from PySpark: python dev/pyspark_oracle.py --generate

  setup do
    %{session: Session.from_url!("sc://h")}
  end

  describe "read/2 against PySpark" do
    test "a format and one path", %{session: session} do
      session
      |> Latu.read(format: "json", path: "/fixtures/people.json")
      |> assert_wire("read_json")
    end

    test "an absent schema is still present, as the empty string", %{session: session} do
      session
      |> Latu.read(format: "parquet", path: "/fixtures/people.parquet")
      |> assert_wire("read_parquet")
    end

    test "schema and options ride along", %{session: session} do
      session
      |> Latu.read(
        format: "csv",
        schema: "id INT, name STRING",
        path: "/fixtures/people.csv",
        header: true,
        sep: ";"
      )
      |> assert_wire("read_csv_schema_options")
    end

    test "several paths", %{session: session} do
      session
      |> Latu.read(format: "csv", paths: ["/fixtures/a.csv", "/fixtures/b.csv"])
      |> assert_wire("read_two_paths")
    end
  end

  describe "table/2,3 against PySpark" do
    test "by name, as a string or an atom", %{session: session} do
      assert_wire(Latu.table(session, "people"), "read_table")
      assert_wire(Latu.table(session, :people), "read_table")
    end

    test "with options", %{session: session} do
      assert_wire(Latu.table(session, "people", merge_schema: true), "read_table_options")
    end
  end

  describe "read/2 validation" do
    test ":path and :paths together are refused", %{session: session} do
      assert_raise ArgumentError, ~r/not both/, fn ->
        Latu.read(session, path: "/a", paths: ["/b"])
      end
    end

    test "a schema must be a string", %{session: session} do
      assert_raise ArgumentError, ~r/schema is a string/, fn ->
        Latu.read(session, schema: [id: :int])
      end
    end

    test "a path must be a string", %{session: session} do
      assert_raise ArgumentError, ~r/path is a string/, fn ->
        Latu.read(session, path: :people)
      end
    end
  end

  describe "Plan.to_options/1" do
    test "snake_case atom keys become camelCase" do
      assert Plan.to_options(infer_schema: true, header: false) ==
               [{"inferSchema", "true"}, {"header", "false"}]
    end

    test "string keys pass verbatim" do
      assert Plan.to_options(%{"spark.sql.x" => 1}) == [{"spark.sql.x", "1"}]
    end

    test "values stringify the way PySpark's to_str does" do
      assert Plan.to_options(a: 5, b: 2.5, c: :permissive, d: "x") ==
               [{"a", "5"}, {"b", "2.5"}, {"c", "permissive"}, {"d", "x"}]
    end

    test "a nil value drops its pair" do
      assert Plan.to_options(a: nil, b: 1) == [{"b", "1"}]
    end

    test "an unusable key or value is refused" do
      assert_raise ArgumentError, ~r/option key/, fn -> Plan.to_options([{1, "x"}]) end
    end

    # A refusal of a value names which one it was. An options map is written
    # all at once, so "an option value is a string" without the key sends you reading the lot.
    test "a bad option value names the key it belongs to" do
      assert_raise ArgumentError, ~r/^:mergeSchema is a string, number, boolean or atom/, fn ->
        Plan.to_options(mergeSchema: [1])
      end
    end
  end
end
