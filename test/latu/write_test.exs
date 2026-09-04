defmodule Latu.WriteTest do
  use ExUnit.Case, async: true

  import Latu.Wire

  alias Latu.Column
  alias Latu.DataFrame
  alias Latu.Plan
  alias Latu.Session

  # Golden plans come from PySpark: python dev/pyspark_oracle.py --generate

  setup do
    %{session: Session.from_url!("sc://h")}
  end

  describe "write_command/2 against PySpark" do
    test "format and path; an omitted mode stays off the wire", %{session: session} do
      session
      |> Latu.range(5)
      |> DataFrame.write_command(format: "parquet", path: "/tmp/latu/out")
      |> assert_wire_command("write_save")
    end

    test "mode and options ride along", %{session: session} do
      session
      |> Latu.range(5)
      |> DataFrame.write_command(
        format: "csv",
        path: "/tmp/latu/out_csv",
        mode: :overwrite,
        header: true,
        sep: ";"
      )
      |> assert_wire_command("write_csv_options")
    end

    test "partitioning, sorting, bucketing", %{session: session} do
      session
      |> Latu.range(10)
      |> DataFrame.write_command(
        format: "parquet",
        path: "/tmp/latu/buckets",
        partition_by: [:id],
        sort_by: [:id],
        bucket_by: {4, [:id]}
      )
      |> assert_wire_command("write_partition_sort_bucket")
    end
  end

  describe "table writes against PySpark" do
    test "save_as_table", %{session: session} do
      session
      |> Latu.range(5)
      |> DataFrame.save_as_table_command("people_tbl", mode: :append)
      |> assert_wire_command("write_table")
    end

    test "insert_into with overwrite", %{session: session} do
      session
      |> Latu.range(5)
      |> DataFrame.insert_into_command("people_tbl", overwrite: true)
      |> assert_wire_command("write_insert_into")
    end
  end

  describe "write_v2_command/3 against PySpark" do
    test "create with a provider", %{session: session} do
      session
      |> Latu.range(5)
      |> DataFrame.write_v2_command("t", mode: :create, using: "parquet")
      |> assert_wire_command("write_v2_create")
    end

    test "overwrite carries its condition", %{session: session} do
      session
      |> Latu.range(5)
      |> DataFrame.write_v2_command("t", mode: :overwrite, condition: Column.greater(:id, 3))
      |> assert_wire_command("write_v2_overwrite")
    end

    test "append with a table property and expression partitioning", %{session: session} do
      session
      |> Latu.range(5)
      |> DataFrame.write_v2_command("t",
        mode: :append,
        table_properties: [k: "v"],
        partition_by: [:id]
      )
      |> assert_wire_command("write_v2_append_props")
    end
  end

  describe "validation" do
    setup %{session: session} do
      %{df: Latu.range(session, 5)}
    end

    test "a subquery cannot ride in a write command", %{session: session, df: df} do
      scalar = Latu.scalar(Latu.range(session, 1))

      assert_raise ArgumentError, ~r/condition: a subquery cannot travel/, fn ->
        Plan.write_v2(df.plan, "t", mode: :overwrite, condition: scalar)
      end

      assert_raise ArgumentError, ~r/partition_by: a subquery cannot travel/, fn ->
        Plan.write_v2(df.plan, "t", mode: :create, partition_by: [scalar])
      end
    end

    test ":path and :table are a oneof", %{df: df} do
      assert_raise ArgumentError, ~r/never both/, fn ->
        Plan.write(df.plan, path: "/a", table: {"t", :save_as_table})
      end
    end

    test "an unknown save mode lists the spellings", %{df: df} do
      assert_raise ArgumentError, ~r/append/, fn ->
        DataFrame.write_command(df, path: "/a", mode: :errorifexists)
      end
    end

    test ":bucket_by is {buckets, columns}", %{df: df} do
      assert_raise ArgumentError, ~r/bucket_by/, fn ->
        DataFrame.write_command(df, path: "/a", bucket_by: [4, :id])
      end
    end

    test "write_v2 needs a mode", %{df: df} do
      assert_raise ArgumentError, ~r/:mode/, fn ->
        DataFrame.write_v2_command(df, "t", using: "parquet")
      end
    end

    test "a condition without :overwrite is refused", %{df: df} do
      assert_raise ArgumentError, ~r/overwrite/, fn ->
        DataFrame.write_v2_command(df, "t", mode: :append, condition: Column.greater(:id, 3))
      end
    end
  end
end
