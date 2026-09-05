defmodule Latu.Integration.RelationsTest do
  use ExUnit.Case, async: true

  # The last of the in-scope `Relation` arms against a real server. Several are questions rather
  # than smoke tests:
  #
  #   * **`col_regex/2`** needs Spark's backtick-quoted pattern syntax to be what the analyzer
  #     wants. Nothing offline can say whether a given pattern matches anything.
  #   * **`metadata_column/2`** asks for a column that is not in the schema. Whether
  #     `_metadata` exists at all depends on the source being a file source.
  #   * **`parse/2`**'s `:schema` goes through `parse_ddl_type/2`, because PySpark parses the
  #     DDL client-side and Latu will not.
  #   * **`table_function/3`** covers `spark.tvf`'s whole surface generically; whether any given
  #     name resolves is the server's business.
  #   * **`table_changes/3`** wants a change feed, which a plain parquet table does not have.
  #     The honest assertion is that the server refuses it *for that reason*.
  #   * **`zip_with_index/2`** rests on `distributed_sequence_id` resolving at all — it is
  #     registered with `registerInternalExpression`, so nothing in the catalog lists it.
  #
  # Needs a Spark Connect server on :15002 and the fixtures generated.
  alias Latu.Column
  alias Latu.Functions, as: F

  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session, release: true) end)

    %{session: session, people: Latu.read(session, format: "csv", path: "/fixtures/people.csv")}
  end

  describe "col_regex/2" do
    test "matches by pattern, and the backticks are Spark's syntax", %{session: session} do
      df = Latu.select_expr(Latu.range(session, 3), ["id", "id * 2 as id_doubled", "1 as other"])

      assert df |> Latu.select(Latu.col_regex(df, "`id.*`")) |> Latu.columns!() ==
               ["id", "id_doubled"]
    end

    test "a pattern matching nothing gives no columns, not an error", %{session: session} do
      df = Latu.range(session, 3)

      assert Latu.columns!(Latu.select(df, Latu.col_regex(df, "`nosuch.*`"))) == []
    end
  end

  describe "metadata_column/2" do
    test "a file source carries _metadata, which the schema does not list", %{people: people} do
      refute "_metadata" in Latu.columns!(people)

      rows =
        people
        |> Latu.select(path: Column.expr("_metadata.file_path"))
        |> Latu.limit(1)
        |> Latu.collect!()

      assert [%{path: path}] = rows
      assert path =~ "people.csv"
    end

    test "metadata_column/2 asks for it by name", %{people: people} do
      assert people
             |> Latu.select(meta: Latu.metadata_column(people, "_metadata"))
             |> Latu.limit(1)
             |> Latu.count!() == 1
    end
  end

  describe "with_metadata/3" do
    test "the data is untouched", %{session: session} do
      df = Latu.range(session, 3)
      tagged = Latu.with_metadata(df, :id, %{"comment" => "the key"})

      assert Latu.collect!(tagged) == Latu.collect!(df)
      assert Latu.dtypes!(tagged) == Latu.dtypes!(df)
    end
  end

  describe "repartition_by_range/3" do
    test "the rows are the same, however they are partitioned", %{session: session} do
      df = Latu.range(session, 20)

      assert df
             |> Latu.repartition_by_range(:id, num_partitions: 4)
             |> Latu.sort([:id])
             |> Latu.collect!() == Latu.collect!(Latu.sort(df, [:id]))
    end

    test "a descending key is accepted", %{session: session} do
      assert session
             |> Latu.range(10)
             |> Latu.repartition_by_range(Column.desc(:id), num_partitions: 2)
             |> Latu.count!() == 10
    end
  end

  describe "cross_join/2" do
    test "every pairing", %{session: session} do
      assert session
             |> Latu.range(4)
             |> Latu.cross_join(Latu.range(session, 3))
             |> Latu.count!() == 12
    end
  end

  describe "parse/2" do
    test "json strings become a structured frame", %{session: session} do
      strings =
        Latu.select_expr(Latu.range(session, 3), [
          ~s|concat('{"n": ', id, ', "label": "row', id, '"}') as value|
        ])

      parsed = Latu.parse(strings, format: :json)

      assert Latu.dtypes!(parsed) == [{"label", "string"}, {"n", "bigint"}]

      assert parsed |> Latu.sort([:n]) |> Latu.collect!() == [
               %{n: 0, label: "row0"},
               %{n: 1, label: "row1"},
               %{n: 2, label: "row2"}
             ]
    end

    test "an explicit schema goes through parse_ddl_type/2", %{session: session} do
      strings = Latu.select_expr(Latu.range(session, 2), [~s|concat('{"n": ', id, '}') as value|])

      parsed =
        Latu.parse(strings,
          format: :json,
          schema: Latu.parse_ddl_type!(session, "n INT")
        )

      assert Latu.dtypes!(parsed) == [{"n", "int"}]
    end

    test "csv strings parse too", %{session: session} do
      strings = Latu.select_expr(Latu.range(session, 2), [~s|concat(id, ',x') as value|])

      assert strings |> Latu.parse(format: :csv) |> Latu.count!() == 2
    end
  end

  describe "table_function/3" do
    test "explode over an array literal", %{session: session} do
      rows =
        session
        |> Latu.table_function("explode", [F.array([Column.lit(1), Column.lit(2)])])
        |> Latu.collect!()

      assert length(rows) == 2
    end

    test "one that takes no arguments", %{session: session} do
      assert session |> Latu.table_function("sql_keywords") |> Latu.count!() > 0
    end

    test "an unknown function is the server's error", %{session: session} do
      assert {:error, %Latu.Error{kind: :rpc}} =
               Latu.collect(Latu.table_function(session, "no_such_tvf"))
    end
  end

  describe "zip_with_index/2" do
    test "indices are consecutive from zero across the whole frame", %{session: session} do
      rows = Latu.collect!(Latu.zip_with_index(Latu.range(session, 5)))

      assert Enum.map(rows, & &1.index) == [0, 1, 2, 3, 4]
      assert Enum.map(rows, & &1.id) == [0, 1, 2, 3, 4]
    end

    test "the index column is named as asked, and is a long", %{session: session} do
      df = Latu.zip_with_index(Latu.range(session, 3), :row_num)

      assert Latu.dtypes!(df) == [{"id", "bigint"}, {"row_num", "bigint"}]
    end
  end

  describe "table_changes/3" do
    test "a plain table has no change feed, and the server says so", %{session: session} do
      table = "changes_#{System.system_time(:millisecond)}"

      :ok = Latu.save_as_table(Latu.range(session, 3), table, mode: :overwrite)

      assert {:error, %Latu.Error{kind: :rpc}} = Latu.collect(Latu.table_changes(session, table))
    end
  end
end
