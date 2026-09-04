defmodule Latu.Integration.AnalyzeTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO
  import Latu.Column

  alias Latu.Functions, as: F

  # The schema arms, and the one thing no fixture can vouch for: that Latu names a type the
  # way Spark does. `typeof` is the server's own answer, so it is the oracle for
  # Latu.Result.Schema.simple_string/1 rather than a second opinion written by hand.
  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session) end)
    %{session: session}
  end

  describe "schema/1, columns/1, dtypes/1" do
    test "a schema is names, types and nullability", %{session: session} do
      df = Latu.select(Latu.range(session, 5), id: :id, doubled: multiply(:id, 2))

      assert Latu.schema!(df) == [
               %{name: "id", type: "bigint", nullable: false},
               %{name: "doubled", type: "bigint", nullable: false}
             ]
    end

    test "columns and dtypes are the same answer, narrowed", %{session: session} do
      df = Latu.select(Latu.range(session, 5), id: :id, name: lit("a"))

      assert Latu.columns!(df) == ["id", "name"]
      assert Latu.dtypes!(df) == [{"id", "bigint"}, {"name", "string"}]
    end

    test "analysis needs no execution: a plan that cannot run still has a schema", %{
      session: session
    } do
      df = Latu.filter(Latu.range(session, 5), expr("1 / 0 > 0"))

      assert Latu.columns!(df) == ["id"]
    end

    test "a plan the server cannot analyse is an error, not a raise", %{session: session} do
      df = Latu.select(Latu.range(session, 5), [:nope])

      assert {:error, %Latu.Error{}} = Latu.schema(df)
    end

    test "nullability is the server's, not a guess", %{session: session} do
      df = Latu.select(Latu.range(session, 1), never: :id, sometimes: expr("nullif(id, 0)"))

      assert [%{nullable: false}, %{nullable: true}] = Latu.schema!(df)
    end
  end

  describe "simple_string against Spark's own typeof" do
    test "every type Latu renders is the name Spark gives it", %{session: session} do
      df =
        session
        |> Latu.range(1)
        |> Latu.select(
          bool: expr("true"),
          byte: cast(1, "tinyint"),
          short: cast(1, "smallint"),
          int: cast(1, "int"),
          long: :id,
          float: cast(1.5, "float"),
          double: cast(1.5, "double"),
          dec: cast("1.50", "decimal(10,2)"),
          str: expr("'a'"),
          bin: cast("a", "binary"),
          d: cast("2026-01-02", "date"),
          ts: cast("2026-01-02 03:04:05", "timestamp"),
          ts_ntz: cast("2026-01-02 03:04:05", "timestamp_ntz"),
          nul: expr("NULL"),
          arr: expr("array(1, 2)"),
          nested: expr("array(map('k', array(1)))"),
          strct: expr("named_struct('a', 1, 'b', 'x')"),
          mp: expr("map('k', 1)"),
          dur: expr("INTERVAL '1 02:03:04' DAY TO SECOND"),
          ym: expr("INTERVAL '1-2' YEAR TO MONTH")
        )

      names = Latu.columns!(df)
      asked = for name <- names, do: {String.to_atom(name), expr("typeof(#{name})")}
      [answers] = Latu.collect!(Latu.select(df, asked))

      assert Map.new(Latu.dtypes!(df)) ==
               Map.new(answers, fn {name, type} -> {to_string(name), type} end)
    end

    test "char and varchar do not survive a query's output schema — Spark erases them", %{
      session: session
    } do
      df =
        Latu.select(Latu.range(session, 1), c: cast("a", "char(3)"), v: cast("a", "varchar(3)"))

      # Catalyst's `CharVarcharUtils.replaceCharVarcharWithStringInSchema` rewrites both to
      # StringType on the analysed plan, keeping the original spelling only in the field's
      # metadata — which PySpark's own `dtypes` does not read either. So this is parity, and
      # `simple_string/1` still renders `char(n)`/`varchar(n)` for the place they do survive: a
      # DDL parse.
      assert Latu.dtypes!(df) == [{"c", "string"}, {"v", "string"}]
    end
  end

  describe "print_schema/2 and tree_string/2" do
    test "the tree is Spark's, rendered server-side", %{session: session} do
      df = Latu.range(session, 5)

      assert Latu.tree_string!(df) =~ "root"
      assert Latu.tree_string!(df) =~ "|-- id: long (nullable = false)"
    end

    test "level bounds the depth, so a nested field disappears", %{session: session} do
      df = Latu.select(Latu.range(session, 1), who: expr("named_struct('inner', 1)"))

      assert Latu.tree_string!(df) =~ "inner"
      refute Latu.tree_string!(df, level: 1) =~ "inner"
    end

    test "print_schema writes the tree and returns :ok", %{session: session} do
      df = Latu.range(session, 5)

      printed = capture_io(fn -> assert Latu.print_schema!(df) == :ok end)

      assert printed == Latu.tree_string!(df)
    end
  end

  describe "explain/2" do
    test "the default mode is the physical plan", %{session: session} do
      plan = Latu.explain_string!(Latu.range(session, 5))

      assert plan =~ "Physical Plan"
      assert plan =~ "Range"
    end

    test "extended says more than simple does", %{session: session} do
      df = Latu.range(session, 5)

      assert String.length(Latu.explain_string!(df, mode: :extended)) >
               String.length(Latu.explain_string!(df))

      assert Latu.explain_string!(df, mode: :extended) =~ "Analyzed Logical Plan"
    end

    test "every mode the server accepts", %{session: session} do
      df = Latu.range(session, 5)

      for mode <- [:simple, :extended, :codegen, :cost, :formatted] do
        assert {:ok, string} = Latu.explain_string(df, mode: mode)
        assert is_binary(string) and string != ""
      end
    end

    test "explain prints what explain_string returns", %{session: session} do
      df = Latu.range(session, 5)

      printed = capture_io(fn -> assert Latu.explain!(df) == :ok end)

      assert printed == Latu.explain_string!(df)
    end
  end

  describe "the boolean arms" do
    test "a range is not local — it needs a cluster to run", %{session: session} do
      refute Latu.is_local!(Latu.range(session, 5))
    end

    test "a frame built from local data is", %{session: session} do
      df = Latu.create_dataframe!(session, [%{id: 1}])

      assert Latu.is_local!(df)
    end

    test "nothing Latu builds is streaming", %{session: session} do
      refute Latu.is_streaming!(Latu.range(session, 5))
    end

    test "is_empty asks the server, and a filter that keeps nothing is empty", %{
      session: session
    } do
      refute Latu.is_empty!(Latu.range(session, 5))
      assert Latu.is_empty!(Latu.range(session, 0))
      assert Latu.range(session, 5) |> Latu.filter(greater(:id, 99)) |> Latu.is_empty!()
    end
  end

  describe "input_files/1" do
    test "a computed frame reads nothing", %{session: session} do
      assert Latu.input_files!(Latu.range(session, 5)) == []
    end

    test "a parquet read names its file", %{session: session} do
      df = Latu.read(session, format: "parquet", path: "/fixtures/people.parquet")
      [file] = Latu.input_files!(df)

      assert file =~ "people.parquet"
    end
  end

  describe "same_semantics/2 and semantic_hash/1" do
    test "two frames built separately compute the same thing", %{session: session} do
      one = Latu.select(Latu.range(session, 10), [:id])
      two = Latu.select(Latu.range(session, 10), [:id])

      assert Latu.same_semantics!(one, two)
      assert Latu.semantic_hash!(one) == Latu.semantic_hash!(two)
    end

    test "and a different expression does not", %{session: session} do
      one = Latu.select(Latu.range(session, 10), x: :id)
      two = Latu.select(Latu.range(session, 10), x: multiply(:id, 2))

      refute Latu.same_semantics!(one, two)
    end

    test "two sessions are refused before the server sees it", %{session: session} do
      other = Latu.connect!(@url)
      on_exit(fn -> Latu.disconnect(other) end)

      assert_raise ArgumentError, ~r/session/, fn ->
        Latu.same_semantics(Latu.range(session, 5), Latu.range(other, 5))
      end
    end
  end

  describe "persist, cache and unpersist" do
    test "an uncached frame is stored nowhere", %{session: session} do
      level = Latu.storage_level!(Latu.range(session, 5))

      assert %{use_memory: false, use_disk: false} = level
    end

    test "cache registers it at Spark's default level, and pipes", %{session: session} do
      df = Latu.range(session, 5)

      assert Latu.cache!(df) |> Latu.count!() == 5
      assert %{name: :memory_and_disk_deser} = Latu.storage_level!(df)

      assert {:ok, ^df} = Latu.unpersist(df)
      assert %{use_memory: false, use_disk: false} = Latu.storage_level!(df)
    end

    test "persisting computes nothing — the plan whose execution would raise still persists", %{
      session: session
    } do
      # Verified against the server (SparkConnectAnalyzeHandler): the PERSIST arm does
      # `Dataset.ofRows(...).persist(level)`, which registers the plan with the CacheManager
      # and materialises nothing. So caching stays lazy over Connect exactly as in Scala, and
      # the round trip is analysis, not execution.
      boom = Latu.select(Latu.range(session, 5), x: F.raise_error(lit("boom")))

      assert {:ok, _df} = Latu.persist(boom)
      assert {:error, %Latu.Error{}} = Latu.collect(boom)

      Latu.unpersist!(boom)
    end

    test "a persist mid-pipeline changes nothing about what comes out", %{session: session} do
      before = session |> Latu.range(10) |> Latu.filter(greater(:id, 2))
      after_persist = before |> Latu.persist!() |> Latu.filter(less(:id, 8)) |> Latu.select([:id])

      assert Latu.collect!(after_persist) == Enum.map(3..7, &%{id: &1})

      Latu.unpersist!(before)
    end

    test "persist takes one of Spark's names", %{session: session} do
      df = Latu.range(session, 6)

      assert %{name: :disk_only} = df |> Latu.persist!(level: :disk_only) |> Latu.storage_level!()

      Latu.unpersist!(df)
    end

    test "blocking: true is accepted", %{session: session} do
      df = Latu.range(session, 7)

      Latu.cache!(df)

      assert %{use_memory: false} = df |> Latu.unpersist!(blocking: true) |> Latu.storage_level!()
    end
  end

  describe "parse_ddl/2" do
    test "a DDL string comes back in schema/1's shape", %{session: session} do
      assert Latu.parse_ddl!(session, "id INT, tags ARRAY<STRING>") == [
               %{name: "id", type: "int", nullable: true},
               %{name: "tags", type: "array<string>", nullable: true}
             ]
    end

    test "and char and varchar survive a parse, where a query schema erases them", %{
      session: session
    } do
      assert [%{type: "char(3)"}, %{type: "varchar(5)"}] =
               Latu.parse_ddl!(session, "c CHAR(3), v VARCHAR(5)")
    end

    test "a broken schema is an error, not a raise", %{session: session} do
      assert {:error, %Latu.Error{}} = Latu.parse_ddl(session, "id NOT A TYPE")
    end

    test "the other direction, which is not API but is the only test of that arm", %{
      session: session
    } do
      json =
        ~s({"type":"struct","fields":) <>
          ~s([{"name":"id","type":"integer","nullable":true,"metadata":{}}]})

      assert {:ok, ddl} = Latu.to_ddl(session, json)
      assert ddl =~ "id"
    end
  end
end
