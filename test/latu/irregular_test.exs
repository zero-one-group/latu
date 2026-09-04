defmodule Latu.IrregularTest do
  use ExUnit.Case, async: true

  import Latu.Column
  import Latu.Wire

  alias Latu.Functions, as: F
  alias Latu.Protocol.Spark.Connect, as: Proto
  alias Latu.Session
  alias Latu.Window, as: W

  # The functions no registry shape covers. Every one of them encodes cleanly when it is wrong —
  # a reversed argument order, a wire name that should have changed with the arity, a default
  # that should or should not have been sent — so each has a fixture rather than a reading.
  setup do
    {:ok, session: Session.from_url!("sc://localhost:15002")}
  end

  describe "against PySpark" do
    test "trim sends its characters first", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(t: F.trim("xxhixx", "x"))
      |> assert_wire("trim_chars")
    end

    test "log changes its wire name with the arity", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(a: F.log(8.0), b: F.log(2.0, 8.0))
      |> assert_wire("log_forms")
    end

    test "lag always sends the offset and only sometimes the fallback", %{session: session} do
      window = W.partition_by([:id]) |> W.order_by([:id])

      session
      |> Latu.range(10)
      |> Latu.with_columns(p: over(F.lag(:id, 2, 0), window))
      |> assert_wire("lag_default")
    end

    test "convert_timezone omits its optional first argument", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(c: F.convert_timezone("Asia/Jakarta", "2026-01-01 00:00:00"))
      |> assert_wire("convert_timezone_2")
    end

    test "unix_timestamp fills in Spark's format", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(u: F.unix_timestamp("2026-01-02 03:04:05"))
      |> assert_wire("unix_timestamp_format")
    end

    test "a seed given explicitly", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(r: F.rand(42))
      |> assert_wire("rand_seeded")
    end
  end

  describe "against PySpark, the last irregulars" do
    test "call_function is a different wire node", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(a: F.call_function("abs", [-1]))
      |> assert_wire("call_function")
    end

    test "make_timestamp is overloaded on arity alone", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(t: F.make_timestamp(2026, 1, 2, 3, 4, 5.0))
      |> assert_wire("make_timestamp_parts")
    end

    test "window takes a duration and optionally a slide", %{session: session} do
      time = cast("2026-01-01 00:00:00", "timestamp")

      session
      |> Latu.range(1)
      |> Latu.select(w: F.window(time, "10 minutes", "5 minutes"))
      |> assert_wire("window_slide")
    end

    test "listagg_distinct is distinct with an optional delimiter", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(l: F.listagg_distinct("a", ","))
      |> assert_wire("listagg_distinct_delim")
    end

    test "a generated sketch function fills its substituted defaults", %{session: session} do
      # One of the 46 the extractor could not see until it learned to follow a wire name held
      # in a local variable. Generated, not hand-written — this checks the reading.
      session
      |> Latu.range(1)
      |> Latu.select(s: F.tuple_sketch_agg_integer(1, 2))
      |> assert_wire("sketch_defaults")
    end
  end

  describe "the parsing family against PySpark" do
    test "from_json sends the schema string as a literal", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(j: F.from_json(:s, "a INT"))
      |> assert_wire("from_json_fn")
    end

    test "options ride as a map function call, in order", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(j: F.from_json(:s, "a INT", allow_comments: true, mode: "PERMISSIVE"))
      |> assert_wire("from_json_options")
    end

    test "from_csv and from_xml take the same shape", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(c: F.from_csv(:s, "a INT, b STRING"))
      |> assert_wire("from_csv_fn")

      session
      |> Latu.range(1)
      |> Latu.select(x: F.from_xml(:s, "a INT"))
      |> assert_wire("from_xml_fn")
    end

    test "to_json, with and without options", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(j: F.to_json(F.struct([:id])))
      |> assert_wire("to_json_fn")

      session
      |> Latu.range(1)
      |> Latu.select(j: F.to_json(F.struct([:id]), ignore_null_fields: false))
      |> assert_wire("to_json_options")
    end

    test "to_csv and to_xml", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(c: F.to_csv(F.struct([:id])))
      |> assert_wire("to_csv_fn")

      session
      |> Latu.range(1)
      |> Latu.select(x: F.to_xml(F.struct([:id])))
      |> assert_wire("to_xml_fn")
    end

    test "schema_of_json takes the sample string as a literal", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(s: F.schema_of_json(~S({"a": 1})))
      |> assert_wire("schema_of_json_fn")
    end

    test "schema_of_csv with options, schema_of_xml without", %{session: session} do
      session
      |> Latu.range(1)
      |> Latu.select(s: F.schema_of_csv("1;a", sep: ";"))
      |> assert_wire("schema_of_csv_options")

      session
      |> Latu.range(1)
      |> Latu.select(s: F.schema_of_xml("<r><a>1</a></r>"))
      |> assert_wire("schema_of_xml_fn")
    end

    test "a bare atom is refused where a schema belongs" do
      assert_raise ArgumentError, ~r/not a column name/, fn -> F.from_json(:s, :schema) end
    end
  end

  describe "shapes that cannot have a fixture" do
    test "an omitted seed is drawn, so the plan differs between builds" do
      # The reason there is no golden test for the seedless form, stated as an assertion rather
      # than a comment. `Latu.sample/3` behaves the same way, deliberately.
      refute arguments(F.rand()) == arguments(F.rand())
      assert arguments(F.rand(42)) == arguments(F.rand(42))
    end

    test "every seeded function sends a seed either way" do
      for {built, seeded} <- [
            {F.rand(), F.rand(1)},
            {F.randn(), F.randn(1)},
            {F.uuid(), F.uuid(1)},
            {F.randstr(4), F.randstr(4, 1)},
            {F.shuffle(:xs), F.shuffle(:xs, 1)},
            {F.uniform(1, 9), F.uniform(1, 9, 1)},
            {F.count_min_sketch(:x, 0.1, 0.9), F.count_min_sketch(:x, 0.1, 0.9, 1)}
          ] do
        assert length(arguments(built)) == length(arguments(seeded))
      end
    end

    test "mean is avg on the wire" do
      assert %Proto.Expression{expr_type: {:unresolved_function, call}} = F.mean(:x)
      assert call.function_name == "avg"
    end
  end

  defp arguments(%Proto.Expression{expr_type: {:unresolved_function, call}}), do: call.arguments
end
