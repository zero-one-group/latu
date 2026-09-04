defmodule Latu.DataFrameTest do
  use ExUnit.Case, async: true

  import Latu.Wire

  alias Latu.Column
  alias Latu.DataFrame
  alias Latu.Functions, as: F
  alias Latu.Plan
  alias Latu.Protocol.Spark.Connect, as: Proto
  alias Latu.Session

  # Golden plans come from PySpark: python dev/pyspark_oracle.py --generate

  setup do
    %{session: Session.from_url!("sc://h")}
  end

  describe "select/2" do
    test "takes a name as a string or an atom", %{session: session} do
      assert_wire(Latu.select(Latu.range(session, 10), [:id]), "project_cols")
      assert_wire(Latu.select(Latu.range(session, 10), ["id"]), "project_cols")
    end

    test "takes one column without a list", %{session: session} do
      assert_wire(Latu.select(Latu.range(session, 10), :id), "project_cols")
    end

    test "reads * as every column, not as a column called *", %{session: session} do
      assert_wire(Latu.select(Latu.range(session, 10), "*"), "project_star")
      assert_wire(Latu.select(Latu.range(session, 10), :*), "project_star")
    end

    test "names a trailing keyword's expression", %{session: session} do
      df = Latu.select(Latu.range(session, 10), id_plus_1: Column.add(:id, 1))

      assert_wire(df, "project_expr")
    end

    test "keyword sugar is the explicit spelling", %{session: session} do
      input = Latu.range(session, 10)
      sugar = Latu.select(input, [:id, doubled: Column.multiply(:id, 2)])
      explicit = Latu.select(input, [Column.col(:id), Plan.as(Column.multiply(:id, 2), :doubled)])

      assert Plan.normalize_ids(sugar.plan) == Plan.normalize_ids(explicit.plan)
    end

    test "says what a name should look like", %{session: session} do
      assert_raise ArgumentError, ~r/string or an atom/, fn ->
        Latu.select(Latu.range(session, 10), [{"id", 1}])
      end
    end
  end

  describe "filter/2" do
    test "takes an expression", %{session: session} do
      df = Latu.filter(Latu.range(session, 10), Column.greater(:id, 3))

      assert_wire(df, "filter_gt")
    end

    test "reads a string as SQL", %{session: session} do
      assert_wire(Latu.filter(Latu.range(session, 10), "id > 3"), "filter_sql_string")
    end

    test "takes a combined predicate", %{session: session} do
      condition = Column.all([Column.greater(:id, 3), Column.less(:id, 8)])

      assert_wire(Latu.filter(Latu.range(session, 10), condition), "filter_and")
    end

    test "compares a column to a string literal inside an expression", %{session: session} do
      df =
        session
        |> Latu.range(10)
        |> Latu.select(cat: Column.lit("books"))
        |> Latu.filter(Column.equal(:cat, "books"))

      assert_wire(df, "filter_string_eq")
    end

    test "where/2 is the same verb", %{session: session} do
      assert_wire(Latu.where(Latu.range(session, 10), Column.greater(:id, 3)), "filter_gt")
    end
  end

  describe "with_columns/2" do
    test "one column is Spark's withColumn", %{session: session} do
      df = Latu.with_columns(Latu.range(session, 10), double: Column.multiply(:id, 2))

      assert_wire(df, "with_column")
    end

    test "several, in order", %{session: session} do
      df =
        Latu.with_columns(Latu.range(session, 10),
          a: Column.add(:id, 1),
          b: Column.subtract(:id, 1)
        )

      assert_wire(df, "with_columns")
    end

    test "values are coerced", %{session: session} do
      df = Latu.with_columns(Latu.range(session, 10), x: 1)

      assert [column] = df.plan.rel_type |> elem(1) |> Map.fetch!(:aliases)
      assert column.name == ["x"]
      assert {:literal, %{literal_type: {:integer, 1}}} = column.expr.expr_type
    end

    test "refuses an unordered map", %{session: session} do
      assert_raise ArgumentError, ~r/ordered/, fn ->
        Latu.with_columns(Latu.range(session, 10), %{x: 1})
      end
    end
  end

  describe "drop/2" do
    test "takes names", %{session: session} do
      df =
        session
        |> Latu.range(10)
        |> Latu.with_columns(x: Column.lit(1))
        |> Latu.drop(:x)

      assert_wire(df, "drop")
    end

    test "sends a name as a name and an expression as an expression", %{session: session} do
      input = Latu.range(session, 10)

      assert %{column_names: ["a"], columns: []} = dropped(Latu.drop(input, "a"))
      assert %{column_names: [], columns: [_]} = dropped(Latu.drop(input, Column.col(:a)))
    end
  end

  describe "rename/2" do
    test "a list renames every column, positionally", %{session: session} do
      assert_wire(Latu.rename(Latu.range(session, 10), [:renamed]), "to_df")
    end

    test "pairs rename by mapping", %{session: session} do
      assert_wire(Latu.rename(Latu.range(session, 10), id: :n), "rename")
      assert_wire(Latu.rename(Latu.range(session, 10), [{"id", "n"}]), "rename")
      assert_wire(Latu.rename(Latu.range(session, 10), %{"id" => "n"}), "rename")
    end
  end

  describe "row verbs" do
    test "limit", %{session: session} do
      assert_wire(Latu.limit(Latu.range(session, 10), 3), "limit")
    end

    test "offset", %{session: session} do
      assert_wire(Latu.offset(Latu.range(session, 10), 3), "offset")
    end

    test "distinct keys on every column", %{session: session} do
      assert_wire(Latu.distinct(Latu.range(session, 10)), "distinct")
    end

    test "distinct keys on a subset", %{session: session} do
      assert_wire(Latu.distinct(Latu.range(session, 10), :id), "drop_duplicates_subset")
      assert_wire(Latu.distinct(Latu.range(session, 10), [:id]), "drop_duplicates_subset")
    end
  end

  describe "sort/2" do
    test "a bare name is ascending, nulls first", %{session: session} do
      assert_wire(Latu.sort(Latu.range(session, 10), "id"), "sort_asc")
      assert_wire(Latu.sort(Latu.range(session, 10), :id), "sort_asc")
      assert_wire(Latu.order_by(Latu.range(session, 10), :id), "sort_asc")
    end

    test "asc/1 says the same thing explicitly", %{session: session} do
      assert_wire(Latu.sort(Latu.range(session, 10), Column.asc(:id)), "sort_asc")
      assert_wire(Latu.sort(Latu.range(session, 10), [Column.asc_nulls_first(:id)]), "sort_asc")
    end

    test "desc/1 means nulls last, as SQL and PySpark do", %{session: session} do
      assert_wire(Latu.sort(Latu.range(session, 10), Column.desc(:id)), "sort_desc_nulls_last")

      assert_wire(
        Latu.sort(Latu.range(session, 10), Column.desc_nulls_last(:id)),
        "sort_desc_nulls_last"
      )
    end

    test "keys keep their order", %{session: session} do
      df = Latu.sort(Latu.range(session, 10), [Column.desc(:id), :other])

      assert [first, second] = df.plan.rel_type |> elem(1) |> Map.fetch!(:order)
      assert first.direction == :SORT_DIRECTION_DESCENDING
      assert second.direction == :SORT_DIRECTION_ASCENDING
    end

    test "a sort key is not an expression", %{session: session} do
      assert_raise ArgumentError, ~r/belongs in sort\/2/, fn ->
        Latu.select(Latu.range(session, 10), x: Column.asc(:id))
      end
    end
  end

  describe "sample/3" do
    test "an explicit seed", %{session: session} do
      assert_wire(Latu.sample(Latu.range(session, 100), 0.1, seed: 42), "sample")
    end

    test "no seed draws one, so the plan is not reproducible", %{session: session} do
      first = Latu.sample(Latu.range(session, 100), 0.1).plan.rel_type |> elem(1)
      second = Latu.sample(Latu.range(session, 100), 0.1).plan.rel_type |> elem(1)

      assert is_integer(first.seed)
      assert first.seed != second.seed
    end

    test "with_replacement is off unless asked", %{session: session} do
      df = Latu.sample(Latu.range(session, 100), 0.1, with_replacement: true)

      assert %{with_replacement: true, lower_bound: +0.0, upper_bound: 0.1} =
               df.plan.rel_type |> elem(1)
    end

    test "an integer fraction becomes a double", %{session: session} do
      df = Latu.sample(Latu.range(session, 100), 1, seed: 1)

      assert df.plan.rel_type |> elem(1) |> Map.fetch!(:upper_bound) === 1.0
    end
  end

  describe "repartition/2,3" do
    test "a count shuffles", %{session: session} do
      assert_wire(Latu.repartition(Latu.range(session, 10), 4), "repartition_n")
    end

    test "a count and columns partition by expression", %{session: session} do
      assert_wire(Latu.repartition(Latu.range(session, 10), 4, :id), "repartition_by")
      assert_wire(Latu.repartition(Latu.range(session, 10), 4, [:id]), "repartition_by")
    end

    test "columns alone leave the count to Spark", %{session: session} do
      df = Latu.repartition(Latu.range(session, 10), [:id])

      assert %{num_partitions: nil, partition_exprs: [_]} = df.plan.rel_type |> elem(1)
    end
  end

  describe "set operations" do
    test "union keeps duplicates, as Spark's does", %{session: session} do
      left = Latu.range(session, 5)
      right = Latu.range(session, 5)

      assert_wire(Latu.union(left, right), "union")
    end

    test "union by name", %{session: session} do
      df = Latu.union(Latu.range(session, 5), Latu.range(session, 5), by_name: true)

      assert_wire(df, "union_by_name")
    end

    test "union by name, filling what is missing", %{session: session} do
      df =
        Latu.union(Latu.range(session, 5), Latu.range(session, 5),
          by_name: true,
          allow_missing_columns: true
        )

      assert_wire(df, "union_by_name_missing")
    end

    test "intersect is distinct, and all: true is intersectAll", %{session: session} do
      left = Latu.range(session, 5)
      right = Latu.range(session, 3)

      assert_wire(Latu.intersect(left, right), "intersect")
      assert_wire(Latu.intersect(left, right, all: true), "intersect_all")
    end

    test "except is distinct, and all: true is exceptAll", %{session: session} do
      left = Latu.range(session, 5)
      right = Latu.range(session, 3)

      assert_wire(Latu.except(left, right), "subtract")
      assert_wire(Latu.except(left, right, all: true), "except_all")
    end

    test "allow_missing_columns needs by_name", %{session: session} do
      left = Latu.range(session, 5)

      assert_raise ArgumentError, ~r/needs by_name/, fn ->
        Latu.union(left, left, allow_missing_columns: true)
      end
    end

    test "by_name applies to union only", %{session: session} do
      left = Latu.range(session, 5)

      assert_raise ArgumentError, ~r/applies to :union only/, fn ->
        Latu.intersect(left, left, by_name: true)
      end
    end

    test "two sessions are refused here rather than by the server", %{session: session} do
      other = Session.from_url!("sc://elsewhere")

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        Latu.union(Latu.range(session, 5), Latu.range(other, 5))
      end
    end
  end

  describe "partition verbs" do
    test "sort_within_partitions is not global", %{session: session} do
      df = Latu.sort_within_partitions(Latu.range(session, 10), "id")

      assert_wire(df, "sort_within_partitions")
    end

    test "coalesce does not shuffle", %{session: session} do
      assert_wire(Latu.coalesce(Latu.range(session, 10), 2), "coalesce")
    end
  end

  describe "as/2" do
    test "names the DataFrame", %{session: session} do
      assert_wire(Latu.as(Latu.range(session, 10), "a"), "alias_df")
    end
  end

  describe "actions, offline" do
    test "count sends PySpark's exact aggregate — count(1), unaliased", %{session: session} do
      assert_wire(DataFrame.count_plan(Latu.range(session, 10)), "count_action")
    end

    test "collect validates its keys option before any network", %{session: session} do
      df = Latu.range(session, 3)

      assert_raise ArgumentError, ~r/keys: :atoms or :strings/, fn ->
        Latu.collect(df, keys: :charlists)
      end

      assert_raise ArgumentError, fn -> Latu.collect(df, atom_keys: true) end
    end

    test "take refuses a negative count", %{session: session} do
      assert_raise FunctionClauseError, fn -> Latu.take(Latu.range(session, 3), -1) end
    end

    test "actions on an unconnected session return the connect error", %{session: session} do
      df = Latu.range(session, 3)

      assert {:error, %Latu.Error{kind: :connect}} = Latu.collect(df)
      assert {:error, %Latu.Error{kind: :connect}} = Latu.count(df)
      assert {:error, %Latu.Error{kind: :connect}} = Latu.first(df)
      assert {:error, %Latu.Error{kind: :connect}} = Latu.to_explorer(df)
      assert {:error, %Latu.Error{kind: :connect}} = Latu.to_arrow(df)
      assert_raise Latu.Error, ~r/not connected/, fn -> Latu.count!(df) end
    end

    test "stream raises at the call site, not on the first element", %{session: session} do
      assert_raise Latu.Error, ~r/not connected/, fn -> Latu.stream(Latu.range(session, 3)) end
    end

    # `to_explorer/2` once took a `limit:`, and the session a `:to_explorer_limit`.
    # Spark bounds a result in the plan — `df.limit(n).collect()` — so `limit/2` is the only way
    # now, and this is the guard on the removal rather than on what replaced it: re-adding the
    # option would turn this red before anyone had to notice the docs were wrong.
    test "to_explorer takes no row limit of its own — that is limit/2's job", %{
      session: session
    } do
      df = Latu.range(session, 3)

      assert_raise ArgumentError, ~r/:limit/, fn -> Latu.to_explorer(df, limit: 5) end
      assert_raise ArgumentError, fn -> Latu.to_explorer(df, max_rows: 5) end
    end
  end

  test "verbs need no connection", %{session: session} do
    df =
      session
      |> Latu.range(10)
      |> Latu.filter("id > 3")
      |> Latu.with_columns(doubled: Column.multiply(:id, 2))
      |> Latu.distinct([:doubled])
      |> Latu.offset(1)
      |> Latu.limit(2)
      |> Latu.rename(id: :n)
      |> Latu.drop(:doubled)
      |> Latu.sort(Column.desc(:n))
      |> Latu.sample(0.5, seed: 1)
      |> Latu.repartition(2, :n)
      |> Latu.coalesce(1)
      |> Latu.sort_within_partitions(:n)
      |> Latu.as("a")
      |> Latu.select(:n)

    assert %DataFrame{session: %Session{channel: nil}} = df
  end

  describe "tail/2" do
    test "the Tail relation, which PySpark spells as an action", %{session: session} do
      assert_wire(DataFrame.tail_plan(Latu.range(session, 5), 3), "tail")
    end
  end

  describe "hint/3" do
    test "a bare join hint", %{session: session} do
      assert_wire(Latu.hint(Latu.range(session, 5), "broadcast"), "hint_broadcast")
    end

    test "an atom names the hint, as everywhere else", %{session: session} do
      assert_wire(Latu.hint(Latu.range(session, 5), :broadcast), "hint_broadcast")
    end

    test "parameters are literals, so a string stays a string", %{session: session} do
      assert_wire(Latu.hint(Latu.range(session, 5), "repartition", [4, "id"]), "hint_params")
    end

    test "an atom parameter is a column reference, where a string is a literal",
         %{session: session} do
      atoms = Latu.hint(Latu.range(session, 5), "repartition", [4, :id])
      strings = Latu.hint(Latu.range(session, 5), "repartition", [4, "id"])

      assert [_four, column] = elem(atoms.plan.rel_type, 1).parameters
      assert match?({:unresolved_attribute, _}, column.expr_type)
      assert [_four, literal] = elem(strings.plan.rel_type, 1).parameters
      assert match?({:literal, _}, literal.expr_type)
    end

    test "a nested list parameter is refused, as to_expr refuses every list",
         %{session: session} do
      df = Latu.range(session, 5)

      assert_raise ArgumentError, ~r/cannot make a Spark literal/, fn ->
        Latu.hint(df, "repartition", [[4]])
      end
    end
  end

  describe "unpivot/3" do
    setup %{session: session} do
      %{wide: session |> Latu.range(5) |> Latu.with_columns(a: Column.lit(1), b: Column.lit(2))}
    end

    test "named values", %{wide: wide} do
      wide
      |> Latu.unpivot([:id],
        values: [:a, :b],
        variable_column_name: "key",
        value_column_name: "val"
      )
      |> assert_wire("unpivot")
    end

    test "absent values is a different message from an empty list", %{session: session} do
      df = session |> Latu.range(5) |> Latu.with_columns(a: Column.lit(1))

      absent =
        Latu.unpivot(df, [:id], variable_column_name: "key", value_column_name: "val")

      empty =
        Latu.unpivot(df, [:id],
          values: [],
          variable_column_name: "key",
          value_column_name: "val"
        )

      assert_wire(absent, "unpivot_all")
      assert elem(absent.plan.rel_type, 1).values == nil
      assert elem(empty.plan.rel_type, 1).values != nil
    end

    test "both names are required", %{wide: wide} do
      assert_raise ArgumentError, ~r/variable_column_name is required/, fn ->
        Latu.unpivot(wide, [:id], value_column_name: "val")
      end

      assert_raise ArgumentError, ~r/value_column_name is required/, fn ->
        Latu.unpivot(wide, [:id], variable_column_name: "key")
      end
    end
  end

  describe "transpose/2" do
    test "with no index column", %{session: session} do
      assert_wire(Latu.transpose(Latu.range(session, 5)), "transpose")
    end

    test "with one", %{session: session} do
      session
      |> Latu.range(5)
      |> Latu.with_columns(a: Column.lit(1))
      |> Latu.transpose(:id)
      |> assert_wire("transpose_index")
    end
  end

  describe "select_expr/2" do
    test "a list of SQL strings", %{session: session} do
      session
      |> Latu.range(5)
      |> Latu.select_expr(["id", "id * 2 as doubled"])
      |> assert_wire("select_expr")
    end

    test "one string without a list", %{session: session} do
      df = Latu.select_expr(Latu.range(session, 5), "id")

      assert %DataFrame{} = df
      assert match?({:project, _}, df.plan.rel_type)
    end
  end

  describe "grouping_sets/3" do
    test "the fifth GroupType, with the grand total as an empty set", %{session: session} do
      session
      |> Latu.range(5)
      |> Latu.with_columns(a: Column.lit(1))
      |> Latu.grouping_sets([[:id, :a], [:id], []], [:id, :a])
      |> Latu.agg(n: F.count(Column.lit(1)))
      |> assert_wire("group_grouping_sets")
    end

    test "a set that is not a list says so", %{session: session} do
      grouped = Latu.grouping_sets(Latu.range(session, 5), [:id], [:id])

      assert_raise ArgumentError, ~r/each grouping set is a list/, fn ->
        Latu.agg(grouped, n: F.count(Column.lit(1)))
      end
    end
  end

  describe "col_regex/2 and metadata_column/2" do
    test "a regex is tagged to the frame it matches against", %{session: session} do
      df = Latu.range(session, 5)

      assert_wire(Latu.select(df, Latu.col_regex(df, "`id`")), "col_regex")
    end

    test "a metadata column is col/2 with the flag set", %{session: session} do
      df = Latu.range(session, 5)

      assert_wire(Latu.select(df, Latu.metadata_column(df, "_metadata")), "metadata_column")

      {:unresolved_attribute, plain} = Latu.col(df, "_metadata").expr_type
      {:unresolved_attribute, meta} = Latu.metadata_column(df, "_metadata").expr_type

      assert plain.is_metadata_column == false
      assert meta.is_metadata_column == true
      assert meta.plan_id == plain.plan_id
    end
  end

  # No golden fixture here: `Alias.metadata` is a JSON *string*, and `json.dumps` writes
  # `{"a": 1}` where `JSON.encode!` writes `{"a":1}` — with multi-key order differing on top.
  # Spark parses the string, so neither spelling is more correct and byte-identity is neither
  # available nor meaningful. So this asserts the message structure and the *decoded* metadata.
  # docs/deviations.md.
  describe "with_metadata/3" do
    test "one WithColumns alias, over a tagged reference to the column", %{session: session} do
      df = Latu.range(session, 5)
      tagged = Latu.with_metadata(df, :id, %{"a" => 1})

      assert {:with_columns, with_columns} = tagged.plan.rel_type
      assert with_columns.input == df.plan
      assert [%{name: ["id"], expr: expr, metadata: metadata}] = with_columns.aliases
      assert expr == Latu.col(df, :id)
      assert JSON.decode!(metadata) == %{"a" => 1}
    end

    test "several keys survive the round trip, whatever order they encode in", %{
      session: session
    } do
      meta = %{"comment" => "the key", "unit" => "count", "since" => "v2"}
      df = Latu.with_metadata(Latu.range(session, 5), :id, meta)
      {:with_columns, %{aliases: [%{metadata: metadata}]}} = df.plan.rel_type

      assert JSON.decode!(metadata) == meta
    end
  end

  describe "repartition_by_range/3" do
    test "sort orders, not bare expressions", %{session: session} do
      session
      |> Latu.range(5)
      |> Latu.repartition_by_range(:id, num_partitions: 4)
      |> assert_wire("repartition_by_range")
    end

    test "a sort key keeps its direction", %{session: session} do
      session
      |> Latu.range(5)
      |> Latu.repartition_by_range(Column.desc(:id), num_partitions: 2)
      |> assert_wire("repartition_by_range_desc")
    end

    test "the count is optional", %{session: session} do
      session
      |> Latu.range(5)
      |> Latu.repartition_by_range(:id)
      |> assert_wire("repartition_by_range_no_count")
    end
  end

  describe "cross_join/2" do
    test "a Join with no condition and how: :cross", %{session: session} do
      session
      |> Latu.range(5)
      |> Latu.cross_join(Latu.range(session, 3))
      |> assert_wire("cross_join")
    end

    test "two sessions are refused", %{session: session} do
      other = Session.from_url!("sc://elsewhere")

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        Latu.cross_join(Latu.range(session, 5), Latu.range(other, 5))
      end
    end
  end

  describe "parse/2" do
    setup %{session: session} do
      %{strings: Latu.select_expr(Latu.range(session, 5), ["cast(id as string) as value"])}
    end

    test "json, with an option", %{strings: strings} do
      strings
      |> Latu.parse(format: :json, options: [multi_line: true])
      |> assert_wire("parse_json")
    end

    test "csv, with nothing else", %{strings: strings} do
      assert_wire(Latu.parse(strings, format: :csv), "parse_csv")
    end

    test "the format is required and closed", %{strings: strings} do
      assert_raise ArgumentError, ~r/format is required/, fn -> Latu.parse(strings, []) end

      assert_raise ArgumentError, ~r/unknown parse format :avro/, fn ->
        Latu.parse(strings, format: :avro)
      end
    end
  end

  describe "table_function/3" do
    test "with arguments", %{session: session} do
      session
      |> Latu.table_function("explode", [F.array([Column.lit(1), Column.lit(2)])])
      |> assert_wire("table_function")
    end

    test "with none", %{session: session} do
      assert_wire(Latu.table_function(session, "sql_keywords"), "table_function_no_args")
    end
  end

  describe "table_changes/3" do
    test "a bare change feed", %{session: session} do
      assert_wire(Latu.table_changes(session, "orders"), "table_changes")
    end

    test "options are camelCased, as reader options are", %{session: session} do
      session
      |> Latu.table_changes("orders", options: [starting_version: 3])
      |> assert_wire("table_changes_options")
    end
  end

  describe "join_as_of/3" do
    setup %{session: session} do
      %{left: Latu.range(session, 5), right: Latu.range(session, 5)}
    end

    test "the as-of columns are tagged to their own frames", %{left: left, right: right} do
      left
      |> Latu.join_as_of(right, left_as_of: :id, right_as_of: :id)
      |> assert_wire("as_of_join")
    end

    test "every option at once, with an equality key", %{left: left, right: right} do
      left
      |> Latu.join_as_of(right,
        left_as_of: :id,
        right_as_of: :id,
        on: :id,
        how: :left,
        tolerance: 5,
        allow_exact_matches: false,
        direction: :forward
      )
      |> assert_wire("as_of_join_full")
    end

    test "a condition goes in join_expr, not using_columns", %{left: left, right: right} do
      left
      |> Latu.join_as_of(right,
        left_as_of: :id,
        right_as_of: :id,
        on: Column.greater(:id, 1)
      )
      |> assert_wire("as_of_join_condition")
    end

    test "an already-built as-of expression passes through", %{left: left, right: right} do
      tagged = Latu.join_as_of(right, right, left_as_of: :id, right_as_of: :id)

      built =
        Latu.join_as_of(right, right,
          left_as_of: Latu.col(right, :id),
          right_as_of: Latu.col(right, :id)
        )

      assert Plan.normalize_ids(tagged.plan) == Plan.normalize_ids(built.plan)
      assert %DataFrame{} = left
    end

    test "the as-of columns are required", %{left: left, right: right} do
      assert_raise ArgumentError, ~r/left_as_of is required/, fn ->
        Latu.join_as_of(left, right, right_as_of: :id)
      end

      assert_raise ArgumentError, ~r/right_as_of is required/, fn ->
        Latu.join_as_of(left, right, left_as_of: :id)
      end
    end

    test "an unknown direction lists the three", %{left: left, right: right} do
      assert_raise ArgumentError, ~r/unknown as-of direction/, fn ->
        Latu.join_as_of(left, right, left_as_of: :id, right_as_of: :id, direction: :sideways)
      end
    end

    test "two sessions are refused here rather than by the server", %{
      session: session,
      left: left
    } do
      other = Session.from_url!("sc://elsewhere")

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        Latu.join_as_of(left, Latu.range(other, 5), left_as_of: :id, right_as_of: :id)
      end

      assert %Session{} = session
    end
  end

  describe "lateral_join/3" do
    test "inner by default", %{session: session} do
      session
      |> Latu.range(5)
      |> Latu.lateral_join(Latu.range(session, 5))
      |> assert_wire("lateral_join")
    end

    test "a condition and a left join", %{session: session} do
      session
      |> Latu.range(5)
      |> Latu.lateral_join(Latu.range(session, 5), on: Column.greater(:id, 1), how: :left)
      |> assert_wire("lateral_join_left")
    end

    test "only three join types, and the message says which", %{session: session} do
      left = Latu.range(session, 5)
      right = Latu.range(session, 5)

      error =
        assert_raise ArgumentError, fn -> Latu.lateral_join(left, right, how: :full) end

      assert error.message =~ "unknown lateral join type"
      assert error.message =~ "[:inner, :left, :cross]"
    end
  end

  describe "nearest_by_join/4" do
    setup %{session: session} do
      %{left: Latu.range(session, 5), right: Latu.range(session, 5)}
    end

    test "approx by distance", %{left: left, right: right} do
      left
      |> Latu.nearest_by_join(right, Column.col(:id),
        num_results: 3,
        mode: :approx,
        direction: :distance
      )
      |> assert_wire("nearest_by_join")
    end

    test "exact by similarity, left outer", %{left: left, right: right} do
      left
      |> Latu.nearest_by_join(right, Column.col(:id),
        num_results: 2,
        mode: :exact,
        direction: :similarity,
        how: :left
      )
      |> assert_wire("nearest_by_join_left")
    end

    test "num_results is bounded, and required", %{left: left, right: right} do
      opts = [mode: :approx, direction: :distance]

      for bad <- [nil, 0, -1, 100_001, 1.5] do
        assert_raise ArgumentError, ~r/num_results is required/, fn ->
          Latu.nearest_by_join(left, right, Column.col(:id), [{:num_results, bad} | opts])
        end
      end

      assert %DataFrame{} =
               Latu.nearest_by_join(left, right, Column.col(:id), [
                 {:num_results, 100_000} | opts
               ])
    end

    test "mode and direction are required and closed", %{left: left, right: right} do
      assert_raise ArgumentError, ~r/mode is required/, fn ->
        Latu.nearest_by_join(left, right, Column.col(:id),
          num_results: 1,
          direction: :distance
        )
      end

      assert_raise ArgumentError, ~r/direction is required/, fn ->
        Latu.nearest_by_join(left, right, Column.col(:id), num_results: 1, mode: :approx)
      end

      assert_raise ArgumentError, ~r/unknown nearest-by mode/, fn ->
        Latu.nearest_by_join(left, right, Column.col(:id),
          num_results: 1,
          mode: :fuzzy,
          direction: :distance
        )
      end
    end
  end

  describe "to/2" do
    # The DataType is built by hand here because `parse_ddl_type/2` needs a server and the
    # golden tests do not have one. The integration test uses the real path; this pins the
    # wire shape, including that StructField.metadata stays absent when it is empty — which is
    # what PySpark sends (connect/types.py only sets it for a non-empty dict).
    test "the ToSchema relation, with the server's own type message", %{session: session} do
      long = %Proto.DataType{kind: {:long, %Proto.DataType.Long{}}}

      schema = %Proto.DataType{
        kind:
          {:struct,
           %Proto.DataType.Struct{
             fields: [%Proto.DataType.StructField{name: "n", data_type: long, nullable: true}]
           }}
      }

      assert_wire(Latu.to(Latu.range(session, 5), schema), "to_schema")
    end
  end

  describe "random_split/3" do
    test "the slices tile [0.0, 1.0] and share one seed", %{session: session} do
      assert [first, second] = Latu.random_split(Latu.range(session, 10), [0.8, 0.2], seed: 42)

      assert_wire(first, "random_split_first")
      assert_wire(second, "random_split_second")
    end

    test "weights are normalised, so [8, 2] is [0.8, 0.2]", %{session: session} do
      assert [first, second] = Latu.random_split(Latu.range(session, 10), [8, 2], seed: 42)

      assert_wire(first, "random_split_first")
      assert_wire(second, "random_split_second")
    end

    test "one seed is drawn for every slice, not one each", %{session: session} do
      seeds =
        session
        |> Latu.range(10)
        |> Latu.random_split([1, 1, 1])
        |> Enum.map(&elem(&1.plan.rel_type, 1).seed)

      assert length(Enum.uniq(seeds)) == 1
    end

    test "refuses weights that cannot make slices", %{session: session} do
      df = Latu.range(session, 10)

      assert_raise ArgumentError, ~r/non-negative/, fn -> Latu.random_split(df, [1, -1]) end
      assert_raise ArgumentError, ~r/more than 0/, fn -> Latu.random_split(df, [0, 0]) end
      assert_raise ArgumentError, ~r/more than 0/, fn -> Latu.random_split(df, []) end
    end
  end

  describe "zip_with_index/2" do
    test "a projection of every column plus the internal sequence id", %{session: session} do
      session
      |> Latu.range(5)
      |> Latu.zip_with_index()
      |> assert_wire("zip_with_index")
    end

    test "the index column can be named", %{session: session} do
      session
      |> Latu.range(5)
      |> Latu.zip_with_index("row_num")
      |> assert_wire("zip_with_index_named")
    end

    test "an atom names it the same way", %{session: session} do
      session
      |> Latu.range(5)
      |> Latu.zip_with_index(:row_num)
      |> assert_wire("zip_with_index_named")
    end
  end

  defp dropped(df), do: df.plan.rel_type |> elem(1)
end
