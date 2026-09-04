#!/usr/bin/env python3
"""PySpark as a proto oracle for Latu.

Write the PySpark expression you already know, print its protobuf plan, make Latu produce the
same thing. See dev/README.md for the workflow and the gotchas.

    export SPARK_REMOTE='sc://localhost:15002'
    python dev/pyspark_oracle.py 'spark.range(10).filter(F.col("id") > 3)'
    python dev/pyspark_oracle.py --generate
"""

from __future__ import annotations

import argparse
import os
import re
import pathlib
import signal
import sys

from google.protobuf import text_format
from google.protobuf.message import Message

OUT_DIR = pathlib.Path("test/wire")

# What a reference to a relation outside the plan normalises to; keep in step with
# Latu.Plan's @unresolved. PySpark's raw ids come from a process-global counter, so a dangling
# one would change whenever an earlier fixture gains a relation.
UNRESOLVED_PLAN_ID = -1

# Fixtures: (name, expression). `spark`, `F`, `W`, `datetime` and `decimal` are in scope. One
# line here == one golden test in Elixir, so keep the names matching.

FIXTURES: list[tuple[str, str]] = [
    # first vertical slice
    ("range", "spark.range(5)"),
    ("range_step", "spark.range(0, 10, 2)"),
    ("range_partitions", "spark.range(0, 10, 2, 4)"),

    # relational verbs
    ("project_cols", 'spark.range(10).select("id")'),
    ("project_expr", 'spark.range(10).select((F.col("id") + 1).alias("id_plus_1"))'),
    ("project_star", 'spark.range(10).select("*")'),
    ("filter_gt", 'spark.range(10).filter(F.col("id") > 3)'),
    ("filter_and", 'spark.range(10).filter((F.col("id") > 3) & (F.col("id") < 8))'),
    ("filter_string_eq",
     'spark.range(10).select(F.lit("books").alias("cat")).filter(F.col("cat") == "books")'),
    ("filter_sql_string", 'spark.range(10).filter("id > 3")'),
    ("with_column", 'spark.range(10).withColumn("double", F.col("id") * 2)'),
    ("with_columns",
     'spark.range(10).withColumns({"a": F.col("id") + 1, "b": F.col("id") - 1})'),
    ("drop", 'spark.range(10).withColumn("x", F.lit(1)).drop("x")'),
    ("limit", "spark.range(10).limit(3)"),
    ("offset", "spark.range(10).offset(3)"),
    ("sort_asc", 'spark.range(10).orderBy("id")'),
    ("sort_desc_nulls_last", 'spark.range(10).orderBy(F.col("id").desc_nulls_last())'),
    ("distinct", "spark.range(10).distinct()"),
    ("drop_duplicates_subset", 'spark.range(10).dropDuplicates(["id"])'),
    ("alias_df", 'spark.range(10).alias("a")'),
    ("to_df", 'spark.range(10).toDF("renamed")'),
    ("rename", 'spark.range(10).withColumnRenamed("id", "n")'),
    ("union", "spark.range(5).union(spark.range(5))"),
    ("union_by_name", "spark.range(5).unionByName(spark.range(5))"),
    ("intersect", "spark.range(5).intersect(spark.range(3))"),
    ("except_all", "spark.range(5).exceptAll(spark.range(3))"),
    # `is_all` differs between the two spellings of each operation, and `union` is the odd one
    # out: it keeps duplicates where `intersect` and `subtract` do not.
    ("intersect_all", "spark.range(5).intersectAll(spark.range(3))"),
    ("subtract", "spark.range(5).subtract(spark.range(3))"),
    ("union_by_name_missing",
     "spark.range(5).unionByName(spark.range(5), allowMissingColumns=True)"),
    # All three positionally: `sample(0.1, seed=42)` goes through PySpark's overload
    # juggling and the seed does not reach the wire, so the fixture was random per run.
    ("sample", "spark.range(100).sample(False, 0.1, 42)"),
    ("sort_within_partitions", 'spark.range(10).sortWithinPartitions("id")'),
    ("coalesce", "spark.range(10).coalesce(2)"),
    ("repartition_n", "spark.range(10).repartition(4)"),
    ("repartition_by", 'spark.range(10).repartition(4, "id")'),

    # Operators. Spark's spellings are not uniform — symbols for comparison and arithmetic,
    # words for boolean, and `power` for `**` — so pin every one of them rather than guess.
    ("op_arith",
     'spark.range(10).select((F.col("id") + 1).alias("a"), (F.col("id") - 1).alias("s"),'
     ' (F.col("id") * 2).alias("m"), (F.col("id") / 2).alias("d"),'
     ' (F.col("id") % 2).alias("r"), (F.col("id") ** 2).alias("p"))'),
    ("op_compare",
     'spark.range(10).select((F.col("id") == 1).alias("eq"), (F.col("id") != 1).alias("ne"),'
     ' (F.col("id") > 1).alias("gt"), (F.col("id") >= 1).alias("ge"),'
     ' (F.col("id") < 1).alias("lt"), (F.col("id") <= 1).alias("le"),'
     ' F.col("id").eqNullSafe(1).alias("ns"))'),
    ("op_boolean",
     'spark.range(10).select(((F.col("id") > 1) & (F.col("id") < 5)).alias("a"),'
     ' ((F.col("id") > 1) | (F.col("id") < 5)).alias("o"),'
     ' (~(F.col("id") > 1)).alias("n"))'),
    ("op_chain",
     'spark.range(10).filter((F.col("id") > 1) & (F.col("id") < 8) & (F.col("id") != 5))'),

    # Aggregation
    ("group_count", 'spark.range(10).groupBy("id").count()'),
    ("group_agg_multi",
     'spark.range(10).groupBy("id").agg('
     'F.sum("id").alias("total"), F.count("id").alias("n"))'),
    ("agg_no_group", 'spark.range(10).agg(F.sum("id").alias("total"))'),
    ("group_rollup", 'spark.range(10).rollup("id").count()'),
    ("group_cube", 'spark.range(10).cube("id").count()'),
    ("group_pivot", 'spark.range(10).groupBy("id").pivot("id").count()'),
    # Pivot values are bare Literals, not Expressions, and the pivot column carries the input
    # relation's plan_id because PySpark builds it as `df[name]`.
    ("group_pivot_values",
     'spark.range(10).groupBy("id").pivot("id", [1, 2]).count()'),

    # Joins
    ("join_using", 'spark.range(10).join(spark.range(5), on="id")'),
    ("join_on_expr",
     'spark.range(10).alias("l").join(spark.range(5).alias("r"),'
     ' on=F.col("l.id") == F.col("r.id"), how="left")'),
    ("join_cross", "spark.range(3).crossJoin(spark.range(3))"),

    # functions, casts, windows
    ("cast", 'spark.range(10).select(F.col("id").cast("string").alias("s"))'),
    ("try_cast", 'spark.range(10).select(F.col("id").try_cast("string").alias("s"))'),
    # Column predicates. Spark's names here are camelCase, unlike its SQL functions, and
    # `between` and `isin` are not functions at all: one composes, the other is called "in".
    ("col_predicates",
     'spark.range(10).select(F.col("id").isNull().alias("n"),'
     ' F.col("id").isNotNull().alias("nn"), F.col("id").isNaN().alias("nan"),'
     ' F.col("id").between(1, 5).alias("b"), F.col("id").isin([1, 2, 3]).alias("i"))'),
    ("string_predicates",
     'spark.range(10).select(F.lit("abc").contains("b").alias("c"),'
     ' F.lit("abc").startswith("a").alias("s"), F.lit("abc").endswith("c").alias("e"),'
     ' F.lit("abc").like("a%").alias("l"), F.lit("abc").rlike("^a").alias("r"),'
     ' F.lit("abc").ilike("A%").alias("il"))'),
    # A self-join needs no hoisting: both branches ARE the referenced relation. Kept because
    # it looks like it should need one, and because it pins that Latu agrees.
    ("self_join", '(lambda d: d.join(d, on=d.id == d.id))(spark.range(10))'),
    # Here `a` is not in `b`'s tree, so the reference cannot resolve — and Spark refuses it
    # hoisted or not (docs/decisions.md, M9.1).
    ("cross_frame_col", '(lambda a, b: b.select(a.id))(spark.range(10), spark.range(5))'),
    ("case_when",
     'spark.range(10).select('
     'F.when(F.col("id") > 5, "big").otherwise("small").alias("size"))'),
    ("coalesce_fn", 'spark.range(10).select(F.coalesce(F.col("id"), F.lit(0)).alias("c"))'),
    ("string_fns",
     'spark.range(10).select(F.upper(F.lit("abc")).alias("u"),'
     ' F.concat_ws("-", F.lit("a"), F.lit("b")).alias("j"))'),
    ("count_distinct", 'spark.range(10).agg(F.count_distinct("id").alias("d"))'),
    ("window_row_number",
     'spark.range(10).withColumn("rn",'
     ' F.row_number().over(W.partitionBy("id").orderBy(F.col("id").desc())))'),
    # `rowsBetween`, so a ROW frame.
    ("window_rows_frame",
     'spark.range(10).withColumn("s", F.sum("id").over('
     'W.partitionBy("id").orderBy("id").rowsBetween(W.unboundedPreceding, W.currentRow)))'),
    # A ROW offset encodes as `integer` and a RANGE offset as `long`, for the same number.
    # These two exist to pin that, because `lit/1` picks its width by magnitude and would get
    # RANGE wrong every time.
    ("window_rows_offsets",
     'spark.range(10).withColumn("s", F.sum("id").over('
     'W.partitionBy("id").orderBy("id").rowsBetween(-1, 1)))'),
    ("window_range_offsets",
     'spark.range(10).withColumn("s", F.sum("id").over('
     'W.partitionBy("id").orderBy("id").rangeBetween(-1, 1)))'),
    # No partition_spec at all: PySpark warns and sends the field empty.
    ("window_no_partition",
     'spark.range(10).withColumn("rn", F.row_number().over(W.orderBy("id")))'),
    # PySpark names lambda parameters x, y, z *by position* and ignores what the Python lambda
    # calls them, so `lambda acc, x:` still sends x_n and y_m. Every one of these carries a
    # counter suffix that only normalize_lambda_names makes reproducible — before that pass,
    # adding any fixture below silently rewrote the ones above it.
    ("higher_order_transform",
     'spark.range(1).select('
     'F.transform(F.array(F.lit(1), F.lit(2)), lambda x: x + 1).alias("t"))'),
    ("higher_order_filter",
     'spark.range(1).select('
     'F.filter(F.array(F.lit(1), F.lit(2), F.lit(3)), lambda x: x > 1).alias("f"))'),
    # A two-parameter lambda: transform passes the element and its index.
    ("higher_order_transform_index",
     'spark.range(1).select('
     'F.transform(F.array(F.lit(10), F.lit(20)), lambda x, i: x + i).alias("t"))'),
    ("higher_order_aggregate",
     'spark.range(1).select(F.aggregate('
     'F.array(F.lit(1), F.lit(2)), F.lit(0), lambda acc, x: acc + x).alias("a"))'),
    # Two lambdas in one call, so three variables across two scopes.
    ("higher_order_aggregate_finish",
     'spark.range(1).select(F.aggregate('
     'F.array(F.lit(1), F.lit(2)), F.lit(0), lambda acc, x: acc + x, lambda acc: acc * 2)'
     '.alias("a"))'),
    # Nested, and the inner lambda shadows the outer one's name — which is the whole reason the
    # suffix exists. Renumbering must be by creation order, outer before inner.
    ("higher_order_nested",
     'spark.range(1).select(F.transform(F.array(F.lit(1), F.lit(2)), lambda x: F.aggregate('
     'F.array(x, x), F.lit(0), lambda a, b: a + b)).alias("n"))'),
    ("higher_order_zip_with",
     'spark.range(1).select(F.zip_with('
     'F.array(F.lit(1)), F.array(F.lit(2)), lambda x, y: x + y).alias("z"))'),
    # array_sort takes an *optional* lambda: without one no lambda reaches the wire at all.
    # An optional argument Spark has no absent form for: PySpark substitutes a constant and
    # always sends it. The constant is in the *body*, not the signature — `mask`'s four
    # parameters all default to None and send "X", "x", "n" and NULL. Reading the signature
    # would have sent four NULLs, and nothing but a fixture says otherwise.
    # A different wire node: CallFunction rather than UnresolvedFunction, the only one in Latu.
    ("call_function", 'spark.range(1).select(F.call_function("abs", F.lit(-1)).alias("a"))'),
    # Overloaded on arity alone — six date parts here, a date and a time in the other form.
    ("make_timestamp_parts",
     'spark.range(1).select(F.make_timestamp('
     'F.lit(2026), F.lit(1), F.lit(2), F.lit(3), F.lit(4), F.lit(5.0)).alias("t"))'),
    ("window_slide",
     'spark.range(1).select(F.window('
     'F.lit("2026-01-01 00:00:00").cast("timestamp"), "10 minutes", "5 minutes").alias("w"))'),
    ("listagg_distinct_delim",
     'spark.range(1).select(F.listagg_distinct(F.lit("a"), F.lit(",")).alias("l"))'),
    # One of the 46 sketch functions, which are generated rows rather than hand-written: this
    # pins that their substituted defaults were read correctly too.
    ("sketch_defaults",
     'spark.range(1).select(F.tuple_sketch_agg_integer(F.lit(1), F.lit(2)).alias("s"))'),
    # Shapes no rule covers, each of which encodes perfectly cleanly when it is wrong.
    # trim sends its optional argument FIRST; log changes its wire name with arity; lag always
    # sends the offset but only sometimes the fallback; convert_timezone's optional argument is
    # the first one; unix_timestamp fills in Spark's format string.
    ("trim_chars", 'spark.range(1).select(F.trim(F.lit("xxhixx"), F.lit("x")).alias("t"))'),
    ("log_forms",
     'spark.range(1).select(F.log(F.lit(8.0)).alias("a"), F.log(2.0, F.lit(8.0)).alias("b"))'),
    ("lag_default",
     'spark.range(10).withColumn("p", F.lag("id", 2, 0).over('
     'W.partitionBy("id").orderBy("id")))'),
    ("convert_timezone_2",
     'spark.range(1).select(F.convert_timezone('
     'None, F.lit("Asia/Jakarta"), F.lit("2026-01-01 00:00:00")).alias("c"))'),
    ("unix_timestamp_format",
     'spark.range(1).select(F.unix_timestamp(F.lit("2026-01-02 03:04:05")).alias("u"))'),
    ("rand_seeded", 'spark.range(1).select(F.rand(42).alias("r"))'),
    ("default_split", 'spark.range(1).select(F.split(F.lit("a,b"), ",").alias("s"))'),
    ("default_mask", 'spark.range(1).select(F.mask(F.lit("AbC1")).alias("m"))'),
    ("default_mask_partial",
     'spark.range(1).select(F.mask(F.lit("AbC1"), F.lit("*")).alias("m"))'),
    ("array_sort_comparator",
     'spark.range(1).select('
     'F.array_sort(F.array(F.lit(2), F.lit(1)), lambda x, y: x - y).alias("s"))'),

    # Literal typing corners. Note a Python list is NOT an array literal: PySpark compiles
    # both F.lit([1,2,3]) and F.array(...) to an `array` function call.
    ("lit_scalars",
     'spark.range(1).select(F.lit(1).alias("i"), F.lit(1.5).alias("d"),'
     ' F.lit(True).alias("b"), F.lit("s").alias("s"))'),
    ("lit_null", 'spark.range(1).select(F.lit(None).alias("n"))'),
    ("lit_decimal", 'spark.range(1).select(F.lit(decimal.Decimal("1.50")).alias("m"))'),
    ("lit_date_py", 'spark.range(1).select(F.lit(datetime.date(2026, 1, 2)).alias("d"))'),
    # An explicit tzinfo, because PySpark converts a naive datetime using the *client's* local
    # timezone — which would bake whichever machine generated the fixture into the bytes.
    ("lit_timestamp",
     'spark.range(1).select(F.lit(datetime.datetime('
     '2026, 1, 2, 3, 4, tzinfo=datetime.timezone.utc)).alias("t"))'),
    ("array_fn", 'spark.range(1).select(F.array(F.lit(1), F.lit(2), F.lit(3)).alias("a"))'),
    ("to_date_fn", 'spark.range(1).select(F.to_date(F.lit("2026-01-02")).alias("d"))'),
    # The registry generates several argument shapes; these pin the ones the fixtures above do
    # not, because a shape read out of PySpark's source rather than pinned has been wrong before.
    #   * an optional trailing argument when it is *present* (absent is `to_date_fn`)
    #   * a distinct variant over more than one argument
    #   * a function that takes none at all
    ("round_scale", 'spark.range(1).select(F.round(F.lit(2.5), 1).alias("r"))'),
    ("count_distinct_multi",
     'spark.range(10).agg(F.count_distinct(F.col("id"), F.lit(1)).alias("d"))'),
    ("zero_arg_fn", 'spark.range(1).select(F.current_date().alias("today"))'),

    # actions. df.count() is agg(count(lit(1))) — UNALIASED, unlike GroupedData.count,
    # which aliases the same call as "count" (fixture group_count). Read from
    # pyspark/sql/connect/dataframe.py; this fixture is what pins it.
    ("count_action", 'spark.range(10).agg(F.count(F.lit(1)))'),

    # readers and the parse-function family. Two wire facts these pin, both read from
    # pyspark/sql/connect/readwriter.py and functions/builtin.py: the reader's schema is a
    # STRING field sent verbatim (DDL; a StructType goes as its JSON form) and is PRESENT as ""
    # when unset -- the reader inits _schema = "" and always assigns it. Options are
    # map<string,string> (to_str: booleans lowercased); the from_json family instead sends them
    # as a `map` FUNCTION CALL, where argument order reaches the wire.
    ("read_parquet", 'spark.read.parquet("/fixtures/people.parquet")'),
    ("read_json", 'spark.read.format("json").load("/fixtures/people.json")'),
    ("read_csv_schema_options",
     'spark.read.format("csv").schema("id INT, name STRING")'
     '.options(header=True, sep=";").load("/fixtures/people.csv")'),
    ("read_two_paths",
     'spark.read.format("csv").load(["/fixtures/a.csv", "/fixtures/b.csv"])'),
    ("read_table", 'spark.read.table("people")'),
    ("read_table_options", 'spark.read.options(mergeSchema=True).table("people")'),
    ("from_json_fn", 'spark.range(1).select(F.from_json(F.col("s"), "a INT").alias("j"))'),
    ("from_json_options",
     'spark.range(1).select(F.from_json(F.col("s"), "a INT",'
     ' {"allowComments": "true", "mode": "PERMISSIVE"}).alias("j"))'),
    ("from_csv_fn",
     'spark.range(1).select(F.from_csv(F.col("s"), "a INT, b STRING").alias("c"))'),
    ("from_xml_fn", 'spark.range(1).select(F.from_xml(F.col("s"), "a INT").alias("x"))'),
    ("to_json_fn", 'spark.range(1).select(F.to_json(F.struct("id")).alias("j"))'),
    ("to_json_options",
     'spark.range(1).select(F.to_json(F.struct("id"),'
     ' {"ignoreNullFields": "false"}).alias("j"))'),
    ("to_csv_fn", 'spark.range(1).select(F.to_csv(F.struct("id")).alias("c"))'),
    ("to_xml_fn", 'spark.range(1).select(F.to_xml(F.struct("id")).alias("x"))'),
    ("schema_of_json_fn",
     'spark.range(1).select(F.schema_of_json(\'{"a": 1}\').alias("s"))'),
    ("schema_of_csv_options",
     'spark.range(1).select(F.schema_of_csv("1;a", {"sep": ";"}).alias("s"))'),
    ("schema_of_xml_fn",
     'spark.range(1).select(F.schema_of_xml("<r><a>1</a></r>").alias("s"))'),

    # writes are Commands, built through the oracle's save/save_as_table/insert_into/v2
    # helpers (PySpark's terminal methods minus the execution). Presence facts these pin: an
    # unset mode stays SAVE_MODE_UNSPECIFIED; bucket_by appears only when num_buckets > 0;
    # `table` and `path` are a oneof; V2's partitioning columns are EXPRESSIONS where V1's are
    # strings; V2's overwrite_condition rides only under MODE_OVERWRITE.
    ("write_save", 'save(spark.range(5).write.format("parquet"), "/tmp/latu/out")'),
    ("write_csv_options",
     'save(spark.range(5).write.format("csv").mode("overwrite")'
     '.options(header=True, sep=";"), "/tmp/latu/out_csv")'),
    ("write_partition_sort_bucket",
     'save(spark.range(10).write.format("parquet").partitionBy("id")'
     '.bucketBy(4, "id").sortBy("id"), "/tmp/latu/buckets")'),
    ("write_table", 'save_as_table(spark.range(5).write.mode("append"), "people_tbl")'),
    ("write_insert_into", 'insert_into(spark.range(5).write, "people_tbl", overwrite=True)'),
    ("write_v2_create", 'v2(spark.range(5).writeTo("t").using("parquet"), "create")'),
    ("write_v2_overwrite", 'v2(spark.range(5).writeTo("t"), "overwrite", F.col("id") > 3)'),
    ("write_v2_append_props",
     'v2(spark.range(5).writeTo("t").tableProperty("k", "v")'
     '.partitionedBy(F.col("id")), "append")'),
    # SQL and views are Commands (sql_cmd/view_cmd = PySpark's methods minus the
    # execution); catalog operations are *relations* that answer eagerly. Presence facts:
    # empty args stay off the wire entirely; named arguments are a map of full Expressions;
    # an unset optional (db_name, pattern) is absent, never ""; false flags are proto3
    # defaults and vanish.
    ("sql_command", 'sql_cmd("SELECT 1 AS one")'),
    ("sql_pos_args", 'sql_cmd("SELECT ? AS n, ? AS s", [42, "x"])'),
    ("sql_named_args",
     'sql_cmd("SELECT * FROM range(10) WHERE id > :min AND id < :max", {"min": 2, "max": 8})'),
    ("create_temp_view", 'view_cmd(spark.range(3), "v_people")'),
    ("create_view_global_replace",
     'view_cmd(spark.range(3), "v_people", is_global=True, replace=True)'),
    ("catalog_current_database", "catalog_plan(cat.CurrentDatabase())"),
    ("catalog_list_tables",
     'catalog_plan(cat.ListTables(db_name="default", pattern="latu_*"))'),
    ("catalog_table_exists",
     'catalog_plan(cat.TableExists(table_name="people", db_name="default"))'),
    ("catalog_drop_table", 'catalog_plan(cat.DropTable(table_name="scratch", if_exists=True))'),
    ("catalog_drop_temp_view", 'catalog_plan(cat.DropTempView(view_name="v_people"))'),
    ("catalog_cache_table", 'catalog_plan(cat.CacheTable(table_name="people"))'),
    # local data. A data-bearing LocalRelation fixture cannot exist: LocalRelation.data
    # is an Arrow IPC stream, and PyArrow's bytes are not Explorer's — same values, different
    # writer (the seedless-rand precedent: the absence is deliberate; integration tests are
    # the authority there). The schema-only shape can be pinned. PySpark ddl-parses "id INT"
    # server-side and sends StructType().json(); Latu sends the user's string verbatim, so its
    # golden test feeds this fixture's own JSON back through create_dataframe.
    ("local_relation_schema_only", 'spark.createDataFrame([], "id INT")'),

    # cross-DataFrame references. A subquery is the only one Spark resolves, and it is the
    # only expression PySpark hoists into WithRelations — a bare a.id inside a plan built on b
    # is sent untouched and refused by the analyzer (docs/decisions.md, M9.1).
    ("subquery_scalar", "(lambda a, b: b.select(a.scalar()))(spark.range(1), spark.range(5))"),
    ("subquery_filter",
     "(lambda a, b: b.filter(b.id < a.scalar()))(spark.range(1), spark.range(5))"),
    ("subquery_exists", "(lambda a, b: b.filter(a.exists()))(spark.range(1), spark.range(5))"),
    ("subquery_in", "(lambda a, b: b.filter(b.id.isin(a)))(spark.range(2), spark.range(5))"),
    # References are collected from the whole expression tree, not just its top level.
    ("subquery_nested",
     "(lambda a, b: b.select(F.abs(a.scalar() * -1)))(spark.range(1), spark.range(5))"),
    ("subquery_two_refs",
     "(lambda a, b, c: c.select(a.scalar(), b.scalar()))"
     "(spark.range(1), spark.range(2), spark.range(5))"),
    # A struct on the left sends the struct's CHILDREN as the values, so a multi-column
    # subquery matches. PySpark's own rule, in Column.isin.
    # A DataFrame named into a query: a SubqueryAlias in WithRelations.references, and no
    # temp view anywhere — the connect formatter only invents a name.
    ("sql_views",
     'sql_cmd("SELECT count(*) AS n FROM orders", views=[(spark.range(5), "orders")])'),
    ("subquery_in_struct",
     "(lambda a, b: b.filter(F.struct(b.id, b.id).isin(a)))(spark.range(2), spark.range(5))"),

    # AnalyzePlan arms. These are NOT Plans — the fixture is the request arm submessage,
    # built by `analyze_arm` (client/core.py's `_analyze` branch minus the RPC), so Latu's
    # golden test decodes it with that message's own module. `level` has presence and PySpark
    # guards it with `if level and ...`, so an unset level and a level of 0 are the same wire.
    ("analyze_schema", 'analyze_arm("schema", spark.range(5))'),
    ("analyze_tree_string", 'analyze_arm("tree_string", spark.range(5))'),
    ("analyze_tree_string_level", 'analyze_arm("tree_string", spark.range(5), level=2)'),

    # the rest of the arms. Presence and shape facts these pin: an explain with no
    # argument sends MODE_SIMPLE explicitly rather than leaving it UNSPECIFIED; unpersist's
    # `blocking` has presence and PySpark sends it either way; persist always sends a storage
    # level, defaulting to MEMORY_AND_DISK_DESER; and persist/unpersist/get_storage_level carry
    # a bare Relation where every other arm carries a Plan.
    ("analyze_explain", 'analyze_arm("explain", spark.range(5))'),
    ("analyze_explain_formatted",
     'analyze_arm("explain", spark.range(5), mode="formatted")'),
    ("analyze_is_local", 'analyze_arm("is_local", spark.range(5))'),
    ("analyze_is_streaming", 'analyze_arm("is_streaming", spark.range(5))'),
    ("analyze_input_files", 'analyze_arm("input_files", spark.range(5))'),
    ("analyze_semantic_hash", 'analyze_arm("semantic_hash", spark.range(5))'),
    ("analyze_same_semantics",
     'analyze_arm("same_semantics", spark.range(5), other=spark.range(3))'),
    ("analyze_persist", 'analyze_arm("persist", spark.range(5))'),
    ("analyze_persist_level",
     'analyze_arm("persist", spark.range(5), level=StorageLevel.DISK_ONLY_2)'),
    ("analyze_unpersist", 'analyze_arm("unpersist", spark.range(5))'),
    ("analyze_unpersist_blocking",
     'analyze_arm("unpersist", spark.range(5), blocking=True)'),
    ("analyze_get_storage_level", 'analyze_arm("get_storage_level", spark.range(5))'),
    ("analyze_ddl_parse", 'analyze_arm("ddl_parse", ddl="id INT, tags ARRAY<STRING>")'),
    ("analyze_json_to_ddl",
     'analyze_arm("json_to_ddl", json=\'{"type":"struct","fields":'
     '[{"name":"id","type":"integer","nullable":true,"metadata":{}}]}\')'),

    # missing data. Presence facts these pin: how="any" leaves min_non_nulls OFF the
    # wire entirely (an absent one already means "every column"), while how="all" sends 1 and
    # thresh overrides both; a per-column fill map REPLACES subset rather than intersecting it;
    # and NAFill.values and NAReplace's old/new are bare Literals, not Expressions.
    ("na_fill_scalar", 'spark.range(5).fillna(0)'),
    ("na_fill_subset", 'spark.range(5).fillna(0, subset=["id"])'),
    ("na_fill_per_column", 'spark.range(5).fillna({"id": 0})'),
    ("na_drop_any", "spark.range(5).dropna()"),
    ("na_drop_all", 'spark.range(5).dropna(how="all")'),
    ("na_drop_thresh", "spark.range(5).dropna(thresh=2)"),
    ("na_drop_subset", 'spark.range(5).dropna(subset=["id"])'),
    ("na_replace", 'spark.range(5).replace({1: 10}, subset=["id"])'),

    # the stat family. Presence facts these pin: freqItems' `support` and corr's
    # `method` are both sent even when the caller left them out, and `summary()` with no
    # statistics sends an empty list rather than Spark's default eight — the server fills
    # those in. A sampleBy stratum is a bare Literal built by F.lit's OWN magnitude rule, not
    # NAFill's — 1 goes as an int32 here and as a long there.
    ("stat_summary", "spark.range(5).summary()"),
    ("stat_summary_named", 'spark.range(5).summary("count", "min", "max")'),
    ("stat_describe", "spark.range(5).describe()"),
    ("stat_describe_cols", 'spark.range(5).describe("id")'),
    ("stat_crosstab", 'spark.range(5).crosstab("id", "id")'),
    ("stat_freq_items", 'spark.range(5).freqItems(["id"])'),
    ("stat_freq_items_support", 'spark.range(5).freqItems(["id"], 0.4)'),
    ("stat_sample_by", 'spark.range(5).sampleBy("id", {1: 0.5, 2: 1.0}, seed=42)'),
    ("stat_sample_by_strings",
     'spark.range(5).sampleBy(F.col("id").cast("string"), {"a": 0.5}, seed=42)'),
    ("stat_cov",
     'node_plan(cat.StatCov(child=spark.range(5)._plan, col1="id", col2="id"))'),
    ("stat_corr",
     'node_plan(cat.StatCorr(child=spark.range(5)._plan, col1="id", col2="id", '
     'method="pearson"))'),
    ("stat_approx_quantile",
     'node_plan(cat.StatApproxQuantile(child=spark.range(5)._plan, cols=["id"], '
     'probabilities=[0.0, 0.5, 1.0], relativeError=0.01))'),

    # observe / CollectMetrics. The string form of `observe` is the one with a
    # deterministic name — the Observation form generates a UUID when given none.
    ("observe_one", 'spark.range(5).observe("checks", F.count("id").alias("total"))'),
    ("observe_two",
     'spark.range(5).observe("checks", F.count("id").alias("total"),'
     ' F.min("id").alias("lowest"))'),
    ("observe_under_verb",
     'spark.range(5).observe("checks", F.count("id").alias("total")).filter(F.col("id") > 1)'),
    ("observe_write",
     'save(spark.range(5).observe("q", F.count("id").alias("rows"))'
     '.write.format("parquet"), "/tmp/latu/out")'),

    # the plain verbs. `tail` and `randomSplit` execute or return a list, so
    # they go through node_plan; the rest are ordinary DataFrame expressions.
    ("tail", "node_plan(cat.Tail(child=spark.range(5)._plan, limit=3))"),
    ("hint_broadcast", 'spark.range(5).hint("broadcast")'),
    ("hint_params", 'spark.range(5).hint("repartition", 4, "id")'),
    # withColumns, not two chained withColumn calls: Latu's with_columns/2 builds ONE
    # WithColumns node with both aliases, and the chained form builds two nested ones.
    ("unpivot", 'spark.range(5).withColumns({"a": F.lit(1), "b": F.lit(2)})'
     '.unpivot(["id"], ["a", "b"], "key", "val")'),
    ("unpivot_all", 'spark.range(5).withColumn("a", F.lit(1))'
     '.unpivot(["id"], None, "key", "val")'),
    ("transpose", 'spark.range(5).transpose()'),
    ("transpose_index", 'spark.range(5).withColumn("a", F.lit(1)).transpose("id")'),
    ("select_expr", 'spark.range(5).selectExpr("id", "id * 2 as doubled")'),
    ("group_grouping_sets",
     'spark.range(5).withColumn("a", F.lit(1))'
     '.groupingSets([["id", "a"], ["id"], []], "id", "a")'
     '.agg(F.count(F.lit(1)).alias("n"))'),
    # randomSplit hands its DataFrames back, so these pin Latu against PySpark's OWN weight
    # normalisation rather than against a hand-built Sample asserting what it ought to be.
    # `to` takes a StructType built client-side; Latu's comes from parse_ddl_type/2, which is
    # the server's own parse of the same DDL. StructField.metadata is only sent when non-empty.
    ("to_schema",
     'spark.range(5).to(T.StructType([T.StructField("n", T.LongType(), True)]))'),
    ("random_split_first",
     'node_plan(spark.range(10).randomSplit([0.8, 0.2], seed=42)[0]._plan)'),
    ("random_split_second",
     'node_plan(spark.range(10).randomSplit([0.8, 0.2], seed=42)[1]._plan)'),

    # the joins. `_joinAsOf` is private in PySpark (pandas-on-Spark's
    # merge_asof is its only caller), but the relation is public protocol, so Latu ships it.
    # Note `leftAsOfColumn="id"` becomes `df._col("id")` — a reference TAGGED with the left
    # plan's id, not a bare unresolved attribute.
    ("as_of_join",
     'spark.range(5)._joinAsOf(spark.range(5), leftAsOfColumn="id", rightAsOfColumn="id")'),
    ("as_of_join_full",
     'spark.range(5)._joinAsOf(spark.range(5), leftAsOfColumn="id", rightAsOfColumn="id",'
     ' on="id", how="leftouter", tolerance=F.lit(5), allowExactMatches=False,'
     ' direction="forward")'),
    ("as_of_join_condition",
     'spark.range(5)._joinAsOf(spark.range(5), leftAsOfColumn="id", rightAsOfColumn="id",'
     ' on=F.col("id") > 1)'),
    ("lateral_join", "spark.range(5).lateralJoin(spark.range(5))"),
    ("lateral_join_left",
     'spark.range(5).lateralJoin(spark.range(5), F.col("id") > 1, "left")'),
    ("nearest_by_join",
     'spark.range(5).nearestByJoin(spark.range(5), F.col("id"), 3, "approx", "distance")'),
    ("nearest_by_join_left",
     'spark.range(5).nearestByJoin(spark.range(5), F.col("id"), 2, "exact", "similarity",'
     ' joinType="leftouter")'),

    # the tail. A walrus binds the frame so a tagged expression can name it —
    # `colRegex` and `metadataColumn` are both tagged, as `df._col` is.
    ("col_regex", '(df := spark.range(5)).select(df.colRegex("`id`"))'),
    ("metadata_column", '(df := spark.range(5)).select(df.metadataColumn("_metadata"))'),
    # No `with_metadata` fixture: `Alias.metadata` is a JSON *string*, and Python's json.dumps
    # writes `{"a": 1}` where Elixir's JSON writes `{"a":1}` — and multi-key order differs too.
    # Spark parses the string, so neither is more correct; byte-identity is not available and
    # would not mean anything. The structural test asserts the decoded metadata instead.
    ("repartition_by_range", 'spark.range(5).repartitionByRange(4, "id")'),
    ("repartition_by_range_desc",
     'spark.range(5).repartitionByRange(2, F.col("id").desc())'),
    ("repartition_by_range_no_count", 'spark.range(5).repartitionByRange("id")'),
    ("cross_join", "spark.range(5).crossJoin(spark.range(3))"),
    ("parse_json",
     'spark.read.option("multiLine", True)'
     '.json(spark.range(5).selectExpr("cast(id as string) as value"))'),
    ("parse_csv", 'spark.read.csv(spark.range(5).selectExpr("cast(id as string) as value"))'),
    ("table_function", 'spark.tvf.explode(F.array(F.lit(1), F.lit(2)))'),
    ("table_function_no_args", "spark.tvf.sql_keywords()"),
    ("table_changes", 'spark.read.changes("orders")'),
    ("table_changes_options",
     'spark.read.option("startingVersion", 3).changes("orders")'),

    # checkpoint. `CachedRemoteRelation`'s __del__ sends a release, so it is built with
    # a None session — the finalizer's own first guard — which keeps the oracle side-effect
    # free. `RemoveRemoteCachedRelation` only reads the id off it.
    ("checkpoint",
     'command_plan(cat.Checkpoint(child=spark.range(5)._plan, local=False, eager=True)'
     '.command(spark.client))'),
    ("checkpoint_local",
     'command_plan(cat.Checkpoint(child=spark.range(5)._plan, local=True, eager=False,'
     ' storage_level=StorageLevel(True, True, False, False, 1)).command(spark.client))'),
    ("cached_remote_relation", 'node_plan(cat.CachedRemoteRelation("abc", None))'),
    ("remove_cached_relation",
     'command_plan(cat.RemoveRemoteCachedRelation(cat.CachedRemoteRelation("abc", None))'
     '.command(spark.client))'),

    # HtmlString, Spark's own _repr_html_. No public DataFrame method returns its
    # plan — PySpark builds it inside _repr_html_ — so it goes through node_plan like
    # ShowString does, and note it takes no `vertical`.
    ("html_string_range", 'node_plan(cat.HtmlString(spark.range(5)._plan, 20, 20))'),
    ("html_string_options", 'node_plan(cat.HtmlString(spark.range(5)._plan, 3, 0))'),

    # zip_with_index. Not a relation — PySpark's zipWithIndex is a Project of `*` plus
    # `distributed_sequence_id`, an internal expression hidden from DESCRIBE FUNCTION.
    ("zip_with_index", "spark.range(5).zipWithIndex()"),
    ("zip_with_index_named", 'spark.range(5).zipWithIndex("row_num")'),

    # merge_into. The source is aliased so the condition can name both sides — the
    # target by its table name, the source by the alias. Note the assignment KEY goes through
    # `expr(k)`, so it is an ExpressionString and not an UnresolvedAttribute.
    ("merge_update_insert",
     'merge_cmd(spark.range(5).selectExpr("id", "id * 2 as n").alias("s")'
     '.mergeInto("people", F.expr("people.id = s.id"))'
     '.whenMatched().update({"n": F.col("s.n")})'
     '.whenNotMatched().insertAll())'),
    ("merge_all_three",
     'merge_cmd(spark.range(5).alias("s")'
     '.mergeInto("people", F.expr("people.id = s.id"))'
     '.whenMatched().updateAll()'
     '.whenNotMatched().insertAll()'
     '.whenNotMatchedBySource().delete())'),
    ("merge_conditional",
     'merge_cmd(spark.range(5).selectExpr("id", "id * 2 as n").alias("s")'
     '.mergeInto("people", F.expr("people.id = s.id"))'
     '.whenMatched(F.expr("s.n > 4")).delete()'
     '.whenMatched().update({"n": F.col("s.n")})'
     '.withSchemaEvolution())'),
    ("merge_insert_assignments",
     'merge_cmd(spark.range(5).alias("s")'
     '.mergeInto("people", F.expr("people.id = s.id"))'
     '.whenNotMatched().insert({"id": F.col("s.id"), "live": F.lit(True)}))'),
]


def get_session():
    """Connect via SPARK_REMOTE, or exit with instructions."""
    if not os.environ.get("SPARK_REMOTE"):
        sys.exit(
            "SPARK_REMOTE is not set.\n"
            "  export SPARK_REMOTE='sc://localhost:15002'\n"
            "and check the server is up: docker compose up -d spark-connect"
        )
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def plan_for(spark, source: str):
    """Evaluate a PySpark expression and return its proto.Plan."""
    import datetime
    import decimal

    import pyspark.sql.connect.functions as F  # noqa: F401
    import pyspark.sql.connect.proto as proto
    from pyspark.sql.connect.window import Window as W  # noqa: F401

    # A write is a Command, and PySpark's terminal methods (save, saveAsTable, insertInto,
    # create/append/...) both finish the builder AND execute it. These helpers are those
    # methods minus execute_command — each line mirrors readwriter.py — so a fixture uses
    # PySpark's own builders without touching the server (never hand-build what PySpark
    # exposes).
    def command_plan(command):
        plan = proto.Plan()
        plan.command.CopyFrom(command)
        return plan

    def save(writer, path=None):
        writer._write.path = path
        return command_plan(writer._write.command(spark.client))

    def save_as_table(writer, name):
        writer._write.table_name = name
        writer._write.table_save_method = "save_as_table"
        return command_plan(writer._write.command(spark.client))

    def insert_into(writer, name, overwrite=None):
        if overwrite is not None:
            writer.mode("overwrite" if overwrite else "append")
        writer._write.table_name = name
        writer._write.table_save_method = "insert_into"
        return command_plan(writer._write.command(spark.client))

    def v2(writer, mode, condition=None):
        # readwriter.py's F is `functions.builtin`; the package re-exports only public names,
        # so `_to_col` must come from the submodule.
        from pyspark.sql.connect.functions import builtin

        writer._write.mode = mode
        if condition is not None:
            writer._write.overwrite_condition = builtin._to_col(condition)
        return command_plan(writer._write.command(spark.client))

    def merge_cmd(writer):
        # MergeIntoWriter.merge minus execute_command — mirrors connect/merge.py. `mergeInto`
        # and every whenMatched/whenNotMatched call is already side-effect free; only the
        # terminal `merge()` touches the server.
        def a2e(action):
            return proto.Expression(merge_action=action)

        merge = proto.MergeIntoTableCommand(
            target_table_name=writer._target_table,
            source_table_plan=writer._source_plan.plan(spark.client),
            merge_condition=writer._condition.to_plan(spark.client),
            match_actions=[a2e(a) for a in writer._matched_actions],
            not_matched_actions=[a2e(a) for a in writer._not_matched_actions],
            not_matched_by_source_actions=[
                a2e(a) for a in writer._not_matched_by_source_actions
            ],
            with_schema_evolution=writer._schema_evolution_enabled,
        )
        return command_plan(proto.Command(merge_into_table_command=merge))

    def sql_cmd(query, args=None, views=None):
        # session.sql minus execute_command and the kwargs formatter — mirrors session.py.
        # A list binds positionally, a dict by name; values go through F.lit either way.
        # `views` are (df, name) pairs: what the formatter builds for each {df} in the query.
        from pyspark.sql.connect.plan import SQL, SubqueryAlias

        pos = [F.lit(v) for v in args] if isinstance(args, list) else []
        named = {k: F.lit(v) for k, v in args.items()} if isinstance(args, dict) else {}
        aliases = [SubqueryAlias(df._plan, name) for df, name in (views or [])]
        return command_plan(SQL(query, pos, named, aliases).command(spark.client))

    def view_cmd(df, name, is_global=False, replace=False):
        # createTempView and its three siblings minus execute_command — mirrors dataframe.py.
        from pyspark.sql.connect.plan import CreateView

        cmd = CreateView(child=df._plan, name=name, is_global=is_global, replace=replace)
        return command_plan(cmd.command(spark.client))

    def analyze_arm(method, df=None, **kwargs):
        # client/core.py's `_analyze` branch, minus the RPC. The request envelope (session id,
        # user context, client type) is identical for every arm and varies per run, so the
        # fixture is the arm submessage alone. Note which arms take a *Relation* rather than a
        # Plan: persist, unpersist and get_storage_level, as dataframe.py sends them.
        from pyspark.sql.connect.conversion import storage_level_to_proto
        from pyspark.storagelevel import StorageLevel

        req = proto.AnalyzePlanRequest()
        plan = df._plan.to_proto(spark.client) if df is not None else None
        if method in ("schema", "is_local", "is_streaming", "input_files", "semantic_hash"):
            getattr(req, method).plan.CopyFrom(plan)
        elif method == "tree_string":
            req.tree_string.plan.CopyFrom(plan)
            level = kwargs.get("level")
            if level and isinstance(level, int):  # PySpark's own guard: 0 is falsy, so unset
                req.tree_string.level = level
        elif method == "explain":
            req.explain.plan.CopyFrom(plan)
            modes = {
                "simple": proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_SIMPLE,
                "extended": proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_EXTENDED,
                "codegen": proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_CODEGEN,
                "cost": proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_COST,
                "formatted": proto.AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_FORMATTED,
            }
            req.explain.explain_mode = modes[kwargs.get("mode", "simple")]
        elif method == "same_semantics":
            req.same_semantics.target_plan.CopyFrom(plan)
            req.same_semantics.other_plan.CopyFrom(
                kwargs["other"]._plan.to_proto(spark.client)
            )
        elif method == "persist":
            req.persist.relation.CopyFrom(df._plan.plan(spark.client))
            level = kwargs.get("level", StorageLevel.MEMORY_AND_DISK_DESER)
            req.persist.storage_level.CopyFrom(storage_level_to_proto(level))
        elif method == "unpersist":
            req.unpersist.relation.CopyFrom(df._plan.plan(spark.client))
            req.unpersist.blocking = kwargs.get("blocking", False)  # always sent; has presence
        elif method == "get_storage_level":
            req.get_storage_level.relation.CopyFrom(df._plan.plan(spark.client))
        elif method == "ddl_parse":
            req.ddl_parse.ddl_string = kwargs["ddl"]
        elif method == "json_to_ddl":
            req.json_to_ddl.json_string = kwargs["json"]
        else:
            raise ValueError(f"analyze_arm: no branch for {method}")
        return getattr(req, method)

    def catalog_plan(node):
        # Wrap any LogicalPlan as a root Plan. catalog.py runs its relations eagerly
        # (_execute_and_fetch) and so do the three stat actions (cov, corr, approxQuantile),
        # so no public method hands back their plan; the fixture pins the relation itself.
        # `cat` in fixture scope is the connect plan module the classes live in, and
        # `node_plan` is this same helper under the name the stat fixtures read better with.
        plan = proto.Plan()
        plan.root.CopyFrom(node.plan(spark.client))
        return plan

    import pyspark.sql.connect.plan as cat
    import pyspark.sql.types as T  # noqa: F401
    from pyspark.storagelevel import StorageLevel

    env = {
        "spark": spark, "F": F, "W": W, "datetime": datetime, "decimal": decimal,
        "save": save, "save_as_table": save_as_table, "insert_into": insert_into, "v2": v2,
        "sql_cmd": sql_cmd, "view_cmd": view_cmd, "catalog_plan": catalog_plan, "cat": cat,
        "analyze_arm": analyze_arm, "StorageLevel": StorageLevel, "T": T,
        "command_plan": command_plan, "merge_cmd": merge_cmd,
        "node_plan": catalog_plan,
    }
    result = eval(source, env)  # noqa: S307
    if isinstance(result, Message):
        return result  # a Plan, or an AnalyzePlan request arm
    return result._plan.to_proto(spark.client)


def show_string_plan(spark, num_rows=20, truncate=20, vertical=False):
    """Build the plan behind df.show(). No public DataFrame method returns one."""
    from pyspark.sql.connect.plan import ShowString

    plan = ShowString(spark.range(5)._plan, num_rows, truncate, vertical)
    return plan.to_proto(spark.client)


def _submessages(msg):
    """Yield every submessage of msg, including inside repeated and map fields."""
    for fd, value in list(msg.ListFields()):
        if fd.type != fd.TYPE_MESSAGE:
            continue
        if fd.message_type.GetOptions().map_entry:
            if fd.message_type.fields_by_name["value"].type == fd.TYPE_MESSAGE:
                yield from value.values()
        elif fd.is_repeated:
            yield from value
        else:
            yield value


def strip_origins(msg):
    """Drop Origin metadata, which PySpark stamps on every expression with its call site.

    Latu will never emit `python_origin`, so leaving it in makes every fixture unmatchable.
    An ExpressionCommon holding only an origin is dropped entirely: in protobuf a
    present-but-empty submessage is not equal to an absent one.
    """
    if "origin" in msg.DESCRIPTOR.fields_by_name:
        msg.ClearField("origin")
    for child in _submessages(msg):
        strip_origins(child)
    for fd, value in list(msg.ListFields()):
        if fd.name == "common" and not fd.containing_oneof and fd.type == fd.TYPE_MESSAGE:
            if not fd.is_repeated and value.ByteSize() == 0:
                msg.ClearField("common")
    return msg


def normalize_plan(plan):
    """Make a plan reproducible: renumber plan_ids and lambda variable names, in place.

    Mirrors `Latu.Plan.normalize_ids/1` in lib/latu/plan.ex. **Keep the two in step** — if they
    drift, every golden test fails at once and the cause is not obvious.
    """
    normalize_plan_ids(plan)
    normalize_lambda_names(plan)
    return plan


def normalize_lambda_names(plan):
    """Renumber lambda variable names from 0, preserving x/y/z, in place.

    PySpark names them from a *process-global* counter — `fresh_var_name` gives `x_0`, then
    `x_1`, `y_2` and so on for the life of the process — so the numbers a fixture gets depend
    on how many lambdas were built before it. Generating one more would silently rewrite every
    earlier one. Same class of defect as the timezone baked into lit_timestamp.

    Renumbering is by **creation order, which the suffix already records**, not by traversal
    order. That is deliberate: it keeps this and the Elixir implementation from drifting on the
    order they happen to visit nodes in, which would be very hard to debug.
    """
    variable = "spark.connect.Expression.UnresolvedNamedLambdaVariable"
    names = set()

    def collect(msg):
        if msg.DESCRIPTOR.full_name == variable:
            names.update(msg.name_parts)
        for child in _submessages(msg):
            collect(child)

    def created_at(name):
        match = re.search(r"_(\d+)$", name)
        return (0, int(match.group(1)), name) if match else (1, 0, name)

    # Walk from the Plan itself: a fixture is a `root` relation OR a `command`, and
    # _submessages descends into whichever oneof arm is set.
    collect(plan)

    mapping, index = {}, 0
    for name in sorted(names, key=created_at):
        match = re.match(r"^(.*)_(\d+)$", name)
        if match:
            mapping[name] = "%s_%d" % (match.group(1), index)
            index += 1

    def rewrite(msg):
        if msg.DESCRIPTOR.full_name == variable:
            renamed = [mapping.get(part, part) for part in msg.name_parts]
            del msg.name_parts[:]
            msg.name_parts.extend(renamed)
        for child in _submessages(msg):
            rewrite(child)

    rewrite(plan)
    return plan


def has_plan_id(msg):
    """Whether msg's plan_id is set — for both spellings the protocol uses.

    UnresolvedAttribute and UnresolvedStar declare it `optional`, so it has presence and
    HasField answers. SubqueryExpression declares a plain int64, which has none: HasField
    raises, and the field is always meaningful there (the message only exists to point at a
    relation), so it is always set.
    """
    try:
        return msg.HasField("plan_id")
    except ValueError:
        return True


def normalize_plan_ids(plan):
    """Renumber plan_ids depth-first from 0, in place.

    Column references (UnresolvedAttribute, UnresolvedStar, SubqueryExpression) carry the
    plan_id of the relation they came from, so they are remapped too. Without that, a fixture
    like group_pivot ends up referencing a relation id that no longer exists.
    """
    mapping, counter = {}, [0]

    def assign(msg):
        for child in _submessages(msg):
            assign(child)
        is_rel = msg.DESCRIPTOR.full_name == "spark.connect.Relation"
        if is_rel and msg.common.HasField("plan_id"):
            mapping[msg.common.plan_id] = counter[0]
            msg.common.plan_id = counter[0]
            counter[0] += 1

    def rewrite(msg):
        if msg.DESCRIPTOR.full_name != "spark.connect.RelationCommon":
            if "plan_id" in msg.DESCRIPTOR.fields_by_name and has_plan_id(msg):
                msg.plan_id = mapping.get(msg.plan_id, UNRESOLVED_PLAN_ID)
        for child in _submessages(msg):
            rewrite(child)

    assign(plan)
    rewrite(plan)
    return plan


def generate(spark, normalize):
    """Write test/wire/<name>.{bin,txtpb} for every fixture. Returns the failures."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for stale in list(OUT_DIR.glob("*.bin")) + list(OUT_DIR.glob("*.txtpb")):
        stale.unlink()  # renamed fixtures must not linger

    def write(name, plan, source=None):
        strip_origins(plan)
        if normalize:
            normalize_plan(plan)
        header = f"# generated by pyspark_oracle.py\n#   {source}\n\n" if source else ""
        # deterministic= sorts map entries. Python's runtime otherwise serialises map fields
        # (reader/writer options, sql named_arguments) in an order that varies per PROCESS, so
        # two --generate runs could write the same message as different bytes. Decoded-struct
        # comparison never sees it — only "regenerate and diff" does, which is the check it
        # was quietly destroying.
        (OUT_DIR / f"{name}.bin").write_bytes(plan.SerializeToString(deterministic=True))
        (OUT_DIR / f"{name}.txtpb").write_text(header + text_format.MessageToString(plan))
        print(f"  ok   {name}")

    failed = []
    for name, source in FIXTURES:
        try:
            write(name, plan_for(spark, source), source)
        except Exception as exc:  # noqa: BLE001
            failed.append((name, f"{type(exc).__name__}: {exc}"))
    write("show_string_range", show_string_plan(spark))
    return failed


def main():
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)  # allow `| head` without a traceback

    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("expr", nargs="?", help="PySpark expression; spark, F, W in scope")
    p.add_argument("--generate", action="store_true", help=f"write fixtures to {OUT_DIR}/")
    p.add_argument("--raw-ids", action="store_true", help="keep PySpark's plan_ids as-is")
    args = p.parse_args()

    if not (args.expr or args.generate):
        p.print_help()
        return

    spark = get_session()
    normalize = not args.raw_ids

    if args.generate:
        failed = generate(spark, normalize)
        print(f"\n{len(FIXTURES) + 1 - len(failed)} fixtures written to {OUT_DIR}")
        if failed:
            print(f"\n{len(failed)} failed:")
            for name, err in failed:
                print(f"  FAIL {name}: {err}")
            sys.exit(1)
    else:
        plan = strip_origins(plan_for(spark, args.expr))
        if normalize:
            normalize_plan(plan)
        print(text_format.MessageToString(plan))


if __name__ == "__main__":
    main()
