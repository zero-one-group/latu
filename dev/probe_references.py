#!/usr/bin/env python3
"""What the server does with cross-frame references and subqueries.

Settled two design decisions — see docs/decisions.md (M9.1).
Behaviour only; wire shapes come from `python dev/pyspark_oracle.py '<expr>'`.

    export SPARK_REMOTE='sc://localhost:15002'
    dev/.venv/bin/python dev/probe_references.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import SQL, Project, SubqueryAlias, WithRelations

WIDTH = 46


def condition(err):
    """Spark's error class, which is the measurement — not the message."""
    for name in ("getCondition", "getErrorClass"):
        fun = getattr(err, name, None)
        if fun is not None:
            try:
                return fun() or type(err).__name__
            except Exception:
                pass
    return type(err).__name__


def run(label, fun):
    try:
        print(f"  {label:<{WIDTH}}OK    {fun()}")
    except Exception as err:  # the error class is what we came for
        first = str(err).strip().splitlines()[0][:70]
        print(f"  {label:<{WIDTH}}FAIL  {condition(err)} | {first}")


def shape(spark, target):
    """Top relation arm and reference count, straight off the wire. Takes a frame or a plan."""
    root = getattr(target, "_plan", target).to_proto(spark.client).root
    arm = root.WhichOneof("rel_type")
    refs = len(root.with_relations.references) if arm == "with_relations" else 0
    return f"{arm}, refs={refs}"


def main():
    spark = SparkSession.builder.getOrCreate()
    print(f"Spark {spark.version}\n")

    a = spark.range(10).withColumnRenamed("id", "aid")
    b = spark.range(5)

    print("1. bare cross-frame reference (no hoisting — what PySpark sends)")
    print(f"  {'wire shape of b.select(a.aid)':<{WIDTH}}      {shape(spark, b.select(a.aid))}")
    run("b.select(a.aid)", lambda: b.select(a.aid).collect())
    run("b.select(a.id) — name also in b", lambda: b.select(spark.range(10).id).collect())
    run("b.filter(a.aid > 3)", lambda: b.filter(a.aid > 3).collect())

    print("\n2. the same reference hoisted into WithRelations by hand")
    hoisted = DataFrame(WithRelations(Project(b._plan, [a.aid]), [a._plan]), spark)
    print(f"  {'wire shape':<{WIDTH}}      {shape(spark, hoisted)}")
    run("collect", lambda: [r[0] for r in hoisted.collect()])
    tagged = DataFrame(WithRelations(Project(b._plan, [b.id, a.aid]), [a._plan]), spark)
    run("both frames' columns", lambda: tagged.collect()[:2])

    print("\n3. ambiguity, and alias as the fix")
    d = spark.range(5)
    run("d.join(d, on=d.id == d.id).select(d.id)",
        lambda: d.join(d, on=d.id == d.id).select(d.id).count())
    left, right = d.alias("l"), d.alias("r")
    run("aliased join, select(left.id)",
        lambda: left.join(right, on=left.id == right.id).select(left.id).count())

    print("\n4. subqueries — the references PySpark does hoist")
    one = spark.range(1).select(F.lit(10).alias("x"))
    print(f"  {'wire shape of b.select(one.scalar())':<{WIDTH}}      "
          f"{shape(spark, b.select(one.scalar()))}")
    run("select(one.scalar())", lambda: [r[0] for r in b.select(one.scalar()).collect()])
    run("filter(id < one.scalar())", lambda: b.filter(b.id < one.scalar()).count())
    run("filter(one.exists())", lambda: b.filter(one.exists()).count())
    run("filter(id.isin(subquery))",
        lambda: b.filter(b.id.isin(spark.range(2))).count())
    run("nested: select(abs(one.scalar() * -1))",
        lambda: [r[0] for r in b.select(F.abs(one.scalar() * -1)).collect()])
    run("referenced frame already in the tree",
        lambda: b.filter(b.id < b.select(F.max("id")).scalar()).count())
    print(f"  {'wire shape of that one':<{WIDTH}}      "
          f"{shape(spark, b.filter(b.id < b.select(F.max('id')).scalar()))}")

    print("\n5. sql with DataFrame arguments")
    # spark.sql is eager and hands back the result relation, so the plan comes from SQL itself.
    sql_plan = SQL("select count(*) as n from v", [], {}, [SubqueryAlias(b._plan, "v")])
    print(f"  {'wire shape':<{WIDTH}}      {shape(spark, sql_plan)}")
    run("sql('... from {v}', v=b)",
        lambda: spark.sql("select count(*) as n from {v}", v=b).collect()[0][0])

    print("\n6. identity: which plan_id a hoisting verb hands downstream")
    p = b.select(b.id, one.scalar().alias("s"))
    root = p._plan.to_proto(spark.client).root
    print(f"  root_plan_id={p._plan._root_plan_id} "
          f"plan_id_with_rel={p._plan._plan_id_with_rel} "
          f"wire top plan_id={root.common.plan_id}")
    print(f"  a downstream p.s tags plan_id="
          f"{p.s._expr.to_plan(spark.client).unresolved_attribute.plan_id}")


if __name__ == "__main__":
    main()
