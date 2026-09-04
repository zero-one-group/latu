#!/usr/bin/env python3
"""Control for dev/probe_writes.exs: the same writes through PySpark.

The Elixir probe measured an intermittent ~60s lockstep stall in write stages (~1 run in 8).
PySpark stalling too means the server/container; PySpark never stalling across a few runs
means it is Latu's.

    export SPARK_REMOTE='sc://localhost:15002'
    dev/.venv/bin/python dev/probe_writes.py
"""

import os
import time

from pyspark.sql import SparkSession


def timed(label, fun):
    start = time.monotonic()
    fun()
    print(f"{label:<44}{(time.monotonic() - start) * 1000:>10.0f} ms")


def main():
    spark = SparkSession.builder.getOrCreate()
    print(f"Spark {spark.version} — one write per row, wall time\n")

    df = spark.range(5)
    base = f"/tmp/latu_probe_py/{int(time.time())}"

    timed("collect (baseline query, same session)", lambda: df.collect())

    # six samples: the stall hit roughly 1 in 8 writes on the Elixir side
    for n in range(1, 7):
        timed(f"write parquet #{n} to /tmp, same session",
              lambda n=n: df.write.parquet(f"{base}/p{n}"))

    timed("write csv to /tmp, same session",
          lambda: df.write.option("header", True).csv(f"{base}/c1"))

    spark2 = SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).create()
    df2 = spark2.range(5)

    for n in (1, 2):
        timed(f"write parquet #{n}, FRESH session",
              lambda n=n: df2.write.parquet(f"{base}/s2_p{n}"))

    timed("save_as_table (warehouse bind mount)",
          lambda: df2.write.saveAsTable(f"latu_probe_py_{int(time.time())}"))


if __name__ == "__main__":
    main()
