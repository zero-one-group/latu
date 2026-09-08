#!/usr/bin/env python3
"""Does a streaming query outlive the client that started it, and what stops it.

The companion to `dev/probe_streaming.py`, and a separate process on purpose: the client that
started the query is gone by the time this runs. It joins the same server-side session by id,
the way Latu's interrupt recipe has a second `connect/2` join an existing `session_id:`.

Four questions, and together they decide whether a streaming query is a session-lifetime
resource or an orphan the caller has to end: whether the query is still listed, whether a
handle can be recovered from its id alone, whether `interruptAll` reaches it, and whether
`stop` does. The remaining `StreamingQueryManagerCommand` arms answer here too, so between the
two probes every arm of all three streaming commands has answered once, except the two that
carry a serialised listener.

    dev/.venv/bin/python dev/probe_streaming.py      # prints an `export LATU_SID=` line
    export LATU_SID=...
    dev/.venv/bin/python dev/probe_streaming_rejoin.py

It stops every query it finds, so it is also the cleanup for the first probe.
"""

import os
import time

from pyspark.sql import SparkSession

REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:15003")


def listed(spark):
    return [(query.name, str(query.id)) for query in spark.streams.active]


def section(number, title, fun):
    print(f"\n=== {number}. {title} ".ljust(78, "="))
    try:
        fun()
    except Exception as error:  # one failed question must not cost the rest
        print(f"FAILED: {type(error).__name__}: {error}")


def main():
    session_id = os.environ.get("LATU_SID")
    if not session_id:
        raise SystemExit("set LATU_SID to the id dev/probe_streaming.py printed")

    spark = SparkSession.builder.remote(f"{REMOTE}/;session_id={session_id}").getOrCreate()
    print(f"Spark {spark.version} at {REMOTE}, rejoined session {session_id}")

    active = listed(spark)

    def outlived():
        print(f"active   {active}")
        print(
            "A query outlives its client, so stop is load-bearing and a forgotten query has "
            "no bound."
            if active
            else "Closing the channel ended it, so a query is a session-lifetime resource."
        )

    def recover():
        if not active:
            print("nothing active, so there is no handle to recover")
            return
        query = spark.streams.get(active[0][1])
        print(f"streams.get({active[0][1]}) -> name {query.name}, isActive {query.isActive}")
        print(f"status   {query.status}")

    def await_any():
        start = time.monotonic()
        terminated = spark.streams.awaitAnyTermination(3)
        print(
            f"awaitAnyTermination(3s) returned {terminated} after "
            f"{time.monotonic() - start:.1f} s"
        )

    def interrupt():
        if not active:
            print("nothing active to interrupt")
            return
        print(f"interruptAll -> {spark.interruptAll()}")
        time.sleep(2)
        remaining = listed(spark)
        print(f"active   {remaining}")
        print(
            "Interrupt does not reach a streaming query, so stop is the only route."
            if remaining
            else "Interrupt stops streaming queries, so Latu's interrupt verbs already do, "
            "which also means an unrelated interrupt_all kills them."
        )

    def stop():
        for query in spark.streams.active:
            query.stop()
        time.sleep(2)
        print(f"active   {listed(spark)}")
        print(f"resetTerminated -> {spark.streams.resetTerminated()}")

    section(1, "does the query outlive the client that started it", outlived)
    section(2, "recovering a handle from an id alone", recover)
    section(3, "awaitAnyTermination on a live query", await_any)
    section(4, "does interruptAll reach a streaming query", interrupt)
    section(5, "stop, and the session afterwards", stop)


if __name__ == "__main__":
    main()
