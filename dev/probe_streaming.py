#!/usr/bin/env python3
"""What a streaming query looks like on the wire, and what its commands cost.

Written before Latu builds any of it, so every answer comes from a server rather than from the
protos. The wire shapes a golden would pin, and the semantics no golden can reach: what a
blocking command costs in reattaches, what the progress JSON actually contains, and whether
`AvailableNow` terminates by itself.

    docker compose up -d spark-reattach
    SPARK_REMOTE=sc://localhost:15003 dev/.venv/bin/python dev/probe_streaming.py
    docker compose logs --tail 60 spark-reattach     # the console sink and ProgressReporter

**Section 3 only answers on a server with a short sender.** It reads
`senderMaxStreamDuration` off the session and sizes the wait to span it three times, and says
so and skips rather than wait twenty minutes on the 2m default. `SPARK_REMOTE` is probably
already exported to `:15002` from `probe_writes.py`, which is exactly how that section comes
back empty.

Every blocking command runs on a daemon thread with a deadline, because a probe asking what
blocking calls cost must not be able to block forever. `processAllAvailable` is why: it never
returns on an unbounded source, since the rate source always has more data available, so it is
asked of a bounded file source in section 5 rather than of the rate query in section 2.

It leaves the `probe` query running and prints an `export LATU_SID=` line, because
`dev/probe_streaming_rejoin.py` asks whether that query outlives this process. That probe is
also the cleanup; `docker compose restart spark-reattach` is the blunt version.

Re-run at any Spark bump.
"""

import json
import os
import threading
import time

from pyspark.sql import SparkSession

REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:15003")
SENDER_CONF = "spark.connect.execute.reattachable.senderMaxStreamDuration"


class Counter:
    """Counts the two streaming RPCs, and keeps every streaming Command that went out.

    Hooked at the stub rather than at a client method, because PySpark reaches the listener
    bus from a background thread by a route that changes between releases. Whatever the Python
    path, it ends in one of these two RPCs.
    """

    STREAMING = (
        "write_stream_operation_start",
        "streaming_query_command",
        "streaming_query_manager_command",
        "streaming_query_listener_bus_command",
    )

    def __init__(self, client):
        self.n = {"ExecutePlan": 0, "ReattachExecute": 0}
        self.commands = []
        for name in self.n:
            self._wrap(client, name)

    def _wrap(self, client, name):
        original = getattr(client._stub, name)

        def counted(request, *args, **kwargs):
            self.n[name] += 1
            self._note(request)
            return original(request, *args, **kwargs)

        setattr(client._stub, name, counted)

    def _note(self, request):
        plan = getattr(request, "plan", None)
        if plan is None:
            return
        arm = plan.command.WhichOneof("command_type")
        if arm in self.STREAMING:
            self.commands.append((arm, plan.command))

    def reset(self):
        for name in self.n:
            self.n[name] = 0

    def __str__(self):
        return " ".join(f"{name}={count}" for name, count in self.n.items())


def capture(owner, method):
    """Intercept a method on one object, keeping (args, result). None if it is not there."""
    original = getattr(owner, method, None)
    if original is None:
        return None
    seen = []

    def spy(*args, **kwargs):
        result = original(*args, **kwargs)
        seen.append((args, result))
        return result

    setattr(owner, method, spy)
    return seen


def blocking(label, fun, deadline, rpc):
    """Call something that may never return, and report either way."""
    outcome = {}

    def run():
        try:
            outcome["value"] = fun()
        except BaseException as error:  # noqa: BLE001 - reported, not handled
            outcome["error"] = error

    rpc.reset()
    thread = threading.Thread(target=run, daemon=True)
    start = time.monotonic()
    thread.start()
    thread.join(deadline)
    elapsed = time.monotonic() - start

    if thread.is_alive():
        print(f"{label}  NO RETURN in {elapsed:.1f} s, {rpc} (left running)")
    elif "error" in outcome:
        error = outcome["error"]
        print(f"{label}  raised {type(error).__name__} after {elapsed:.1f} s, {rpc}: {error}")
    else:
        print(f"{label}  returned {outcome['value']!r} after {elapsed:.1f} s, {rpc}")
    return outcome.get("value")


def seconds(duration):
    """Spark's time conf spelling, enough of it: 5s, 2m, 1h, or bare millis."""
    if duration is None:
        return None
    text = duration.strip().lower()
    for suffix, scale in (("ms", 0.001), ("s", 1), ("m", 60), ("h", 3600)):
        if text.endswith(suffix):
            return float(text[: -len(suffix)]) * scale
    return float(text) / 1000


def input_rows(progress):
    """Rows per batch, off the sources, because the top-level field arrives null over Connect."""
    return sum(
        (source.get("numInputRows") or 0)
        for report in progress
        for source in report.get("sources", [])
    )


def section(number, title, fun):
    print(f"\n=== {number}. {title} ".ljust(78, "="))
    try:
        fun()
    except Exception as error:  # one failed question must not cost the others
        print(f"FAILED: {type(error).__name__}: {error}")


def main():
    spark = SparkSession.builder.remote(REMOTE).getOrCreate()
    session_id = spark.client._session_id
    base = f"/tmp/latu_probe_stream/{int(time.time())}"

    try:
        sender = spark.conf.get(SENDER_CONF)
    except Exception as error:
        sender = None
        print(f"could not read {SENDER_CONF}: {type(error).__name__}: {error}")

    print(f"Spark {spark.version} at {REMOTE}")
    print(f"session {session_id}")
    print(f"{SENDER_CONF} = {sender}")
    print(f"checkpoints and sources under {base}, fresh per run so offsets start at 0")

    sent = capture(spark.client, "execute_command")
    rpc = Counter(spark.client)

    query = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", "5")
        .load()
        .withWatermark("timestamp", "10 seconds")
        .writeStream.format("console")
        .queryName("probe")
        .trigger(processingTime="1 second")
        .option("checkpointLocation", f"{base}/probe")
        .start()
    )
    results = capture(query, "_execute_streaming_query_cmd")

    def wire():
        print(sent[-1][0][0])
        print(f"id      {query.id}")
        print(f"runId   {query.runId}")
        print(f"name    {query.name}")

    def arms():
        time.sleep(6)
        print(f"isActive     {query.isActive}")
        print(f"status       {query.status}")
        print(f"exception    {query.exception()}")
        print("explain:")
        query.explain(extended=False)

    def await_termination():
        span = seconds(sender)
        if span is None or span > 10:
            print(
                f"skipped: {SENDER_CONF} is {sender}, so a wait long enough to span it three "
                f"times is not worth {(span or 0) * 3 / 60:.0f} minutes. Re-run with "
                "SPARK_REMOTE=sc://localhost:15003 against spark-reattach."
            )
            return
        wait = int(span * 3) + 2
        blocking(
            f"awaitTermination({wait}s)", lambda: query.awaitTermination(wait), wait * 2, rpc
        )
        print(
            f"About {wait // int(span)} ReattachExecute means one empty reattach per sender "
            "duration, so Latu's @max_empty_reattaches (100) caps a blocking wait at "
            f"{100 * span / 60:.0f} minutes here and 200 on the 2m default. 0 means the server "
            "holds the stream for streaming commands and the guard never sees it."
        )

    def progress():
        last = query.lastProgress
        if results:
            print("--- the StreamingQueryCommandResult off the wire ---")
            print(str(results[-1][1])[:3000])
        print(
            f"\n--- what PySpark parses it into: a {type(last).__name__}, "
            f"id a {type(last.get('id')).__name__} ---"
        )
        print(json.dumps(last, indent=2, default=str))
        print(f"recentProgress   {len(query.recentProgress)} entries")

    def bounded():
        source = f"{base}/src"
        for _ in range(3):
            spark.range(2).repartition(1).write.mode("append").parquet(source)
        print(f"wrote 3 part files, 6 rows, to {source}")

        def stream(name, trigger):
            return (
                spark.readStream.schema("id BIGINT")
                .option("maxFilesPerTrigger", "1")
                .parquet(source)
                .writeStream.format("console")
                .queryName(name)
                .trigger(**trigger)
                .option("checkpointLocation", f"{base}/{name}")
                .start()
            )

        drained = stream("bounded", {"processingTime": "1 second"})
        try:
            blocking("processAllAvailable  ", lambda: drained.processAllAvailable(), 60, rpc)
            reports = drained.recentProgress
            print(f"batches {len(reports)}, rows {input_rows(reports)} (expect 3 and 6)")
        finally:
            drained.stop()

        once = stream("available_now", {"availableNow": True})
        try:
            blocking("AvailableNow await   ", lambda: once.awaitTermination(60), 90, rpc)
            reports = once.recentProgress
            print(
                f"isActive {once.isActive}, batches {len(reports)}, rows {input_rows(reports)}"
            )
        finally:
            if once.isActive:
                once.stop()

    def listeners():
        from pyspark.sql.streaming.listener import StreamingQueryListener

        class Listener(StreamingQueryListener):
            def onQueryStarted(self, event):
                pass

            def onQueryProgress(self, event):
                pass

            def onQueryIdle(self, event):
                pass

            def onQueryTerminated(self, event):
                pass

        manager = spark.streams
        before = len(rpc.commands)
        manager.addListener(Listener())
        time.sleep(3)

        for arm, command in rpc.commands[before:]:
            print(f"--- {arm} ---")
            print(str(command)[:900])
        if len(rpc.commands) == before:
            print("addListener sent no streaming command through either RPC")

        lister = getattr(manager, "listListeners", None)
        print(f"listListeners    {lister() if lister else 'no such method on 4.2.0 Connect'}")

    section(1, "the WriteStreamOperationStart PySpark sends", wire)
    section(2, "the non-blocking StreamingQueryCommand arms", arms)
    section(3, "a blocking awaitTermination against this server's sender", await_termination)
    section(4, "the progress JSON, off the wire and as PySpark parses it", progress)
    section(5, "a bounded source: processAllAvailable, then AvailableNow", bounded)
    section(6, "what addListener puts on the wire", listeners)

    print(f"\nThe `probe` query is still running. Next:\n\n    export LATU_SID={session_id}")
    print(f"    SPARK_REMOTE={REMOTE} dev/.venv/bin/python dev/probe_streaming_rejoin.py")


if __name__ == "__main__":
    main()
