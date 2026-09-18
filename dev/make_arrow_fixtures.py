"""Arrow IPC stream fixtures for `Latu.Result.Arrow` and `Latu.Result.Nx`.

    python dev/make_arrow_fixtures.py            # rewrite test/arrow/*.arrow
    python dev/make_arrow_fixtures.py --check    # exit 1 if any file is out of date

Each file is one complete IPC *stream* — schema, one record batch, end marker — which is the
shape Spark Connect sends per batch, and the shape `Latu.to_arrow/2` hands back. Written by
pyarrow so the reader is checked against Arrow's own encoder rather than against itself; the
expected values live in the tests, where they can be read.

Needs pyarrow, which `dev/.venv` already has as a PySpark dependency. It reaches no server:
these are bytes, not results.
"""

from __future__ import annotations

import argparse
import pathlib
import sys

import pyarrow as pa

OUT = pathlib.Path("test/arrow")


def stream(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def stream_batches(schema: pa.Schema, batches: list[pa.RecordBatch]) -> bytes:
    """Write explicit record batches, so a present-but-empty batch survives.

    `stream()` goes through `write_table`, which drops a zero-row chunk; Spark sends one
    for `... WHERE false`, and the reader has to answer it rather than let Nx raise.
    """
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        for batch in batches:
            writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def vector_udt(vectors, dense=True):
    """Spark's VectorUDT sqlType, which is what a Vector column is in Arrow.

    struct<type:int8, size:int32, indices:list<int32>, values:list<double>>, with size and
    indices null on a dense row.
    """
    fields = [
        pa.field("type", pa.int8()),
        pa.field("size", pa.int32()),
        pa.field("indices", pa.list_(pa.int32())),
        pa.field("values", pa.list_(pa.float64())),
    ]
    rows = [
        {
            "type": 1 if dense else 0,
            "size": None if dense else len(v),
            "indices": None if dense else list(range(len(v))),
            "values": list(v),
        }
        for v in vectors
    ]
    return pa.array(rows, type=pa.struct(fields))


def cases() -> dict[str, pa.Table]:
    return {
        # The arms that decode.
        "doubles": pa.table({"v": pa.array([1.5, 2.5, 0.0, -3.25], pa.float64())}),
        "int64s": pa.table({"v": pa.array([1, -2, 3, 4], pa.int64())}),
        "int32s": pa.table({"v": pa.array([1, -2, 3], pa.int32())}),
        "float32s": pa.table({"v": pa.array([1.5, 2.5], pa.float32())}),
        "two_columns": pa.table(
            {
                "a": pa.array([1.0, 2.0, 3.0], pa.float64()),
                "b": pa.array([10, 20, 30], pa.int64()),
            }
        ),
        # What `vector_to_array` gives, and what a fitted `features` column is.
        "list_uniform": pa.table(
            {"v": pa.array([[1.5, 2.5], [0.5, 3.5], [0.0, 0.0]], pa.list_(pa.float64()))}
        ),
        "vector_dense": pa.table(
            {"features": vector_udt([[1.5, 2.5], [0.5, 3.5], [0.0, 0.0], [4.0, 4.5]])}
        ),
        # What must be refused.
        "list_ragged": pa.table(
            {"v": pa.array([[1.0, 2.0], [3.0]], pa.list_(pa.float64()))}
        ),
        "vector_sparse": pa.table(
            {"features": vector_udt([[1.5, 2.5], [0.5, 3.5]], dense=False)}
        ),
        "doubles_with_null": pa.table({"v": pa.array([1.0, None, 3.0], pa.float64())}),
        # Arrow allows one name twice; a Spark join whose sides share a non-key name sends it.
        "duplicate_names": pa.Table.from_arrays(
            [pa.array([1.0, 2.0], pa.float64()), pa.array([10.0, 20.0], pa.float64())],
            names=["v", "v"],
        ),
        "strings": pa.table({"v": pa.array(["a", "b"], pa.string())}),
        "booleans": pa.table({"v": pa.array([True, False, True], pa.bool_())}),
        # A schema and no batch at all, which pyarrow writes for an empty table and Spark does
        # not — the reader has to answer for both.
        "empty": pa.table({"v": pa.array([], pa.float64())}),
        # One row whose list is empty: a zero-width numeric list, which Nx cannot reshape.
        "empty_lists": pa.table({"n": pa.array([[]], pa.list_(pa.float64()))}),
        # A binary column beside a numeric one, each way round. Binary has three buffers, so
        # the batch walk misaligns and pruning fails unless the reader counts them.
        "binary_before_num": pa.table(
            {
                "b": pa.array([b"\x01\x02"], pa.binary()),
                "n": pa.array([42], pa.int64()),
            }
        ),
        "num_before_binary": pa.table(
            {
                "n": pa.array([42], pa.int64()),
                "b": pa.array([b"\x01\x02"], pa.binary()),
            }
        ),
        # Big enough that its buffers are refc binaries. Under 64 bytes the BEAM copies into
        # the process heap whatever the reader does, so a small batch cannot show whether a
        # column's buffer still points into the whole one.
        "big_two": pa.table(
            {
                "a": pa.array([float(i) for i in range(5000)], pa.float64()),
                "b": pa.array(list(range(5000)), pa.int64()),
            }
        ),
    }


def raw_cases() -> dict[str, bytes]:
    """Fixtures that need explicit record batches rather than a table."""
    n = pa.schema([pa.field("n", pa.int64())])
    zero_rows = pa.record_batch([pa.array([], pa.int64())], schema=n)
    # The same shape as list_uniform, with no rows: an empty partition's batch ahead of a full
    # one must not be read as a width-0 column contradicting the width-2 one.
    v = pa.schema([pa.field("v", pa.list_(pa.float64()))])
    zero_row_list = pa.record_batch([pa.array([], pa.list_(pa.float64()))], schema=v)
    # The same, for a dense Vector column: an empty batch has no type discriminator to read, so
    # the reader must drop it before the sparse check rather than refuse it as sparse.
    vec = pa.schema([pa.field("features", vector_udt([]).type)])
    zero_row_vector = pa.record_batch([vector_udt([])], schema=vec)
    return {
        # A present but empty batch: group() passes, and build() must not reach Nx empty.
        "zero_row_batch": stream_batches(n, [zero_rows]),
        "zero_row_list_batch": stream_batches(v, [zero_row_list]),
        "zero_row_vector_batch": stream_batches(vec, [zero_row_vector]),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="exit 1 if a file is stale")
    args = parser.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    streams = {name: stream(table) for name, table in cases().items()}
    streams.update(raw_cases())
    stale = []

    for name, wanted in streams.items():
        path = OUT / f"{name}.arrow"

        if args.check:
            if not path.exists() or path.read_bytes() != wanted:
                stale.append(path)
        else:
            path.write_bytes(wanted)
            print(f"{name:20s} {len(wanted):7d} bytes")

    if args.check:
        for path in stale:
            print(f"stale: {path}", file=sys.stderr)
        print("up to date" if not stale else f"{len(stale)} stale", file=sys.stderr)
        return 1 if stale else 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
