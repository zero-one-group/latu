#!/usr/bin/env python3
"""Generate the data files the integration tests use.

Deterministic and tiny; fixtures/ is gitignored, so run this once per checkout:

    dev/.venv/bin/python dev/make_data_fixtures.py

The Spark containers mount ./fixtures at /fixtures (read-only), which is the path the
tests use.
"""

import pathlib

ROWS = [(1, "Ada"), (2, "Grace"), (3, "Linus")]

# The na/stat fixture. Nulls in every column and in every combination the tests need,
# including a row that is null all the way across — without one, `how: :all` drops nothing and
# the test would pass for the wrong reason. `score` repeats a value so freq_items and crosstab
# have something to find, and `team` is a string, so filling it with a number can be shown to
# do nothing at all.
#
# Non-null counts per row: 4, 4, 4, 3, 3, 2, 3, 0. Which makes:
#   drop_na()                  -> 3   (rows with no nulls at all)
#   drop_na(how: :all)         -> 7   (only the all-null row goes)
#   drop_na(min_non_nulls: 3)  -> 6
#   drop_na(subset: [:score])  -> 5
MEASUREMENTS = [
    # id, score, weight, team
    (1, 10.0, 1.0, "red"),
    (2, 10.0, 2.0, "red"),
    (3, 30.0, 3.0, "blue"),
    (4, None, 4.0, "blue"),
    (5, 50.0, None, "red"),
    (6, None, None, "blue"),
    (7, 70.0, 7.0, None),
    (None, None, None, None),
]

OUT = pathlib.Path("fixtures")


def main():
    OUT.mkdir(exist_ok=True)

    csv = "id,name\n" + "".join(f"{i},{n}\n" for i, n in ROWS)
    (OUT / "people.csv").write_text(csv)

    json = "".join('{"id": %d, "name": "%s"}\n' % row for row in ROWS)
    (OUT / "people.json").write_text(json)

    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table(
        {"id": pa.array([i for i, _ in ROWS], pa.int32()), "name": [n for _, n in ROWS]}
    )
    pq.write_table(table, OUT / "people.parquet")

    measurements = pa.table(
        {
            "id": pa.array([r[0] for r in MEASUREMENTS], pa.int32()),
            "score": pa.array([r[1] for r in MEASUREMENTS], pa.float64()),
            "weight": pa.array([r[2] for r in MEASUREMENTS], pa.float64()),
            "team": pa.array([r[3] for r in MEASUREMENTS], pa.string()),
        }
    )
    pq.write_table(measurements, OUT / "measurements.parquet")
    print(f"4 files written to {OUT}/")


if __name__ == "__main__":
    main()
