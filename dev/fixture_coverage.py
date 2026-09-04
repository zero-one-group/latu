#!/usr/bin/env python3
"""How many PySpark fixtures are covered by a golden test.

The project's progress metric, and easy to get wrong by hand — twice so far. Needs neither
Spark nor PySpark.

    python dev/fixture_coverage.py
    python dev/fixture_coverage.py --uncovered
"""

from __future__ import annotations

import argparse
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent
NAME = re.compile(r'"([a-z_0-9]+)"')

# Lines to read from an assert_wire call, because a long one wraps and the name lands below it.
WINDOW = 5


def fixtures() -> set[str]:
    """Every name in test/wire, which is what --generate wrote."""
    return {p.stem for p in (ROOT / "test/wire").glob("*.bin")}


def covered() -> set[str]:
    """Fixture names in an assert_wire call, which the formatter may have split over lines."""
    known = fixtures()
    hits: set[str] = set()
    for path in (ROOT / "test").rglob("*.exs"):
        lines = path.read_text().splitlines()
        for index, line in enumerate(lines):
            if "assert_wire" in line:
                hits |= set(NAME.findall("\n".join(lines[index:index + WINDOW]))) & known
    return hits


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uncovered", action="store_true", help="list what is left")
    args = parser.parse_args()

    all_names, done = fixtures(), covered()
    print(f"{len(done)} of {len(all_names)} fixtures covered by a golden test")

    if args.uncovered:
        for name in sorted(all_names - done):
            print(f"  {name}")


if __name__ == "__main__":
    main()
