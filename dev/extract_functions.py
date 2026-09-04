#!/usr/bin/env python3
"""Derive Latu's function registry from PySpark's Connect client.

The Connect client is the only correct source. `pyspark/sql/functions/builtin.py` — the classic
client — names Catalyst functions that Connect spells differently: `count_distinct` is
`count_distinct` there and `count(is_distinct=True)` on the wire. Read
`pyspark/sql/connect/functions/builtin.py`, which builds the protos Latu has to match.

Shapes, not names, are what this decides. See `docs/decisions.md`.

    python dev/extract_functions.py
    python dev/extract_functions.py --json
"""

import argparse
import ast
import json
import pathlib
import sys

HELPERS = {
    "_invoke_function",
    "_invoke_function_over_columns",
    "_invoke_binary_math_function",
    "_invoke_higher_order_function",
}

# Not Latu's to wrap. UDFs are out of scope permanently; the camelCase spellings
# are PySpark's own deprecated aliases; the sort helpers and `lit`/`col`/`expr` already live on
# `Latu.Column`; the partitioning transforms are a separate namespace in PySpark too.
EXCLUDE = {
    "udf", "udtf", "arrow_udtf", "call_udf", "broadcast",
    # `column` is PySpark's own alias for `col` and is absent from 4.2.0; kept so that it stays
    # excluded if a later version brings it back.
    "lit", "col", "column", "expr", "asc", "desc",
    "asc_nulls_first", "asc_nulls_last", "desc_nulls_first", "desc_nulls_last",
    "bucket", "days", "hours", "months", "years",
    # PySpark's own deprecated camelCase aliases of functions Latu already has under Spark's
    # snake_case name. The comment above always said these were excluded; now they are.
    "approxCountDistinct", "bitwiseNOT", "countDistinct", "shiftLeft", "shiftRight",
    "shiftRightUnsigned", "sumDistinct", "toDegrees", "toRadians",
    # Spark ships two spellings of each of these — a Column method and a SQL function, with
    # different wire names for the same meaning (`isNull` and `isnull`). `Latu.Column` owns
    # them, so the registry must not generate a twin. `contains` and `rlike` are worse: same
    # Latu name, same wire name, two modules. See docs/deviations.md.
    "isnull", "isnotnull", "isnan",
    "startswith", "endswith", "contains",
    "like", "ilike", "rlike",
    # Same rule, from the operator table rather than the predicates: `Latu.Column.pow/2`
    # already sends `power`, and two names for one wire call is what that rule prevents.
    "pow",
}


def _str(node):
    ok = isinstance(node, ast.Constant) and isinstance(node.value, str)
    return node.value if ok else None


def _params(fn):
    a = fn.args
    pos = [p.arg for p in a.posonlyargs] + [p.arg for p in a.args]
    ndef = len(a.defaults)
    req = pos[: len(pos) - ndef] if ndef else pos
    opt = pos[len(pos) - ndef :] if ndef else []
    return req, opt, (a.vararg.arg if a.vararg else None)


def _substituted(fn, param):
    """What an omitted optional parameter is replaced with, if the body replaces it.

    **The signature default is usually not what reaches the wire.** PySpark writes
    `_mode = lit("GCM") if mode is None else mode`, so a parameter defaulting to `None` sends
    `"GCM"`. Thirty-five of Spark's fifty-four optional parameters do this, and reading the
    signature instead would send NULL for every one.
    """
    for node in ast.walk(fn):
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.IfExp):
            continue

        test = node.value.test
        if not (isinstance(test, ast.Compare) and isinstance(test.left, ast.Name)
                and test.left.id == param and len(test.ops) == 1
                and isinstance(test.comparators[0], ast.Constant)
                and test.comparators[0].value is None):
            continue

        # `x is None` puts the replacement first; `x is not None` puts it second.
        if isinstance(test.ops[0], ast.Is):
            return node.value.body
        if isinstance(test.ops[0], ast.IsNot):
            return node.value.orelse

    return None


def _sent_default(fn, param, default):
    """The value actually sent for an omitted parameter, or None if it is not a constant.

    Returns `(ok, value)`. A computed replacement — `lit(py_random.randint(...))` for a seed,
    or a Decimal — is not a constant, and the function stays hand-written.
    """
    replacement = _substituted(fn, param)

    if replacement is None:
        try:
            return True, ast.literal_eval(default)
        except ValueError:
            return False, None

    if not (isinstance(replacement, ast.Call)
            and getattr(replacement.func, "id", "") == "lit"
            and len(replacement.args) == 1):
        return False, None

    inner = replacement.args[0]
    if isinstance(inner, ast.Constant):
        return True, inner.value

    # `lit(decimal.Decimal(0))` — a constant, but not a Python literal. Spark distinguishes a
    # decimal zero from an integer zero on the wire, so it cannot be flattened to 0.
    if (isinstance(inner, ast.Call)
            and isinstance(inner.func, ast.Attribute) and inner.func.attr == "Decimal"
            and len(inner.args) == 1 and isinstance(inner.args[0], ast.Constant)):
        return True, {"decimal": inner.args[0].value}

    return False, None


def _arg_name(node):
    """The parameter an argument came from, seen through `_to_col` / `lit` wrappers."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
        if node.func.id in ("_to_col", "lit", "_to_seq") and node.args:
            return _arg_name(node.args[0])
    if isinstance(node, ast.Starred):
        return "*" + _arg_name(node.value)
    return "<expr>"


def _local_strings(fn):
    """Local `name = "literal"` bindings, so a wire name held in a variable is still visible.

    Spark's 46 sketch functions all open with `fn = "kll_sketch_agg_double"` and then call
    `_invoke_function_over_columns(fn, ...)`. Reading only literal arguments made every one of
    them look like it built no call at all, and they were nearly hand-written on that basis.
    """
    bindings = {}
    for node in ast.walk(fn):
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)):
            bindings[node.targets[0].id] = node.value.value

    return bindings


def _invocations(fn):
    """Every `_invoke_*` / `UnresolvedFunction` call in the body."""
    locals_ = _local_strings(fn)

    def name_of(node):
        return _str(node) or (locals_.get(node.id) if isinstance(node, ast.Name) else None)

    out = []
    for node in ast.walk(fn):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)):
            continue
        if node.func.id in HELPERS and node.args:
            name = name_of(node.args[0])
            if name:
                out.append((name, [_arg_name(a) for a in node.args[1:]], False))
        elif node.func.id == "UnresolvedFunction" and node.args:
            name = name_of(node.args[0])
            distinct = any(
                k.arg == "is_distinct" and isinstance(k.value, ast.Constant) and k.value.value
                for k in node.keywords
            )
            if name:
                out.append((name, ["<exprs>"], distinct))
    return out


def classify(fn):
    """Shape of one PySpark function, or why it is irregular.

    Uniform shapes get a registry row. Anything else is hand-written, because "optional
    argument" is at least five different behaviours: omit-or-append (`round`), argument order
    reversing (`trim`), a non-nil default always sent (`split`), a mix of both (`lag`), and an
    arity that changes the wire name (`log`).
    """
    name = fn.name
    req, opt, vararg = _params(fn)
    calls = _invocations(fn)
    wires = sorted({c[0] for c in calls})
    higher_order = any(
        isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "_invoke_higher_order_function"
        for n in ast.walk(fn)
    )
    distinct = any(c[2] for c in calls)

    if not wires:
        return dict(name=name, shape="irregular", reason="builds no UnresolvedFunction")
    if len(wires) > 1:
        reason = "wire name varies with arity: %s" % wires
        return dict(name=name, shape="irregular", reason=reason)
    wire = wires[0]
    row = dict(name=name, wire=wire, distinct=distinct)

    if higher_order:
        return dict(row, shape="irregular", reason="higher-order")

    # A shape with no optional argument must invoke the function exactly one way. More than
    # one means a conditional body, and a generated row would send whichever branch happened
    # to be read. `convert_timezone` is the one function in 383 this catches: its first
    # parameter is Optional with no default, so it looks fixed-arity and is not.
    branches = {tuple(call[1]) for call in calls}
    if not opt and len(branches) != 1:
        return dict(row, shape="irregular", reason="conditional body: %s" % sorted(branches))

    # And the one branch must pass every parameter. A local rebinding renames an argument
    # (`pos` becomes `_pos`), which is harmless, but a different *count* means the wrapper
    # drops or adds one, and a generated row would be wrong in a way no fixture covers.
    if not opt:
        branch = next(iter(branches))
        wanted = len(req) + (1 if vararg else 0)

        # `<exprs>` means the arguments were built by a comprehension before the call, as
        # `count_distinct` does, so there is nothing to count. The wire name and the distinct
        # flag are still read from it.
        if branch != ("<exprs>",) and len(branch) != wanted:
            reason = "sends %d of %d args" % (len(branch), wanted)
            return dict(row, shape="irregular", reason=reason)

    if vararg and not req:
        return dict(row, shape="variadic")
    if vararg and len(req) == 1 and not opt:
        return dict(row, shape="req_variadic", arity=1)
    if vararg:
        return dict(row, shape="irregular", reason="required args plus varargs")
    if opt:
        shapes = sorted({tuple(c[1]) for c in calls})

        # Omit-or-append: one branch per prefix of the optional arguments. A leading underscore
        # is PySpark's rebinding convention (`_endianness = lit(endianness) ...`) and says
        # nothing about order, so it is stripped before comparing — unlike `trim`, which really
        # does reorder and is still caught.
        bare = sorted(tuple(a.lstrip("_") for a in shape) for shape in shapes)
        wanted = sorted(tuple(req + opt[:i]) for i in range(len(opt) + 1))
        if bare == wanted:
            return dict(row, shape="optional_tail", arity=len(req), optional=len(opt))

        # Always sent: one branch carrying every argument, with a constant standing in for each
        # omitted one. `split`'s limit is -1 and `mask`'s four characters are "X", "x", "n" and
        # NULL — none of which can be inferred from the signature.
        if len(shapes) == 1 and len(shapes[0]) == len(req) + len(opt):
            resolved = [_sent_default(fn, name_, default)
                        for name_, default in zip(opt, fn.args.defaults)]
            if all(ok for ok, _value in resolved):
                return dict(row, shape="default_tail", arity=len(req),
                            defaults=[value for _ok, value in resolved])
            return dict(row, shape="irregular", reason="a default is computed, not constant")

        return dict(row, shape="irregular", reason="non-uniform optional args: %s" % (shapes,))
    return dict(row, shape="fixed", arity=len(req))


def unaccounted(path, rows):
    """Public functions in PySpark's client that are neither a row nor deliberately excluded.

    A Spark upgrade adding functions is exactly the moment this matters: without it, new ones
    are simply absent and nothing says so.
    """
    tree = ast.parse(path.read_text())
    public = {n.name for n in tree.body
              if isinstance(n, ast.FunctionDef) and not n.name.startswith("_")}

    return sorted(public - {r["name"] for r in rows} - EXCLUDE)


def collect(path):
    tree = ast.parse(path.read_text())
    best = {}
    for fn in tree.body:
        if not isinstance(fn, ast.FunctionDef) or fn.name.startswith("_"):
            continue
        if fn.name in EXCLUDE:
            continue
        row = classify(fn)
        # @overload stubs share a name with the real definition; keep the one that builds
        # something.
        prev = best.get(fn.name)
        if prev is None or (prev["shape"] == "irregular" and row["shape"] != "irregular"):
            best[fn.name] = row
    return [best[k] for k in sorted(best)]


REGISTRY_HEADER = """# Generated by dev/extract_functions.py from dev/.venv. Do not hand-edit.
#
# Rows come from PySpark's *Connect* client, which is the only client that builds the protos
# Latu has to match — see docs/decisions.md. Regenerate with:
#
#     python dev/extract_functions.py --write
#
# `mix format` afterwards, and expect an empty diff unless the pinned Spark version moved.
defmodule Latu.Functions.Registry do
  @moduledoc false

"""


def elixir_literal(value):
    """A Python constant as Elixir source. Only what a Spark default can be."""
    if value is None:
        return "nil"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False).replace("#{", "\\#{")
    if isinstance(value, dict) and "decimal" in value:
        # Evaluated when the registry module compiles; `Latu.Plan.lit/1` sends it as a decimal.
        return "Decimal.new(%s)" % value["decimal"]
    raise ValueError("cannot render %r as an Elixir literal" % (value,))


def elixir_rows(rows, shape, render):
    """One table, sorted, as Elixir source."""
    chosen = sorted((r for r in rows if r["shape"] == shape and not r.get("distinct")),
                    key=lambda r: r["name"])
    body = "".join("    %s,\n" % render(r) for r in chosen)
    return body.rstrip(",\n") + "\n" if body else ""


def write_registry(rows, path):
    """Emit lib/latu/functions/registry.ex. Returns the source, so --check can compare."""
    tables = [
        ("fixed", "fixed", lambda r: '{:%s, "%s", %d}' % (r["name"], r["wire"], r["arity"])),
        ("variadic", "variadic", lambda r: '{:%s, "%s"}' % (r["name"], r["wire"])),
        ("req_variadic", "req_variadic", lambda r: '{:%s, "%s"}' % (r["name"], r["wire"])),
        ("optional_tail", "optional_tail",
         lambda r: '{:%s, "%s", %d}' % (r["name"], r["wire"], r["arity"])),
        ("default_tail", "default_tail",
         lambda r: '{:%s, "%s", %d, [%s]}'
                   % (r["name"], r["wire"], r["arity"],
                      ", ".join(elixir_literal(v) for v in r["defaults"]))),
    ]

    out = [REGISTRY_HEADER]
    for name, shape, render in tables:
        out.append("  @%s [\n%s  ]\n\n" % (name, elixir_rows(rows, shape, render)))

    distinct = sorted((r for r in rows if r.get("distinct") and r["shape"] != "irregular"),
                      key=lambda r: r["name"])
    lines = []
    for r in distinct:
        fixed = "{:fixed, %d}" % r["arity"]
        shape = ":req_variadic" if r["shape"] == "req_variadic" else fixed
        lines.append('    {:%s, "%s", %s}' % (r["name"], r["wire"], shape))
    out.append("  @distinct [\n%s\n  ]\n\n" % ",\n".join(lines))

    for name, _shape, _render in tables:
        out.append("  def %s, do: @%s\n\n" % (name, name))
    out.append("  def distinct, do: @distinct\n")
    out.append("end\n")

    source = "".join(out)
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)
    return source


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pyspark", type=pathlib.Path, help="path to the pyspark package")
    ap.add_argument("--json", action="store_true", help="dump rows as JSON, not a summary")
    ap.add_argument("--write", action="store_true", help="rewrite the registry module")
    ap.add_argument("--check", action="store_true", help="exit 1 if that file is out of date")
    args = ap.parse_args()

    root = args.pyspark
    if root is None:
        here = pathlib.Path(__file__).resolve().parent
        found = list(here.glob(".venv/lib/python*/site-packages/pyspark"))
        if not found:
            sys.exit("no pyspark in dev/.venv — pip install -r dev/requirements.txt")
        root = found[0]

    source = root / "sql" / "connect" / "functions" / "builtin.py"
    rows = collect(source)

    missing = unaccounted(source, rows)
    if missing:
        sys.exit("not a row and not excluded: %s" % ", ".join(missing))

    if args.json:
        json.dump(rows, sys.stdout, indent=1)
        return

    root = pathlib.Path(__file__).resolve().parent.parent
    registry = root / "lib/latu/functions/registry.ex"

    if args.check:
        current = registry.read_text() if registry.exists() else ""
        if current != write_registry(rows, None):
            sys.exit(
                "%s is out of date — run: python dev/extract_functions.py --write" % registry
            )
        print("%s is current, and every PySpark function is accounted for" % registry)
        return

    if args.write:
        write_registry(rows, registry)
        generated = len([r for r in rows if r["shape"] != "irregular"])
        print("%d rows -> %s (run `mix format` next)" % (generated, registry))
        return

    by_shape = {}
    for r in rows:
        by_shape.setdefault(r["shape"], []).append(r)
    print("%d functions, all of PySpark's accounted for" % len(rows))
    for shape in sorted(by_shape, key=lambda s: -len(by_shape[s])):
        print("  %4d  %s" % (len(by_shape[shape]), shape))
    print()
    for r in by_shape.get("irregular", []):
        print("  irregular  %-28s %s" % (r["name"], r["reason"]))


if __name__ == "__main__":
    main()
