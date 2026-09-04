# Does every wrapped Spark function actually resolve? The one thing a golden fixture cannot
# answer: it proves Latu sends the bytes PySpark sends, not that the server accepts them.
#
#     docker compose up -d spark-connect
#     mix run dev/probe_functions.exs            # report only
#     mix run dev/probe_functions.exs --write    # + write dev/function_args.exs
#     mix run dev/probe_functions.exs --only lag,upper
#
# Asks for the SCHEMA rather than collecting: analysis is what "does it resolve" means, it
# needs no data, and a function whose *execution* raises (`raise_error`) still answers.
#
# Arguments are guessed, not known: each function is tried against a list of type profiles
# until one analyses. A profile is a list of column kinds by position, the last repeating.
# Functions no profile can serve get explicit arguments below; functions nothing in a single
# projection can serve are excluded, each with a reason.

import Latu.Column

alias Latu.Functions, as: F
alias Latu.Functions.Registry
alias Latu.Window, as: W

url = System.get_env("SPARK_REMOTE") || "sc://localhost:15002"
session = Latu.connect!(url)

{opts, _rest} = OptionParser.parse!(System.argv(), strict: [write: :boolean, only: :string])

# One row, every type a profile can ask for.
frame =
  session
  |> Latu.range(1)
  |> Latu.select(
    n: :id,
    d: cast(1.5, "double"),
    s: lit("abc"),
    b: lit(true),
    bin: cast("abc", "binary"),
    dt: cast("2026-01-02", "date"),
    ts: cast("2026-01-02 03:04:05", "timestamp"),
    arr: expr("array(1, 2, 3)"),
    sarr: expr("array('a', 'b')"),
    aarr: expr("array(array(1, 2), array(3))"),
    starr: expr("array(named_struct('a', 1, 'b', 'x'))"),
    entries: expr("array(named_struct('key', 'k', 'value', 1))"),
    m: expr("map('k', 1)"),
    st: expr("named_struct('a', 1, 'b', 'x')"),
    var: expr(~s|parse_json('{"a": 1}')|),
    j: lit(~s({"a": 1}))
  )

window = W.partition_by([:n]) |> W.order_by([:n])

kind = fn
  :int_lit -> lit(2)
  :str_lit -> lit("a")
  :dbl_lit -> lit(0.5)
  :bool_lit -> lit(true)
  :field_lit -> lit("YEAR")
  column -> col(column)
end

# Ordered by how often they win, so the common case costs one round trip.
profiles = [
  [:n],
  [:s],
  [:d],
  [:arr],
  [:m],
  [:b],
  [:ts],
  [:dt],
  [:bin],
  [:st],
  [:sarr],
  [:aarr],
  [:starr],
  [:entries],
  [:var],
  [:s, :s],
  [:s, :n],
  [:n, :s],
  [:n, :int_lit],
  [:n, :dbl_lit],
  [:n, :bool_lit],
  [:arr, :n],
  [:arr, :int_lit],
  [:arr, :s],
  [:arr, :arr],
  [:arr, :bool_lit],
  [:m, :s],
  [:dt, :n],
  [:ts, :s],
  [:ts, :field_lit],
  [:s, :str_lit],
  [:s, :int_lit],
  [:s, :bool_lit],
  [:d, :int_lit],
  [:d, :dbl_lit],
  [:j, :str_lit],
  [:sarr, :str_lit],
  [:var, :str_lit],
  [:str_lit],
  [:int_lit],
  [:field_lit, :ts],
  [:b, :s],
  [:s, :s, :n],
  [:s, :n, :n],
  [:n, :dbl_lit, :int_lit],
  [:n, :int_lit, :bool_lit],
  [:ts, :int_lit],
  [:m, :m]
]

# What no profile can express: a lambda, an options list, a chain struct, a name-plus-arguments
# pair. Each is already covered by its own golden and integration tests; here they only need to
# reach the server in *some* accepted shape.
lambda1 = fn x -> x end
lambda2 = fn a, b -> subtract(a, b) end

overrides = %{
  {:transform, 2} => [col(:arr), lambda1],
  {:transform_keys, 2} => [col(:m), fn k, _v -> k end],
  {:transform_values, 2} => [col(:m), fn _k, v -> v end],
  {:map_filter, 2} => [col(:m), fn _k, v -> greater(v, 0) end],
  {:map_zip_with, 3} => [col(:m), col(:m), fn k, _v1, _v2 -> k end],
  {:filter, 2} => [col(:arr), fn x -> greater(x, 0) end],
  {:exists, 2} => [col(:arr), fn x -> greater(x, 0) end],
  {:forall, 2} => [col(:arr), fn x -> greater(x, 0) end],
  {:zip_with, 3} => [col(:arr), col(:arr), lambda2],
  {:array_sort, 2} => [col(:arr), lambda2],
  {:aggregate, 3} => [col(:arr), lit(0), lambda2],
  {:aggregate, 4} => [col(:arr), lit(0), lambda2, lambda1],
  {:reduce, 3} => [col(:arr), lit(0), lambda2],
  {:reduce, 4} => [col(:arr), lit(0), lambda2, lambda1],
  {:from_json, 2} => [col(:j), "a INT"],
  {:from_json, 3} => [col(:j), "a INT", [allow_comments: true]],
  {:from_csv, 2} => [lit("1,a"), "a INT, b STRING"],
  {:from_csv, 3} => [lit("1,a"), "a INT, b STRING", [sep: ","]],
  {:from_xml, 2} => [lit("<r><a>1</a></r>"), "a INT"],
  {:from_xml, 3} => [lit("<r><a>1</a></r>"), "a INT", [mode: "PERMISSIVE"]],
  {:to_json, 2} => [col(:st), [ignore_null_fields: false]],
  {:to_csv, 2} => [col(:st), [sep: ","]],
  {:to_xml, 2} => [col(:st), [row_tag: "r"]],
  {:schema_of_json, 2} => [~s({"a": 1}), [allow_comments: true]],
  {:schema_of_csv, 2} => ["1;a", [sep: ";"]],
  {:schema_of_xml, 2} => ["<r><a>1</a></r>", [mode: "PERMISSIVE"]],
  {:when_, 3} => [F.when_(lit(true), lit(1)), lit(false), lit(2)],
  {:otherwise, 2} => [F.when_(lit(true), lit(1)), lit(0)],
  {:call_function, 2} => ["abs", [lit(-1)]],
  {:shuffle, 2} => [col(:arr), 42],
  {:window, 4} => [col(:ts), "10 minutes", "5 minutes", "0 seconds"],
  {:count_min_sketch, 3} => [col(:n), lit(0.1), lit(0.9)],
  {:count_min_sketch, 4} => [col(:n), lit(0.1), lit(0.9), lit(42)],
  # These want a *value*, not a type: a real collation name, a real format, a real path.
  {:approx_count_distinct, 2} => [col(:n), 0.05],
  {:collate, 2} => [col(:s), "UTF8_BINARY"],
  {:to_binary, 2} => [col(:s), lit("utf-8")],
  {:to_number, 2} => [lit("123"), lit("999")],
  {:try_to_number, 2} => [lit("123"), lit("999")],
  {:variant_get, 3} => [col(:var), lit("$.a"), lit("int")],
  {:try_variant_get, 3} => [col(:var), lit("$.a"), lit("int")],
  {:array_insert, 3} => [col(:arr), lit(2), lit(9)],
  {:nth_value, 3} => [col(:n), lit(1), lit(true)],
  {:approx_percentile, 3} => [col(:n), lit(0.5), lit(100)],
  {:percentile, 3} => [col(:n), lit(0.5), lit(1)],
  {:percentile_approx, 3} => [col(:n), lit(0.5), lit(100)],
  {:time_bucket, 2} => [expr("INTERVAL '1' HOUR"), col(:ts)],
  # The origin is foldable or nothing: a timestamp literal, never a column.
  {:time_bucket, 3} => [
    expr("INTERVAL '1' HOUR"),
    col(:ts),
    expr("TIMESTAMP '2026-01-01 00:00:00'")
  ],
  # window_time reads the struct `window` produces; nothing else in the fixture is one.
  {:window_time, 1} => [F.window(col(:ts), "10 minutes")]
}

# Nothing a single projection can build. Excluded on purpose, each with the reason, so the
# residue below is only ever news.
excluded = [
  {~w(make_time time_diff time_from_micros time_from_millis time_from_seconds time_to_micros
      time_to_millis time_to_seconds time_trunc to_time try_to_time)a,
   "Spark 4.2 refuses the TIME type outright: UNSUPPORTED_TIME_TYPE. Nothing to do until a " <>
     "Spark that ships it"},
  # By arity, not by name: make_timestamp(year..sec[, tz]) is fine, and only the forms taking
  # a date and a TIME are refused.
  {[
     {:make_timestamp, 2},
     {:make_timestamp, 3},
     {:make_timestamp_ntz, 2},
     {:try_make_timestamp, 2},
     {:try_make_timestamp, 3},
     {:try_make_timestamp_ntz, 2}
   ], "the short forms take a date and a TIME, which Spark 4.2 refuses"},
  {~w(kll_merge_agg_bigint kll_merge_agg_double kll_merge_agg_float kll_sketch_agg_bigint
      kll_sketch_agg_double kll_sketch_agg_float hll_union hll_union_agg theta_union
      theta_union_agg tuple_intersection_agg_double tuple_intersection_agg_integer
      tuple_intersection_double tuple_intersection_integer tuple_intersection_theta_double
      tuple_intersection_theta_integer tuple_sketch_agg_double tuple_sketch_agg_integer
      tuple_sketch_summary_double tuple_sketch_summary_integer tuple_union_agg_double
      tuple_union_agg_integer tuple_union_double tuple_union_integer tuple_union_theta_double
      tuple_union_theta_integer)a,
   "takes a sketch buffer produced by the matching *_agg function; one projection cannot " <>
     "make one, and nesting the aggregates is not a plan Spark accepts"},
  {~w(st_asbinary st_setsrid st_srid)a,
   "needs a GEOGRAPHY or GEOMETRY column, which Latu has no way to build"},
  {~w(unwrap_udt)a,
   "needs a user-defined-type column; Latu ships no UDT and design 12 rules them out"},
  {~w(grouping grouping_id)a,
   "only valid inside a cube, rollup or grouping-sets aggregate, and the sweep builds a " <>
     "projection"},
  {~w(inline inline_outer posexplode posexplode_outer)a,
   "a generator that expands to more than one column, so a single aliased projection cannot " <>
     "hold it — explode and explode_outer yield one column and are swept normally"},
  {~w(java_method reflect try_reflect)a,
   "calls a JVM method by name, so the class must be a foldable string naming something on " <>
     "the server's classpath"}
]

# A key is a name, or a {name, arity} when only some arities are out.
excluded_keys = for {keys, reason} <- excluded, key <- keys, into: %{}, do: {key, reason}

out? = fn name, arity ->
  Map.has_key?(excluded_keys, name) or Map.has_key?(excluded_keys, {name, arity})
end

# A variadic function takes ONE list, and a required-plus-variadic one takes a value and a
# list, so a bare column in either position is a client-side refusal rather than a finding.
variadic = MapSet.new(Registry.variadic(), &elem(&1, 0))

req_variadic =
  MapSet.new(
    Enum.map(Registry.req_variadic(), &elem(&1, 0)) ++
      for({name, _wire, :req_variadic} <- Registry.distinct(), do: name)
  )

args_for = fn name, profile, arity ->
  last = List.last(profile)
  positional = for position <- 1..arity//1, do: kind.(Enum.at(profile, position - 1, last))

  cond do
    name in variadic -> [Enum.map(1..2, fn _ -> kind.(hd(profile)) end)]
    name in req_variadic -> [kind.(hd(profile)), [kind.(last)]]
    true -> positional
  end
end

# A call either analyses, is refused by Latu before the wire, or is refused by the server.
attempt = fn name, args, over? ->
  try do
    call = apply(F, name, args)
    call = if over?, do: over(call, window), else: call

    case Latu.schema(Latu.select(frame, x: call)) do
      {:ok, _fields} -> :ok
      {:error, error} -> {:server, error.message}
    end
  rescue
    error -> {:client, Exception.message(error)}
  catch
    kind, value -> {:client, inspect({kind, value})}
  end
end

every = Enum.uniq(F.registered() ++ F.handwritten())

wanted =
  case opts[:only] do
    nil ->
      Enum.reject(every, fn {name, arity} -> out?.(name, arity) end)

    only ->
      names = String.split(only, ",")
      Enum.filter(every, fn {name, _arity} -> to_string(name) in names end)
  end

skipped = length(every) - length(wanted)

IO.puts("probing #{length(wanted)} function/arity pairs against #{url}")
IO.puts("(#{skipped} excluded by name — see the bottom of dev/probe_functions.exs)\n")

{resolved, unresolved} =
  Enum.reduce(wanted, {[], []}, fn {name, arity}, {ok, bad} ->
    candidates =
      case Map.fetch(overrides, {name, arity}) do
        {:ok, args} -> [{:override, args}]
        :error -> Enum.map(profiles, &{&1, args_for.(name, &1, arity)})
      end

    found =
      Enum.find_value(candidates, fn {profile, args} ->
        if attempt.(name, args, false) == :ok, do: {profile, false}
      end) ||
        Enum.find_value(candidates, fn {profile, args} ->
          if attempt.(name, args, true) == :ok, do: {profile, true}
        end)

    case found do
      {profile, over?} ->
        IO.write(".")
        {[{name, arity, profile, over?} | ok], bad}

      nil ->
        # Every distinct reason, because the last candidate's is rarely the informative one.
        errors =
          candidates
          |> Enum.map(fn {_profile, args} -> attempt.(name, args, false) end)
          |> Enum.reject(&(&1 == :ok))
          |> Enum.map(fn {source, message} ->
            "[#{source}] " <> (message |> String.split("\n") |> hd() |> String.slice(0, 130))
          end)
          |> Enum.uniq()

        IO.write("x")
        {ok, [{name, arity, errors} | bad]}
    end
  end)

resolved = Enum.reverse(resolved)
unresolved = Enum.reverse(unresolved)

IO.puts("\n\n#{length(resolved)} resolved, #{length(unresolved)} did not\n")

if unresolved != [] do
  IO.puts("Unresolved — each needs explicit arguments or an exclusion with a reason:\n")

  for {name, arity, errors} <- unresolved do
    IO.puts("  #{name}/#{arity}")
    for error <- Enum.take(errors, 3), do: IO.puts("      #{error}")
  end

  IO.puts("")
end

if opts[:write] do
  rows =
    Enum.map_join(resolved, ",\n", fn {name, arity, profile, over?} ->
      "    {#{inspect(name)}, #{arity}, #{inspect(profile)}, #{over?}}"
    end)

  skips =
    Enum.map_join(excluded_keys, ",\n", fn {key, reason} ->
      "    {#{inspect(key)}, #{inspect(reason)}}"
    end)

  File.write!(
    "dev/function_args.exs",
    """
    # Generated by dev/probe_functions.exs --write. Do not hand-edit.
    #
    # What every wrapped function was found to resolve with, measured against a live server:
    # {name, arity, profile, over?}, where :override means the explicit arguments in the probe.
    # Not a spec — a record. `test/latu/function_sweep_test.exs` asserts it still covers the
    # surface exactly, so a function added by a Spark bump fails offline until the probe is
    # re-run; re-running it and diffing is the check that the shapes have not moved.
    %{
      resolved: [
    """ <>
      rows <>
      "\n  ],\n  excluded: [\n" <>
      skips <> "\n  ]\n}\n"
  )

  IO.puts(
    "wrote dev/function_args.exs " <>
      "(#{length(resolved)} resolved, #{map_size(excluded_keys)} excluded)"
  )
end

Latu.disconnect(session)
if unresolved != [], do: System.halt(1)
