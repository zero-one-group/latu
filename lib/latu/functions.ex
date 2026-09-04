defmodule Latu.Functions do
  @moduledoc """
  Spark's function library.

      alias Latu.Functions, as: F

      df
      |> Latu.group_by(:suburb)
      |> Latu.agg(avg: F.avg(:price), sold: F.count_distinct(:id))
      |> Latu.select([:suburb, :sold, rounded: F.round(:avg, 2)])

  **Alias this module; it cannot be imported.** Spark's names are kept exactly, and `quote/1` is
  one of them — a special form, which Elixir refuses to import over. Nine more (`abs`, `ceil`,
  `floor`, `length`, `max`, `min`, `round`, `struct`, `trunc`) collide with `Kernel`'s
  auto-imports. A qualified call is always unambiguous, so under `F.` none of it costs anything,
  and `F.abs(:x)` is PySpark's own `F.abs(...)` idiom.

  That is the opposite of `Latu.Column`, which is import-first — twelve operators, no collisions.
  The renames there (`divide` not `div`, `not_` not `not`) were forced by `import`; **do not
  generalise them to this module.**

  ## Argument conventions

  Arguments go through `Latu.Plan.to_expr/1`: an atom is a column, anything else is a literal.

  A variadic Spark function takes a list here, so it composes with `Enum`:

      F.coalesce([:a, :b, 0])

  A function with an optional trailing argument is two clauses rather than a default, because
  Spark distinguishes "absent" from "present with a default value" — `F.round(:x)` sends one
  argument and `F.round(:x, 2)` sends two.

  ## Coverage

  Rows are derived from `pyspark/sql/connect/functions/builtin.py` by
  `dev/extract_functions.py`, never typed by hand; it writes `Latu.Functions.Registry`, which is
  checked in. Most functions have one of five uniform shapes and are generated from a row; the
  rest are hand-written, and the script names every one of them. Several rows generate two
  arities, which is why ~500 functions make ~670 name/arity pairs.

  The higher-order functions are all hand-written: a lambda is an ordinary argument, but their
  arities and optional lambdas vary too much for a row. `Latu.Plan.higher_order/3` is the
  builder they share.

  For a function with no wrapper yet, `Latu.Column.fun/3` is the escape hatch:

      fun("soundex", [:name])
  """

  alias Latu.CaseWhen
  alias Latu.Functions.Docs
  alias Latu.Functions.Registry
  alias Latu.Plan

  # =============================================
  # Generated
  # =============================================

  # Kernel is qualified throughout this section, deliberately. The registry defines `length/1`,
  # `count/1`, `abs/1` and `round/1` among others, and inside this module those shadow Kernel's —
  # `length(defaults)` in a comprehension stops compiling the moment Spark's `length` is
  # generated above it. Naming `Kernel.` explicitly also survives the registry growing.

  for {name, wire, arity} <- Registry.fixed() do
    args = Macro.generate_arguments(arity, __MODULE__)
    types = List.duplicate(quote(do: term()), arity)

    @doc Docs.doc_for(wire)
    @spec unquote(name)(unquote_splicing(types)) :: Plan.expression()
    def unquote(name)(unquote_splicing(args)), do: Plan.fun(unquote(wire), unquote(args))
  end

  for {name, wire} <- Registry.variadic() do
    @doc Docs.doc_for(wire, "Variadic in Spark; takes a list here.")
    @spec unquote(name)([term()]) :: Plan.expression()
    def unquote(name)(columns) when is_list(columns), do: Plan.fun(unquote(wire), columns)
  end

  for {name, wire} <- Registry.req_variadic() do
    @doc Docs.doc_for(wire, "Variadic in Spark; takes a list here.")
    @spec unquote(name)(term(), [term()]) :: Plan.expression()
    def unquote(name)(first, columns) when is_list(columns) do
      Plan.fun(unquote(wire), [first | columns])
    end
  end

  # Always sent: Spark has no absent form, so omitting one supplies its default rather than
  # dropping it. The defaults come from PySpark's *body*, not its signature — a parameter that
  # defaults to `None` there sends "GCM", or "X", or 0, depending on the function.
  for {name, wire, arity, defaults} <- Registry.default_tail(),
      supplied <- 0..Kernel.length(defaults) do
    args = Macro.generate_arguments(arity + supplied, __MODULE__)
    types = List.duplicate(quote(do: term()), arity + supplied)
    filled = Enum.drop(defaults, supplied)

    note =
      if filled == [] do
        "Every argument is sent."
      else
        "Sends Spark's defaults for the rest: " <>
          Enum.map_join(filled, ", ", &Kernel.inspect/1) <> "."
      end

    @doc Docs.doc_for(wire, note)
    @spec unquote(name)(unquote_splicing(types)) :: Plan.expression()
    def unquote(name)(unquote_splicing(args)) do
      Plan.fun(unquote(wire), unquote(args ++ Enum.map(filled, &Macro.escape/1)))
    end
  end

  for {name, wire, arity} <- Registry.optional_tail() do
    args = Macro.generate_arguments(arity, __MODULE__)
    types = List.duplicate(quote(do: term()), arity)
    extra = Macro.var(:optional, __MODULE__)

    @doc Docs.doc_for(wire, "The trailing argument is optional.")
    @spec unquote(name)(unquote_splicing(types)) :: Plan.expression()
    def unquote(name)(unquote_splicing(args)), do: Plan.fun(unquote(wire), unquote(args))

    @doc Docs.doc_for(wire, "With the optional trailing argument.")
    @spec unquote(name)(unquote_splicing(types), term()) :: Plan.expression()
    def unquote(name)(unquote_splicing(args), unquote(extra)) do
      Plan.fun(unquote(wire), unquote(args ++ [extra]))
    end
  end

  for {name, wire, shape} <- Registry.distinct() do
    case shape do
      {:fixed, arity} ->
        args = Macro.generate_arguments(arity, __MODULE__)
        types = List.duplicate(quote(do: term()), arity)

        @doc Docs.doc_for(wire, "Over distinct values: `#{wire}` with `is_distinct` set.")
        @spec unquote(name)(unquote_splicing(types)) :: Plan.expression()
        def unquote(name)(unquote_splicing(args)) do
          Plan.fun(unquote(wire), unquote(args), distinct: true)
        end

      :req_variadic ->
        @doc Docs.doc_for(wire, "Over distinct values: `#{wire}` with `is_distinct` set.")
        @spec unquote(name)(term()) :: Plan.expression()
        def unquote(name)(first), do: Plan.fun(unquote(wire), [first], distinct: true)

        @doc Docs.doc_for(wire, "Over distinct values, across several arguments.")
        @spec unquote(name)(term(), [term()]) :: Plan.expression()
        def unquote(name)(first, more) when is_list(more) do
          Plan.fun(unquote(wire), [first | more], distinct: true)
        end
    end
  end

  # =============================================
  # Higher-order
  # =============================================

  # A lambda is an ordinary argument to an ordinary UnresolvedFunction, so these differ from a
  # registry row only in taking a function where a column would go. Two shapes repeat; the rest
  # vary enough to be written out.
  @with_lambda [
    :transform,
    :filter,
    :exists,
    :forall,
    :map_filter,
    :transform_keys,
    :transform_values
  ]

  @two_columns_with_lambda [:zip_with, :map_zip_with]

  for name <- @with_lambda do
    @doc Docs.doc_for(Kernel.to_string(name), "Takes a lambda — `fn x -> ... end`.")
    @spec unquote(name)(term(), function()) :: Plan.expression()
    def unquote(name)(column, fun) when is_function(fun) do
      Plan.higher_order(unquote(Kernel.to_string(name)), [column], [fun])
    end
  end

  for name <- @two_columns_with_lambda do
    @doc Docs.doc_for(Kernel.to_string(name), "Takes two columns and a lambda.")
    @spec unquote(name)(term(), term(), function()) :: Plan.expression()
    def unquote(name)(left, right, fun) when is_function(fun) do
      Plan.higher_order(unquote(Kernel.to_string(name)), [left, right], [fun])
    end
  end

  @doc """
  Fold an array, left to right.

      F.aggregate(:xs, 0, fn acc, x -> add(acc, x) end)
      F.aggregate(:xs, 0, fn acc, x -> add(acc, x) end, fn acc -> divide(acc, F.size(:xs)) end)

  `merge` takes the accumulator and an element; the optional `finish` transforms the result.
  """
  @spec aggregate(term(), term(), function()) :: Plan.expression()
  def aggregate(column, initial, merge) when is_function(merge, 2) do
    Plan.higher_order("aggregate", [column, initial], [merge])
  end

  @doc "`aggregate/3` with the finishing lambda, which Spark applies to the result."
  @spec aggregate(term(), term(), function(), function()) :: Plan.expression()
  def aggregate(column, initial, merge, finish)
      when is_function(merge, 2) and is_function(finish, 1) do
    Plan.higher_order("aggregate", [column, initial], [merge, finish])
  end

  @doc "`aggregate/3,4` under Spark's other name for it. Same plan but for the function name."
  @spec reduce(term(), term(), function()) :: Plan.expression()
  def reduce(column, initial, merge) when is_function(merge, 2) do
    Plan.higher_order("reduce", [column, initial], [merge])
  end

  @doc "`reduce/3` with the finishing lambda. Spark's other name for `aggregate/4`."
  @spec reduce(term(), term(), function(), function()) :: Plan.expression()
  def reduce(column, initial, merge, finish)
      when is_function(merge, 2) and is_function(finish, 1) do
    Plan.higher_order("reduce", [column, initial], [merge, finish])
  end

  @doc """
  Sort an array, optionally by a comparator returning a negative, zero or positive number.

  Without one this is an ordinary call and no lambda reaches the wire, which is why the two
  clauses build different things.
  """
  @spec array_sort(term()) :: Plan.expression()
  def array_sort(column), do: Plan.fun("array_sort", [column])

  @doc "Sort by a comparator of two elements, rather than by Spark's natural order."
  @spec array_sort(term(), function()) :: Plan.expression()
  def array_sort(column, comparator) when is_function(comparator, 2) do
    Plan.higher_order("array_sort", [column], [comparator])
  end

  # =============================================
  # A seed drawn when you omit one
  # =============================================

  # Spark has no unseeded form of these: PySpark always sends a seed and invents a random one
  # when the caller leaves it out, so the *plan* differs between builds. Same rule as
  # `Latu.sample/3`. The number is the count of leading arguments before the seed.
  @random_seeded [
    {:rand, "rand", 0},
    {:randn, "randn", 0},
    {:uuid, "uuid", 0},
    {:randstr, "randstr", 1},
    {:shuffle, "shuffle", 1},
    {:uniform, "uniform", 2},
    {:count_min_sketch, "count_min_sketch", 3}
  ]

  for {name, wire, arity} <- @random_seeded do
    args = Macro.generate_arguments(arity, __MODULE__)
    types = List.duplicate(quote(do: term()), arity)

    @doc Docs.doc_for(
           wire,
           "Draws a random seed, so **the plan is not reproducible** — pass one to fix it."
         )
    @spec unquote(name)(unquote_splicing(types)) :: Plan.expression()
    def unquote(name)(unquote_splicing(args)) do
      Plan.fun(unquote(wire), unquote(args) ++ [Plan.random_seed()])
    end

    @doc Docs.doc_for(wire, "With an explicit seed, so the plan is reproducible.")
    @spec unquote(name)(unquote_splicing(types), term()) :: Plan.expression()
    def unquote(name)(unquote_splicing(args), seed) do
      Plan.fun(unquote(wire), unquote(args) ++ [seed])
    end
  end

  # =============================================
  # Shapes no rule covers
  # =============================================

  @doc """
  Strip characters from both ends.

  **The wire order is reversed**: `trim(col, chars)` sends `trim(chars, col)`, which is Spark's
  own argument order and PySpark's. Latu keeps the column first, as every other function does.
  """
  @spec trim(term()) :: Plan.expression()
  def trim(column), do: Plan.fun("trim", [column])

  @doc "Trim the given characters from both ends. Spark takes them first; Latu does not."
  @spec trim(term(), term()) :: Plan.expression()
  def trim(column, characters), do: Plan.fun("trim", [characters, column])

  @doc "Strip characters from the left. Reversed on the wire, as `trim/2` is."
  @spec ltrim(term()) :: Plan.expression()
  def ltrim(column), do: Plan.fun("ltrim", [column])

  @doc "Trim the given characters from the left. Spark takes them first; Latu does not."
  @spec ltrim(term(), term()) :: Plan.expression()
  def ltrim(column, characters), do: Plan.fun("ltrim", [characters, column])

  @doc "Strip characters from the right. Reversed on the wire, as `trim/2` is."
  @spec rtrim(term()) :: Plan.expression()
  def rtrim(column), do: Plan.fun("rtrim", [column])

  @doc "Trim the given characters from the right. Spark takes them first; Latu does not."
  @spec rtrim(term(), term()) :: Plan.expression()
  def rtrim(column, characters), do: Plan.fun("rtrim", [characters, column])

  @doc """
  The value some rows behind the current one, within a window.

      F.lag(:price) |> over(window)

  The offset is **always sent** and defaults to 1; the fallback value is sent only when given.
  Two different answers to "the caller left it out", in one signature.
  """
  @spec lag(term()) :: Plan.expression()
  def lag(column), do: lag(column, 1)

  @doc "Look back `offset` rows. NULL past the start of the partition."
  @spec lag(term(), term()) :: Plan.expression()
  def lag(column, offset), do: Plan.fun("lag", [column, offset])

  @doc "Look back `offset` rows, with `default` past the start of the partition."
  @spec lag(term(), term(), term()) :: Plan.expression()
  def lag(column, offset, default), do: Plan.fun("lag", [column, offset, default])

  @doc "The value some rows ahead of the current one. `lag/1,2,3` in the other direction."
  @spec lead(term()) :: Plan.expression()
  def lead(column), do: lead(column, 1)

  @doc "Look ahead `offset` rows. NULL past the end of the partition."
  @spec lead(term(), term()) :: Plan.expression()
  def lead(column, offset), do: Plan.fun("lead", [column, offset])

  @doc "Look ahead `offset` rows, with `default` past the end of the partition."
  @spec lead(term(), term(), term()) :: Plan.expression()
  def lead(column, offset, default), do: Plan.fun("lead", [column, offset, default])

  @doc """
  A logarithm — natural with one argument, to a base with two.

      F.log(:x)        # ln(x)
      F.log(2, :x)     # log base 2 of x

  **The wire name changes with the arity**: one argument sends `ln`, two send `log`. The first
  argument means different things in the two forms, which is PySpark's design rather than a
  choice made here; `F.ln/1` says the first one unambiguously.
  """
  @spec log(term()) :: Plan.expression()
  def log(column), do: Plan.fun("ln", [column])

  @doc "The logarithm of `column` in `base` — Spark's `log`, where `log/1` sends `ln`."
  @spec log(term(), term()) :: Plan.expression()
  def log(base, column), do: Plan.fun("log", [base, column])

  @doc """
  Move a timestamp between time zones.

  With two arguments the source zone is the session's; the optional one comes **first**, which is
  why this cannot be a registry row — a generated wrapper would have sent three arguments always,
  and `nil` for the first would encode a NULL rather than an absence.
  """
  @spec convert_timezone(term(), term()) :: Plan.expression()
  def convert_timezone(target_tz, source_ts) do
    Plan.fun("convert_timezone", [target_tz, source_ts])
  end

  @doc "With the source timezone given, rather than the session's."
  @spec convert_timezone(term(), term(), term()) :: Plan.expression()
  def convert_timezone(source_tz, target_tz, source_ts) do
    Plan.fun("convert_timezone", [source_tz, target_tz, source_ts])
  end

  @doc "The average. Spark's own name for it on the wire is `avg`, which is what this sends."
  @spec mean(term()) :: Plan.expression()
  def mean(column), do: Plan.fun("avg", [column])

  @doc """
  Seconds since the epoch — of now with no arguments, of a timestamp otherwise.

  The format is always sent once a timestamp is given, defaulting to Spark's own
  `yyyy-MM-dd HH:mm:ss`.
  """
  @spec unix_timestamp() :: Plan.expression()
  def unix_timestamp, do: Plan.fun("unix_timestamp", [])

  @doc "Parse with Spark's default format, `yyyy-MM-dd HH:mm:ss`, which it always sends."
  @spec unix_timestamp(term()) :: Plan.expression()
  def unix_timestamp(timestamp), do: unix_timestamp(timestamp, "yyyy-MM-dd HH:mm:ss")

  @doc "Parse with the given format."
  @spec unix_timestamp(term(), term()) :: Plan.expression()
  def unix_timestamp(timestamp, format) do
    Plan.fun("unix_timestamp", [timestamp, format])
  end

  # Overloaded on arity alone: every form passes its arguments straight through, and the arities
  # do not overlap, so one name serves all of them. `make_timestamp` takes either six date parts
  # or a date and a time, each optionally with a timezone; `window` takes a duration, optionally
  # a slide and a start.
  @overloaded [
    {:make_timestamp, "make_timestamp", [2, 3, 6, 7]},
    {:make_timestamp_ntz, "make_timestamp_ntz", [2, 6]},
    {:try_make_timestamp, "try_make_timestamp", [2, 3, 6, 7]},
    {:try_make_timestamp_ntz, "try_make_timestamp_ntz", [2, 6]},
    {:window, "window", [2, 3, 4]}
  ]

  for {name, wire, arities} <- @overloaded, arity <- arities do
    args = Macro.generate_arguments(arity, __MODULE__)
    types = List.duplicate(quote(do: term()), arity)

    @doc Docs.doc_for(wire, "One of several arities; they do not overlap.")
    @spec unquote(name)(unquote_splicing(types)) :: Plan.expression()
    def unquote(name)(unquote_splicing(args)), do: Plan.fun(unquote(wire), unquote(args))
  end

  # Distinct *and* an optional argument, built by a comprehension in PySpark so the extractor
  # cannot see the arguments. Two clauses each.
  @distinct_optional [{:listagg_distinct, "listagg"}, {:string_agg_distinct, "string_agg"}]

  for {name, wire} <- @distinct_optional do
    @doc Docs.doc_for(wire, "Over distinct values, with an optional delimiter.")
    @spec unquote(name)(term()) :: Plan.expression()
    def unquote(name)(column), do: Plan.fun(unquote(wire), [column], distinct: true)

    @doc Docs.doc_for(wire, "Over distinct values, with the delimiter given.")
    @spec unquote(name)(term(), term()) :: Plan.expression()
    def unquote(name)(column, delimiter) do
      Plan.fun(unquote(wire), [column, delimiter], distinct: true)
    end
  end

  @doc """
  Call a function by name through Spark's catalog.

      F.call_function("my_udf", [:id])

  A different wire node from everything else here — `CallFunction` rather than
  `UnresolvedFunction` — which is how Spark reaches a registered or user-defined function. Latu
  cannot ship a UDF, but it can call one that is already registered.
  """
  @spec call_function(String.t(), [term()]) :: Plan.expression()
  defdelegate call_function(name, arguments), to: Plan

  # =============================================
  # Parsing: JSON, CSV, XML
  # =============================================

  # The whole family shares two argument conventions the registry cannot express, decided at
  # M8.1 (docs/decisions.md): a schema is a STRING sent verbatim as a literal -- DDL or Spark's
  # JSON schema form, never a column name -- or a built expression such as `schema_of_json/1`;
  # and options ride as a `map` function call whose keys and values go through
  # `Latu.Plan.to_options/1`, so argument order reaches the wire.

  @parse_fns [
    {:from_json, "from_json", :schema},
    {:from_csv, "from_csv", :schema},
    {:from_xml, "from_xml", :schema},
    {:to_json, "to_json", nil},
    {:to_csv, "to_csv", nil},
    {:to_xml, "to_xml", nil},
    {:schema_of_json, "schema_of_json", nil},
    {:schema_of_csv, "schema_of_csv", nil},
    {:schema_of_xml, "schema_of_xml", nil}
  ]

  for {name, wire, :schema} <- @parse_fns do
    @doc Docs.doc_for(
           wire,
           "The schema is a string (DDL, or Spark's JSON schema form) or a built expression;" <>
             " options follow `Latu.read/2`'s key and value rules."
         )
    @spec unquote(name)(term(), term()) :: Plan.expression()
    def unquote(name)(column, schema) do
      Plan.fun(unquote(wire), [column, schema_arg(schema)])
    end

    @doc Docs.doc_for(wire, "With parser options, following `Latu.read/2`'s rules.")
    @spec unquote(name)(term(), term(), keyword() | map()) :: Plan.expression()
    def unquote(name)(column, schema, options) do
      Plan.fun(unquote(wire), [column, schema_arg(schema), options_arg(options)])
    end
  end

  for {name, wire, nil} <- @parse_fns do
    @doc Docs.doc_for(wire, "Options follow `Latu.read/2`'s key and value rules.")
    @spec unquote(name)(term()) :: Plan.expression()
    def unquote(name)(column), do: Plan.fun(unquote(wire), [column])

    @doc Docs.doc_for(wire, "With parser options, following `Latu.read/2`'s rules.")
    @spec unquote(name)(term(), keyword() | map()) :: Plan.expression()
    def unquote(name)(column, options) do
      Plan.fun(unquote(wire), [column, options_arg(options)])
    end
  end

  # A binary is the schema itself, never a column name: a bare atom here would silently build a
  # column reference the server rejects as non-foldable.
  defp schema_arg(schema) when is_binary(schema), do: Plan.lit(schema)

  defp schema_arg(schema) when is_atom(schema) do
    raise ArgumentError,
          "a schema is a string (DDL or Spark's JSON schema form) or a built expression " <>
            "such as F.schema_of_json/1, not a column name: #{inspect(schema)}"
  end

  defp schema_arg(schema), do: schema

  defp options_arg(options) do
    Plan.fun("map", options |> Plan.to_options() |> Enum.flat_map(fn {k, v} -> [k, v] end))
  end

  # =============================================
  # Conditionals
  # =============================================

  @doc """
  A conditional branch.

      F.when_(greater(:id, 5), "big") |> F.otherwise("small")

      F.when_(greater(:id, 100), "huge")
      |> F.when_(greater(:id, 5), "big")
      |> F.otherwise("small")

  Spark spells this `when`, which is an Elixir *operator* rather than a special form — and
  unlike `alias` it is a **syntax error** to define, so not even a qualified call is possible.
  Hence the underscore, as in `Latu.Column.not_/1`.

  Returns a `Latu.CaseWhen`, not an expression: the wire carries one `when` call and the chain is
  client-side, so the half-built state lives in a struct exactly as `group_by`'s does.
  `Latu.Plan.to_expr/1` coerces it wherever an expression is accepted.

  With no `otherwise/2` the else branch is NULL, as in SQL.
  """
  @spec when_(term(), term()) :: CaseWhen.t()
  def when_(condition, value), do: CaseWhen.new(condition, value)

  @doc "Add a branch to an existing chain. Refused once `otherwise/2` has closed it."
  @spec when_(CaseWhen.t(), term(), term()) :: CaseWhen.t()
  def when_(%CaseWhen{} = chain, condition, value), do: CaseWhen.when_(chain, condition, value)

  @doc "The else branch, ending a `when_/2` chain."
  @spec otherwise(CaseWhen.t(), term()) :: CaseWhen.t()
  def otherwise(%CaseWhen{} = chain, value), do: CaseWhen.otherwise(chain, value)

  # The registry is the spec, and these two let the tests enumerate it. They cover the generated
  # rows only — the higher-order and conditional functions are hand-written. Not API.
  @doc false
  @spec registered() :: [{atom(), non_neg_integer()}]
  def registered do
    generated =
      Enum.map(Registry.fixed(), fn {name, _wire, arity} -> {name, arity} end) ++
        Enum.map(Registry.variadic(), fn {name, _wire} -> {name, 1} end) ++
        Enum.map(Registry.req_variadic(), fn {name, _wire} -> {name, 2} end) ++
        Enum.flat_map(Registry.optional_tail(), fn {n, _w, a} -> [{n, a}, {n, a + 1}] end) ++
        Enum.flat_map(Registry.default_tail(), fn {n, _w, a, d} ->
          for extra <- 0..Kernel.length(d), do: {n, a + extra}
        end) ++
        Enum.flat_map(Registry.distinct(), fn
          {name, _wire, {:fixed, arity}} -> [{name, arity}]
          {name, _wire, :req_variadic} -> [{name, 1}, {name, 2}]
        end)

    Enum.sort(generated)
  end

  @doc false
  @spec handwritten() :: [{atom(), non_neg_integer()}]
  def handwritten do
    varying = [
      from_json: 2,
      from_json: 3,
      from_csv: 2,
      from_csv: 3,
      from_xml: 2,
      from_xml: 3,
      to_json: 1,
      to_json: 2,
      to_csv: 1,
      to_csv: 2,
      to_xml: 1,
      to_xml: 2,
      schema_of_json: 1,
      schema_of_json: 2,
      schema_of_csv: 1,
      schema_of_csv: 2,
      schema_of_xml: 1,
      schema_of_xml: 2,
      when_: 2,
      when_: 3,
      otherwise: 2,
      aggregate: 3,
      aggregate: 4,
      reduce: 3,
      reduce: 4,
      array_sort: 1,
      array_sort: 2,
      trim: 1,
      trim: 2,
      ltrim: 1,
      ltrim: 2,
      rtrim: 1,
      rtrim: 2,
      lag: 1,
      lag: 2,
      lag: 3,
      lead: 1,
      lead: 2,
      lead: 3,
      log: 1,
      log: 2,
      convert_timezone: 2,
      convert_timezone: 3,
      mean: 1,
      unix_timestamp: 0,
      unix_timestamp: 1,
      unix_timestamp: 2,
      call_function: 2
    ]

    (varying ++
       Enum.map(@with_lambda, &{&1, 2}) ++
       Enum.map(@two_columns_with_lambda, &{&1, 3}) ++
       Enum.flat_map(@random_seeded, fn {name, _wire, arity} ->
         [{name, arity}, {name, arity + 1}]
       end) ++
       Enum.flat_map(@overloaded, fn {name, _wire, arities} ->
         Enum.map(arities, &{name, &1})
       end) ++
       Enum.flat_map(@distinct_optional, fn {name, _wire} -> [{name, 1}, {name, 2}] end))
    |> Enum.sort()
  end

  @doc false
  @spec wire_name(atom()) :: String.t() | nil
  def wire_name(name) do
    tables =
      Enum.map(Registry.fixed(), fn {n, w, _} -> {n, w} end) ++
        Registry.variadic() ++
        Registry.req_variadic() ++
        Enum.map(Registry.optional_tail(), fn {n, w, _} -> {n, w} end) ++
        Enum.map(Registry.default_tail(), fn {n, w, _a, _d} -> {n, w} end) ++
        Enum.map(Registry.distinct(), fn {n, w, _} -> {n, w} end)

    case List.keyfind(tables, name, 0) do
      {^name, wire} -> wire
      nil -> nil
    end
  end
end
