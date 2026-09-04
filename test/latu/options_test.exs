defmodule Latu.OptionsTest do
  use ExUnit.Case, async: true

  # An option nobody documents is an option nobody can use.
  #
  # `h Latu.join` in IEx, and an editor's hover, are where a verb's contract actually gets
  # read — not the module the facade delegates to, and not a link out of it. So the **facade's**
  # docstring is what has to name every option the implementation accepts, even when the
  # implementation and its longer prose live on `Latu.DataFrame` or `Latu.Plan`.
  #
  # Reads source rather than the compiled module: `Keyword.validate!(opts, [...])` is where a
  # verb declares its closed set, and the keys are only in the AST. Same technique as
  # `test/latu/examples_test.exs` and `check_offline.exs`'s `Grouping` check.
  #
  # A verb whose option list is *generated* into its docstring — `Latu.Plan.join/3` interpolates
  # `@join_types` — passes this for free, which is the point: generate where the doc is a list,
  # and let this catch the prose.

  @unanalysable_ok []

  # A plan builder is not always named after the verb that reaches it, so a closed set found in
  # `Latu.Plan.as_of_join/3` has to be checked against `Latu.join_as_of/3`'s docstring. Every
  # builder carrying a closed set is either mapped here or exempt below — a name that is neither
  # fails, because a check that skips quietly is worse than no check.
  @verb_for %{
    as_of_join: :join_as_of,
    join_type: :join,
    save_mode: :write,
    nearest_direction!: :nearest_by_join,
    analyze: :explain,
    lateral_join: :lateral_join,
    nearest_by_join: :nearest_by_join,
    parse: :parse
  }

  # The same idea for *option lists* rather than closed sets, and the reason it needs its own
  # map: one builder can serve several verbs. `Latu.Plan.set_op/4` validates `:all`, `:by_name`
  # and `:allow_missing_columns` for `union/3`, `intersect/3` and `except/3` alike, so all three
  # docstrings have to carry them; `Latu.DataFrame.insert_into_command/3` validates
  # `Latu.insert_into/3`'s only option under a name no reader would guess.
  #
  # A validating function that is neither a facade verb, nor listed here, nor named internal
  # below, fails the accounting test, so nothing can be skipped in silence.
  @verbs_for %{
    insert_into_command: [:insert_into],
    create_view: [:create_temp_view],
    show_string: [:show],
    html_string: [:to_html],
    as_of_join: [:join_as_of],
    from_url: [:connect],
    write: [:write, :save_as_table],
    write_command: [:write],
    save_as_table_command: [:save_as_table],
    write_v2_command: [:write_v2],
    interrupt_scope: [:interrupt],
    quantiles: [:approx_quantile],
    set_op: [:union, :intersect, :except],
    tree_string: [:tree_string, :print_schema],
    explain: [:explain, :explain_string],
    merge_action: [:when_matched, :when_not_matched, :when_not_matched_by_source]
  }

  # Validating functions no facade option reaches at all. Each is a decision, not an oversight.
  #
  #   * `Latu.Catalog`'s verbs — `list_tables`, `drop_table` and the rest live on that module
  #     with their own docstrings; this file reads `lib/latu.ex` only.
  #   * `new`, `decode`, `subquery`, `aggregate`, `sort_order` — internal builders and
  #     carriers, reached positionally or by a verb of their own.
  @not_a_facade_verb [
    :aggregate,
    :decode,
    :drop_table,
    :drop_view,
    :list_catalogs,
    :list_columns,
    :list_databases,
    :list_tables,
    :new,
    :sort_order,
    :subquery,
    :table_exists
  ]

  # Closed sets that no facade option reaches: their value arrives positionally, from a verb of
  # its own, or from a builder Latu calls internally.
  #
  #   * `aggregate` — the group type is chosen by which verb you called (`group_by`, `rollup`,
  #     `cube`, `pivot`, `grouping_sets`), never passed as an option.
  #   * `sort_order` — `:asc`/`:desc` come from `Latu.Column.asc/1` and friends, and `:nulls` is
  #     their option, not `Latu.sort/2`'s. `Latu.Column`'s docs are not read here.
  @not_an_option [
    :storage_level,
    :subquery,
    :write_v2,
    :merge_action,
    :set_op,
    :catalog,
    :aggregate,
    :sort_order
  ]

  # Options a verb accepts but nobody passes by hand: the `when_*` clause verbs build these.
  @built_by_a_verb %{merge_into: [:matched, :not_matched, :not_matched_by_source]}

  # Builders whose options the facade sets itself, so no caller can pass them. `Latu.sort/2`
  # takes columns and nothing else — `:global` is what `sort_within_partitions/2` sets — and
  # `Latu.repartition/2,3` take no options either, `:shuffle` being what `coalesce/2` sets.
  # Documenting those as options would invent an API. Named here rather than inferred, because
  # "an option validated in a function named X is an option of `Latu.X`" is false for an
  # internal builder.
  @set_by_latu [:repartition, :sort]

  # **A facade that splits its keys before forwarding them makes the rest unreachable.**
  # `Latu.DataFrame.write_command/2` does `Keyword.split(opts, [seven reserved])` and forwards
  # everything else as an opaque writer-option map, so a key the downstream builder validates but
  # the split does not name **cannot be passed by any caller** — it lands inside that map and
  # dies in `Latu.Plan.to_options/1`. The reachable set is the split list, not the builder's, and
  # documenting an option that raises is worse than documenting none.
  #
  # Detected rather than listed: a `Keyword.split` whose *leftover* is forwarded under `:options`
  # is the shape that closes a key set. A split that merely lifts `:progress` out and passes the
  # rest along — `show/2`, `to_html/2` — closes nothing, and is named below so the distinction is
  # a decision rather than a gap.
  @passes_the_rest_along [:show, :to_html]

  # What each of those splits makes unreachable, so the loss is named rather than silent. Every
  # one is a real key of `Latu.Plan.read/1` or `Latu.Plan.write/2`, reachable there and not from
  # the facade. Fixing that is a code patch, not a doc change: `:options` should merge into the
  # leftover instead of being swallowed by it.
  @unreachable_by_split %{
    read: [:options],
    save_as_table: [:options, :path, :table],
    write: [:options, :table],
    write_v2: [:options]
  }

  test "every option a verb accepts is named in the facade's own docstring" do
    facade = docs("lib/latu.ex")
    takes_opts = optioned("lib/latu.ex")

    closing = closing_splits()

    problems =
      for {name, keys} <- validated(),
          verb <- verbs_for(name),
          verb in takes_opts,
          doc = facade[verb],
          doc != nil,
          key <- reachable(verb, keys, closing),
          key not in Map.get(@built_by_a_verb, verb, []),
          not named?(options_section(doc), key) do
        "Latu.#{verb}: accepts `:#{key}` and never explains it under ## Options"
      end

    assert Enum.sort(problems) == []
  end

  # The other side of `reachable/2`. A key dropped there is a key `Latu.Plan` accepts and no
  # caller can pass, which is a defect worth seeing rather than a filter worth trusting — so
  # each one is named, and a new one fails until somebody decides which it is.
  test "every key a facade's own split makes unreachable is named" do
    closing = closing_splits()

    dropped =
      for {name, keys} <- validated(),
          verb <- verbs_for(name),
          lost = Enum.sort(keys -- reachable(verb, keys, closing)),
          lost != [],
          into: %{},
          do: {verb, lost}

    assert dropped == Map.new(@unreachable_by_split, fn {k, v} -> {k, Enum.sort(v)} end)
  end

  # **The reverse direction.** Every check above asks whether a real option is documented; this
  # one asks whether a documented option is real — documenting a key that raises is not a
  # *missing* key, so nothing above sees it.
  #
  # Bullets only. Prose inside the section legitimately names a sibling's options ("`collect/2`'s:
  # `:keys` and `:progress`"); a `* `:key` — ...` line is this verb's own claim about itself.
  test "every option a docstring claims under ## Options is one the verb can accept" do
    facade = docs("lib/latu.ex")
    closing = closing_splits()

    # What a verb can take: the keys its builders validate and its own split can still reach,
    # **plus every key it splits out for itself**. A split key is accepted by definition —
    # `Latu.read/2` normalises `:path` into `Latu.Plan.read/1`'s `:paths` and never forwards it,
    # and `show/2` lifts `:progress` out before the plan sees it. Neither is validated
    # downstream, and both are real.
    from_validation =
      for {name, keys} <- validated(), verb <- verbs_for(name), reduce: %{} do
        acc ->
          Map.update(
            acc,
            verb,
            reachable(verb, keys, closing),
            &(&1 ++ reachable(verb, keys, closing))
          )
      end

    accepted =
      for {name, keys, _kind} <- all_splits(), verb <- verbs_for(name), reduce: from_validation do
        acc -> Map.update(acc, verb, keys, &Enum.uniq(&1 ++ keys))
      end

    problems =
      for {verb, doc} <- facade,
          real = Map.get(accepted, verb, []),
          real != [],
          key <- claimed(options_section(doc)),
          key not in real,
          key not in Map.get(@built_by_a_verb, verb, []) do
        "Latu.#{verb}: claims `:#{key}` under ## Options and cannot accept it"
      end

    assert Enum.sort(problems) == []
  end

  # And the classification itself: a `Keyword.split` that closes a key set looks exactly like one
  # that does not, until you read where the leftover goes. Naming the second kind keeps the
  # difference a decision.
  test "every key split in lib/ either closes a key set or is named as passing the rest along" do
    assert Enum.sort(Map.keys(pass_through_splits())) == Enum.sort(@passes_the_rest_along)
  end

  # The section itself, reported once per verb rather than once per option, so a docstring that
  # never grew one reads as one failure instead of nine.
  test "every option-taking verb has an ## Options section" do
    facade = docs("lib/latu.ex")
    takes_opts = optioned("lib/latu.ex")

    missing =
      for {name, keys} <- validated(),
          verb <- verbs_for(name),
          verb in takes_opts,
          doc = facade[verb],
          doc != nil,
          keys != Map.get(@built_by_a_verb, verb, []),
          options_section(doc) == "",
          uniq: true,
          do: verb

    assert Enum.sort(missing) == []
  end

  # The other side of that filter. Every skip above is a place a real gap can hide, so each one
  # is named here rather than left to silence.
  test "every validating function is a facade verb, mapped to one, or named internal" do
    takes_opts = optioned("lib/latu.ex")

    unaccounted =
      for {name, _keys} <- validated(),
          not Map.has_key?(@verbs_for, name),
          name not in @not_a_facade_verb,
          name not in @set_by_latu,
          Enum.all?(verbs_for(name), &(&1 not in takes_opts)),
          do: name

    assert Enum.sort(unaccounted) == []
  end

  # And the reverse: a name mapped or exempted here that no longer validates anything is a
  # stale entry, which is how an exemption list quietly grows into a blindfold.
  test "no entry in the maps or exemption lists is stale" do
    declaring =
      MapSet.new(Enum.map(validated(), &elem(&1, 0)) ++ Enum.map(all_splits(), &elem(&1, 0)))

    stale =
      Enum.reject(
        Map.keys(@verbs_for) ++ @not_a_facade_verb ++ @set_by_latu,
        &MapSet.member?(declaring, &1)
      )

    assert Enum.sort(stale) == []
  end

  defp verbs_for(name), do: Map.get(@verbs_for, name, [name])

  # The keys a `## Options` section claims for itself: one *top-level* bullet, one key.
  #
  # **Two spaces, not four.** A `@doc """` heredoc arrives here with its common indentation
  # already stripped by the compiler, so a bullet written at four columns is at two by the time
  # this reads it — and a regex for four columns silently matches the *nested* value lists
  # (`:inner`, `:memory_only`) instead, reporting every closed-set value as an option the verb
  # cannot take. The nesting is the difference between "this verb takes this option" and "this
  # option takes this value", so the depth is load-bearing.
  defp claimed(section) do
    ~r/^ {2}\* `:([a-z_]+)`/m
    |> Regex.scan(section)
    |> Enum.map(fn [_line, key] -> String.to_atom(key) end)
    |> Enum.uniq()
  end

  # The keys a caller can actually pass to `verb`: everything the builders validate, narrowed to
  # the facade's own split where the facade has one.
  #
  # `closing_splits/0` re-reads every file in `lib/`, so it is computed once by the caller and
  # threaded in. Calling it per key took this file from 0.7s to 11s.
  defp reachable(verb, keys, closing) do
    case closing[verb] do
      nil -> keys
      allowed -> Enum.filter(keys, &(&1 in allowed))
    end
  end

  # `%{verb => [key]}` for a split whose leftover is forwarded as `:options`.
  defp closing_splits do
    for {name, keys, :closes} <- all_splits(),
        verb <- verbs_for(name),
        reduce: %{} do
      acc -> Map.update(acc, verb, keys, &Enum.uniq(&1 ++ keys))
    end
  end

  defp pass_through_splits do
    for {name, keys, :passes} <- all_splits(), into: %{}, do: {name, keys}
  end

  # A `Keyword.split(opts, [literal])` under a definition, classified by whether `:options`
  # appears in the same definition — which is where the leftover goes when the split closes the
  # key set. Anything whose split list is not a literal is reported by `unanalysable/0` already.
  defp all_splits do
    for path <- sources(), split <- splits(path), do: split
  end

  defp splits(path) do
    {:ok, ast} = path |> File.read!() |> Code.string_to_quoted()

    {_ast, found} =
      Macro.prewalk(ast, [], fn
        {form, _meta, [head, body]} = node, acc when form in [:def, :defp] ->
          {node, acc ++ for(keys <- split_keys(body), do: {clause_name(head), keys, kind(body)})}

        node, acc ->
          {node, acc}
      end)

    found
  end

  defp split_keys(body) do
    {_body, found} =
      Macro.prewalk(body, [], fn
        {{:., _d, [{:__aliases__, _a, [:Keyword]}, :split]}, _m, args} = node, acc
        when length(args) in [1, 2] ->
          {node, acc ++ [keys(List.last(args))]}

        node, acc ->
          {node, acc}
      end)

    found
  end

  # Where the leftover goes is the whole distinction, and it is visible in the source: a split
  # that closes a key set hands the rest on under `:options`.
  defp kind(body) do
    if Macro.to_string(body) =~ ~r/:options\b|\boptions:/, do: :closes, else: :passes
  end

  # A `Keyword.validate!` whose keys are not a literal list tells us nothing, and silence would
  # let a whole verb's options go unchecked. Named here so the exemption is a decision.
  test "every option list in lib/ can be read statically" do
    assert Enum.sort(unanalysable()) == Enum.sort(@unanalysable_ok)
  end

  # Naming an option is not documenting it. `Latu.join`'s docstring showed `how: :left` in an
  # example, which named the option and hid six of its seven values — the gap that started this.
  # `lookup(@constant, opts[:key], _)` is where a verb declares a closed set, so the constant's
  # own keys are the list the facade has to carry.
  test "every value of a closed-set option is named in the facade's own docstring" do
    facade = docs("lib/latu.ex")

    problems =
      for {builder, values} <- closed_sets(),
          builder not in @not_an_option,
          verb = Map.get(@verb_for, builder, builder),
          doc = facade[verb],
          doc != nil,
          value <- values,
          not named?(doc, value) do
        "Latu.#{verb}: `:#{value}` is accepted and never appears"
      end

    assert Enum.sort(problems) == []
  end

  # The other half of the check above: a builder whose closed set reaches no facade docstring at
  # all is invisible to it, so every one is accounted for by name.
  test "every closed set in lib/ is checked against a verb, or exempt by name" do
    facade = docs("lib/latu.ex")

    unaccounted =
      for {builder, _values} <- closed_sets(),
          builder not in @not_an_option,
          facade[Map.get(@verb_for, builder, builder)] == nil,
          do: builder

    assert Enum.sort(unaccounted) == []
  end

  # =============================================
  # Sources
  # =============================================

  defp sources do
    "lib/**/*.ex"
    |> Path.wildcard()
    |> Enum.reject(&String.starts_with?(&1, "lib/latu/protocol/"))
    |> Enum.sort()
  end

  # `%{function name => [option key]}`, unioned across every module that validates for it —
  # `Latu.sample/2` reaches `Latu.DataFrame.sample/2` and `Latu.Plan.sample/3`, and the reader
  # of `Latu.sample` needs both sets.
  defp validated do
    for path <- sources(), {name, keys} <- validators(path), reduce: %{} do
      acc -> Map.update(acc, name, keys, &Enum.uniq(&1 ++ keys))
    end
  end

  # Membership, not equality: a definition's keys are a *list*, so matching `{name,
  # :unanalysable}` never fired and this check could not fail however unreadable the tree got.
  defp unanalysable do
    for path <- sources(),
        {name, keys} <- validators(path),
        :unanalysable in keys,
        uniq: true,
        do: name
  end

  # `%{function name => [allowed value]}` for every option resolved through a closed set.
  defp closed_sets do
    for path <- sources(), {name, values} <- lookups(path), reduce: %{} do
      acc -> Map.update(acc, name, values, &Enum.uniq(&1 ++ values))
    end
  end

  defp lookups(path) do
    {:ok, ast} = path |> File.read!() |> Code.string_to_quoted()
    constants = attributes(ast)
    {_ast, found} = Macro.prewalk(ast, [], &closed_set(&1, &2, constants))

    found
  end

  # `@name [key: _, ...]` — the shape every closed set in `lib/` uses.
  defp attributes(ast) do
    {_ast, found} =
      Macro.prewalk(ast, %{}, fn
        {:@, _meta, [{name, _m, [list]}]} = node, acc when is_list(list) ->
          {node, Map.put(acc, name, keys(list))}

        node, acc ->
          {node, acc}
      end)

    found
  end

  # `defp` too: `Latu.Plan.join/3` resolves `:how` through a private `join_type/1`, so the set
  # that matters most is not in a public definition at all.
  defp closed_set({form, _meta, [head, body]} = node, acc, constants)
       when form in [:def, :defp] do
    case {signature(head), sets(body, constants, options(body) ++ parameters(head))} do
      {name, [_ | _] = values} when is_atom(name) -> {node, [{name, values} | acc]}
      _other -> {node, acc}
    end
  end

  defp closed_set(node, acc, _constants), do: {node, acc}

  # Only the forms that tie a constant to an *option*: `lookup(@const, opts[:key], _)`, and the
  # same after the option has been destructured into a variable of its own name —
  # `lookup(@const, how, _)` where `:how` is one of the keys this function validates. Both
  # spellings are in use, and the second is the one `Plan.join/3` uses, which is the case that
  # started this. A positional argument that is not a validated option is not an option.
  defp sets(body, constants, keys) do
    {_body, found} =
      Macro.prewalk(body, [], fn
        {:lookup, _m, [{:@, _a, [{const, _c, nil}]}, argument | _rest]} = node, acc ->
          {node, acc ++ if(option?(argument, keys), do: Map.get(constants, const, []), else: [])}

        node, acc ->
          {node, acc}
      end)

    Enum.uniq(found)
  end

  defp option?({{:., _m, [Access, :get]}, _a, [{:opts, _o, _c}, key]}, _keys) when is_atom(key),
    do: true

  defp option?({:required!, _m, [argument | _rest]}, keys), do: option?(argument, keys)
  defp option?({name, _m, nil}, keys) when is_atom(name), do: name in keys
  defp option?(_other, _keys), do: false

  defp validators(path) do
    {:ok, ast} = path |> File.read!() |> Code.string_to_quoted()
    constants = attributes(ast)
    {_ast, found} = Macro.prewalk(ast, [], &definition(&1, &2, constants))

    found
  end

  defp definition(node, acc, constants)

  # `defp` too, as the closed-set collector already does. `Latu.interrupt/2`'s whole option
  # set lives in a private `interrupt_scope/1`, so walking public definitions only left
  # `:tag` and `:operation_id` unchecked — a verb's contract does not become less public for
  # being validated in a private function.
  defp definition({form, _meta, [head, body]} = node, acc, constants)
       when form in [:def, :defp] do
    case {clause_name(head), options(body, constants)} do
      {nil, _} -> {node, acc}
      {_name, []} -> {node, acc}
      {name, keys} -> {node, [{name, keys} | acc]}
    end
  end

  defp definition(node, acc, _constants), do: {node, acc}

  # **A clause whose first argument is a literal atom declares options for that atom, not for
  # its own name.** `Latu.Plan.analyze/3` is five clauses under one name — `analyze(:explain,
  # ...)`, `analyze(:persist, ...)`, `analyze(:unpersist, ...)` — and each one's options belong
  # to a different facade verb. Keyed by the function name, the five sets merged into one and
  # `:blocking` would have counted as documented because `Latu.explain/2` mentions `:mode`.
  defp clause_name({:when, _meta, [head | _rest]}), do: clause_name(head)

  defp clause_name({_name, _meta, [first | _rest]})
       when is_atom(first) and not is_nil(first) and not is_boolean(first),
       do: first

  defp clause_name(head), do: signature(head)

  # A one-line builder like `defp join_type(how)` never validates anything: the option arrives
  # already destructured, and the parameter's name is the option's name.
  defp parameters({:when, _meta, [head | _rest]}), do: parameters(head)

  defp parameters({_name, _meta, args}) when is_list(args) do
    for argument <- args, name = parameter(argument), do: name
  end

  defp parameters(_other), do: []

  defp parameter({:\\, _meta, [argument, _default]}), do: parameter(argument)
  defp parameter({name, _meta, nil}) when is_atom(name), do: name
  defp parameter(_other), do: nil

  # The names `Latu` exports with an `opts` parameter — the only ones a caller can pass an
  # option to at all. `defdelegate sort(df, columns)` cannot, whatever `Latu.Plan.sort/3` takes.
  defp optioned(path) do
    {:ok, ast} = path |> File.read!() |> Code.string_to_quoted()

    {_ast, found} =
      Macro.prewalk(ast, [], fn
        {form, _meta, [head | _rest]} = node, acc when form in [:def, :defdelegate] ->
          {node, if(:opts in parameters(head), do: [signature(head) | acc], else: acc)}

        node, acc ->
          {node, acc}
      end)

    found
  end

  defp signature({:when, _meta, [head | _rest]}), do: signature(head)
  defp signature({name, _meta, _args}) when is_atom(name), do: name
  defp signature(_other), do: nil

  # Every `Keyword.validate!(opts, ...)` under one definition, flattened. A non-literal key
  # list reports itself rather than reading as "no options".
  #
  # **Both call shapes.** Piped, the options argument is implicit and the node carries one
  # argument, not two (`opts |> without_progress() |> Keyword.validate!(overwrite: nil)`), and a
  # shape this check cannot read has to fail, not vanish.
  defp options(body, constants \\ %{}) do
    {_body, found} =
      Macro.prewalk(body, [], fn
        {{:., _d, [{:__aliases__, _a, [:Keyword]}, :validate!]}, _m, args} = node, acc
        when length(args) in [1, 2] ->
          {node, acc ++ keys(List.last(args), constants)}

        node, acc ->
          {node, acc}
      end)

    Enum.uniq(found)
  end

  # `Keyword.validate!` takes both shapes in one list: `key: default` allows a key and gives it
  # a default, a bare `:key` allows it with none. Reading only the pairs made every bare-atom
  # list look empty — `Latu.interrupt/2`'s `[:tag, :operation_id]` and `Latu.clone_session/2`'s
  # `[:session_id]` were all invisible here.
  defp keys(list, constants \\ %{})

  defp keys(list, _constants) when is_list(list) do
    for entry <- list, key = key(entry), do: key
  end

  # `Keyword.validate!(opts, @overridable)` — a module attribute holding the list, which
  # `Latu.Session.from_url/2` uses for the twelve knobs `Latu.connect/2` overrides. Resolved
  # rather than reported, since the attribute is a literal list in the same file.
  defp keys({:@, _meta, [{const, _c, nil}]}, constants) do
    Map.get(constants, const, [:unanalysable])
  end

  defp keys(_other, _constants), do: [:unanalysable]

  defp key({key, _default}) when is_atom(key), do: key
  defp key(key) when is_atom(key), do: key
  defp key(_other), do: nil

  # =============================================
  # Docs
  # =============================================

  defp docs(path) do
    {:ok, ast} = path |> File.read!() |> Code.string_to_quoted()
    {_ast, found} = Macro.prewalk(ast, %{}, &collect_doc/2)

    found
  end

  # `@doc` precedes the definition it belongs to, so the last one seen is the one in force.
  defp collect_doc({:@, _meta, [{:doc, _m, [doc]}]} = node, acc) do
    {node, Map.put(acc, :pending, text(doc))}
  end

  defp collect_doc({form, _meta, [head | _rest]} = node, acc)
       when form in [:def, :defdelegate] do
    case {signature(head), Map.get(acc, :pending)} do
      {name, doc} when is_atom(name) and is_binary(doc) ->
        {node, acc |> Map.delete(:pending) |> Map.update(name, doc, &(&1 <> "\n" <> doc))}

      _other ->
        {node, Map.delete(acc, :pending)}
    end
  end

  defp collect_doc(node, acc), do: {node, acc}

  defp text(doc) when is_binary(doc), do: doc
  defp text({:<<>>, _meta, parts}), do: parts |> Enum.filter(&is_binary/1) |> Enum.join(" ")
  defp text(_other), do: nil

  # Both spellings the docs use — `:progress` and `progress:` — plus the interpolated form a
  # generated list produces once it has been rendered.
  #
  # **On a word boundary**, or `:on` is satisfied by `condition:`, `:tag` by `:tags` and `:path`
  # by `:paths`.
  defp named?(doc, key) do
    Regex.match?(~r/(?<![A-Za-z0-9_]):#{key}(?![A-Za-z0-9_])/, doc) or
      Regex.match?(~r/(?<![A-Za-z0-9_])#{key}:/, doc)
  end

  # **The `## Options` section, not the whole docstring.** An option that appears only in an
  # example is named without being explained; scoping the search here is what makes
  # "documented" mean a line saying what the option does and what it defaults to.
  #
  # A docstring with no such section yields "", so every key in it is reported: that is the
  # intended failure, and `options_section?/1` reports it once by name instead of once per key.
  defp options_section(doc) do
    case String.split(doc, "## Options", parts: 2) do
      [_before, after_it] -> after_it |> String.split("## Examples", parts: 2) |> hd()
      [_only] -> ""
    end
  end
end
