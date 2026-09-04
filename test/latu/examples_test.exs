defmodule Latu.ExamplesTest do
  use ExUnit.Case, async: true

  # Every code example in the docs, checked against the real API without a server.
  #
  # Three tiers verify Latu's examples: doctests run the pure ones, the guides run under
  # `test/integration/guides_test.exs`, and this is the third. It cannot run an example, but it
  # proves every function an example names exists at the arity it is called with — the whole
  # class of rot a doc acquires by sitting still while the API moves.
  #
  # Reads source, not `Code.fetch_docs/1`: five modules cannot be compiled offline and carry a
  # third of the corpus. What that misses is `Latu.Functions`' harvested
  # `@doc`s, built by `Docs.doc_for/2` at compile time — Spark's own SQL, with no Elixir in it.
  #
  # Extends `test/latu/function_calls_test.exs`, which does this for `F.` call sites in code:
  # same depipe, same reason.

  # What has to be kept in sync by hand, and what keeps it honest:
  #
  #   * `@extras` — the prose that is checked. A new page joins neither this nor the guides by
  #     itself, so "every markdown doc is accounted for" below fails until someone decides.
  #   * `@excused` — the pages deliberately not checked, each with the reason.
  #   * `@not_elixir` — the first-line shapes that are output rather than code. A new one shows
  #     up as a parse failure naming the block, which is the prompt to decide, not a silent miss.
  #   * `@aliased` — the aliases the docs use. A new one (`alias Latu.Catalog, as: C`) would go
  #     unresolved rather than wrong.
  #   * the corpus floor — raise it as the docs grow.

  @extras [
    "README.md",
    "CHANGELOG.md",
    "CONTRIBUTING.md",
    "usage-rules.md",
    "CLAUDE.md",
    "docs/deviations.md"
  ]

  @excused %{
    "docs/decisions.md" =>
      "a dated maintainer's log: its snippets record what the API was when a decision was made",
    "dev/README.md" =>
      "contributor notes: its code is shell, YAML, textproto and Python, not Latu examples",
    "assets/README.md" =>
      "the identity assets: palette, fonts and placement rules; its one fence is HTML"
  }

  # A block that is not Elixir, by its first line. Everything else must parse — a block nobody
  # can parse is reported, never skipped, or the opt-out is where a broken example hides.
  @not_elixir ["$ ", "+-", "|", "> ", "root", "sc://", "mix ", "docker ", "#=>", "** ("]

  @aliased %{[:F] => Latu.Functions, [:W] => Latu.Window}

  # The corpus is `@extras` plus `lib/`, and a page joins it by being named. So a doc nobody
  # checks is exactly how this file rots — quietly, while every assertion above stays green.
  test "every markdown doc is checked here, run as a guide, or excused by name" do
    accounted = @extras ++ Path.wildcard("docs/guides/*.md") ++ Map.keys(@excused)

    assert markdown() -- accounted == [],
           "neither checked, nor a guide, nor excused: " <> inspect(markdown() -- accounted)

    assert Map.keys(@excused) -- markdown() == [],
           "@excused names a file that is gone: " <> inspect(Map.keys(@excused) -- markdown())
  end

  # A floor, not a target. Every check below passes in silence on an empty corpus, so if the
  # extractor breaks — a markdown shape it stops recognising, a wildcard matching nothing —
  # this is what notices. Raise it when the docs grow; never lower it to make a run pass.
  #
  # The messages carry the count so raising the floor does not mean bisecting the assertion to
  # find out what it is now.
  test "the corpus is still there" do
    kinds = Enum.frequencies_by(blocks(), fn {_source, _line, block} -> kind(block) end)

    assert kinds[:elixir] >= 250, "Elixir blocks fell to #{kinds[:elixir]}"
    assert kinds[:doctest] >= 8, "doctest blocks fell to #{kinds[:doctest]}"
    assert length(references()) >= 650, "references fell to #{length(references())}"
  end

  test "every code block in the docs parses as Elixir" do
    problems =
      for {source, line, block} <- blocks(),
          kind(block) == :elixir,
          {:error, reason} <- [Code.string_to_quoted(joined(block))] do
        "#{source}:#{line}: does not parse (#{trim(reason)}) — starts: #{hd(block)}"
      end

    assert problems == []
  end

  test "every qualified Latu call in an example exists with that arity" do
    problems =
      for {source, line, block} <- blocks(),
          kind(block) == :elixir,
          {:ok, ast} <- [Code.string_to_quoted(joined(block))],
          {module, name, arity, offset} <- calls(ast),
          not exported?(module, name, arity) do
        "#{source}:#{line + offset}: #{inspect(module)}.#{name}/#{arity}" <>
          " — #{arities(module, name)}"
      end

    assert problems == []
  end

  # **Inline code in prose gets neither of the checks above.** `blocks/0` reads fenced blocks and
  # `references/0` reads `Mod.fun/arity` mentions, so a `Latu.select(df, [:a, :b])` written in a
  # sentence or a table cell is invisible to both. A PySpark-to-Latu translation table is *made*
  # of those, which would have left the most useful page in the docs as its least checked one.
  test "every qualified Latu call written inline in prose exists with that arity" do
    problems =
      for {source, line, code} <- inline_calls(),
          {:ok, ast} <- [Code.string_to_quoted(code)],
          {module, name, arity, _offset} <- calls(ast),
          not exported?(module, name, arity) do
        "#{source}:#{line}: `#{inspect(module)}.#{name}/#{arity}`" <>
          " — #{arities(module, name)}"
      end

    assert problems == []
  end

  test "every `Module.fun/arity` reference in the docs resolves" do
    problems =
      for {source, line, module, name, arity} <- references(),
          not exported?(module, name, arity) do
        "#{source}:#{line}: `#{inspect(module)}.#{name}/#{arity}` — #{arities(module, name)}"
      end

    assert problems == []
  end

  # A doctest whose expected value is a struct is asserting on `Inspect`, which
  # `CLAUDE.md` forbids: it breaks on a harmless change to a renderer. Where the rendering IS
  # the documented feature — the plan spine — the example calls `inspect/1` itself, so the
  # expected value is a quoted string and this check passes without an exception list.
  test "a doctest asserts data, not an inspected Latu struct" do
    problems =
      for {source, line, block} <- blocks(),
          kind(block) == :doctest,
          {expected, offset} <- expectations(block),
          String.starts_with?(expected, "#Latu") do
        "#{source}:#{line + offset}: expected value renders a struct — #{expected}"
      end

    assert problems == []
  end

  test "a doctest line has examples to run, and examples have a doctest line to run them" do
    declared = declared_doctests()
    documented = documented_doctests()

    assert declared -- documented == [],
           "these `doctest` lines run nothing — no `iex>` in the module's own docs: " <>
             inspect(declared -- documented)

    assert documented -- declared == [],
           "these modules have `iex>` examples no `doctest` line runs: " <>
             inspect(documented -- declared)
  end

  # =============================================
  # Sources
  # =============================================

  defp lib_sources do
    "lib/**/*.ex"
    |> Path.wildcard()
    |> Enum.reject(&String.starts_with?(&1, "lib/latu/protocol/"))
    |> Enum.sort()
  end

  # Every markdown doc the repo *owns*. Walked rather than listed, so a page in a directory
  # nobody thought of still has to be accounted for — but build output is not ours: `doc/` is
  # where ExDoc writes, and it appears the moment anyone runs `mix docs`.
  defp markdown do
    "**/*.md"
    |> Path.wildcard()
    |> Enum.reject(&String.starts_with?(&1, ["deps/", "_build/", "doc/"]))
    |> Enum.sort()
  end

  # `{line, text}` for every `@doc`/`@moduledoc` carrying a string. An interpolated heredoc
  # keeps its literal segments; the interpolations are module attributes, not code to copy.
  defp docstrings(path) do
    {:ok, ast} = path |> File.read!() |> Code.string_to_quoted()
    {_ast, found} = Macro.prewalk(ast, [], &docstring/2)

    Enum.reverse(found)
  end

  defp docstring({:@, _meta, [{tag, meta, [doc]}]} = node, acc)
       when tag in [:doc, :moduledoc] do
    case doc_text(doc) do
      nil -> {node, acc}
      text -> {node, [{meta[:line], text} | acc]}
    end
  end

  defp docstring(node, acc), do: {node, acc}

  defp doc_text(doc) when is_binary(doc), do: doc

  defp doc_text({:<<>>, _meta, parts}) do
    parts |> Enum.filter(&is_binary/1) |> Enum.join(" ... ")
  end

  defp doc_text(_other), do: nil

  # `{source, line, lines}` for every code block in every doc. Lines are 1-based and
  # approximate for a docstring: the `@doc` line plus the offset inside the heredoc.
  defp blocks do
    from_lib =
      for path <- lib_sources(),
          {line, text} <- docstrings(path),
          {offset, block} <- code_blocks(text),
          do: {path, line + offset + 1, block}

    # **Guides included, even though tier 2 runs them.** Running a fence is strictly stronger
    # than parsing it, so this is redundant for an executed one — and it is the only check a
    # fence marked `> **Not executed.**` gets at all. Rather than teach this file the marker and
    # risk the two definitions of "skipped" drifting apart, it checks every guide fence and lets
    # the redundancy pay for the guarantee: no fence in `docs/guides/` is unchecked by both.
    from_prose =
      for path <- @extras ++ Path.wildcard("docs/guides/*.md"),
          {offset, block} <- code_blocks(File.read!(path)),
          do: {path, offset + 1, block}

    from_lib ++ from_prose
  end

  # =============================================
  # Markdown
  # =============================================

  defp code_blocks(text) do
    text |> String.split("\n") |> Enum.with_index() |> gather([], true)
  end

  defp gather([], acc, _after_blank), do: Enum.reverse(acc)

  defp gather([{line, index} | rest], acc, after_blank) do
    cond do
      fence?(line) ->
        {body, tail} = Enum.split_while(rest, fn {l, _i} -> not fence?(l) end)
        block = if elixir_fence?(line), do: Enum.map(body, &elem(&1, 0)), else: []
        gather(Enum.drop(tail, 1), keep(block, index + 1, acc), true)

      # Markdown needs a blank line before an indented code block; requiring one keeps an
      # indented continuation of a bullet out of the corpus.
      #
      # A blank line inside such a block does not end it, in markdown — but it does separate
      # one example from the next, and ExUnit reads two `iex>` runs either side of a blank as
      # two doctests. Splitting there is what stops a docstring whose first example is a
      # doctest from hiding every example after it: the merged block reads as one doctest and
      # nothing else in it is ever parsed.
      after_blank and indented?(line) ->
        {body, tail} =
          Enum.split_while([{line, index} | rest], fn {l, _i} -> indented?(l) or blank?(l) end)

        gather(tail, segments(body) ++ acc, true)

      true ->
        gather(rest, acc, blank?(line))
    end
  end

  defp keep([], _index, acc), do: acc
  defp keep(block, index, acc), do: [{index, dedent(block)} | acc]

  # One indexed block in, its blank-separated runs out, each keeping its own line number.
  defp segments(body) do
    body
    |> Enum.chunk_by(fn {line, _index} -> blank?(line) end)
    |> Enum.reject(fn [{line, _index} | _rest] -> blank?(line) end)
    |> Enum.reduce([], fn [{_line, index} | _rest] = run, acc ->
      keep(Enum.map(run, &elem(&1, 0)), index, acc)
    end)
  end

  defp fence?(line), do: String.starts_with?(String.trim_leading(line), "```")

  # A fence declares its own language, so trust it: a ```bash or ```json fence is illustration.
  # An untagged one is code, which is what makes a missing tag show up here rather than pass.
  defp elixir_fence?(line) do
    tag = line |> String.trim() |> String.trim_leading("`")

    tag in ["", "elixir"]
  end

  defp indented?(line), do: Regex.match?(~r/^ {4,}\S/, line)
  defp blank?(line), do: String.trim(line) == ""

  defp dedent(lines) do
    margin =
      lines
      |> Enum.reject(&blank?/1)
      |> Enum.map(fn line -> String.length(line) - String.length(String.trim_leading(line)) end)
      |> Enum.min(fn -> 0 end)

    Enum.map(lines, fn line -> String.slice(line, margin..-1//1) || "" end)
  end

  defp joined(block), do: Enum.join(block, "\n")

  defp kind([first | _rest] = block) do
    cond do
      String.starts_with?(first, "iex>") -> :doctest
      Enum.any?(@not_elixir, &String.starts_with?(first, &1)) -> :prose
      Enum.all?(block, &blank?/1) -> :prose
      true -> :elixir
    end
  end

  defp expectations(block) do
    block
    |> Enum.with_index()
    |> Enum.reject(fn {line, _i} ->
      blank?(line) or String.starts_with?(line, "iex>") or String.starts_with?(line, "...>")
    end)
    |> Enum.map(fn {line, i} -> {String.trim(line), i} end)
  end

  # =============================================
  # Resolution
  # =============================================

  defp calls(ast) do
    {_ast, found} = ast |> depipe() |> uncapture() |> Macro.prewalk([], &collect/2)

    Enum.reverse(found)
  end

  # `Code.string_to_quoted/1` does not expand pipes, so `x |> Latu.limit(2)` leaves the call
  # node holding ONE argument. Rewriting `|>` first is what makes the arity real.
  defp depipe(ast) do
    Macro.prewalk(ast, fn
      {:|>, _meta, [left, {call, meta, args}]} when is_list(args) -> {call, meta, [left | args]}
      node -> node
    end)
  end

  # `&Latu.show!/1` is a call node with no arguments and the arity beside it. Rewriting it into
  # a call of that arity is what keeps a capture in the corpus rather than reading as `/0`.
  defp uncapture(ast) do
    Macro.prewalk(ast, fn
      {:&, _meta, [{:/, _s, [{{:., _d, _t} = target, meta, []}, arity]}]}
      when is_integer(arity) ->
        {target, meta, List.duplicate(nil, arity)}

      node ->
        node
    end)
  end

  defp collect({{:., _dot, [{:__aliases__, _a, parts}, name]}, meta, args} = node, acc)
       when is_atom(name) and is_list(args) do
    case module_for(parts) do
      nil -> {node, acc}
      module -> {node, [{module, name, length(args), meta[:line] - 1} | acc]}
    end
  end

  defp collect(node, acc), do: {node, acc}

  defp module_for(parts) do
    cond do
      Map.has_key?(@aliased, parts) -> Map.fetch!(@aliased, parts)
      hd(parts) == :Latu and :Protocol not in parts -> Module.concat(parts)
      true -> nil
    end
  end

  defp exported?(module, name, arity) do
    Code.ensure_loaded?(module) and
      ({name, arity} in module.__info__(:functions) or
         {name, arity} in module.__info__(:macros))
  end

  defp arities(module, name) do
    if Code.ensure_loaded?(module) do
      case for {^name, arity} <- module.__info__(:functions), do: arity do
        [] -> "no #{name} in #{inspect(module)}"
        found -> "#{name} takes #{Enum.join(found, " or ")}"
      end
    else
      "#{inspect(module)} is not a module"
    end
  end

  # =============================================
  # References
  # =============================================

  @qualified ~r/`([A-Z][A-Za-z0-9_.]*)\.([a-z_][A-Za-z0-9_]*[?!]?)\/(\d+)`/
  @bare ~r/`([a-z_][A-Za-z0-9_]*[?!]?)\/(\d+)`/

  # A bare `fun/2` in a module's own docs is what ExDoc autolinks against that module, falling
  # back to `Kernel`. In an extra or a guide it autolinks against nothing, so only the
  # qualified form is checked there — and a guide's *code* is checked by running it instead
  # (`test/integration/guides_test.exs`).
  defp references do
    from_lib =
      for path <- lib_sources(),
          {doc_line, text} <- docstrings(path),
          {line, offset} <- text |> String.split("\n") |> Enum.with_index(),
          {module, name, arity} <- qualified(line) ++ bare(line, own_module(path)),
          do: {path, doc_line + offset + 1, module, name, arity}

    from_prose =
      for path <- @extras ++ Path.wildcard("docs/guides/*.md"),
          {line, offset} <- path |> File.read!() |> String.split("\n") |> Enum.with_index(1),
          {module, name, arity} <- qualified(line),
          do: {path, offset, module, name, arity}

    from_lib ++ from_prose
  end

  # A backticked span naming a `Latu.` call: the `(` is what tells a call from a reference, since
  # `Latu.join/3` parses as a division and would otherwise be read as a zero-arity call — the
  # references test above is what checks those. A span that does not parse is skipped rather than
  # reported: prose is full of code-ish fragments, and `blocks/0` is where a parse failure is a
  # defect.
  defp inline_calls do
    for path <- @extras ++ Path.wildcard("docs/guides/*.md"),
        {line, number} <- path |> File.read!() |> String.split("\n") |> Enum.with_index(1),
        [_span, code] <- Regex.scan(~r/`(Latu\.[^`]+)`/, line),
        String.contains?(code, "("),
        do: {path, number, code}
  end

  defp qualified(line) do
    for [_all, module, name, arity] <- Regex.scan(@qualified, line),
        module = Module.concat([module]),
        match?(["Latu" | _rest], Module.split(module)),
        "Protocol" not in Module.split(module),
        do: {module, String.to_atom(name), String.to_integer(arity)}
  end

  defp bare(line, module) do
    for [_all, name, arity] <- Regex.scan(@bare, line),
        name = String.to_atom(name),
        arity = String.to_integer(arity),
        not exported?(Kernel, name, arity),
        do: {module, name, arity}
  end

  defp own_module(path) do
    path
    |> Path.rootname()
    |> Path.split()
    |> Enum.drop(2)
    |> Enum.map(&Macro.camelize/1)
    |> then(&Module.concat(["Latu" | &1]))
  end

  # =============================================
  # Doctest bookkeeping
  # =============================================

  defp declared_doctests do
    for path <- Path.wildcard("test/**/*.exs"),
        [_line, module] <- Regex.scan(~r/^\s*doctest\s+([A-Z][\w.]*)/m, File.read!(path)),
        do: Module.concat([module])
  end

  defp documented_doctests do
    for path <- lib_sources(),
        {_line, text} <- docstrings(path),
        String.contains?(text, "iex>"),
        uniq: true,
        do: own_module(path)
  end

  defp trim(reason), do: reason |> inspect() |> String.slice(0, 120)
end
