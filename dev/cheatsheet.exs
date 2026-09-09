# Generates `docs/cheatsheet.cheatmd` — ExDoc's cheatsheet format — from the **compiled**
# modules, because `Latu.Column`'s operators, predicates and sort keys are generated inside
# `for` comprehensions and a source parse would miss them.
#
#     mix run dev/cheatsheet.exs --write
#
# Without `--write` it only defines `Latu.Dev.Cheatsheet`, which `test/latu/cheatsheet_test.exs`
# uses to regenerate the page and diff it against the file. Sections come from each module's own
# `# ====` banners, so the grouping cannot drift from the file's organisation. `Latu.Functions`
# is absent: the function reference is its own ExDoc page, grouped by Spark's categories.

defmodule Latu.Dev.Cheatsheet do
  @moduledoc false

  @output "docs/cheatsheet.cheatmd"

  @modules [
    {Latu, "lib/latu.ex", "Verbs", "Called qualified, the way `Enum` is."},
    {Latu.Column, "lib/latu/column.ex", "Expressions",
     "`import Latu.Column` — small enough to compose by hand."},
    {Latu.Window, "lib/latu/window.ex", "Windows", "`alias Latu.Window, as: W`."},
    {Latu.Catalog, "lib/latu/catalog.ex", "Catalog",
     "Databases, tables and views — `spark.catalog` in PySpark."},
    {Latu.StreamingQuery, "lib/latu/streaming_query.ex", "Streaming queries",
     "The handle `Latu.write_stream/2` returns, and `spark.streams` — session-first."}
  ]

  def output, do: @output

  def render do
    [preamble() | Enum.map(@modules, &module/1)]
    |> Enum.join("\n")
    |> String.trim_trailing()
    |> Kernel.<>("\n")
  end

  def write do
    File.write!(@output, render())
    IO.puts("wrote #{@output} (#{length(String.split(render(), "\n"))} lines)")
  end

  defp preamble do
    """
    # Cheatsheet

    Generated from the compiled modules by `dev/cheatsheet.exs`; `test/latu/cheatsheet_test.exs`
    fails if it drifts. **`+ !`** marks a verb with a raising twin of the same arity.

    Spark's ~500 functions are not here — they are a reference, not a cheatsheet, and
    `Latu.Functions` is that page, grouped by Spark's own categories.
    """
  end

  defp module({module, path, title, blurb}) do
    sections = sections(File.read!(path))

    grouped =
      module
      |> entries()
      |> Enum.group_by(fn {_name, _arities, _summary, _twin?, line} ->
        section_at(sections, line)
      end)

    cards =
      sections
      |> Enum.map(fn {_line, name} -> name end)
      |> Enum.concat([nil])
      |> Enum.uniq()
      |> Enum.map(&card(&1, grouped[&1]))
      |> Enum.reject(&is_nil/1)

    "## #{title}\n\n#{blurb}\n\n" <> Enum.join(cards, "\n")
  end

  defp card(_name, nil), do: nil
  defp card(_name, []), do: nil

  defp card(name, entries) do
    rows =
      entries
      |> Enum.sort_by(fn {n, _a, _s, _t, _l} -> to_string(n) end)
      |> Enum.map_join("\n", fn {n, arities, summary, twin?, _line} ->
        row("#{n}#{arities}", summary, twin?)
      end)

    "### #{name || "General"}\n\n| | |\n| --- | --- |\n#{rows}\n"
  end

  # `{name, "/2,3", summary, twin?, line}` per public function, with the `!` twins folded into the
  # entry they raise for. A twin's own docstring is "Like `x/2`, raising on failure." — a line
  # nobody needs on a cheatsheet, and dropping it halves the page.
  defp entries(module) do
    {:docs_v1, _anno, _lang, _format, _moduledoc, _meta, docs} = Code.fetch_docs(module)

    public =
      for {{:function, name, arity}, line, _signature, %{"en" => doc}, _meta} <- docs,
          do: {name, arity, line, doc}

    bangs =
      for {name, arity, _line, _doc} <- public,
          String.ends_with?(to_string(name), "!"),
          into: MapSet.new(),
          do: {String.trim_trailing(to_string(name), "!"), arity}

    public
    |> Enum.reject(fn {name, _a, _l, _d} -> String.ends_with?(to_string(name), "!") end)
    |> Enum.group_by(fn {name, _a, _l, _d} -> name end)
    |> Enum.map(fn {name, clauses} ->
      arities = clauses |> Enum.map(&elem(&1, 1)) |> Enum.sort() |> Enum.uniq()
      {_n, _a, line, doc} = Enum.min_by(clauses, &elem(&1, 2))
      twin? = Enum.any?(arities, &MapSet.member?(bangs, {to_string(name), &1}))

      {name, "/" <> Enum.join(arities, ","), summary(doc), twin?, line}
    end)
  end

  # A table row cannot wrap, so the summary is cut to fit the repo's 98 columns rather than
  # letting a generated file carry 48 over-long lines. A cheatsheet entry that needs more than
  # this is not a cheatsheet entry — the module page is where the sentence belongs.
  @width 98
  @marker " **+ !**"

  # The marker is appended after clamping and its width reserved before, so a long summary
  # loses words rather than the fact that the verb has a raising twin. Public, like
  # `first_sentence/1`, only so `cheatsheet_test.exs` can call it: the module is `@moduledoc
  # false`, `dev/` is not in `elixirc_paths`, and it ships in no package.
  def row(call, text, twin?) do
    fixed = String.length("| `#{call}` |  |")
    budget = @width - fixed - if(twin?, do: String.length(@marker), else: 0)
    "| `#{call}` | #{clamp(text, budget)}#{if twin?, do: @marker, else: ""} |"
  end

  defp clamp(text, budget) do
    if String.length(text) <= budget do
      text
    else
      text
      |> String.split(" ")
      |> Enum.reduce_while([], fn word, taken ->
        candidate = Enum.reverse([word | taken])

        if candidate |> Enum.join(" ") |> String.length() > budget - 1,
          do: {:halt, taken},
          else: {:cont, [word | taken]}
      end)
      |> Enum.reverse()
      |> Enum.join(" ")
      |> String.trim_trailing(",")
      |> Kernel.<>("…")
    end
  end

  # The first **sentence** of the docstring, which `test/latu/docs_test.exs` guarantees exists.
  # A summary that wrapped in the source is rejoined, so the cell is one line.
  defp summary(doc) do
    doc
    |> String.split("\n\n", parts: 2)
    |> hd()
    |> String.split("\n")
    |> Enum.map_join(" ", &String.trim/1)
    |> String.trim()
    |> first_sentence()
    |> String.replace("|", "\\|")
  end

  # Splitting on ". " rather than "." keeps `Latu.show` intact. It does not keep `e.g.` intact —
  # that is `e.g` followed by ". " — so a part ending in an abbreviation is rejoined with the
  # next. A trailing full stop is put back only when something was actually dropped, since the
  # cut is then the sentence's own end.
  @abbreviations ~w(e.g i.e cf vs etc resp)

  def first_sentence(text) do
    parts = String.split(text, ". ")
    taken = take_sentence(parts)
    joined = Enum.join(taken, ". ")

    if length(taken) == length(parts), do: joined, else: joined <> "."
  end

  defp take_sentence([part | rest]) when rest != [] do
    if abbreviation?(part), do: [part | take_sentence(rest)], else: [part]
  end

  defp take_sentence(parts), do: parts

  defp abbreviation?(part) do
    Enum.any?(@abbreviations, &(part == &1 or String.ends_with?(part, " " <> &1)))
  end

  # `{line, name}` for every `# ====` / `# Name` / `# ====` banner, in file order.
  defp sections(source) do
    lines = source |> String.split("\n") |> Enum.with_index(1)

    for {line, number} <- lines,
        Regex.match?(~r/^\s*# ={10,}\s*$/, line),
        {next, _n} = Enum.at(lines, number, {"", 0}),
        [_all, name] <- [Regex.run(~r/^\s*#\s+(\S.*?)\s*$/, next)],
        not Regex.match?(~r/^={10,}$/, name),
        do: {number, name}
  end

  defp section_at([], _line), do: nil

  defp section_at(sections, line) do
    case Enum.filter(sections, fn {at, _name} -> at < line end) do
      [] -> nil
      earlier -> earlier |> Enum.max_by(&elem(&1, 0)) |> elem(1)
    end
  end
end

if "--write" in System.argv(), do: Latu.Dev.Cheatsheet.write()
