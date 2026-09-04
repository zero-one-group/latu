defmodule Latu.DocsTest do
  use ExUnit.Case, async: true

  # Geni asserted this too (docs_test.clj): documentation as a test, not a habit. It earns its
  # keep here because nearly the whole surface is generated, so a gap is never one forgotten
  # function — it is a shape (the second arity of every optional-argument row, once).

  test "every public function in a documented module has a doc" do
    undocumented =
      Enum.flat_map(modules(), fn module ->
        case Code.fetch_docs(module) do
          {:docs_v1, _, _, _, moduledoc, _, docs} when moduledoc not in [:hidden, :none] ->
            for {{:function, name, arity}, _line, _sig, :none, _meta} <- docs,
                do: "#{inspect(module)}.#{name}/#{arity}"

          _other ->
            []
        end
      end)

    assert undocumented == []
  end

  test "and every module says whether it is public, rather than leaving it open" do
    silent =
      for module <- modules(),
          match?({:docs_v1, _, _, _, :none, _, _}, Code.fetch_docs(module)),
          do: inspect(module)

    assert silent == []
  end

  # `mix.exs`'s `:groups_for_modules` is a hand-maintained list, so a new public module would
  # land in ExDoc's default group and nobody would see it. Same shape as the `@extras`
  # accounting in `Latu.ExamplesTest`: a list that has to be kept in step gets a check.
  #
  # Reads the real project config, so this one is `mix`-only.
  test "every documented module has a place in the docs sidebar, and every place is real" do
    grouped =
      Mix.Project.config()
      |> Keyword.fetch!(:docs)
      |> Keyword.fetch!(:groups_for_modules)
      |> Enum.flat_map(fn {_group, modules} -> modules end)

    documented =
      for module <- modules(),
          match?(
            {:docs_v1, _, _, _, doc, _, _} when doc not in [:hidden, :none],
            Code.fetch_docs(module)
          ),
          do: module

    assert Enum.sort(documented) -- grouped == [],
           "not in any `groups_for_modules`: " <> inspect(Enum.sort(documented) -- grouped)

    assert grouped -- Enum.sort(documented) == [],
           "grouped but not a documented module: " <> inspect(grouped -- Enum.sort(documented))
  end

  # `:extras` is hand-maintained too, and a new guide is otherwise invisible: tier 2 runs its
  # fences, `Latu.ExamplesTest` counts it as a guide, and `mix docs` renders no page for it. A
  # check with a hand-maintained input list needs a check on the list.
  #
  # Prose extras are named individually because their order is the sidebar's; guides are a
  # directory, so the wildcard is the list and any file in it has to appear in both `:extras`
  # and the Guides group. `docs/cheatsheet.cheatmd` is generated, and `cheatsheet_test.exs` keeps
  # it current, but it still has to be reachable, which is this list's job.
  @prose_extras [
    "README.md",
    "CHANGELOG.md",
    "docs/cheatsheet.cheatmd",
    "usage-rules.md",
    "docs/deviations.md",
    "CONTRIBUTING.md"
  ]

  test "every guide is an extra, in the Guides group, and every extra is a real file" do
    docs = Mix.Project.config() |> Keyword.fetch!(:docs)
    # An extra is a path, or `{path, opts}` when ExDoc needs a title it cannot read from the file.
    extras = docs |> Keyword.fetch!(:extras) |> Enum.map(&extra_path/1)
    guides = "docs/guides/*.md" |> Path.wildcard() |> Enum.sort()

    in_guides_group =
      docs |> Keyword.fetch!(:groups_for_extras) |> Keyword.fetch!(:Guides) |> Enum.sort()

    assert guides != [], "docs/guides/ is empty; the wildcard would make this pass vacuously"

    assert guides -- extras == [],
           "a guide that renders nowhere: " <> inspect(guides -- extras)

    assert guides -- in_guides_group == [],
           "a guide outside the Guides group: " <> inspect(guides -- in_guides_group)

    assert in_guides_group -- guides == [],
           "grouped as a guide but not in docs/guides/: " <> inspect(in_guides_group -- guides)

    assert Enum.sort(extras) == Enum.sort(guides ++ @prose_extras),
           "extras and the files on disk disagree: " <>
             inspect(Enum.sort(extras) -- Enum.sort(guides ++ @prose_extras)) <>
             " / " <> inspect(Enum.sort(guides ++ @prose_extras) -- Enum.sort(extras))

    for extra <- extras do
      assert File.exists?(extra), "`:extras` names #{extra}, which does not exist"
    end
  end

  defp extra_path({path, _opts}), do: to_string(path)
  defp extra_path(path), do: path

  # **ExDoc resolves an extra link by basename alone** (`config.extras[Path.basename(path)]`), so
  # a path that is wrong in a clone renders fine on the docs site, and a basename belonging to a
  # *different* extra retargets silently — `[dev/README.md](dev/README.md)` once rendered as a
  # link to the project README. Both halves are checked: does the path resolve on disk relative
  # to the file it is in (GitHub's question), and which extra will ExDoc pick (must be the file
  # the path names). A link whose basename is no extra at all 404s on the published site, so
  # there are none — repo-only files are plain code spans.
  #
  # Mirrors ExDoc's own `@builtin_ext`. If a future ExDoc consults the whole path instead of the
  # basename, this test gets stricter than ExDoc rather than wrong.
  @exdoc_extensions [".md", ".livemd", ".cheatmd", ".txt", ""]

  test "every relative link in the docs resolves, and to the file it names" do
    by_basename = Map.new(extras(), &{Path.basename(&1), &1})

    problems =
      for {source, line, text, href} <- links(),
          problem <- link_problem(source, href, by_basename),
          do: "#{source}:#{line}: [#{text}](#{href}) — #{problem}"

    assert problems == []
  end

  defp link_problem(source, href, by_basename) do
    path = href |> String.split("#") |> hd()
    target = path |> Path.expand(Path.dirname(source)) |> rel()
    resolved = by_basename[Path.basename(path)]

    cond do
      path == "" -> []
      not File.exists?(target) -> ["no such file: #{target}"]
      Path.extname(path) not in @exdoc_extensions -> []
      resolved == target -> []
      resolved != nil -> ["ExDoc matches the basename, so this renders as a link to #{resolved}"]
      true -> ["no extra has this basename, so it stays a relative href and 404s in `mix docs`"]
    end
  end

  @link ~r/\[([^\]]*)\]\(([^)\s]+)\)/

  defp links do
    for source <- extras(),
        {line, number} <- source |> File.read!() |> String.split("\n") |> Enum.with_index(1),
        [_all, text, href] <- Regex.scan(@link, line),
        not String.starts_with?(href, ["http://", "https://", "#", "mailto:"]),
        do: {source, number, text, href}
  end

  # `{"README.md", title: "Latu"}` is a legal `:extras` entry, so the list is not all strings.
  defp extras do
    Mix.Project.config()
    |> Keyword.fetch!(:docs)
    |> Keyword.fetch!(:extras)
    |> Enum.map(fn
      {path, _opts} -> to_string(path)
      path -> to_string(path)
    end)
  end

  defp rel(absolute), do: Path.relative_to_cwd(absolute)

  # The generated protocol modules are protoc's output and carry no docs by construction.
  defp modules do
    :latu
    |> Application.spec(:modules)
    |> Enum.reject(&(&1 |> Atom.to_string() |> String.starts_with?("Elixir.Latu.Protocol")))
  end
end
