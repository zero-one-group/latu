defmodule Latu.Guides do
  @moduledoc false
  # What a guide's fences are, and how to run a set of them. Shared by
  # test/integration/guides_test.exs (the executed ones, in `mix check.all`) and
  # test/integration/s3_guide_test.exs (the not-executed ones, behind `--include s3`), because
  # two definitions of "a fence" and "the marker" would drift. Loaded by test/test_helper.exs.

  import ExUnit.Assertions

  @doc """
  `{line, code, :runs | {:skipped, reason}}` for every ```elixir fence, the line being the
  fence's first line of code.
  """
  def fences(text) do
    text |> String.split("\n") |> Enum.with_index(1) |> gather([], :runs)
  end

  @doc """
  Evaluate `fences` in order, threading one binding and one environment.

  `Code.eval_string/3` hands back no environment, so imports would not survive from one fence
  to the next. A fence asserts by matching, and a raise is reported as the guide's own line.
  """
  def run(guide, fences) do
    ExUnit.CaptureIO.capture_io(fn ->
      Enum.reduce(fences, {[], Code.env_for_eval([])}, &eval(&1, &2, guide))
    end)
  end

  defp eval({line, code}, {binding, env}, guide) do
    quoted = Code.string_to_quoted!(code, file: guide, line: line)
    {_value, binding, env} = Code.eval_quoted_with_env(quoted, binding, env)
    {binding, env}
  rescue
    error ->
      flunk("""
      #{guide}:#{line} raised #{inspect(error.__struct__)}

      #{Exception.message(error)}

      #{code |> String.split("\n") |> hd()}
      """)
  end

  defp gather([], acc, _pending), do: Enum.reverse(acc)

  defp gather([{line, number} | rest], acc, pending) do
    cond do
      String.trim(line) == "```elixir" ->
        {body, tail} = Enum.split_while(rest, fn {l, _n} -> String.trim(l) != "```" end)
        code = body |> Enum.map(&elem(&1, 0)) |> Enum.join("\n")

        gather(Enum.drop(tail, 1), [{number + 1, code, pending} | acc], :runs)

      marker = marker(line) ->
        gather(rest, acc, marker)

      # A blank line between the marker and its fence is fine; anything else clears it, so a
      # marker cannot leak onto a fence further down the page.
      String.trim(line) == "" ->
        gather(rest, acc, pending)

      true ->
        gather(rest, acc, :runs)
    end
  end

  # `> **Not executed.** <reason>` — the reason is what the accounting test carries, so it has
  # to be on the same line and non-empty.
  defp marker(line) do
    case Regex.run(~r/^>\s+\*\*Not executed\.\*\*\s+(\S.*)$/, String.trim_trailing(line)) do
      [_all, reason] -> {:skipped, String.trim(reason)}
      nil -> nil
    end
  end
end
