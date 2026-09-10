defmodule JSON do
  @moduledoc false
  # Stand-in for Elixir's `JSON`, in the standard library since 1.18 and absent from the offline
  # container's 1.14. Enough for `Latu.StreamingQuery.decode_progress/1`: objects, arrays,
  # strings without escapes, integers, floats, the three literals. Anything else raises, which
  # is the right answer for a stand-in. `decode/1` and `encode!/1` exist so the callers in
  # `lib/` compile; neither is exercised here.

  def decode(json) when is_binary(json) do
    {:ok, decode!(json)}
  rescue
    error -> {:error, error}
  end

  def encode!(_term), do: "{}"

  def decode!(json) when is_binary(json) do
    {term, rest} = value(String.trim_leading(json))

    if String.trim(rest) == "" do
      term
    else
      raise ArgumentError, "trailing input: #{inspect(rest)}"
    end
  end

  defp value("{" <> rest), do: object(String.trim_leading(rest), %{})
  defp value("[" <> rest), do: array(String.trim_leading(rest), [])
  defp value("\"" <> rest), do: string(rest, "")
  defp value("true" <> rest), do: {true, rest}
  defp value("false" <> rest), do: {false, rest}
  defp value("null" <> rest), do: {nil, rest}

  defp value(rest) do
    [digits | _groups] = Regex.run(~r/^-?\d+(\.\d+)?([eE][-+]?\d+)?/, rest)
    size = byte_size(digits)
    <<_::binary-size(size), tail::binary>> = rest

    if String.contains?(digits, [".", "e", "E"]) do
      {String.to_float(digits), tail}
    else
      {String.to_integer(digits), tail}
    end
  end

  defp object("}" <> rest, acc), do: {acc, rest}

  defp object("\"" <> rest, acc) do
    {key, rest} = string(rest, "")
    ":" <> rest = String.trim_leading(rest)
    {val, rest} = value(String.trim_leading(rest))

    case String.trim_leading(rest) do
      "," <> rest -> object(String.trim_leading(rest), Map.put(acc, key, val))
      "}" <> rest -> {Map.put(acc, key, val), rest}
    end
  end

  defp array("]" <> rest, acc), do: {Enum.reverse(acc), rest}

  defp array(rest, acc) do
    {val, rest} = value(rest)

    case String.trim_leading(rest) do
      "," <> rest -> array(String.trim_leading(rest), [val | acc])
      "]" <> rest -> {Enum.reverse([val | acc]), rest}
    end
  end

  defp string("\"" <> rest, acc), do: {acc, rest}
  defp string(<<char::utf8, rest::binary>>, acc), do: string(rest, acc <> <<char::utf8>>)
end
