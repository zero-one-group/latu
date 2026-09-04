defmodule Latu.Internal.UUID do
  @moduledoc false

  @doc "A random UUID v4, lowercase and hyphenated. Spark keys session state on this string."
  @spec v4() :: String.t()
  def v4 do
    <<a::48, _::4, b::12, _::2, c::62>> = :crypto.strong_rand_bytes(16)

    <<a::48, 4::4, b::12, 2::2, c::62>>
    |> Base.encode16(case: :lower)
    |> hyphenate()
  end

  @doc "Whether a string is a lowercase hyphenated UUID."
  @spec valid?(term()) :: boolean()
  def valid?(value) when is_binary(value) do
    Regex.match?(~r/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/, value)
  end

  def valid?(_), do: false

  defp hyphenate(<<a::binary-8, b::binary-4, c::binary-4, d::binary-4, e::binary-12>>) do
    a <> "-" <> b <> "-" <> c <> "-" <> d <> "-" <> e
  end
end
