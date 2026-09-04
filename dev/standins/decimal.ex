defmodule Decimal do
  @moduledoc """
  Stand-in for the `:decimal` package, for `dev/check_offline.exs` only.

  Not a Latu module — a stand-in for a *dependency* rather than a layer.
  `Latu.Functions.Registry` evaluates `Decimal.new(0)` at compile time, because Spark's
  `make_interval` sends a decimal zero for its seconds and an integer zero is a different
  literal on the wire.

  `parse/1`, `new/1` over a binary and `equal?/2` are here for `Latu.Result.Literal`, which
  reads a decimal back off the wire as the string Spark sends. They cover sign, digits and one
  decimal point, which is every form Spark produces — not the package's grammar (no exponents,
  no `Inf`, no `NaN`). The real package is the authority under `mix`.
  """

  defstruct [:coef, sign: 1, exp: 0]

  def new(coef) when is_integer(coef), do: %__MODULE__{coef: abs(coef), sign: sign(coef)}

  def new(string) when is_binary(string) do
    case parse(string) do
      {decimal, ""} -> decimal
      _ -> raise ArgumentError, "cannot parse #{inspect(string)} as a decimal"
    end
  end

  def parse(string) when is_binary(string) do
    {sign, digits} = split_sign(string)

    case String.split(digits, ".") do
      [whole] -> integral(sign, whole, "")
      [whole, fraction] -> integral(sign, whole, fraction)
      _ -> :error
    end
  end

  def equal?(%__MODULE__{} = one, %__MODULE__{} = other) do
    scale = min(one.exp, other.exp)
    scaled(one, scale) == scaled(other, scale)
  end

  defp scaled(%__MODULE__{} = decimal, scale) do
    decimal.sign * decimal.coef * 10 ** (decimal.exp - scale)
  end

  defp split_sign("-" <> rest), do: {-1, rest}
  defp split_sign("+" <> rest), do: {1, rest}
  defp split_sign(rest), do: {1, rest}

  defp integral(sign, whole, fraction) do
    case Integer.parse(whole <> fraction) do
      {coef, ""} -> {%__MODULE__{coef: coef, sign: sign, exp: -String.length(fraction)}, ""}
      _ -> :error
    end
  end

  defp sign(coef) when coef < 0, do: -1
  defp sign(_coef), do: 1
end
