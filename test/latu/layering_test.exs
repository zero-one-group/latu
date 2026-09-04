defmodule Latu.LayeringTest do
  use ExUnit.Case, async: true

  # Three rules. The plan layer must never reach the transport — the load-bearing one, because
  # it is what lets the golden tests run with no server. gRPC belongs to Latu.Client alone. The
  # generated protos are shared by the transport, the plan layer, which builds them, and the
  # result schema guard, which reads the DataType the transport hands back.
  #
  # Read from BEAM import tables rather than source, so a module named only in a docstring does
  # not count. That also means this sees calls, not mentions: struct literals and patterns
  # compile away, so the guard is mechanically invisible here today and named anyway.

  @grpc ~r/^GRPC\b/
  @transport ~r/^Latu\.Client\b/

  @proto ~r/^Latu\.Protocol\b/
  @proto_layers ~r/^Latu\.(Client|Plan|Result\.(Schema|Literal))\b/

  @plan ~r/^Latu\.Plan\b/

  test "the plan layer never reaches the transport" do
    modules = Enum.filter(latu_modules(), &(inspect(&1) =~ @plan))
    assert modules != [], "no plan layer found — has it moved?"

    for module <- modules do
      offenders = module |> remote_calls() |> Enum.filter(&(&1 =~ @transport))

      assert offenders == [],
             "#{inspect(module)} calls #{inspect(offenders)}. Plan building is pure: take " <>
               "values in, hand a relation back, and let the layer above do the RPC."
    end
  end

  test "only the transport layer calls gRPC" do
    assert_confined(@grpc, @transport, "Route it through Latu.Client")
  end

  test "only the transport and plan layers call the generated protos" do
    assert_confined(@proto, @proto_layers, "Keep the protos below the API layer")
  end

  test "the transport layer is the one that does call them" do
    calls = remote_calls(Latu.Client)

    assert Enum.any?(calls, &(&1 =~ @grpc)), "nothing calls gRPC — has the transport moved?"
    assert Enum.any?(calls, &(&1 =~ @proto))
  end

  defp assert_confined(pattern, allowed, hint) do
    for module <- latu_modules(), not (inspect(module) =~ allowed) do
      offenders = module |> remote_calls() |> Enum.filter(&(&1 =~ pattern))

      assert offenders == [],
             "#{inspect(module)} calls #{inspect(offenders)}. #{hint}, or widen the layer."
    end
  end

  defp latu_modules do
    :latu
    |> Application.spec(:modules)
    |> Enum.reject(&(inspect(&1) =~ ~r/^Latu\.Protocol\./))
  end

  defp remote_calls(module) do
    case :beam_lib.chunks(:code.which(module), [:imports]) do
      {:ok, {_module, [imports: imports]}} ->
        imports |> Enum.map(&(&1 |> elem(0) |> inspect())) |> Enum.uniq()

      _ ->
        flunk("cannot read imports for #{inspect(module)}")
    end
  end
end
