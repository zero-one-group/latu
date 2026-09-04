defmodule Latu.ClientTest do
  use ExUnit.Case, async: true

  alias Latu.Client
  alias Latu.Error
  alias Latu.Plan
  alias Latu.Session

  # Everything that reaches the network lives in test/integration/.

  test "execute/2 on an unconnected session says so, rather than crashing" do
    assert {:error, %Error{kind: :connect, message: message}} =
             Client.execute(Session.from_url!("sc://h"), plan())

    assert message =~ "not connected"
  end

  defp plan, do: Plan.new(Plan.range(0, 5, 1))
end
