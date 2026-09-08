defmodule Latu.SqlGoldenTest do
  use ExUnit.Case, async: true

  # The guard against a generated suite's own failure mode: a case list that quietly empties, or
  # a query added without the golden that makes it mean anything. Needs no server.
  test "every case has a golden checked in" do
    assert Latu.SqlGolden.check!() == :ok
  end
end
