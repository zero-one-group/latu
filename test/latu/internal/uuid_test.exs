defmodule Latu.Internal.UUIDTest do
  use ExUnit.Case, async: true

  alias Latu.Internal.UUID

  # Bit-twiddling that is easy to get silently wrong, and Spark validates the format.
  test "v4/0 is a well-formed UUID v4" do
    for _ <- 1..200 do
      uuid = UUID.v4()
      assert UUID.valid?(uuid), uuid
      assert String.at(uuid, 14) == "4", "version nibble: #{uuid}"
      assert String.at(uuid, 19) in ["8", "9", "a", "b"], "variant nibble: #{uuid}"
    end
  end
end
