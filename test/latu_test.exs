defmodule LatuTest do
  use ExUnit.Case, async: true

  alias Latu.Error

  # Everything here fails before a socket is opened, so no server is needed.
  # Tests that do reach the network live in test/integration/.

  test "a malformed URL fails without touching the network" do
    assert {:error, %Error{kind: :invalid_url}} = Latu.connect("nope://h")
  end

  test "refuses a bearer token in cleartext to a remote host" do
    assert {:error, %Error{kind: :connect, message: message}} =
             Latu.connect("sc://spark.example.com/;token=s3cr3t")

    assert message =~ "cleartext"
    assert message =~ "use_ssl=true"
    refute message =~ "s3cr3t"
  end

  test "spark_version/1 on an unconnected session says so" do
    session = Latu.Session.from_url!("sc://h")

    assert {:error, %Error{kind: :connect, message: message}} = Latu.spark_version(session)
    assert message =~ "not connected"
  end

  test "connect!/1 raises" do
    assert_raise Error, ~r/must start with/, fn -> Latu.connect!("nope") end
  end

  test "options with an already-built session raise, rather than being ignored" do
    session = Latu.Session.from_url!("sc://h")

    assert_raise ArgumentError, ~r/options apply to a URL/, fn ->
      Latu.connect(session, user_id: "alice")
    end
  end
end
