defmodule Latu.ConfigTest do
  use ExUnit.Case, async: true

  alias Latu.Error
  alias Latu.Session

  # What the config family refuses before it sends anything, and what it says with no channel.
  # What Spark does with a well-formed request is test/integration/config_test.exs.

  setup do
    %{session: Session.from_url!("sc://localhost:15002")}
  end

  describe "refusals, before any round trip" do
    test "a config key is a string, whichever call takes it", %{session: session} do
      calls = [
        {:conf, [session, :"spark.sql.ansi.enabled"]},
        {:conf, [session, :x, "1"]},
        {:conf, [session, :x, nil]},
        {:fetch_conf, [session, :x]},
        {:set_conf, [session, :x, "1"]},
        {:unset_conf, [session, :x]},
        {:is_modifiable, [session, :x]},
        {:confs, [session, [prefix: :spark]]}
      ]

      for {fun, args} <- calls do
        assert_raise ArgumentError, ~r/^a config key is a string, not :/, fn ->
          apply(Latu, fun, args)
        end
      end
    end

    test "nil is not a value, and the refusal names the way to remove one", %{session: session} do
      assert_raise ArgumentError, ~r|cannot be set to nil — unset_conf/2|, fn ->
        apply(Latu, :set_conf, [session, "a.b", nil])
      end
    end

    test "a value refusal names the key it belongs to", %{session: session} do
      assert_raise ArgumentError, ~r/^a\.b is a string, number, boolean or atom, not \[1\]/, fn ->
        apply(Latu, :set_conf, [session, "a.b", [1]])
      end
    end

    test "a mistyped option is refused, not ignored", %{session: session} do
      assert_raise ArgumentError, ~r/prefixx/, fn ->
        apply(Latu, :confs, [session, [prefixx: "spark."]])
      end
    end
  end

  describe "with no channel" do
    test "every call says so rather than crashing", %{session: session} do
      calls = [
        {:conf, [session, "a.b"]},
        {:conf, [session, "a.b", "x"]},
        {:conf, [session, "a.b", nil]},
        {:fetch_conf, [session, "a.b"]},
        {:confs, [session]},
        {:confs, [session, [prefix: "a."]]},
        {:set_conf, [session, "a.b", 1]},
        {:set_confs, [session, %{"a.b" => 1}]},
        {:unset_conf, [session, "a.b"]},
        {:is_modifiable, [session, "a.b"]}
      ]

      for {fun, args} <- calls do
        assert {:error, %Error{kind: :connect, message: message}} = apply(Latu, fun, args)
        assert message =~ "not connected"
      end
    end

    test "the twins raise it", %{session: session} do
      assert_raise Error, ~r/not connected/, fn -> Latu.conf!(session, "a.b") end
      assert_raise Error, ~r/not connected/, fn -> Latu.conf!(session, "a.b", "x") end
      assert_raise Error, ~r/not connected/, fn -> Latu.fetch_conf!(session, "a.b") end
      assert_raise Error, ~r/not connected/, fn -> Latu.confs!(session) end
      assert_raise Error, ~r/not connected/, fn -> Latu.set_conf!(session, "a.b", 1) end
      assert_raise Error, ~r/not connected/, fn -> Latu.set_confs!(session, []) end
      assert_raise Error, ~r/not connected/, fn -> Latu.unset_conf!(session, "a.b") end
      assert_raise Error, ~r/not connected/, fn -> Latu.is_modifiable!(session, "a.b") end
    end
  end
end
