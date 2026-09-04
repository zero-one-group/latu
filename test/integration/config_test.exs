defmodule Latu.Integration.ConfigTest do
  use ExUnit.Case, async: true

  alias Latu.Error
  alias Latu.Functions, as: F

  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  #
  # The three read arms differ in a way no proto comment says (SparkConnectConfigHandler,
  # RuntimeConfig, SQLConf):
  #
  #   * GetOption       — set value, else Spark's own default, else nil.        conf/2
  #   * Get             — set value, else Spark's own default, else SQL_CONF_NOT_FOUND. fetch_conf/2
  #   * GetWithDefault  — set value, else *your* default; Spark's own default is ignored. conf/3
  #
  # Also pinned below: GetAll returns only what the session has set; with a prefix the server
  # strips it off the keys and Latu puts it back; `is_modifiable` false does **not** mean
  # `set_conf/3` will fail — it is false for every key Spark does not define.
  #
  # Each test connects its own session, and a Connect session gets a cloned SQLConf, so setting
  # a conf here cannot reach another test. That is what makes this file async.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  # Registered, so Spark has a default for it, and nothing in docker-compose.yml sets it — which
  # is exactly the state that tells the three read arms apart. A bytes conf, so a default of
  # "12345" type-checks and "not a number" does not.
  @unset "spark.sql.autoBroadcastJoinThreshold"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session) end)
    %{session: session}
  end

  describe "conf/2" do
    test "reads what the server was started with", %{session: session} do
      assert Latu.conf!(session, "spark.sql.session.timeZone") == "UTC"
      assert Latu.conf!(session, "spark.sql.shuffle.partitions") == "4"
    end

    test "falls back to Spark's own default for a conf nobody set", %{session: session} do
      value = Latu.conf!(session, @unset)

      assert is_binary(value) and value != ""
      # And it is a fallback rather than a setting: GetAll does not carry it.
      refute Map.has_key?(Latu.confs!(session), @unset)
    end

    test "a key Spark does not define is nil, as Map.get/2 would say", %{session: session} do
      assert Latu.conf!(session, "latu.test.nope") == nil
    end
  end

  describe "fetch_conf/2" do
    test "is conf/2 where Spark has an answer", %{session: session} do
      assert Latu.fetch_conf!(session, @unset) == Latu.conf!(session, @unset)
    end

    test "a key Spark does not define is an error, not an empty answer", %{session: session} do
      assert {:error, %Error{} = error} = Latu.fetch_conf(session, "latu.test.nope")
      assert error.error_class == "SQL_CONF_NOT_FOUND"
    end
  end

  describe "conf/3" do
    test "overrides Spark's own default rather than following it", %{session: session} do
      assert Latu.conf!(session, @unset) != "12345"
      assert Latu.conf!(session, @unset, "12345") == "12345"
    end

    test "a nil default is not conf/2", %{session: session} do
      # The sharpest measured distinction in this file: GetWithDefault ignores Spark's default,
      # GetOption returns it. Both are reading the same unset, registered conf.
      assert Latu.conf!(session, @unset, nil) == nil
      assert is_binary(Latu.conf!(session, @unset))
    end

    test "the default is type-checked against the conf it belongs to", %{session: session} do
      assert {:error, %Error{}} = Latu.conf(session, @unset, "not a number")
    end

    test "an unknown key takes the default, unchecked", %{session: session} do
      assert Latu.conf!(session, "latu.test.nope", "anything") == "anything"
    end
  end

  describe "confs/2" do
    test "is what the session has set, not the registry", %{session: session} do
      confs = Latu.confs!(session)

      assert confs["spark.sql.session.timeZone"] == "UTC"
      # A registry dump would be hundreds; this is the startup confs and nothing else.
      assert map_size(confs) < 100
    end

    test "a prefix filters on the server and the keys come back whole", %{session: session} do
      confs = Latu.confs!(session, prefix: "spark.sql.")

      assert Map.has_key?(confs, "spark.sql.session.timeZone")
      assert Enum.all?(Map.keys(confs), &String.starts_with?(&1, "spark.sql."))
      # The whole-key assertion above passes on a doubled prefix too, so say that separately.
      refute Enum.any?(Map.keys(confs), &String.starts_with?(&1, "spark.sql.spark.sql."))
    end
  end

  describe "set_conf/3" do
    test "coerces an integer, boolean or atom to Spark's string form", %{session: session} do
      :ok = Latu.set_conf!(session, "spark.sql.shuffle.partitions", 8)
      :ok = Latu.set_conf!(session, "latu.test.flag", true)
      :ok = Latu.set_conf!(session, "latu.test.mode", :fast)

      assert Latu.conf!(session, "spark.sql.shuffle.partitions") == "8"
      assert Latu.conf!(session, "latu.test.flag") == "true"
      assert Latu.conf!(session, "latu.test.mode") == "fast"
    end

    test "a value Spark cannot parse for that conf is refused", %{session: session} do
      assert {:error, %Error{}} = Latu.set_conf(session, "spark.sql.shuffle.partitions", "eight")
    end

    test "a static conf is refused, and is_modifiable/2 says so first", %{session: session} do
      key = "spark.sql.warehouse.dir"

      refute Latu.is_modifiable!(session, key)
      assert {:error, %Error{} = error} = Latu.set_conf(session, key, "/tmp/latu-nope")
      assert error.error_class == "CANNOT_MODIFY_STATIC_CONFIG"
    end

    test "a key Spark never heard of is stored anyway, though not modifiable", %{
      session: session
    } do
      refute Latu.is_modifiable!(session, "latu.test.unknown")
      assert :ok = Latu.set_conf(session, "latu.test.unknown", "1")
      assert Latu.conf!(session, "latu.test.unknown") == "1"
    end

    test "the progress report interval is settable", %{
      session: session
    } do
      key = "spark.connect.progress.reportInterval"

      assert Latu.is_modifiable!(session, key)
      :ok = Latu.set_conf!(session, key, "50ms")
      assert Latu.conf!(session, key) == "50ms"
    end
  end

  describe "set_confs/2" do
    test "sets several in one round trip", %{session: session} do
      :ok = Latu.set_confs!(session, %{"latu.test.a" => 1, "latu.test.b" => "two"})

      assert Latu.conf!(session, "latu.test.a") == "1"
      assert Latu.conf!(session, "latu.test.b") == "two"
    end

    test "a rejected conf fails the call, and the ones after it are not set", %{session: session} do
      pairs = [{"spark.sql.warehouse.dir", "/tmp/latu-nope"}, {"latu.test.after", "yes"}]

      assert {:error, %Error{}} = Latu.set_confs(session, pairs)
      assert Latu.conf!(session, "latu.test.after") == nil
    end
  end

  describe "unset_conf/2" do
    test "puts a conf back to Spark's default", %{session: session} do
      :ok = Latu.set_conf!(session, @unset, "12345")
      assert Latu.conf!(session, @unset) == "12345"

      :ok = Latu.unset_conf!(session, @unset)
      assert Latu.conf!(session, @unset) != "12345"
      refute Map.has_key?(Latu.confs!(session), @unset)
    end

    test "a conf that was never set is not an error", %{session: session} do
      assert :ok = Latu.unset_conf(session, "latu.test.never")
    end

    test "a static conf is refused here too", %{session: session} do
      assert {:error, %Error{}} = Latu.unset_conf(session, "spark.sql.warehouse.dir")
    end
  end

  describe "the session's own defaults" do
    test "range's num_partitions reaches the server", %{session: session} do
      count =
        session
        |> Latu.range(0, 12, 1, num_partitions: 3)
        |> Latu.select(p: F.spark_partition_id())
        |> Latu.distinct()
        |> Latu.count!()

      assert count == 3
    end
  end
end
