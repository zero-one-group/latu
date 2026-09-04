defmodule Latu.Integration.ConnectionTest do
  use ExUnit.Case, async: true

  alias Latu.Error
  alias Latu.Session

  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  # Run with: mix test --include integration
  #
  # capture_log keeps elixir-grpc's connection chatter out of a passing run, while still
  # printing it for any test that fails, which is exactly when you want it.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  test "opens and closes a channel" do
    assert {:ok, session} = Latu.connect(@url)
    assert session.channel
    assert {:ok, session} = Latu.disconnect(session)
    refute session.channel
  end

  test "disconnect is idempotent" do
    session = Latu.connect!(@url)
    assert {:ok, session} = Latu.disconnect(session)
    assert {:ok, ^session} = Latu.disconnect(session)
  end

  test "accepts an already-parsed session" do
    session = Session.from_url!(@url)
    assert {:ok, connected} = Latu.connect(session)
    assert connected.session_id == session.session_id
    Latu.disconnect(connected)
  end

  test "refuses to connect a session that already has a channel" do
    session = Latu.connect!(@url)
    assert {:error, %Error{kind: :connect, message: message}} = Latu.connect(session)
    assert message =~ "already connected"
    Latu.disconnect(session)
  end

  test "reads SPARK_REMOTE when given no URL" do
    # Set here rather than assumed: a checkout that followed CONTRIBUTING.md has no SPARK_REMOTE,
    # and neither does CI. Nothing else in the suite calls `connect/0`, so the global is safe to
    # touch under `async: true`; restored afterwards all the same.
    previous = System.get_env("SPARK_REMOTE")
    System.put_env("SPARK_REMOTE", @url)

    on_exit(fn ->
      if previous do
        System.put_env("SPARK_REMOTE", previous)
      else
        System.delete_env("SPARK_REMOTE")
      end
    end)

    assert {:ok, session} = Latu.connect()
    Latu.disconnect(session)
  end

  test "allows a cleartext token to loopback, where there is nothing to intercept" do
    # Policy must not reject this; reaching the network and failing there is fine.
    result = Latu.connect("sc://localhost:1/;token=s3cr3t", connect_timeout: 500)
    refute match?({:error, %Error{message: "refusing" <> _}}, result)
  end

  test "spark_version/1 reports the server's version" do
    session = Latu.connect!(@url)
    assert {:ok, version} = Latu.spark_version(session)
    assert version =~ ~r/^4\.2\./, "expected the 4.2 image, got #{version}"
    Latu.disconnect(session)
  end

  test "spark_version/1 pins nothing the caller did not ask for" do
    session = Latu.connect!(@url)
    assert {:ok, _} = Latu.spark_version(session)
    # The facade returns the version only; pinning is opt-in via Latu.Client.analyze/2.
    assert session.server_session_id == nil
    Latu.disconnect(session)
  end

  test "an unreachable server fails at connect, not later" do
    assert {:error, %Error{kind: :connect, message: message}} =
             Latu.connect("sc://localhost:1", connect_timeout: 500)

    assert message =~ "cannot reach localhost:1"
  end
end
