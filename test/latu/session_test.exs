defmodule Latu.SessionTest do
  use ExUnit.Case, async: true

  alias Latu.Error
  alias Latu.Internal.UUID
  alias Latu.Session

  doctest Latu.Session

  @uuid "8c9d2f1e-0000-4000-8000-000000000001"

  describe "from_url/2 — host and port" do
    test "defaults the port to Spark Connect's 15002" do
      assert {:ok, %Session{host: "localhost", port: 15002}} = Session.from_url("sc://localhost")
    end

    test "takes an explicit port" do
      assert {:ok, %Session{host: "spark.internal", port: 443}} =
               Session.from_url("sc://spark.internal:443")
    end

    test "tolerates a trailing slash with no parameters" do
      assert {:ok, %Session{host: "localhost", port: 15002}} = Session.from_url("sc://localhost/")
    end

    test "brackets an IPv6 host" do
      assert {:ok, %Session{host: "::1", port: 15002}} = Session.from_url("sc://[::1]")
      assert {:ok, %Session{host: "::1", port: 15002}} = Session.from_url("sc://[::1]:15002")
    end
  end

  describe "from_url/2 — parameters" do
    test "parses every recognised parameter" do
      url =
        "sc://spark.internal:443/;use_ssl=true;token=s3cr3t;user_id=alice" <>
          ";user_agent=custom/1.0;session_id=#{@uuid}"

      assert {:ok, session} = Session.from_url(url)
      assert session.use_ssl
      assert session.token == "s3cr3t"
      assert session.user_id == "alice"
      assert session.client_type == "custom/1.0"
      assert session.session_id == @uuid
    end

    test "percent-decodes values, which is how a '=' reaches a token" do
      assert {:ok, %Session{token: "abc=="}} = Session.from_url("sc://h/;token=abc%3D%3D")
    end

    test "use_ssl is case-insensitive" do
      assert {:ok, %Session{use_ssl: true}} = Session.from_url("sc://h/;use_ssl=TRUE")
    end

    test "session_id is case-insensitive, as UUIDs are" do
      upper = String.upcase(@uuid)
      assert {:ok, %Session{session_id: ^upper}} = Session.from_url("sc://h/;session_id=#{upper}")
    end

    test "unrecognised params become gRPC metadata headers" do
      url = "sc://h/;x-databricks-cluster-id=0704-abc;token=t"
      assert {:ok, session} = Session.from_url(url)
      assert session.headers == [{"x-databricks-cluster-id", "0704-abc"}]
      assert session.token == "t"
    end

    test "use_ssl defaults to false" do
      assert {:ok, %Session{use_ssl: false}} = Session.from_url("sc://h")
    end

    test "a bare '=' in a value is rejected; percent-encode it" do
      assert {:error, %Error{kind: :invalid_url}} = Session.from_url("sc://h/;token=abc==")
    end
  end

  describe "from_url/2 — rejections" do
    test "wrong scheme" do
      assert {:error, %Error{kind: :invalid_url, message: message}} =
               Session.from_url("grpc://localhost:15002")

      assert message =~ ~s(must start with "sc://")
    end

    test "missing host" do
      assert {:error, %Error{kind: :invalid_url}} = Session.from_url("sc://")
      assert {:error, %Error{kind: :invalid_url}} = Session.from_url("sc://:15002")
    end

    test "unparseable or out-of-range port" do
      for url <- ["sc://h:abc", "sc://h:0", "sc://h:70000", "sc://h:15002x"] do
        assert {:error, %Error{kind: :invalid_url}} = Session.from_url(url), url
      end
    end

    test "a path is rejected; params go after a semicolon" do
      for url <- ["sc://h/user_id=alice", "sc://h//", "sc://h:15002/path/;user_id=alice"] do
        assert {:error, %Error{kind: :invalid_url, message: message}} = Session.from_url(url), url
        assert message =~ "path must be empty"
      end
    end

    test "a query string is rejected rather than silently ignored" do
      assert {:error, %Error{kind: :invalid_url, message: message}} =
               Session.from_url("sc://h?user_id=x")

      assert message =~ ~s(use ";key=value")
    end

    test "an empty token, which would send a bare Bearer header" do
      assert {:error, %Error{kind: :invalid_url, message: message}} =
               Session.from_url("sc://h/;token=")

      assert message =~ "token must not be empty"
    end

    test "an empty bracketed host" do
      assert {:error, %Error{kind: :invalid_url}} = Session.from_url("sc://[]:15002")
    end

    test "a malformed parameter" do
      for url <- ["sc://h/;token", "sc://h/;=value"] do
        assert {:error, %Error{kind: :invalid_url, message: message}} = Session.from_url(url), url
        assert message =~ "parameter must be key=value"
      end
    end

    test "a use_ssl typo, which would silently disable TLS in PySpark" do
      assert {:error, %Error{kind: :invalid_url, message: message}} =
               Session.from_url("sc://h/;use_ssl=ture")

      assert message =~ ~s(use_ssl must be "true" or "false")
    end

    test "session_id that is not a UUID" do
      assert {:error, %Error{kind: :invalid_url, message: message}} =
               Session.from_url("sc://h/;session_id=nope")

      assert message =~ "session_id must be a UUID"
    end

    test "too many colons without IPv6 brackets" do
      assert {:error, %Error{kind: :invalid_url}} = Session.from_url("sc://::1:15002")
    end
  end

  describe "from_url/2 — defaults and overrides" do
    test "generates a UUID session_id when the URL does not carry one" do
      assert {:ok, %Session{session_id: id}} = Session.from_url("sc://h")
      assert UUID.valid?(id)
    end

    test "generates a different session_id each time" do
      {:ok, a} = Session.from_url("sc://h")
      {:ok, b} = Session.from_url("sc://h")
      refute a.session_id == b.session_id
    end

    test "client_type is populated and within Spark's 2048-char limit" do
      {:ok, session} = Session.from_url("sc://h")
      assert session.client_type != ""
      assert byte_size(session.client_type) <= 2048
    end

    test "options override the URL" do
      url = "sc://h/;user_id=from_url;user_agent=from_url;session_id=#{@uuid}"
      other = "11111111-2222-4333-8444-555555555555"

      assert {:ok, session} =
               Session.from_url(url,
                 user_id: "from_opts",
                 client_type: "opts/1.0",
                 session_id: other,
                 timeout: 5_000
               )

      assert session.user_id == "from_opts"
      assert session.client_type == "opts/1.0"
      assert session.session_id == other
      assert session.timeout == 5_000
    end

    test "timeouts have defaults and are overridable" do
      assert {:ok, %Session{timeout: 60_000, connect_timeout: 10_000}} =
               Session.from_url("sc://h")

      assert {:ok, %Session{connect_timeout: 500}} =
               Session.from_url("sc://h", connect_timeout: 500)
    end

    test "user_name is empty unless asked for, matching PySpark's UserContext" do
      assert {:ok, %Session{user_name: ""}} = Session.from_url("sc://h/;user_id=alice")
      assert {:ok, %Session{user_name: "Alice"}} = Session.from_url("sc://h", user_name: "Alice")
    end

    test "tags can be seeded through options" do
      assert {:ok, %Session{tags: ["etl"]}} = Session.from_url("sc://h", tags: ["etl"])
    end

    test "an unknown option raises rather than being ignored" do
      assert_raise ArgumentError, ~r/unknown keys \[:user_idd\]/, fn ->
        Session.from_url("sc://h", user_idd: "typo")
      end
    end
  end

  describe "from_url!/2" do
    test "returns the session" do
      assert %Session{host: "h"} = Session.from_url!("sc://h")
    end

    test "raises on a bad URL" do
      assert_raise Error, ~r/must start with/, fn -> Session.from_url!("nope") end
    end
  end

  describe "confirm/3" do
    setup do
      %{session: Session.from_url!("sc://h", session_id: @uuid)}
    end

    test "latches the server's id on first sight", %{session: session} do
      assert {:ok, confirmed} = Session.confirm(session, @uuid, "srv-1")
      assert confirmed.server_session_id == "srv-1"
    end

    test "accepts the same server id again", %{session: session} do
      {:ok, pinned} = Session.confirm(session, @uuid, "srv-1")
      assert {:ok, ^pinned} = Session.confirm(pinned, @uuid, "srv-1")
    end

    test "rejects an answer for a different session", %{session: session} do
      other = "11111111-2222-4333-8444-555555555555"

      assert {:error, %Error{kind: :session, message: message}} =
               Session.confirm(session, other, "srv-1")

      assert message =~ other
      assert message =~ @uuid
    end

    test "detects a server restart once pinned", %{session: session} do
      {:ok, pinned} = Session.confirm(session, @uuid, "srv-1")

      assert {:error, %Error{kind: :session, message: message}} =
               Session.confirm(pinned, @uuid, "srv-2")

      assert message =~ "restarted"
    end

    test "an unpinned session does not check across calls", %{session: session} do
      assert {:ok, _} = Session.confirm(session, @uuid, "srv-1")
      assert {:ok, _} = Session.confirm(session, @uuid, "srv-2")
    end

    test "tolerates a server that sends no id", %{session: session} do
      assert {:ok, confirmed} = Session.confirm(session, @uuid, "")
      assert confirmed.server_session_id == nil
      assert {:ok, confirmed} = Session.confirm(session, @uuid, nil)
      assert confirmed.server_session_id == nil
    end

    # FetchErrorDetails's handler is the one in the service that never echoes the session, so
    # every one of its responses carries an empty pair. An absent id is no information — the
    # same rule this already applied to the server-side id.
    test "an absent session id is no information, not a mismatch", %{session: session} do
      assert {:ok, ^session} = Session.confirm(session, "", nil)
      assert {:ok, ^session} = Session.confirm(session, nil, nil)
    end

    test "a different session id is still a mismatch", %{session: session} do
      assert {:error, error} = Session.confirm(session, "someone-elses", nil)
      assert error.message =~ "expected #{session.session_id}"
    end

    test "a missing id is no information, not a restart", %{session: session} do
      {:ok, pinned} = Session.confirm(session, @uuid, "srv-1")

      assert {:ok, ^pinned} = Session.confirm(pinned, @uuid, nil)
      assert {:ok, ^pinned} = Session.confirm(pinned, @uuid, "")
    end
  end

  describe "tags" do
    setup do
      %{session: Session.from_url!("sc://h")}
    end

    test "a session starts with none", %{session: session} do
      assert session.tags == []
    end

    test "adding keeps the order they went in, and adding twice does nothing", %{
      session: session
    } do
      tagged = session |> Session.add_tag("etl") |> Session.add_tag("nightly")

      assert tagged.tags == ["etl", "nightly"]
      assert Session.add_tag(tagged, "etl").tags == ["etl", "nightly"]
    end

    test "an atom is a tag, because a keyword list is how you would write one", %{
      session: session
    } do
      assert Session.add_tag(session, :exploration).tags == ["exploration"]
    end

    test "removing one that is not there is not an error", %{session: session} do
      tagged = Session.add_tag(session, "etl")

      assert Session.remove_tag(tagged, "etl").tags == []
      assert Session.remove_tag(tagged, "nope").tags == ["etl"]
    end

    test "a comma is refused, and the message says why", %{session: session} do
      assert_raise ArgumentError, ~r/comma.*Spark joins tags with one/, fn ->
        Session.add_tag(session, "a,b")
      end
    end

    test "an empty tag is refused", %{session: session} do
      assert_raise ArgumentError, ~r/cannot be empty/, fn -> Session.add_tag(session, "") end
    end

    test "anything else says what a tag is", %{session: session} do
      assert_raise ArgumentError, ~r/a tag is a string or an atom/, fn ->
        Session.add_tag(session, 42)
      end
    end

    test "tags given to from_url/2 go through the same rules" do
      assert {:ok, session} = Session.from_url("sc://h", tags: [:etl, "etl", "nightly"])
      assert session.tags == ["etl", "nightly"]

      assert_raise ArgumentError, ~r/comma/, fn ->
        Session.from_url("sc://h", tags: ["a,b"])
      end
    end
  end

  describe "the tuning knobs" do
    test "default to PySpark's values" do
      session = Session.from_url!("sc://h")

      assert session.window_size == 134_217_728
      assert session.keepalive == 60_000
      assert session.keepalive_tolerance == 2
      assert session.retry == %Latu.Retry{}
    end

    test "are connect/2 options, so nothing needs a struct poke" do
      session = Session.from_url!("sc://h", window_size: 1024, keepalive_tolerance: 5)

      assert session.window_size == 1024
      assert session.keepalive_tolerance == 5
    end

    test "retry takes a keyword list or a struct, and validates either" do
      assert Session.from_url!("sc://h", retry: [max_retries: 2]).retry.max_retries == 2

      struct = %Latu.Retry{max_retries: 3}
      assert Session.from_url!("sc://h", retry: struct).retry == struct

      assert_raise ArgumentError, ~r/:max_retries is a non-negative integer/, fn ->
        apply(Session, :from_url!, ["sc://h", [retry: [max_retries: -1]]])
      end
    end

    test "a mistyped knob is still refused" do
      assert_raise ArgumentError, ~r/windo_size/, fn ->
        apply(Session, :from_url!, ["sc://h", [windo_size: 1]])
      end
    end
  end

  describe "inspect/1" do
    test "never leaks a token or a header value" do
      session = Session.from_url!("sc://h/;token=s3cr3t;x-auth=hunter2")
      output = inspect(session)

      refute output =~ "s3cr3t"
      refute output =~ "hunter2"
      assert output =~ "x-auth"
    end
  end
end
