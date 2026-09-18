defmodule Latu.SessionScopeTest do
  use ExUnit.Case, async: true

  # The 2026-09-18 third review, F2: two-input verbs compared only the session id, so two servers
  # handed the same explicit id passed the guard and the right relation was evaluated against the
  # left server. Identity is the server, the user and the id together. No server: `range/3` and
  # `union/2` are builders.

  alias Latu.Session

  @id "8c9d2f1e-0000-4000-8000-000000000001"

  defp sess(url), do: Session.from_url!(url)
  defp frame(session), do: Latu.range(session, 0, 3)

  describe "Session.identity/1" do
    test "is the server, the user and the client id" do
      s = sess("sc://h:15002/;user_id=alice;session_id=#{@id}")
      assert Session.identity(s) == {"h", 15002, "alice", @id}
    end

    test "tells two servers apart even when the id is reused" do
      left = sess("sc://h:15002/;user_id=alice;session_id=#{@id}")
      right = sess("sc://h:15003/;user_id=alice;session_id=#{@id}")
      refute Session.identity(left) == Session.identity(right)
    end

    test "is unchanged by pinning" do
      s = sess("sc://h:15002/;user_id=alice;session_id=#{@id}")
      assert Session.identity(Session.pin(s, "server-xyz")) == Session.identity(s)
    end
  end

  describe "two-input verbs compare full session identity" do
    test "reject two servers that share an explicitly reused id" do
      left = frame(sess("sc://h:15002/;user_id=alice;session_id=#{@id}"))
      right = frame(sess("sc://h:15003/;user_id=alice;session_id=#{@id}"))

      assert_raise ArgumentError, ~r/different sessions/, fn -> Latu.union(left, right) end
    end

    test "reject the same server with different ids" do
      left = frame(sess("sc://h:15002/;user_id=alice;session_id=#{@id}"))
      right = frame(sess("sc://h:15002/;user_id=alice"))

      assert_raise ArgumentError, ~r/different sessions/, fn -> Latu.union(left, right) end
    end

    test "accept a pinned and an unpinned handle of one session" do
      s = sess("sc://h:15002/;user_id=alice;session_id=#{@id}")
      left = frame(s)
      right = %{frame(s) | session: Session.pin(s, "server-xyz")}

      assert %Latu.DataFrame{} = Latu.union(left, right)
    end
  end
end
