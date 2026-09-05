defmodule Latu.Integration.ControlTest do
  # async: false, like write_test and observe_test, and for a sharper reason: the interrupt
  # tests start a deliberately slow query, and `--master local[1]` is a single task slot. Racing
  # nineteen other async tests for it starves them; the sync phase does not.
  use ExUnit.Case, async: false

  alias Latu.Catalog

  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  #
  # `setup` runs a query before anything else because **a Spark Connect session does not exist
  # on the server until it runs something**, and the four RPCs here disagree about that:
  # `Interrupt` (getOrCreateIsolatedSession) creates one as a side effect, `ReleaseSession`
  # (getIsolatedSessionIfPresent) no-ops, `GetStatus` and `CloneSession` (getIsolatedSession)
  # refuse with INVALID_HANDLE.SESSION_NOT_FOUND.
  #
  # **Never kill a task that is running a Latu execution.** An execution is reattachable, so a
  # killed client leaves the query running on the server until the detached timeout — holding
  # `local[1]`'s only task slot and starving every other test. Interrupt, then **await** the
  # task, so Latu's own release runs on the way out.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  # Long enough to still be running when the interrupt lands — an execution is registered as
  # Pending the moment ExecutePlan arrives and Spark maps Pending to :running, so that is well
  # under a second — and **bounded**, so that if every cancel in this file failed the server
  # would burn a few seconds rather than hours. Running in the sync phase means nothing is
  # competing for the task slot, so this needs far less headroom than it looks like it does.
  @slow_enough 500_000_000

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session, release: true) end)

    # Make the session real on the server. See the note above.
    Latu.count!(Latu.range(session, 1))

    %{session: session}
  end

  describe "a session the server has never seen" do
    setup do
      fresh = Latu.connect!(@url)
      on_exit(fn -> Latu.disconnect(fresh, release: true) end)

      %{fresh: fresh}
    end

    test "status/2 says so, rather than saying nothing is running", %{fresh: fresh} do
      assert {:error, error} = Latu.status(fresh)
      assert error.message =~ "the server has no session #{fresh.session_id}"
      assert error.message =~ "becomes real on the server when it first runs something"
    end

    test "clone_session/2 says the same", %{fresh: fresh} do
      assert {:error, error} = Latu.clone_session(fresh)
      assert error.message =~ "the server has no session"
    end

    test "interrupt/2 does not, because Spark's handler creates the session", %{fresh: fresh} do
      assert Latu.interrupt!(fresh) == []

      # And having created it, the session is now real enough for GetStatus.
      assert {:ok, _statuses} = Latu.status(fresh)
    end

    test "release_session/2 does not either — a missing session is a noop", %{fresh: fresh} do
      assert {:ok, _released} = Latu.release_session(fresh)
    end
  end

  describe "interrupt/2" do
    test "cancels a tagged query, and status sees it running first", %{session: session} do
      worker = worker(session, "control-test")
      task = counting(worker)
      on_exit(fn -> Latu.interrupt(session) end)

      assert %{state: :running, operation_id: id} = await_running(session)
      assert id != ""

      assert [_ | _] = interrupted = Latu.interrupt!(session, tag: "control-test")
      assert id in interrupted

      refute_finished(task)
    end

    test "by operation id cancels only that one", %{session: session} do
      worker = worker(session, "control-test-op")
      task = counting(worker)
      on_exit(fn -> Latu.interrupt(session) end)

      assert %{operation_id: id} = await_running(session)
      assert Latu.interrupt!(session, operation_id: id) == [id]

      refute_finished(task)
    end

    test "an idle session has nothing to interrupt", %{session: session} do
      assert Latu.interrupt!(session) == []
    end

    test "a tag nothing carries matches nothing", %{session: session} do
      assert Latu.interrupt!(session, tag: "nobody-uses-this") == []
    end

    test "asking for two scopes at once says so", %{session: session} do
      assert_raise ArgumentError, ~r/:tag or :operation_id, not both/, fn ->
        Latu.interrupt(session, tag: "a", operation_id: "b")
      end
    end

    test "an unknown option names itself", %{session: session} do
      assert_raise ArgumentError, fn -> Latu.interrupt(session, operation: "a") end
    end
  end

  describe "status/2" do
    # The bug the first gate found: the request's operation_status field is what asks for
    # operation statuses at all, and Latu was leaving it absent when given no filter — so the
    # server correctly answered with none, forever. This is the test that would have caught it.
    test "with no filter, reports the operations the session has run", %{session: session} do
      statuses = Latu.status!(session)

      assert [_ | _] = statuses
      assert Enum.all?(statuses, &(&1.operation_id != ""))
    end

    test "an idle session is running nothing of its own", %{session: session} do
      assert Enum.all?(Latu.status!(session), &(&1.state != :running))
    end

    test "a finished operation is reported, not forgotten", %{session: session} do
      before = MapSet.new(Latu.status!(session), & &1.operation_id)
      Latu.count!(Latu.range(session, 1))

      after_run = MapSet.new(Latu.status!(session), & &1.operation_id)

      assert MapSet.size(MapSet.difference(after_run, before)) >= 1
    end

    test "filtering by an id nobody has answers for it anyway, as :unknown", %{session: session} do
      id = "00000000-0000-4000-8000-000000000000"

      # Spark answers per *requested* id rather than per id it has, which is the useful shape:
      # asking about an operation that is gone gets you :unknown rather than silence.
      assert [%{operation_id: ^id, state: state}] = Latu.status!(session, [id])
      assert state in [:unknown, :unspecified]
    end
  end

  describe "clone_session/2" do
    test "the clone is a different session on the same channel", %{session: session} do
      clone = Latu.clone_session!(session)

      assert clone.session_id != session.session_id
      assert clone.channel == session.channel
      assert Latu.spark_version!(clone) == Latu.spark_version!(session)

      Latu.release_session!(clone)
    end

    test "a temp view in the clone is invisible to the parent", %{session: session} do
      clone = Latu.clone_session!(session)

      try do
        clone |> Latu.range(5) |> Latu.create_temp_view!("control_clone_view")

        assert Catalog.table_exists!(clone, "control_clone_view")
        refute Catalog.table_exists!(session, "control_clone_view")
      after
        Latu.release_session!(clone)
      end
    end

    test "and a temp view made before the fork — Spark says state is preserved", %{
      session: session
    } do
      session |> Latu.range(5) |> Latu.create_temp_view!("control_parent_view")
      clone = Latu.clone_session!(session)

      try do
        # Spark's docstring says a clone preserves the session's configuration and state; this
        # is what shows temp views count as state.
        assert Catalog.table_exists!(clone, "control_parent_view")
      after
        Latu.release_session!(clone)
        Catalog.drop_temp_view(session, "control_parent_view")
      end
    end

    test "a caller-named clone must be a UUID", %{session: session} do
      assert_raise ArgumentError, ~r/session_id must be a UUID/, fn ->
        Latu.clone_session(session, session_id: "scratch")
      end
    end

    # The id must be fresh on every run: Spark keeps closed session ids in `closedSessionsCache`
    # and refuses to clone into one (TARGET_SESSION_ID_ALREADY_CLOSED).
    test "and the server uses the id it was given", %{session: session} do
      id = Latu.Internal.UUID.v4()
      clone = Latu.clone_session!(session, session_id: id)

      assert clone.session_id == id

      Latu.release_session!(clone)
    end
  end

  describe "release_session/2" do
    test "ends the session but leaves the channel open", %{session: session} do
      clone = Latu.clone_session!(session)

      assert {:ok, released} = Latu.release_session(clone)
      refute is_nil(released.channel)

      # The parent is untouched, which is the whole point of releasing a clone.
      assert Latu.spark_version!(session) != nil
    end

    test "a released session cannot be used again", %{session: session} do
      clone = Latu.clone_session!(session)
      Latu.release_session!(clone)

      # A question: the server may answer for a fresh session under the same id rather than
      # refusing. Either way this pins which.
      assert {:error, %Latu.Error{}} = Latu.sql(clone, "SELECT 1")
    end
  end

  describe "disconnect/2" do
    # The default leaves the session for Spark to time out: a session can be borrowed by a
    # second channel (worker/2 below), so closing one must not end it. docs/decisions.md.
    test "closes the channel and leaves the session alone by default" do
      other = Latu.connect!(@url)
      Latu.count!(Latu.range(other, 1))

      assert {:ok, closed} = Latu.disconnect(other)
      assert is_nil(closed.channel)

      # Still on the server: a second connection under the same id can still use it.
      again = Latu.connect!(@url, session_id: other.session_id)

      try do
        assert {:ok, _statuses} = Latu.status(again)
      after
        Latu.disconnect(again)
      end
    end

    test "release: true ends the session too" do
      other = Latu.connect!(@url)
      Latu.count!(Latu.range(other, 1))

      assert {:ok, closed} = Latu.disconnect(other, release: true)
      assert is_nil(closed.channel)
    end

    test "is still idempotent" do
      other = Latu.connect!(@url)
      {:ok, closed} = Latu.disconnect(other)

      assert {:ok, ^closed} = Latu.disconnect(closed)
    end
  end

  # A second connection carrying the *same* session id: a Connect session is keyed by that id,
  # not by a channel, which is what lets one process interrupt what another is running. It also
  # keeps the query's channel away from the test's own, so nothing that happens to the task can
  # reach the session every other assertion here uses. It must not *release* on the way out —
  # the session belongs to the parent — which is what disconnect/2 does by default.
  defp worker(%{session_id: id}, tag) do
    worker = Latu.connect!(@url, session_id: id, tags: [tag])
    on_exit(fn -> Latu.disconnect(worker, release: true) end)

    worker
  end

  defp counting(session) do
    Task.async(fn ->
      try do
        session |> Latu.range(@slow_enough) |> Latu.count()
      rescue
        error -> {:raised, error}
      end
    end)
  end

  # Await rather than kill, so Latu releases the execution on the server. An interrupted count
  # comes back as an error, or raises inside the stream — either is a cancelled query; a count
  # is the one thing it must not be.
  defp refute_finished(task) do
    case Task.yield(task, 30_000) do
      {:ok, outcome} -> refute match?({:ok, _count}, outcome)
      nil -> flunk("the interrupted query neither failed nor finished within 30s")
    end
  end

  # An execution is registered the moment ExecutePlan lands, before any work starts, so this
  # normally returns on the first or second look. The budget is for a busy `local[1]` server,
  # not for the query.
  defp await_running(session, attempts \\ 60) do
    case Enum.find(Latu.status!(session), &(&1.state == :running)) do
      nil when attempts > 0 ->
        Process.sleep(50)
        await_running(session, attempts - 1)

      nil ->
        flunk("no execution reached :running within 3s")

      operation ->
        operation
    end
  end
end
