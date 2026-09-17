defmodule Latu.Integration.StreamLifecycleTest do
  use ExUnit.Case, async: false

  # Finding 2 of the 2026-09-17 review: an early-stopped or failed result stream must not leave a
  # Gun response process holding its buffer. `Latu.Client.responses/2`'s after_fun releases the
  # execution and drains the abandoned stream to EOF, which reaps the process;
  # `dev/probe_release_drain.exs` is where the bound on that drain was measured. No offline test
  # can open a real transport stream, so the reap is asserted here against a live server.
  #
  # `async: false` and a per-test baseline, because the process count is global. The result server
  # (:15002, the 2m sender) so one ExecutePlan segment carries the whole result and `Enum.take/1`
  # genuinely abandons a stream with data still buffered.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session, release: true) end)
    %{session: session}
  end

  defp response_processes do
    Enum.count(Process.list(), fn pid ->
      with {:dictionary, dict} <- Process.info(pid, :dictionary),
           {mod, _fun, _arity} <- Keyword.get(dict, :"$initial_call", nil) do
        mod == GRPC.Client.Adapters.Gun.StreamResponseProcess
      else
        _ -> false
      end
    end)
  end

  # The process stops itself once its terminal message is read, which is a step behind the drain
  # returning, so poll rather than assert instantly.
  defp reaped_to?(base, tries \\ 50) do
    cond do
      response_processes() <= base -> true
      tries <= 0 -> false
      true -> Process.sleep(20) && reaped_to?(base, tries - 1)
    end
  end

  test "an early-halted stream reaps its response process", %{session: session} do
    base = response_processes()

    session |> Latu.range(500_000) |> Latu.stream() |> Enum.take(1)

    assert reaped_to?(base)
  end

  test "a raising consumer reaps its response process", %{session: session} do
    base = response_processes()

    assert_raise RuntimeError, fn ->
      session |> Latu.range(500_000) |> Latu.stream() |> Enum.each(fn _ -> raise "stop" end)
    end

    assert reaped_to?(base)
  end
end
