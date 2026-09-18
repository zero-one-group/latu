defmodule Latu.Lifecycle do
  @moduledoc false
  # Counts the Gun per-stream response processes, the thing an abandoned stream used to leak
  # (docs/decisions.md, 2026-09-17). Loaded by test/test_helper.exs; shared by the result-stream
  # and listener-bus lifecycle tests.

  @response_process GRPC.Client.Adapters.Gun.StreamResponseProcess

  @doc "How many Gun response processes are alive right now."
  def response_processes do
    Enum.count(Process.list(), fn pid ->
      with {:dictionary, dict} <- Process.info(pid, :dictionary),
           {mod, _fun, _arity} <- Keyword.get(dict, :"$initial_call", nil) do
        mod == @response_process
      else
        _ -> false
      end
    end)
  end

  @doc """
  Whether the count falls back to `base` within about a second.

  A response process stops itself once its terminal message is read, a step behind the drain
  returning, so this polls rather than asserting instantly. `<=` rather than `==`: another
  module's stream closing concurrently must not fail an unrelated test.
  """
  def reaped_to?(base, tries \\ 50) do
    cond do
      response_processes() <= base -> true
      tries <= 0 -> false
      true -> Process.sleep(20) && reaped_to?(base, tries - 1)
    end
  end
end
