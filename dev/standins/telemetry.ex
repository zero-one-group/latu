defmodule :telemetry do
  @moduledoc false
  # Stand-in for the `:telemetry` application, which the offline container cannot fetch.
  #
  # A *working* mini implementation rather than a no-op, so `test/latu/telemetry_test.exs`
  # cannot pass here without an event being emitted. Handlers live in the process dictionary, so
  # this dispatches only to handlers attached by the same process; the real `:telemetry` keeps
  # them in ETS and dispatches to every one, whatever process emitted. So this stand-in is *more
  # forgiving* than the real thing: **do not write a test that assumes only its own events
  # arrive.** `span/3` adds `telemetry_span_context` to both events, as the real one does.

  def attach(id, event, fun, config), do: attach_many(id, [event], fun, config)

  def attach_many(id, events, fun, config) do
    Enum.each(events, fn event ->
      Process.put({__MODULE__, event}, [{id, fun, config} | handlers(event)])
    end)
  end

  def detach(id) do
    for {{__MODULE__, event}, _} <- Process.get(),
        do: Process.put({__MODULE__, event}, Enum.reject(handlers(event), &(elem(&1, 0) == id)))

    :ok
  end

  def execute(event, measurements, metadata) do
    Enum.each(handlers(event), fn {_id, fun, config} ->
      fun.(event, measurements, metadata, config)
    end)
  end

  def span(prefix, metadata, fun) do
    context = Map.put(metadata, :telemetry_span_context, make_ref())
    execute(prefix ++ [:start], %{system_time: 0}, context)
    {result, stop} = fun.()
    execute(prefix ++ [:stop], %{duration: 0}, Map.merge(context, stop))

    result
  end

  def monotonic_time, do: 0

  defp handlers(event), do: Process.get({__MODULE__, event}, [])
end
