defmodule Latu.Progress do
  @moduledoc """
  What the server says it is doing, while it is still doing it.

  Handed to the `:progress` function an action was given, once per progress message the server
  sends. Spark reports per *stage*, not per row: a stage is a unit of the physical plan, and
  `num_tasks` is how it was split across the cluster.

      Latu.collect(df, progress: &IO.inspect/1)

      Latu.count(df, progress: fn p -> IO.write("\\r" <> to_string(Latu.Progress.percent(p))) end)

  **The function runs in your own process, between batches.** Latu holds no processes, so there
  is nowhere else to run it — a slow handler slows the query, and one that raises fails it. Keep
  it to writing a line.

  **The server reports on a timer, and its default is two seconds**
  (`spark.connect.progress.reportInterval`). So a query that finishes inside two seconds reports
  nothing at all, and there is nothing Latu can do about that from this side — turn the interval
  down on the server if you want finer grain. Nothing here is a guarantee that your handler will
  be called even once.
  """

  defstruct stages: [], inflight: 0

  @typedoc "One stage of the physical plan, as Spark reported it."
  @type stage :: %{
          stage_id: integer(),
          num_tasks: integer(),
          num_completed_tasks: integer(),
          input_bytes_read: integer(),
          done: boolean()
        }

  @type t :: %__MODULE__{stages: [stage()], inflight: integer()}

  # Matched as a plain map rather than by naming `ExecutePlanResponse.ExecutionProgress`: this
  # only reads fields, and a map pattern needs no stand-in in the offline harness. Same reason
  # `Latu.Result.Literal` matches maps.
  @doc false
  @spec new(struct()) :: t()
  def new(%{stages: _, num_inflight_tasks: _} = message) do
    %__MODULE__{
      inflight: message.num_inflight_tasks,
      stages:
        Enum.map(message.stages, fn stage ->
          %{
            stage_id: stage.stage_id,
            num_tasks: stage.num_tasks,
            num_completed_tasks: stage.num_completed_tasks,
            input_bytes_read: stage.input_bytes_read,
            done: stage.done
          }
        end)
    }
  end

  @doc """
  Completed tasks as a percentage of the tasks Spark has told us about, rounded down.

  What PySpark's own progress bar divides. **It can go backwards**: the number is over the
  stages reported *so far*, and a query that reaches a new stage learns about more tasks. It is
  a progress indicator, not an estimate of remaining work.

  `0` when no stage has any tasks yet, rather than a division by zero.

      iex> stage = %{num_tasks: 4, num_completed_tasks: 3}
      iex> Latu.Progress.percent(%Latu.Progress{stages: [stage]})
      75

      iex> Latu.Progress.percent(%Latu.Progress{})
      0
  """
  @spec percent(t()) :: non_neg_integer()
  def percent(%__MODULE__{stages: stages}) do
    total = Enum.sum(Enum.map(stages, & &1.num_tasks))
    done = Enum.sum(Enum.map(stages, & &1.num_completed_tasks))

    if total > 0, do: div(done * 100, total), else: 0
  end
end
