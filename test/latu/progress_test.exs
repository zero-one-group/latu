defmodule Latu.ProgressTest do
  use ExUnit.Case, async: true

  doctest Latu.Progress

  alias Latu.DataFrame
  alias Latu.Progress

  defp stage(id, tasks, done_tasks, done? \\ false) do
    %{
      stage_id: id,
      num_tasks: tasks,
      num_completed_tasks: done_tasks,
      input_bytes_read: 0,
      done: done?
    }
  end

  describe "new/1" do
    test "reads the stages and the inflight count" do
      progress = Progress.new(%{stages: [stage(1, 4, 2, false)], num_inflight_tasks: 2})

      assert progress.inflight == 2
      assert [%{stage_id: 1, num_tasks: 4, num_completed_tasks: 2, done: false}] = progress.stages
    end

    test "a message with no stages is still a progress report" do
      assert %Progress{stages: [], inflight: 0} =
               Progress.new(%{stages: [], num_inflight_tasks: 0})
    end
  end

  # The bug the first gate of batch 3 caught, pinned where it belongs. A writer's option list is
  # open-ended — anything it does not reserve becomes a Spark writer option — and anything it
  # *does* reserve is validated by `Latu.Plan` against a closed set. `:progress` belongs in
  # neither, so it has to leave before the split. Reserving it, which was the first fix, moved
  # the failure rather than removing it.
  describe "the writers accept :progress without passing it on" do
    setup do
      %{df: Latu.range(Latu.Session.from_url!("sc://h"), 5)}
    end

    # The assertion is that building the command does not raise. `Latu.Plan`'s writers validate
    # their options against a closed set, so a `:progress` that reached them raises
    # `unknown keys [:progress]` — which is exactly how the first gate failed. Nothing here
    # inspects the built term: the command's shape is `plan_test.exs`'s business.
    test "write/2", %{df: df} do
      assert DataFrame.write_command(df, format: "parquet", path: "/x", progress: & &1)
    end

    test "save_as_table/3", %{df: df} do
      assert DataFrame.save_as_table_command(df, "t", mode: :overwrite, progress: & &1)
    end

    test "insert_into/3", %{df: df} do
      assert DataFrame.insert_into_command(df, "t", progress: & &1)
    end

    test "write_v2/3", %{df: df} do
      assert DataFrame.write_v2_command(df, "t", mode: :create, progress: & &1)
    end
  end

  describe "percent/1" do
    test "completed over total, across every stage reported so far" do
      progress = Progress.new(%{stages: [stage(1, 4, 4), stage(2, 4, 2)], num_inflight_tasks: 2})

      assert Progress.percent(progress) == 75
    end

    test "rounds down, so it never claims to be finished before it is" do
      progress = Progress.new(%{stages: [stage(1, 3, 2)], num_inflight_tasks: 1})

      assert Progress.percent(progress) == 66
    end

    # The obvious way to write this divides by zero on the first message of a query, which is
    # exactly when a progress bar first draws itself.
    test "no tasks yet is 0, not a crash" do
      assert Progress.percent(Progress.new(%{stages: [], num_inflight_tasks: 0})) == 0

      unstarted = Progress.new(%{stages: [stage(1, 0, 0)], num_inflight_tasks: 0})
      assert Progress.percent(unstarted) == 0
    end

    test "everything done is 100" do
      progress = Progress.new(%{stages: [stage(1, 4, 4, true)], num_inflight_tasks: 0})

      assert Progress.percent(progress) == 100
    end
  end
end
