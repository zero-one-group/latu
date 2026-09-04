defmodule Latu.ExecutionInfoTest do
  use ExUnit.Case, async: true

  alias Latu.ExecutionInfo
  alias Latu.Protocol.Spark.Connect, as: Proto

  # Everything but the literal is built as a plain map, which is what the decoder actually
  # depends on — `literal_test.exs` sets the precedent.
  defp lit(literal_type), do: %Proto.Expression.Literal{literal_type: literal_type}

  # `keys` are strings on the wire — they become atoms on the way out, not on the way in.
  defp observed(name, pairs) do
    {keys, values} = Enum.unzip(pairs)

    %{
      name: name,
      keys: Enum.map(keys, &to_string/1),
      values: Enum.map(values, &lit/1),
      root_error_idx: nil,
      errors: []
    }
  end

  defp node(name, plan_id, parent, metrics) do
    %{
      name: name,
      plan_id: plan_id,
      parent: parent,
      execution_metrics:
        Map.new(metrics, fn {key, {value, type}} ->
          {key, %{name: key, value: value, metric_type: type}}
        end)
    }
  end

  describe "observed" do
    test "one entry per name, atom-keyed both levels" do
      execution = %{observed: %{"checks" => observed("checks", total: {:long, 8})}}

      assert {:ok, info} = ExecutionInfo.new(execution)
      assert info.observed == %{checks: %{total: 8}}
    end

    test "nothing observed is an empty map, not an error" do
      assert {:ok, info} = ExecutionInfo.new(%{observed: %{}})
      assert info.observed == %{}
      assert info.metrics == []
    end

    test "a metric the server could not collect keeps its slot as an error, not a failure" do
      failed = %{
        name: "bad",
        keys: ["n"],
        values: [lit({:long, 1})],
        root_error_idx: 0,
        errors: [%{message: "boom"}]
      }

      good = %{
        name: "good",
        keys: ["n"],
        values: [lit({:long, 1})],
        root_error_idx: nil,
        errors: []
      }

      assert {:ok, info} = ExecutionInfo.new(%{observed: %{"bad" => failed, "good" => good}})
      assert %{good: %{n: 1}, bad: {:error, %Latu.Error{} = error}} = info.observed
      assert error.message =~ "observing bad failed"
      assert error.message =~ "boom"
    end
  end

  describe "Spark's own metrics" do
    test "one entry per plan node, with string keys" do
      execution = %{
        observed: %{},
        metrics: %{
          metrics: [
            node("Scan parquet ", 3, 0, %{"number of output rows" => {8, "sum"}}),
            node("Project", 0, -1, %{})
          ]
        }
      }

      assert {:ok, info} = ExecutionInfo.new(execution)

      assert [scan, project] = info.metrics
      assert scan.name == "Scan parquet "
      assert scan.plan_id == 3
      assert scan.parent == 0
      assert scan.metrics == %{"number of output rows" => %{value: 8, type: "sum"}}
      assert project.metrics == %{}
    end

    # The keys come off the wire, so they must not become atoms — the same rule `collect/2`
    # applies to column names, and a stronger case for it, since a future Spark can invent a
    # metric name Latu has never seen.
    test "a metric name Latu has never seen does not grow the atom table" do
      execution = %{
        observed: %{},
        metrics: %{metrics: [node("New", 1, 0, %{"a metric from spark 4.9" => {1, "sum"}})]}
      }

      assert {:ok, info} = ExecutionInfo.new(execution)
      assert [%{metrics: metrics}] = info.metrics
      assert Map.keys(metrics) == ["a metric from spark 4.9"]
    end

    test "an execution that carried no metrics has an empty list" do
      assert {:ok, %{metrics: []}} = ExecutionInfo.new(%{observed: %{}, metrics: nil})
    end
  end
end
