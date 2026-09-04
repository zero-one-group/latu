defmodule Latu.ExecutionInfo do
  @moduledoc """
  What a run reported besides its result: the metrics you asked for, and the ones Spark keeps.

  What every `*_with_metrics` twin hands back beside its result. Two kinds of metric, and they
  are not the same kind of thing:

    * `observed` — what a `Latu.observe/3` in the plan asked for. **Yours**: the observation name
      and the metric names came out of your own source, which is why they are atoms.
    * `metrics` — Spark's own per-node SQL metrics, one entry per plan node: rows scanned, time
      spent, bytes read. **The server's**, so the names are strings — an atom table must not
      grow with whatever a future Spark decides to call a metric. Same rule as `Latu.collect/2`'s
      `keys: :strings`.

  A node's `metrics` map is keyed by Spark's metric name, and each value carries the number and
  the kind of number it is (`"sum"`, `"timing"`, `"size"`, `"average"` and so on — Spark's own
  `metricType`, passed through rather than interpreted).

      {:ok, rows, info} = Latu.collect_with_metrics(df)

      info.observed
      #=> %{checks: %{total: 8}}

      Enum.find(info.metrics, &(&1.name =~ "Scan"))
      #=> %{name: "Scan parquet ", plan_id: 3, parent: 0,
      #=>   metrics: %{"number of output rows" => %{value: 8, type: "sum"}}}

  `metrics` is empty unless the server sent any; Spark decides what to report and for which
  plans, and inventing a zero would be worse than saying nothing. The same is true of a
  `Latu.observe/3` name the server reported nothing for — it is simply absent.

  An observation the server could not compute — or one Latu could not decode — arrives as
  `{:error, %Latu.Error{}}` under its own name, and the action still succeeds: the rows were
  produced, and Spark reports the failure inside the metrics message rather than failing the
  query. PySpark does the same, raising only when `Observation.get` is called.

      info.observed
      #=> %{checks: {:error, %Latu.Error{message: "observing checks failed: ..."}}}
  """

  alias Latu.Error
  alias Latu.Result

  defstruct observed: %{}, metrics: []

  @typedoc "One `observe/3` name's metrics, decoded — or why they could not be."
  @type observed :: %{optional(atom()) => %{optional(atom()) => term()} | {:error, Error.t()}}

  @typedoc "One plan node's SQL metrics, as Spark reported them."
  @type node_metrics :: %{
          name: String.t(),
          plan_id: integer(),
          parent: integer(),
          metrics: %{optional(String.t()) => %{value: integer(), type: String.t()}}
        }

  @type t :: %__MODULE__{observed: observed(), metrics: [node_metrics()]}

  @doc false
  @spec new(map()) :: {:ok, t()}
  def new(%{observed: observed} = execution) do
    {:ok,
     %__MODULE__{
       observed: observed_metrics(observed),
       metrics: node_metrics(Map.get(execution, :metrics))
     }}
  end

  # `ObservedMetrics` in, one entry per name out. A name the server reported nothing for is
  # simply absent: Spark decides whether a CollectMetrics node fires, and inventing a zero
  # would be worse than saying nothing. A failed one keeps its slot, as `{:error, _}`, so the
  # result the action produced is not thrown away over a metric.
  defp observed_metrics(observed) do
    Map.new(observed, fn {name, metrics} ->
      case observation(metrics) do
        {:ok, values} -> {String.to_atom(name), values}
        {:error, error} -> {String.to_atom(name), {:error, error}}
      end
    end)
  end

  # The server reports a failure to collect a metric in the message itself rather than failing
  # the query, and PySpark hands that to the caller as the Observation's error.
  defp observation(%{root_error_idx: index} = metrics) when is_integer(index) do
    reason =
      case Enum.at(metrics.errors, index) do
        %{message: message} -> message
        _ -> "the server gave no reason"
      end

    {:error, Error.new(:rpc, "observing #{metrics.name} failed: #{reason}")}
  end

  defp observation(%{keys: keys, values: values}) do
    keys
    |> Enum.zip(values)
    |> reduce_ok(fn {key, literal} ->
      with {:ok, value} <- Result.Literal.value(literal), do: {:ok, {String.to_atom(key), value}}
    end)
  end

  # Spark's own metrics need no decoding — every value is already an integer, and the units are
  # in `metric_type`. String keys, deliberately: see the moduledoc.
  defp node_metrics(nil), do: []

  defp node_metrics(%{metrics: nodes}) do
    Enum.map(nodes, fn node ->
      %{
        name: node.name,
        plan_id: node.plan_id,
        parent: node.parent,
        metrics:
          Map.new(node.execution_metrics, fn {name, value} ->
            {name, %{value: value.value, type: value.metric_type}}
          end)
      }
    end)
  end

  defp reduce_ok(enumerable, fun) do
    Enum.reduce_while(enumerable, {:ok, %{}}, fn element, {:ok, acc} ->
      case fun.(element) do
        {:ok, {key, value}} -> {:cont, {:ok, Map.put(acc, key, value)}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end
end
