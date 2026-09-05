# Probe: what the 4.2.0 Connect server says about ML, before `latu_ml` exists.
#
#     docker compose up -d spark-connect
#     mix run dev/probe_ml_server.exs
#
# The M15 plan's 15.4: the mlCache confs, the error class a vanished model produces, and the
# first `Fit` Latu ever sends — which is what proves the `ml_command_result` latch live. The
# fetch at the end also exercises `Latu.Result.Literal`'s UDT arm on a real VectorUDT.
# Every step reports and continues, so one refusal does not cost the others. Fill the answers
# into the plan's table; they settle what `latu_ml` may assume.

alias Latu.Protocol.Spark.Connect, as: Proto

defmodule ProbeMl do
  alias Latu.Protocol.Spark.Connect, as: Proto

  def say(label, value), do: IO.puts("  " <> String.pad_trailing(label, 46) <> value)

  def heading(text) do
    IO.puts("\n" <> text <> "\n" <> String.duplicate("-", String.length(text)))
  end

  # Every ML command rides `Command.ml_command`; the result lands on the execution's
  # `ml_command_result` (M15.1) or nowhere.
  def send(session, command) do
    ml = %Proto.MlCommand{command: command}
    plan = Latu.Plan.new(%Proto.Command{command_type: {:ml_command, ml}})

    Latu.Client.execute_command(session, plan)
  end

  def describe({:error, %Latu.Error{} = error}) do
    "REFUSED " <> inspect(error.error_class) <> " — " <> String.slice(error.message, 0, 110)
  end

  def describe({:ok, %{ml_command_result: nil}}), do: "ok, but ml_command_result is NIL"

  def describe({:ok, %{ml_command_result: %{result_type: {arm, value}}}}) do
    "latched {#{arm}, #{inspect(value, printable_limit: 90, limit: 6)}}"
  end

  def describe(other), do: inspect(other, printable_limit: 120)
end

session = Latu.connect!(System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
IO.puts("Spark #{Latu.spark_version!(session)} — ML server probe")

# =============================================
# 1. The mlCache confs: visible? per-session settable?
# =============================================

ProbeMl.heading("1. spark.connect.session.connectML.mlCache.memoryControl.*")

prefix = "spark.connect.session.connectML"

keys =
  for suffix <- ~w(enabled maxInMemorySize offloadingTimeout maxModelSize maxStorageSize),
      do: "#{prefix}.mlCache.memoryControl.#{suffix}"

for key <- keys do
  value =
    case Latu.conf(session, key) do
      {:ok, nil} -> "(server has no value)"
      {:ok, value} -> value
      {:error, error} -> "conf refused: " <> String.slice(error.message, 0, 60)
    end

  modifiable =
    case Latu.is_modifiable(session, key) do
      {:ok, true} -> "settable per session"
      {:ok, false} -> "STATIC — needs a compose conf"
      {:error, _} -> "modifiable? refused"
    end

  ProbeMl.say(String.replace_prefix(key, prefix <> ".", ""), "#{value}   [#{modifiable}]")
end

case Latu.confs(session, prefix: prefix) do
  {:ok, all} when map_size(all) == 0 ->
    IO.puts("\n  GetAll with that prefix returns nothing — the confs are not enumerable here.")

  {:ok, all} ->
    IO.puts("\n  GetAll with that prefix returns #{map_size(all)} key(s):")
    for {k, v} <- Enum.sort(all), do: ProbeMl.say("  " <> k, inspect(v))

  {:error, error} ->
    IO.puts("\n  GetAll refused: " <> String.slice(error.message, 0, 110))
end

# =============================================
# 2. What a vanished model answers
# =============================================

ProbeMl.heading("2. GetCacheInfo, and a Fetch on an ObjectRef that never existed")

cache_info = ProbeMl.send(session, {:get_cache_info, %Proto.MlCommand.GetCacheInfo{}})
ProbeMl.say("GetCacheInfo", ProbeMl.describe(cache_info))

# `Fetch` is ONE top-level message in relations.proto, shared by `MlCommand.fetch` and
# `MlRelation.fetch` — not two nested ones. Roadmap §2.1 writes it the other way.
missing =
  {:fetch,
   %Proto.Fetch{
     obj_ref: %Proto.ObjectRef{id: "latu-probe-no-such-model"},
     methods: [%Proto.Fetch.Method{method: "coefficients"}]
   }}

ProbeMl.say("Fetch on a missing ref", ProbeMl.describe(ProbeMl.send(session, missing)))

# =============================================
# 3. A features column, and the first Fit Latu ever sends
# =============================================

ProbeMl.heading("3. The ML SQL functions, VectorAssembler, and Fit")

# Roadmap §1 has these two in scope as plain Spark functions, which is what `Latu.ML.Functions`
# would be built on. Anything but UNRESOLVED_ROUTINE means the name resolves.
for name <- ~w(array_to_vector vector_to_array) do
  answer =
    case Latu.sql(session, "SELECT #{name}(array(1.0, 2.0)) AS v") do
      {:ok, df} ->
        case Latu.collect(df) do
          {:ok, _rows} -> "resolves, and evaluates"
          {:error, error} -> "resolves; " <> inspect(error.error_class)
        end

      {:error, error} ->
        inspect(error.error_class) <> " — " <> String.slice(error.message, 0, 70)
    end

  ProbeMl.say(name <> " as a Spark function", answer)
end

# VectorAssembler as an MlRelation.Transform: the route that needs no SQL function, and the one
# that also exercises `Latu.Plan.relation/1` (M15.3) and the laziness §2.1 claims for Transform.
base =
  Latu.sql!(session, """
  SELECT * FROM VALUES (0.0, 0.0, 1.1), (0.0, 0.1, 1.0), (1.0, 2.0, 0.1), (1.0, 2.1, 0.2)
  AS t(label, x1, x2)
  """)

string_type = %Proto.DataType{kind: {:string, %Proto.DataType.String{}}}

input_cols = %Proto.Expression.Literal{
  data_type: %Proto.DataType{
    kind: {:array, %Proto.DataType.Array{element_type: string_type, contains_null: false}}
  },
  literal_type:
    {:array,
     %Proto.Expression.Literal.Array{
       element_type: string_type,
       elements: [
         %Proto.Expression.Literal{literal_type: {:string, "x1"}},
         %Proto.Expression.Literal{literal_type: {:string, "x2"}}
       ]
     }}
}

transform = %Proto.MlRelation{
  ml_type:
    {:transform,
     %Proto.MlRelation.Transform{
       operator:
         {:transformer,
          %Proto.MlOperator{
            name: "org.apache.spark.ml.feature.VectorAssembler",
            uid: "latu_probe_va",
            type: :OPERATOR_TYPE_TRANSFORMER
          }},
       input: base.plan,
       params: %Proto.MlParams{
         params: %{
           "inputCols" => input_cols,
           "outputCol" => %Proto.Expression.Literal{literal_type: {:string, "features"}}
         }
       }
     }}
}

assembled = %Latu.DataFrame{
  session: session,
  plan: Latu.Plan.relation({:ml_relation, transform})
}

# §2.1 says a Transform is pure plan building — this is that claim, asked directly.
case Latu.schema(assembled) do
  {:ok, schema} ->
    ProbeMl.say("VectorAssembler schema (no execute)", inspect(Enum.map(schema, & &1.name)))

  {:error, error} ->
    ProbeMl.say("VectorAssembler schema", "REFUSED — " <> String.slice(error.message, 0, 90))
end

fit =
  {:fit,
   %Proto.MlCommand.Fit{
     estimator: %Proto.MlOperator{
       name: "org.apache.spark.ml.classification.LogisticRegression",
       uid: "latu_probe_lr",
       type: :OPERATOR_TYPE_ESTIMATOR
     },
     params: %Proto.MlParams{
       params: %{"maxIter" => %Proto.Expression.Literal{literal_type: {:integer, 1}}}
     },
     dataset: assembled.plan
   }}

result = ProbeMl.send(session, fit)
ProbeMl.say("Fit", ProbeMl.describe(result))

with {:ok, %{ml_command_result: %{result_type: {:operator_info, info}}}} <- result,
     %Proto.ObjectRef{id: id} <- elem(info.type, 1) do
  ProbeMl.say("  model obj_ref", inspect(id))
  ProbeMl.say("  model uid", inspect(info.uid))
  ProbeMl.say("  warning_message", inspect(info.warning_message))

  # A real VectorUDT literal, back through the M15.2 decoder.
  coefficients =
    {:fetch,
     %Proto.Fetch{
       obj_ref: %Proto.ObjectRef{id: id},
       methods: [%Proto.Fetch.Method{method: "coefficients"}]
     }}

  fetched = ProbeMl.send(session, coefficients)
  ProbeMl.say("Fetch coefficients", ProbeMl.describe(fetched))

  with {:ok, %{ml_command_result: %{result_type: {:param, literal}}}} <- fetched do
    decoded = inspect(Latu.Result.Literal.value(literal), printable_limit: 90)
    ProbeMl.say("  decoded by Latu.Result.Literal", decoded)
  end

  delete = %Proto.MlCommand.Delete{obj_refs: [%Proto.ObjectRef{id: id}]}
  ProbeMl.say("Delete the model", ProbeMl.describe(ProbeMl.send(session, {:delete, delete})))
end

IO.puts("")
Latu.disconnect(session)
