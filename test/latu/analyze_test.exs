defmodule Latu.AnalyzeTest do
  use ExUnit.Case, async: true

  import Latu.Wire

  alias Latu.Plan
  alias Latu.Protocol.Spark.Connect, as: Proto
  alias Latu.Session

  # Golden arms come from PySpark: python dev/pyspark_oracle.py --generate

  setup do
    session = Session.from_url!("sc://h")

    %{session: session, relation: Latu.range(session, 5).plan}
  end

  describe "analyze/3 against PySpark" do
    test "the schema arm is the plan and nothing else", %{relation: relation} do
      {:schema, arm} = Plan.analyze(:schema, relation)

      assert_wire_message(arm, "analyze_schema")
    end

    test "an absent level stays off the wire", %{relation: relation} do
      {:tree_string, arm} = Plan.analyze(:tree_string, relation)

      assert_wire_message(arm, "analyze_tree_string")
    end

    test "a level rides along", %{relation: relation} do
      {:tree_string, arm} = Plan.analyze(:tree_string, relation, level: 2)

      assert_wire_message(arm, "analyze_tree_string_level")
    end
  end

  describe "validation" do
    test "an unknown operation lists the ones there are", %{relation: relation} do
      assert_raise ArgumentError, ~r/unknown analysis :columns.*:tree_string/s, fn ->
        apply(Plan, :analyze, [:columns, relation])
      end
    end

    test "a known operation given the wrong input says so instead", %{relation: relation} do
      assert_raise ArgumentError, ~r/:ddl_parse does not take/, fn ->
        apply(Plan, :analyze, [:ddl_parse, relation])
      end

      assert_raise ArgumentError, ~r/:schema does not take/, fn ->
        apply(Plan, :analyze, [:schema, "a INT"])
      end
    end

    test "an unknown explain mode names the five", %{relation: relation} do
      assert_raise ArgumentError, ~r/explain mode :verbose.*formatted/s, fn ->
        apply(Plan, :analyze, [:explain, relation, [mode: :verbose]])
      end
    end

    test "an unknown storage level names the ten", %{relation: relation} do
      assert_raise ArgumentError, ~r/storage level :memory.*memory_and_disk_deser/s, fn ->
        apply(Plan, :analyze, [:persist, relation, [level: :memory]])
      end
    end

    test "blocking is a flag", %{relation: relation} do
      assert_raise ArgumentError, ~r/blocking: is a flag/, fn ->
        apply(Plan, :analyze, [:unpersist, relation, [blocking: :yes]])
      end
    end

    test "a level must be a positive depth", %{relation: relation} do
      for bad <- [0, -1, "2"] do
        assert_raise ArgumentError, ~r/positive depth/, fn ->
          apply(Plan, :analyze, [:tree_string, relation, [level: bad]])
        end
      end
    end

    test "the schema arm takes no options", %{relation: relation} do
      assert_raise ArgumentError, fn ->
        apply(Plan, :analyze, [:schema, relation, [level: 2]])
      end
    end
  end

  describe "the rest of the arms against PySpark" do
    test "explain sends SIMPLE explicitly when nothing was asked for", %{relation: relation} do
      {:explain, arm} = Plan.analyze(:explain, relation)

      assert_wire_message(arm, "analyze_explain")
    end

    test "and the mode when it was", %{relation: relation} do
      {:explain, arm} = Plan.analyze(:explain, relation, mode: :formatted)

      assert_wire_message(arm, "analyze_explain_formatted")
    end

    test "is_local", %{relation: relation} do
      {:is_local, arm} = Plan.analyze(:is_local, relation)

      assert_wire_message(arm, "analyze_is_local")
    end

    test "is_streaming", %{relation: relation} do
      {:is_streaming, arm} = Plan.analyze(:is_streaming, relation)

      assert_wire_message(arm, "analyze_is_streaming")
    end

    test "input_files", %{relation: relation} do
      {:input_files, arm} = Plan.analyze(:input_files, relation)

      assert_wire_message(arm, "analyze_input_files")
    end

    test "semantic_hash", %{relation: relation} do
      {:semantic_hash, arm} = Plan.analyze(:semantic_hash, relation)

      assert_wire_message(arm, "analyze_semantic_hash")
    end

    test "same_semantics carries two plans, target first", %{session: session} do
      target = Latu.range(session, 5).plan
      other = Latu.range(session, 3).plan
      {:same_semantics, arm} = Plan.analyze(:same_semantics, {target, other})

      assert_wire_message(arm, "analyze_same_semantics")
    end

    test "persist carries a bare relation and Spark's default level", %{relation: relation} do
      {:persist, arm} = Plan.analyze(:persist, relation)

      assert_wire_message(arm, "analyze_persist")
    end

    test "and a named level when given one", %{relation: relation} do
      {:persist, arm} = Plan.analyze(:persist, relation, level: :disk_only_2)

      assert_wire_message(arm, "analyze_persist_level")
    end

    test "unpersist sends blocking either way, because the field has presence", %{
      relation: relation
    } do
      {:unpersist, arm} = Plan.analyze(:unpersist, relation)

      assert_wire_message(arm, "analyze_unpersist")
    end

    test "blocking: true", %{relation: relation} do
      {:unpersist, arm} = Plan.analyze(:unpersist, relation, blocking: true)

      assert_wire_message(arm, "analyze_unpersist_blocking")
    end

    test "get_storage_level", %{relation: relation} do
      {:get_storage_level, arm} = Plan.analyze(:get_storage_level, relation)

      assert_wire_message(arm, "analyze_get_storage_level")
    end

    test "ddl_parse takes a string, not a plan" do
      {:ddl_parse, arm} = Plan.analyze(:ddl_parse, "id INT, tags ARRAY<STRING>")

      assert_wire_message(arm, "analyze_ddl_parse")
    end

    test "json_to_ddl takes the other string" do
      json =
        ~s({"type":"struct","fields":) <>
          ~s([{"name":"id","type":"integer","nullable":true,"metadata":{}}]})

      {:json_to_ddl, arm} = Plan.analyze(:json_to_ddl, json)

      assert_wire_message(arm, "analyze_json_to_ddl")
    end
  end

  describe "normalising an arm" do
    # The M9 WithRelations hazard, second instance: SameSemantics holds two plans whose *term*
    # order (other_plan, target_plan) is the reverse of their field order, and the oracle walks
    # fields. Without the dedicated `walk` clause this comes out {1, 0} and every fixture built
    # on it disagrees with PySpark.
    test "numbers target_plan before other_plan, as the oracle does", %{session: session} do
      target = Latu.range(session, 5).plan
      other = Latu.range(session, 3).plan

      {:same_semantics, arm} = Plan.analyze(:same_semantics, {target, other})
      normalised = Plan.normalize_ids(arm)

      %Proto.Plan{op_type: {:root, target}} = normalised.target_plan
      %Proto.Plan{op_type: {:root, other}} = normalised.other_plan

      assert {target.common.plan_id, other.common.plan_id} == {0, 1}
    end

    test "a plan-only arm renumbers from zero", %{relation: relation} do
      {:schema, arm} = Plan.analyze(:schema, relation)
      %Proto.Plan{op_type: {:root, inner}} = Plan.normalize_ids(arm).plan

      assert inner.common.plan_id == 0
    end
  end

  describe "storage levels" do
    test "every name Spark has maps to flags and back" do
      for name <- [
            :none,
            :disk_only,
            :disk_only_2,
            :disk_only_3,
            :memory_only,
            :memory_only_2,
            :memory_and_disk,
            :memory_and_disk_2,
            :memory_and_disk_deser,
            :off_heap
          ] do
        assert %{name: ^name} = name |> Plan.storage_level() |> Plan.from_storage_level()
      end
    end

    test "the default is the one Scala has used since 3.0" do
      assert Plan.storage_level(:memory_and_disk_deser) == %Proto.StorageLevel{
               use_disk: true,
               use_memory: true,
               use_off_heap: false,
               deserialized: true,
               replication: 1
             }
    end

    test "replication is what separates the numbered ones" do
      assert Plan.storage_level(:disk_only_3).replication == 3
      assert Plan.storage_level(:disk_only).replication == 1
    end

    test "a combination Spark has no name for comes back without one" do
      odd = %Proto.StorageLevel{
        use_disk: false,
        use_memory: true,
        use_off_heap: false,
        deserialized: true,
        replication: 7
      }

      assert Plan.from_storage_level(odd) == %{
               name: nil,
               use_disk: false,
               use_memory: true,
               use_off_heap: false,
               deserialized: true,
               replication: 7
             }
    end
  end
end
