defmodule Latu.Integration.ExecuteTest do
  use ExUnit.Case, async: true

  alias Latu.Client
  alias Latu.Error
  alias Latu.Plan
  alias Latu.Protocol.Spark.Connect, as: Proto

  # Needs a Spark Connect server on :15003 — docker compose up -d spark-reattach.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15003"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session, release: true) end)
    %{session: session}
  end

  test "runs a plan and returns its Arrow batches", %{session: session} do
    plan = Plan.new(Latu.range(session, 5).plan)

    assert {:ok, batches, %{schema: schema}} = Client.execute(session, plan)

    # Spark emits one batch per partition, so the count follows the server's parallelism —
    # range(5) arrives as five single-row batches. The total is what is guaranteed.
    assert Enum.sum(Enum.map(batches, & &1.row_count)) == 5

    # The result's DataType arrives ahead of the batches and is retained: range gives one
    # non-nullable long named id.
    assert %Proto.DataType{kind: {:struct, %Proto.DataType.Struct{fields: [field]}}} = schema
    assert %Proto.DataType.StructField{name: "id", nullable: false} = field
    assert {:long, _} = field.data_type.kind

    # Each is a complete Arrow IPC stream of its own, continuation marker and all. This is why
    # results are decoded per batch and never byte-concatenated.
    assert Enum.all?(batches, &match?(<<0xFF, 0xFF, 0xFF, 0xFF, _::binary>>, &1.data))
  end

  test "ShowString comes back as a single rendered row", %{session: session} do
    df = Latu.range(session, 5)
    plan = df.plan |> Plan.show_string() |> Plan.new()

    assert {:ok, [batch], _executed} = Client.execute(session, plan)
    assert batch.row_count == 1
  end

  test "pins the server's session id on the returned session", %{session: session} do
    plan = Plan.new(Latu.range(session, 1).plan)

    assert {:ok, _batches, %{session: executed}} = Client.execute(session, plan)
    assert is_binary(executed.server_session_id)
    assert session.server_session_id == nil
  end

  test "a plan the server rejects comes back as an error, not a crash", %{session: session} do
    plan = %Proto.Plan{op_type: {:root, %Proto.Relation{}}}

    assert {:error, %Error{kind: :rpc}} = Client.execute(session, plan)
  end
end
