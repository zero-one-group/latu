defmodule Latu.Integration.ReferencesTest do
  use ExUnit.Case, async: true

  alias Latu.Column

  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session) end)

    %{session: session, five: Latu.range(session, 5)}
  end

  test "a reference to a frame outside the plan is refused by Spark", %{
    session: session,
    five: five
  } do
    other = Latu.range(session, 10)

    assert {:error, error} = five |> Latu.select(Latu.col(other, :id)) |> Latu.collect()
    assert error.message =~ "CANNOT_RESOLVE_DATAFRAME_COLUMN"
  end

  test "and so is one that matches two relations at once", %{five: five} do
    condition = Column.equal(Latu.col(five, :id), Latu.col(five, :id))
    joined = Latu.join(five, five, on: condition)

    assert {:error, error} = joined |> Latu.select(Latu.col(five, :id)) |> Latu.collect()
    assert error.message =~ "AMBIGUOUS_COLUMN_REFERENCE"
  end

  test "aliasing the sides is what fixes the ambiguity", %{five: five} do
    left = Latu.as(five, "l")
    right = Latu.as(five, "r")
    condition = Column.equal(Latu.col(left, :id), Latu.col(right, :id))

    joined = Latu.join(left, right, on: condition)

    assert joined |> Latu.select(Latu.col(left, :id)) |> Latu.count!() == 5
  end

  test "a reference into the same plan resolves, which is the case that works", %{five: five} do
    assert five |> Latu.filter(Column.greater(Latu.col(five, :id), 2)) |> Latu.count!() == 2
  end
end
