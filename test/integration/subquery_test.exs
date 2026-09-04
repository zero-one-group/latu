defmodule Latu.Integration.SubqueryTest do
  use ExUnit.Case, async: true

  alias Latu.Column
  alias Latu.Functions, as: F

  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session) end)

    one = Latu.select(Latu.range(session, 1), x: Column.lit(10))

    %{session: session, one: one, five: Latu.range(session, 5)}
  end

  test "a scalar subquery is a value in a projection", %{one: one, five: five} do
    rows = five |> Latu.select(x: Latu.scalar(one)) |> Latu.collect!()

    assert Enum.map(rows, & &1.x) == [10, 10, 10, 10, 10]
  end

  test "and in a condition", %{one: one, five: five} do
    assert five |> Latu.filter(Column.less(:id, Latu.scalar(one))) |> Latu.count!() == 5
  end

  test "a reference collected from inside a call still resolves", %{one: one, five: five} do
    rows =
      five
      |> Latu.select(x: F.abs(Column.multiply(Latu.scalar(one), -1)))
      |> Latu.collect!()

    assert Enum.map(rows, & &1.x) == [10, 10, 10, 10, 10]
  end

  test "exists holds when the frame has rows, and not when it does not", %{
    one: one,
    five: five
  } do
    assert five |> Latu.filter(Latu.exists(one)) |> Latu.count!() == 5

    empty = Latu.filter(one, Column.greater(:x, 100))

    assert five |> Latu.filter(Latu.exists(empty)) |> Latu.count!() == 0
  end

  test "isin over a DataFrame is an IN subquery", %{session: session, five: five} do
    two = Latu.range(session, 2)

    assert five |> Latu.filter(Column.isin(:id, two)) |> Latu.count!() == 2
  end

  test "isin over a struct matches column for column", %{session: session, five: five} do
    pairs = Latu.select(Latu.range(session, 2), a: :id, b: Column.multiply(:id, 2))
    struct = F.struct([Column.col(:id), Column.multiply(:id, 2)])

    assert five |> Latu.filter(Column.isin(struct, pairs)) |> Latu.count!() == 2
  end

  test "a frame already in the tree is referenced, not re-read", %{five: five} do
    highest = Latu.agg(five, m: F.max(:id))

    assert five |> Latu.filter(Column.less(:id, Latu.scalar(highest))) |> Latu.count!() == 4
  end

  test "the session is what the two frames share, nothing else", %{session: session, one: one} do
    other = session |> Latu.range(3) |> Latu.select(id: Column.multiply(:id, 5))

    rows = other |> Latu.select([:id, x: Latu.scalar(one)]) |> Latu.collect!()

    assert rows == [%{id: 0, x: 10}, %{id: 5, x: 10}, %{id: 10, x: 10}]
  end

  test "a scalar subquery with more than one row is Spark's error, not a wrong answer", %{
    session: session,
    five: five
  } do
    two = Latu.range(session, 2)

    assert {:error, %Latu.Error{}} =
             five |> Latu.filter(Column.less(:id, Latu.scalar(two))) |> Latu.count()
  end

  # A *tagged* reference to an outer frame is refused by Spark, but a **qualified name**
  # resolves, so correlation goes through `as/2` plus `expr/1`.
  describe "correlated subqueries, over qualified names" do
    test "an EXISTS subquery correlated to the outer row", %{session: session} do
      outer = Latu.as(Latu.range(session, 5), "o")

      # Rows of the inner frame that match the outer row: only ids 0..2 have one.
      inner = Latu.filter(Latu.range(session, 3), Column.expr("id = o.id"))

      assert outer |> Latu.filter(Latu.exists(inner)) |> Latu.count!() == 3
    end

    test "a correlated scalar subquery", %{session: session} do
      outer = Latu.as(Latu.range(session, 3), "o")

      totals =
        Latu.range(session, 10)
        |> Latu.filter(Column.expr("id < o.id"))
        |> Latu.agg(n: F.count(Column.lit(1)))

      rows =
        outer
        |> Latu.select([:id, n: Latu.scalar(totals)])
        |> Latu.sort([:id])
        |> Latu.collect!()

      assert rows == [%{id: 0, n: 0}, %{id: 1, n: 1}, %{id: 2, n: 2}]
    end

    test "an uncorrelated name inside the subquery is still Spark's error", %{session: session} do
      outer = Latu.as(Latu.range(session, 5), "o")
      inner = Latu.filter(Latu.range(session, 3), Column.expr("id = nosuch.id"))

      assert {:error, %Latu.Error{kind: :rpc}} =
               Latu.count(Latu.filter(outer, Latu.exists(inner)))
    end
  end
end
