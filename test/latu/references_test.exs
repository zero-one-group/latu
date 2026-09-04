defmodule Latu.ReferencesTest do
  @moduledoc """
  Tagged column references, and what happens when one points outside its own plan.

  `Latu.col/2` tags a reference with the relation it came from, and that resolves whenever the
  relation is in the tree. When it is not, **Spark
  refuses the plan** — `CANNOT_RESOLVE_DATAFRAME_COLUMN`, whether or not the relation is
  hoisted into a `WithRelations` node. PySpark sends the same plan and gets
  the same error, so the golden test below pins parity in the failure case; the failure itself
  is `test/integration/references_test.exs`. For a value from another frame, the answer is a
  subquery (`Latu.scalar/1` and friends, `test/latu/subquery_test.exs`).
  """

  use ExUnit.Case, async: true

  import Latu.Wire

  alias Latu.Column
  alias Latu.Plan
  alias Latu.Session

  setup do
    %{session: Session.from_url!("sc://h")}
  end

  test "a reference carries the plan_id of the DataFrame it came from", %{session: session} do
    df = Latu.range(session, 10)
    {:unresolved_attribute, tagged} = Latu.col(df, :id).expr_type
    {:unresolved_attribute, untagged} = Column.col(:id).expr_type

    assert tagged.plan_id == df.plan.common.plan_id
    assert untagged.plan_id == nil
  end

  test "a self-join needs no hoisting, because both branches are the relation", %{
    session: session
  } do
    df = Latu.range(session, 10)
    condition = Column.equal(Latu.col(df, :id), Latu.col(df, :id))

    assert_wire(Latu.join(df, df, on: condition), "self_join")
  end

  test "a reference to a DataFrame outside the plan goes out as PySpark sends it", %{
    session: session
  } do
    a = Latu.range(session, 10)
    b = Latu.range(session, 5)

    # Not hoisted, by either client: WithRelations would not make it resolve. Normalisation
    # turns the dangling id into -1 on both sides, so the fixture says "points at nothing"
    # rather than carrying whatever the builder's counter happened to be.
    assert_wire(Latu.select(b, Latu.col(a, :id)), "cross_frame_col")
  end

  test "and normalising says the reference points at nothing", %{session: session} do
    a = Latu.range(session, 10)
    b = Latu.range(session, 5)

    {:project, project} = Plan.normalize_ids(Latu.select(b, Latu.col(a, :id)).plan).rel_type
    [%{expr_type: {:unresolved_attribute, reference}}] = project.expressions

    assert reference.plan_id == -1
  end

  test "normalising cannot tell a shared subtree from two equal ones", %{session: session} do
    df = Latu.range(session, 10)
    shared = Latu.join(df, df, on: Column.equal(Latu.col(df, :id), Latu.col(df, :id)))

    {:join, join} = Plan.normalize_ids(shared.plan).rel_type
    {:unresolved_function, condition} = join.join_condition.expr_type

    [left, _right] =
      Enum.map(condition.arguments, &(&1.expr_type |> elem(1) |> Map.get(:plan_id)))

    # One relation appears twice, so the walk numbers the occurrences 0 and 1 and the reference
    # ends up pointing at whichever was numbered last. The oracle does exactly the same, which
    # is why the golden test above agrees — but the normalised plan no longer says what the
    # real one says about identity. Documented in docs/decisions.md; matters for M9.
    assert join.left.common.plan_id != join.right.common.plan_id
    assert left == join.right.common.plan_id
  end
end
