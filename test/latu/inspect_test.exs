defmodule Latu.InspectTest do
  use ExUnit.Case, async: true

  alias Latu.Functions, as: F
  alias Latu.Plan
  alias Latu.Session

  # CLAUDE.md says not to assert on inspect/1 formatting. This file is the exception the rule
  # was not written for: the rendering IS the feature — a DataFrame that inspects
  # as `#Latu.DataFrame<project>` tells you nothing after six piped verbs. So the assertions
  # are on `Latu.Plan.Inspect.chain/2`, whose output is its whole contract, and only two touch
  # inspect/1 itself: that the impl is wired up, and that it never raises.

  setup do
    %{session: Session.from_url!("sc://h")}
  end

  defp chain(df), do: Plan.Inspect.chain(df.plan)

  describe "the spine" do
    test "a leaf is its own kind", %{session: session} do
      assert chain(Latu.range(session, 5)) == "range"
    end

    test "reads leaf first, in the order you piped it", %{session: session} do
      df =
        session
        |> Latu.range(10)
        |> Latu.filter(Latu.Column.greater(:id, 3))
        |> Latu.select(id: :id, twice: Latu.Column.multiply(:id, 2))

      assert chain(df) == "range → filter → project(2)"
    end

    test "counts what a node names, where the count is the useful half", %{session: session} do
      df = session |> Latu.range(10) |> Latu.with_columns(a: 1, b: 2) |> Latu.sort(:id)

      assert chain(df) == "range → with_columns(2) → sort(1)"
    end

    test "names a source rather than the relation kind", %{session: session} do
      assert chain(Latu.table(session, "people")) == ~s|table("people")|
      assert chain(Latu.read(session, format: "parquet", path: "/tmp/x")) == "read(parquet)"
    end

    test "an alias shows the name, which is the point of having set one", %{session: session} do
      assert chain(Latu.as(Latu.range(session, 5), "t")) == ~s|range → as("t")|
    end
  end

  describe "where the spine ends" do
    test "a join names both sides and follows neither", %{session: session} do
      left = Latu.range(session, 5)
      right = Latu.table(session, "people")

      assert chain(Latu.join(left, right, on: :id)) == "join(range, read)"
    end

    test "a set operation names itself", %{session: session} do
      df = Latu.union(Latu.range(session, 5), Latu.range(session, 5))

      assert chain(df) == "union(range, range)"
    end

    test "a verb after a join keeps going", %{session: session} do
      left = Latu.range(session, 5)
      right = Latu.range(session, 5)
      df = left |> Latu.join(right, on: :id) |> Latu.limit(3)

      assert chain(df) == "join(range, range) → limit(3)"
    end
  end

  describe "the bound" do
    test "elides the middle and keeps both ends", %{session: session} do
      df = Enum.reduce(1..20, Latu.range(session, 5), fn _i, df -> Latu.limit(df, 1) end)
      rendered = chain(df)

      assert String.starts_with?(rendered, "range → …")
      assert String.ends_with?(rendered, "limit(1)")
      assert length(String.split(rendered, " → ")) == 8
    end

    test "limit: :infinity shows the whole thing", %{session: session} do
      df = Enum.reduce(1..20, Latu.range(session, 5), fn _i, df -> Latu.limit(df, 1) end)
      rendered = Plan.Inspect.chain(df.plan, %Inspect.Opts{limit: :infinity})

      refute rendered =~ "…"
      assert length(String.split(rendered, " → ")) == 21
    end
  end

  describe "what it must never do" do
    test "the DataFrame impl is wired to chain/2", %{session: session} do
      df = session |> Latu.range(5) |> Latu.limit(2)

      assert inspect(df) == "#Latu.DataFrame<range → limit(2)>"
    end

    test "an observed frame renders without reaching the server", %{session: session} do
      df = Latu.observe(Latu.range(session, 5), "checks", total: F.count(:id))

      # No session is connected, so anything that did IO here would raise rather than render.
      assert inspect(df) =~ "collect_metrics"
    end

    test "a plan it cannot read renders rather than raising" do
      assert Plan.Inspect.chain(%Latu.Protocol.Spark.Connect.Relation{}) == "?"
    end
  end
end
