defmodule Latu.Integration.JoinTest do
  use ExUnit.Case, async: true

  # What only the server can answer: whether it takes `join_as_of` at all (PySpark's `_joinAsOf`
  # is private, called only by pandas-on-Spark's merge_asof) and on what key types; whether the
  # released server implements `nearest_by_join`, the newest arm in the vendored proto; and
  # whether the right side of `lateral_join` and `nearest_by_join` can reference the left by
  # qualified name through `as/2` plus `expr/1`, since Spark refuses a tagged cross-frame
  # reference.
  #
  # Columns are keyword lists, never row maps: a map sorts its keys and a positional `schema:`
  # then casts the wrong column to each type, silently.
  # Needs a Spark Connect server on :15002.
  import Latu.Column

  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session) end)

    quotes =
      Latu.create_dataframe!(session, [t: [1, 3, 5], price: [10.0, 30.0, 50.0]],
        schema: "t BIGINT, price DOUBLE"
      )

    trades =
      Latu.create_dataframe!(session, [t: [2, 4, 6], size: [100, 200, 300]],
        schema: "t BIGINT, size INT"
      )

    %{session: session, quotes: quotes, trades: trades}
  end

  describe "join_as_of/3" do
    test "backward matches the latest earlier row", %{trades: trades, quotes: quotes} do
      rows =
        trades
        |> Latu.join_as_of(quotes, left_as_of: :t, right_as_of: :t)
        |> Latu.select([:size, :price])
        |> Latu.sort([:size])
        |> Latu.collect!()

      # trade@2 -> quote@1, trade@4 -> quote@3, trade@6 -> quote@5.
      assert rows == [
               %{size: 100, price: 10.0},
               %{size: 200, price: 30.0},
               %{size: 300, price: 50.0}
             ]
    end

    test "forward matches the earliest later row", %{trades: trades, quotes: quotes} do
      rows =
        trades
        |> Latu.join_as_of(quotes, left_as_of: :t, right_as_of: :t, direction: :forward)
        |> Latu.select([:size, :price])
        |> Latu.sort([:size])
        |> Latu.collect!()

      # trade@2 -> quote@3, trade@4 -> quote@5, trade@6 -> nothing, and inner drops it.
      assert rows == [%{size: 100, price: 30.0}, %{size: 200, price: 50.0}]
    end

    test "a left join keeps the unmatched row with nulls", %{trades: trades, quotes: quotes} do
      rows =
        trades
        |> Latu.join_as_of(quotes,
          left_as_of: :t,
          right_as_of: :t,
          direction: :forward,
          how: :left
        )
        |> Latu.select([:size, :price])
        |> Latu.sort([:size])
        |> Latu.collect!()

      assert rows == [
               %{size: 100, price: 30.0},
               %{size: 200, price: 50.0},
               %{size: 300, price: nil}
             ]
    end

    test "a tolerance drops a match that is too far away", %{trades: trades, quotes: quotes} do
      # Every backward gap here is exactly 1, so a tolerance of 0 keeps nothing.
      assert trades
             |> Latu.join_as_of(quotes, left_as_of: :t, right_as_of: :t, tolerance: 0)
             |> Latu.count!() == 0

      assert trades
             |> Latu.join_as_of(quotes, left_as_of: :t, right_as_of: :t, tolerance: 1)
             |> Latu.count!() == 3
    end

    # Keys are 1, 3, 5 on both sides. Allowing exact matches, every left row matches its own
    # key. Refusing them, each left row falls back to the latest STRICTLY earlier key — so
    # t=3 takes t=1 and t=5 takes t=3, and only t=1 has nothing to fall back to. Asserting the
    # prices rather than the count is what shows the skip actually happened.
    test "allow_exact_matches: false falls back to the previous key", %{
      quotes: quotes,
      session: session
    } do
      # Distinct column names on the right, so nothing needs qualifying: with the same names on
      # both sides every output column is duplicated and `col("t")` is ambiguous.
      same =
        Latu.create_dataframe!(session, [u: [1, 3, 5], mark: [10.0, 30.0, 50.0]],
          schema: "u BIGINT, mark DOUBLE"
        )

      matched = fn opts ->
        quotes
        |> Latu.join_as_of(same, Keyword.merge([left_as_of: :t, right_as_of: :u], opts))
        |> Latu.select([:t, :mark])
        |> Latu.sort([:t])
        |> Latu.collect!()
      end

      assert matched.([]) == [
               %{t: 1, mark: 10.0},
               %{t: 3, mark: 30.0},
               %{t: 5, mark: 50.0}
             ]

      assert matched.(allow_exact_matches: false) == [
               %{t: 3, mark: 10.0},
               %{t: 5, mark: 30.0}
             ]
    end

    test "a frame joined to itself is ambiguous until each side is aliased", %{quotes: quotes} do
      assert {:error, %Latu.Error{message: message}} =
               quotes
               |> Latu.join_as_of(quotes, left_as_of: :t, right_as_of: :t)
               |> Latu.collect()

      assert message =~ "AMBIGUOUS_COLUMN_REFERENCE"

      aliased = Latu.as(quotes, "l")
      other = Latu.as(quotes, "r")

      assert aliased
             |> Latu.join_as_of(other, left_as_of: :t, right_as_of: :t)
             |> Latu.count!() == 3
    end
  end

  describe "lateral_join/3" do
    test "with no condition, every left row sees every right row", %{session: session} do
      left = Latu.range(session, 3)
      right = Latu.range(session, 2)

      assert left |> Latu.lateral_join(right) |> Latu.count!() == 6
    end

    # The interesting one: the right side referencing the left by qualified name, which is where
    # Spark's own examples use it.
    test "the right side can reference the left by qualified name", %{
      quotes: quotes,
      session: session
    } do
      left = Latu.as(quotes, "l")
      right = Latu.filter(Latu.range(session, 10), expr("id < l.t"))

      rows =
        left
        |> Latu.lateral_join(right)
        |> Latu.agg(n: Latu.Functions.count(lit(1)))
        |> Latu.collect!()

      # ids below 1, 3 and 5 respectively: 1 + 3 + 5 = 9.
      assert rows == [%{n: 9}]
    end

    test "a left lateral join keeps a row whose right side is empty", %{session: session} do
      left = Latu.as(Latu.range(session, 2), "l")
      right = Latu.filter(Latu.range(session, 10), expr("id < l.id"))

      assert left |> Latu.lateral_join(right, how: :left) |> Latu.count!() == 2
    end
  end

  describe "nearest_by_join/4" do
    test "keeps the closest right rows by distance", %{session: session} do
      left = Latu.as(Latu.range(session, 3), "q")
      right = Latu.as(Latu.range(session, 10), "b")

      rows =
        left
        |> Latu.nearest_by_join(right, expr("abs(q.id - b.id)"),
          num_results: 2,
          mode: :exact,
          direction: :distance
        )
        |> Latu.count!()

      # Two matches per left row, three left rows.
      assert rows == 6
    end
  end
end
