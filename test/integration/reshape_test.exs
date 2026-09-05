defmodule Latu.Integration.ReshapeTest do
  use ExUnit.Case, async: true

  # The golden fixtures prove each plan is PySpark's; what they cannot see is whether the server
  # accepts it and what it does with it. Three of these are real questions rather than smoke
  # tests:
  #
  #   * `random_split/3` claims its slices *partition* the frame. That rests on Spark sampling
  #     a stable row order with non-overlapping half-open windows, which is what
  #     `deterministic_order` and the shared seed are for. If it is wrong, rows go missing or
  #     get counted twice — silently.
  #   * `grouping_sets/3` sends an **empty** grouping set for the grand total. Legal in SQL;
  #     nothing offline proves Catalyst takes it in this position.
  #   * `transpose/2` is a Spark 4 addition and needs every non-index column to share a type.
  #     Its rules are not written down anywhere Latu can read.
  #
  # Needs a Spark Connect server on :15003.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15003"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session, release: true) end)
    %{session: session}
  end

  describe "tail/2" do
    test "the LAST rows, which is the whole point and no fixture can show it", %{
      session: session
    } do
      assert Latu.tail!(Latu.range(session, 10), 3) == [%{id: 7}, %{id: 8}, %{id: 9}]
    end

    test "more than there are gives everything, not an error", %{session: session} do
      assert length(Latu.tail!(Latu.range(session, 3), 10)) == 3
    end

    test "zero rows is empty", %{session: session} do
      assert Latu.tail!(Latu.range(session, 3), 0) == []
    end
  end

  describe "random_split/3" do
    test "the slices partition the frame: nothing lost, nothing counted twice", %{
      session: session
    } do
      df = Latu.range(session, 1000)

      ids =
        df
        |> Latu.random_split([0.7, 0.3], seed: 42)
        |> Enum.flat_map(fn slice -> slice |> Latu.collect!() |> Enum.map(& &1.id) end)

      assert length(ids) == 1000
      assert Enum.sort(ids) == Enum.to_list(0..999)
    end

    test "three slices partition it too, and roughly by weight", %{session: session} do
      counts =
        session
        |> Latu.range(1000)
        |> Latu.random_split([0.5, 0.3, 0.2], seed: 7)
        |> Enum.map(&Latu.count!/1)

      assert Enum.sum(counts) == 1000

      # Bernoulli sampling, so the proportions are approximate; wide bounds, since the point
      # is that the weights are respected at all, not that the RNG is fair.
      [big, middle, small] = counts
      assert big > middle and middle > small
    end

    test "the same seed gives the same slices twice", %{session: session} do
      df = Latu.range(session, 200)

      first = df |> Latu.random_split([1, 1], seed: 99) |> hd() |> Latu.collect!()
      again = df |> Latu.random_split([1, 1], seed: 99) |> hd() |> Latu.collect!()

      assert first == again
    end
  end

  describe "unpivot/3" do
    # Columns as a KEYWORD LIST, not a list of row maps: a map sorts its keys, so
    # `%{id: 1, jan: 10.0, feb: 20.0}` builds columns in the order feb, id, jan and a positional
    # `schema:` then casts the wrong column to each type — silently. `columns_for/1` says so.
    setup %{session: session} do
      columns = [id: [1, 2], jan: [10.0, 30.0], feb: [20.0, 40.0]]

      %{
        wide: Latu.create_dataframe!(session, columns, schema: "id INT, jan DOUBLE, feb DOUBLE")
      }
    end

    test "two value columns become two rows each", %{wide: wide} do
      rows =
        wide
        |> Latu.unpivot([:id],
          values: [:jan, :feb],
          variable_column_name: "month",
          value_column_name: "sales"
        )
        |> Latu.sort([:id, :month])
        |> Latu.collect!()

      assert rows == [
               %{id: 1, month: "feb", sales: 20.0},
               %{id: 1, month: "jan", sales: 10.0},
               %{id: 2, month: "feb", sales: 40.0},
               %{id: 2, month: "jan", sales: 30.0}
             ]
    end

    test "absent values means every non-id column, worked out by the server", %{wide: wide} do
      named =
        wide
        |> Latu.unpivot([:id],
          values: [:jan, :feb],
          variable_column_name: "month",
          value_column_name: "sales"
        )
        |> Latu.sort([:id, :month])
        |> Latu.collect!()

      inferred =
        wide
        |> Latu.unpivot([:id], variable_column_name: "month", value_column_name: "sales")
        |> Latu.sort([:id, :month])
        |> Latu.collect!()

      assert inferred == named
    end
  end

  describe "grouping_sets/3" do
    setup %{session: session} do
      columns = [
        region: ["north", "north", "south"],
        year: [2025, 2026, 2025],
        sales: [1.0, 2.0, 4.0]
      ]

      %{
        df:
          Latu.create_dataframe!(session, columns,
            schema: "region STRING, year INT, sales DOUBLE"
          )
      }
    end

    test "an empty set is the grand total, and the server takes it", %{df: df} do
      rows =
        df
        |> Latu.grouping_sets([[:region, :year], [:region], []], [:region, :year])
        |> Latu.agg(total: Latu.Functions.sum(:sales))
        |> Latu.collect!()

      # 3 region+year groups, 2 regions, 1 grand total.
      assert length(rows) == 6
      assert Enum.any?(rows, &(&1.region == nil and &1.year == nil and &1.total == 7.0))
      assert Enum.any?(rows, &(&1.region == "north" and &1.year == nil and &1.total == 3.0))
    end

    test "the same sets rollup would produce give rollup's answer", %{df: df} do
      sets =
        df
        |> Latu.grouping_sets([[:region, :year], [:region], []], [:region, :year])
        |> Latu.agg(total: Latu.Functions.sum(:sales))
        |> Latu.collect!()
        |> Enum.sort_by(&{&1.region, &1.year})

      rolled =
        df
        |> Latu.rollup([:region, :year])
        |> Latu.agg(total: Latu.Functions.sum(:sales))
        |> Latu.collect!()
        |> Enum.sort_by(&{&1.region, &1.year})

      assert sets == rolled
    end
  end

  describe "hint/3 and select_expr/2" do
    test "a valid hint changes the plan and not the answer", %{session: session} do
      plain = session |> Latu.range(5) |> Latu.collect!()
      hinted = session |> Latu.range(5) |> Latu.hint("broadcast") |> Latu.collect!()

      assert hinted == plain
    end

    test "a column parameter reaches the server as a literal or as a reference alike", %{
      session: session
    } do
      # `Latu.hint/3`'s docs claim both spellings are valid plans. The server's `transformHint`
      # turns a string literal into an attribute and keeps an attribute as it is, so the two
      # must partition identically; if either is refused, the docstring is wrong, not the test.
      plain = session |> Latu.range(6) |> Latu.collect!()

      for column <- ["id", :id] do
        hinted =
          session
          |> Latu.range(6)
          |> Latu.hint("repartition", [2, column])
          |> Latu.collect!()
          |> Enum.sort_by(& &1.id)

        assert hinted == plain, "hint(\"repartition\", [2, #{inspect(column)}])"
      end
    end

    test "an unknown hint is ignored rather than refused, which is Spark's own rule", %{
      session: session
    } do
      assert session
             |> Latu.range(5)
             |> Latu.hint("not_a_real_hint", [1])
             |> Latu.collect!() == Enum.map(0..4, &%{id: &1})
    end

    test "select_expr takes any SQL the server can parse", %{session: session} do
      assert session
             |> Latu.range(3)
             |> Latu.select_expr(["id", "id * 2 as doubled"])
             |> Latu.collect!() == [
               %{id: 0, doubled: 0},
               %{id: 1, doubled: 2},
               %{id: 2, doubled: 4}
             ]
    end
  end

  describe "to/2, over parse_ddl_type/2" do
    setup %{session: session} do
      columns = [id: [1, 2], name: ["a", "b"]]
      %{df: Latu.create_dataframe!(session, columns, schema: "id INT, name STRING")}
    end

    test "matches by NAME: reorders, and widens int to bigint", %{session: session, df: df} do
      type = Latu.parse_ddl_type!(session, "name STRING, id BIGINT")
      reconciled = Latu.to(df, type)

      assert Latu.dtypes!(reconciled) == [{"name", "string"}, {"id", "bigint"}]

      assert Latu.collect!(Latu.sort(reconciled, [:id])) == [
               %{name: "a", id: 1},
               %{name: "b", id: 2}
             ]
    end

    test "projects away a column the target does not name", %{session: session, df: df} do
      assert Latu.columns!(Latu.to(df, Latu.parse_ddl_type!(session, "id INT"))) == ["id"]
    end

    # Measured, and it contradicts `Dataset.to`'s own scaladoc: `Project.reorderFields` fills a
    # missing *nullable* target field with a null literal, and only a non-nullable one raises.
    # DDL declares nullable by default, so this is the case people hit first.
    test "a missing column the target lets be null becomes nulls, not an error", %{
      session: session,
      df: df
    } do
      type = Latu.parse_ddl_type!(session, "id INT, absent STRING")

      assert Latu.collect!(Latu.to(df, type)) == [
               %{id: 1, absent: nil},
               %{id: 2, absent: nil}
             ]
    end

    test "a missing NOT NULL column is the error the scaladoc promises", %{
      session: session,
      df: df
    } do
      type = Latu.parse_ddl_type!(session, "id INT, absent STRING NOT NULL")

      assert {:error, %Latu.Error{kind: :rpc}} = Latu.collect(Latu.to(df, type))
    end

    test "an incompatible cast is refused rather than nulled", %{session: session, df: df} do
      type = Latu.parse_ddl_type!(session, "id INT, name INT")

      assert {:error, %Latu.Error{kind: :rpc}} = Latu.collect(Latu.to(df, type))
    end

    test "parse_ddl_type refuses nonsense DDL", %{session: session} do
      assert {:error, %Latu.Error{kind: :rpc}} = Latu.parse_ddl_type(session, "not a type at all")
    end
  end

  describe "transpose/2" do
    test "the index column's values become column names", %{session: session} do
      columns = [metric: ["alpha", "beta"], q1: [1.0, 3.0], q2: [2.0, 4.0]]

      df =
        Latu.create_dataframe!(session, columns, schema: "metric STRING, q1 DOUBLE, q2 DOUBLE")

      transposed = df |> Latu.transpose(:metric) |> Latu.collect!()

      # One row per old column, one column per old index value — so `alpha` holds what the
      # alpha row held, read downwards. Guessing `key` for the name column turned out right.
      assert Enum.map(transposed, & &1.key) == ["q1", "q2"]
      assert Enum.map(transposed, & &1.alpha) == [1.0, 2.0]
      assert Enum.map(transposed, & &1.beta) == [3.0, 4.0]
    end
  end
end
