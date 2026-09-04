defmodule Latu.GlimpseTest do
  use ExUnit.Case, async: true

  alias Latu.DataFrame
  alias Latu.Session

  # The rendering *is* the feature here, so it is asserted directly: `glimpse_text/4` is
  # `@doc false` and public for exactly that, on `count_plan/1`'s precedent. What needs a
  # server — that the two round trips agree, and that `Rows:` is exact when it claims to be —
  # is test/integration/glimpse_test.exs.

  @types [{"id", "bigint"}, {"name", "string"}, {"score", "double"}]
  @rows [
    %{"id" => 1, "name" => "Ada", "score" => 1.5},
    %{"id" => 2, "name" => nil, "score" => 2.5}
  ]

  defp render(types, rows, label \\ "2", width \\ :infinity) do
    IO.iodata_to_binary(DataFrame.glimpse_text(types, rows, label, width))
  end

  defp line_of(rows, type, width) do
    render([{"name", type}], rows, "1", width) |> String.split("\n") |> Enum.at(2)
  end

  describe "the rendering" do
    test "is a header and one $ line per column, in the schema's order" do
      assert render(@types, @rows) == """
             Rows: 2
             Columns: 3
             $ id    <bigint> 1, 2
             $ name  <string> "Ada", nil
             $ score <double> 1.5, 2.5
             """
    end

    test "pads the names to the widest, so the types line up" do
      out = render([{"a", "int"}, {"loooong", "int"}], [%{"a" => 1, "loooong" => 2}], "1")

      assert out =~ "$ a       <int> 1"
      assert out =~ "$ loooong <int> 2"
    end

    test "an empty frame is the columns with no values, and no trailing space" do
      assert render(@types, [], "0") == """
             Rows: 0
             Columns: 3
             $ id    <bigint>
             $ name  <string>
             $ score <double>
             """
    end

    test "the label is passed through, because only the caller knows whether it is exact" do
      assert render(@types, @rows, "at least 10") =~ "Rows: at least 10\n"
    end

    test "no columns is a header and nothing else" do
      assert render([], [], "0") == "Rows: 0\nColumns: 0\n"
    end
  end

  describe "width" do
    setup do
      %{wide: [%{"name" => String.duplicate("x", 200)}]}
    end

    test "cuts a long line to exactly the width, ending in an ellipsis", %{wide: wide} do
      line = wide |> line_of("string", 40) |> String.trim_trailing("\n")

      assert String.length(line) == 40
      assert String.ends_with?(line, "…")
    end

    test ":infinity cuts nothing", %{wide: wide} do
      assert wide |> line_of("string", :infinity) |> String.length() > 200
    end
  end

  describe "the options" do
    setup do
      %{df: Latu.range(Session.from_url!("sc://h"), 5)}
    end

    test "num_rows is a positive integer, and the refusal names the cheaper calls", %{df: df} do
      assert_raise ArgumentError, ~r|^num_rows is a positive integer, not 0 — .*dtypes/1|, fn ->
        apply(Latu, :glimpse, [df, [num_rows: 0]])
      end
    end

    test "width is a positive integer or :infinity", %{df: df} do
      assert_raise ArgumentError, ~r/^width is a positive integer or :infinity, not 0$/, fn ->
        apply(Latu, :glimpse, [df, [width: 0]])
      end
    end

    test "a mistyped option is refused, not ignored", %{df: df} do
      assert_raise ArgumentError, ~r/num_row/, fn ->
        apply(Latu, :glimpse, [df, [num_row: 3]])
      end
    end
  end
end
