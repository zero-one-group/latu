defmodule Latu.Integration.ShowTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO
  import Latu.Column

  alias Latu.Error

  # Needs a Spark Connect server on :15003 — docker compose up -d spark-reattach.
  @moduletag :integration
  @moduletag :capture_log

  # Spark renders this; Latu formats nothing. showString ends in a newline and IO.puts adds
  # another, which is exactly what PySpark's print() does.
  @table """
  +---+
  | id|
  +---+
  |  0|
  |  1|
  |  2|
  |  3|
  |  4|
  +---+
  """

  setup do
    session = Latu.connect!("sc://localhost:15003")
    on_exit(fn -> Latu.disconnect(session, release: true) end)
    %{session: session}
  end

  test "prints Spark's table byte for byte", %{session: session} do
    df = Latu.range(session, 5)

    assert capture_io(fn -> assert Latu.show(df) == :ok end) == @table <> "\n"
  end

  test "num_rows bounds the table", %{session: session} do
    output = capture_io(fn -> Latu.show(Latu.range(session, 100), num_rows: 2) end)

    assert output =~ "|  0|\n|  1|\n"
    refute output =~ "|  2|"
    assert output =~ "only showing top 2 rows"
  end

  test "vertical renders one block per row", %{session: session} do
    output = capture_io(fn -> Latu.show(Latu.range(session, 1), vertical: true) end)

    assert output =~ "-RECORD 0"
    assert output =~ ~r/id\s+\|\s+0/
  end

  test "show!/2 returns :ok", %{session: session} do
    df = Latu.range(session, 1)

    assert capture_io(fn -> assert Latu.show!(df) == :ok end) =~ "|  0|"
  end

  test "show!/2 raises what show/2 returns", %{session: session} do
    df = %{Latu.range(session, 1) | session: %{session | channel: nil}}

    assert_raise Error, ~r/not connected/, fn -> Latu.show!(df) end
  end

  # `Dataset.htmlString(numRows, truncate)` calls the **same `getRows`** `showString` does, so
  # `truncate` behaves identically. Kino goes through this call too, so this is also the only
  # coverage Livebook rendering has.
  describe "to_html/2" do
    test "is Spark's own table, not something Latu formats", %{session: session} do
      html = Latu.to_html!(Latu.range(session, 3))

      assert html =~ "<table border='1'>"
      assert html =~ "<tr><th>id</th></tr>"
      assert html =~ "<tr><td>0</td></tr>"
      assert html =~ "</table>"
    end

    test "num_rows bounds it, with the same footer show/2 gets", %{session: session} do
      html = Latu.to_html!(Latu.range(session, 100), num_rows: 2)

      assert html =~ "<tr><td>0</td></tr>"
      refute html =~ "<tr><td>2</td></tr>"
      assert html =~ "only showing top 2 rows"
    end

    test "truncate cuts a cell the way show/2 does", %{session: session} do
      df = Latu.select(Latu.range(session, 1), long: lit("abcdefghij"))

      # getRows keeps `truncate - 3` characters and appends an ellipsis, for truncate >= 4.
      assert Latu.to_html!(df, truncate: 5) =~ "<tr><td>ab...</td></tr>"
      assert Latu.to_html!(df, truncate: false) =~ "<tr><td>abcdefghij</td></tr>"
    end

    test "a value is HTML-escaped, so data cannot inject markup into a cell", %{
      session: session
    } do
      df = Latu.select(Latu.range(session, 1), tag: lit("<b>hi</b>"))

      html = Latu.to_html!(df, truncate: false)

      assert html =~ "&lt;b&gt;hi&lt;/b&gt;"
      refute html =~ "<b>hi</b>"
    end

    test "to_html/2 reports what to_html!/2 raises", %{session: session} do
      df = %{Latu.range(session, 1) | session: %{session | channel: nil}}

      assert {:error, %Error{kind: :connect}} = Latu.to_html(df)
    end
  end
end
