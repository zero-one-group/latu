defmodule Latu.Integration.DtypesTest do
  use ExUnit.Case, async: true

  import Latu.Column

  alias Explorer.DataFrame, as: EDF

  # The Spark ↔ Explorer type mapping, pinned by measurement rather than by reading — the
  # decode path is Polars' and no fixture can vouch for it. If a row here goes red on a Spark
  # or Explorer upgrade, the mapping moved and Latu.Result.Schema's expectations need a look.
  # Needs a Spark Connect server on :15002 — docker compose up -d spark-connect.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session) end)
    %{session: session}
  end

  test "every decodable Spark type arrives as the expected Explorer dtype", %{session: session} do
    df =
      session
      |> Latu.range(1)
      |> Latu.select(
        bool: expr("true"),
        byte: cast(1, "tinyint"),
        short: cast(1, "smallint"),
        int: cast(1, "int"),
        long: :id,
        float: cast(1.5, "float"),
        double: cast(1.5, "double"),
        dec: cast("1.50", "decimal(10,2)"),
        str: expr("'a'"),
        chr: cast("a", "char(3)"),
        vchr: cast("a", "varchar(3)"),
        bin: cast("a", "binary"),
        d: cast("2026-01-02", "date"),
        ts: cast("2026-01-02 03:04:05", "timestamp"),
        ts_ntz: cast("2026-01-02 03:04:05", "timestamp_ntz"),
        nul: expr("NULL"),
        arr: expr("array(1, 2)"),
        strct: expr("named_struct('a', 1)"),
        mp: expr("map('k', 1)"),
        dur: expr("INTERVAL '1 02:03:04' DAY TO SECOND")
      )

    assert {:ok, frame} = Latu.to_explorer(df)
    dtypes = EDF.dtypes(frame)

    assert dtypes["bool"] == :boolean
    assert dtypes["byte"] == {:s, 8}
    assert dtypes["short"] == {:s, 16}
    assert dtypes["int"] == {:s, 32}
    assert dtypes["long"] == {:s, 64}
    assert dtypes["float"] == {:f, 32}
    assert dtypes["double"] == {:f, 64}
    assert dtypes["dec"] == {:decimal, 10, 2}
    assert dtypes["str"] == :string
    assert dtypes["chr"] == :string
    assert dtypes["vchr"] == :string
    assert dtypes["bin"] == :binary
    assert dtypes["d"] == :date
    assert dtypes["ts_ntz"] == {:naive_datetime, :microsecond}
    assert dtypes["nul"] == :null
    assert dtypes["arr"] == {:list, {:s, 32}}
    assert dtypes["strct"] == {:struct, [{"a", {:s, 32}}]}

    # Measured by dev/probe_dtypes.exs: a day-time interval reaches Arrow as duration(us). Its
    # year-month and calendar cousins panic and stay refused.
    assert dtypes["dur"] == {:duration, :microsecond}

    # The zone string is the server session's, so only the shape is Latu's to promise.
    assert match?({:datetime, :microsecond, _tz}, dtypes["ts"])

    # An Arrow map decodes as a list of key/value structs — Polars has no map dtype.
    assert match?({:list, {:struct, _fields}}, dtypes["mp"])
  end

  test "an empty result is a typed 0-row frame, from the schema-bearing batch", %{
    session: session
  } do
    assert {:ok, frame} = session |> Latu.range(0) |> Latu.to_explorer()
    assert EDF.n_rows(frame) == 0
    assert EDF.dtypes(frame) == %{"id" => {:s, 64}}

    df = session |> Latu.range(5) |> Latu.filter("id < 0") |> Latu.select([:id, s: expr("'x'")])
    assert {:ok, frame} = Latu.to_explorer(df)
    assert EDF.n_rows(frame) == 0
    assert EDF.dtypes(frame) == %{"id" => {:s, 64}, "s" => :string}
  end

  test "to_explorer is unbounded, and limit/2 is how you take part of a result", %{
    session: session
  } do
    # `to_explorer/2` once had a `limit:` defaulting to 100,000 rows; Spark bounds a result
    # with `limit(n)` in the plan, not with an argument to the action. 150,000 is past that old
    # default on purpose, so reintroducing one fails here rather than passing quietly.
    assert {:ok, whole} = session |> Latu.range(150_000) |> Latu.to_explorer()
    assert EDF.n_rows(whole) == 150_000

    assert {:ok, part} = session |> Latu.range(150_000) |> Latu.limit(10) |> Latu.to_explorer()
    assert EDF.n_rows(part) == 10
  end

  test "stream decodes one frame per batch and stopping early releases", %{session: session} do
    df = Latu.range(session, 1_000)

    total = df |> Latu.stream() |> Stream.map(&EDF.n_rows/1) |> Enum.sum()
    assert total == 1_000

    assert [%EDF{}] = df |> Latu.stream() |> Enum.take(1)

    # The session is still usable after abandoning a stream mid-result.
    assert session |> Latu.range(3) |> Latu.count() == {:ok, 3}
  end

  test "the guard also fronts the stream, before any decode", %{session: session} do
    df = session |> Latu.range(1) |> Latu.select(gap: expr("INTERVAL '1-2' YEAR TO MONTH"))

    assert_raise Latu.Error, ~r/column gap is a year-month interval/, fn ->
      df |> Latu.stream() |> Enum.to_list()
    end
  end

  test "to_arrow bypasses the decoder and the guard, on purpose", %{session: session} do
    df = session |> Latu.range(1) |> Latu.select(gap: expr("INTERVAL '1-2' YEAR TO MONTH"))

    # The same column collect/2 refuses passes through raw — these bytes are someone else's
    # Arrow reader's business.
    assert {:ok, [_ | _] = blobs} = Latu.to_arrow(df)
    assert Enum.all?(blobs, &match?(<<0xFF, 0xFF, 0xFF, 0xFF, _::binary>>, &1))
  end
end
