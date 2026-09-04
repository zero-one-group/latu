defmodule Latu.Integration.ReadTest do
  use ExUnit.Case, async: true

  alias Latu.Functions, as: F

  # Needs a Spark Connect server on :15002 and the data fixtures the containers mount at
  # /fixtures — `mix fixtures`.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  @people [%{id: 1, name: "Ada"}, %{id: 2, name: "Grace"}, %{id: 3, name: "Linus"}]

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session) end)
    %{session: session}
  end

  describe "read/2" do
    test "csv with a schema", %{session: session} do
      rows =
        session
        |> Latu.read(
          format: "csv",
          schema: "id INT, name STRING",
          path: "/fixtures/people.csv",
          header: true
        )
        |> Latu.sort([:id])
        |> Latu.collect!()

      assert rows == @people
    end

    test "csv options actually reach the server: no header means a first row of strings",
         %{session: session} do
      {:ok, n} =
        session
        |> Latu.read(format: "csv", path: "/fixtures/people.csv", header: false)
        |> Latu.count()

      # the header line decodes as a data row
      assert n == 4
    end

    test "json", %{session: session} do
      rows =
        session
        |> Latu.read(format: "json", path: "/fixtures/people.json")
        |> Latu.select([:id, :name])
        |> Latu.sort([:id])
        |> Latu.collect!()

      assert rows == @people
    end

    test "parquet", %{session: session} do
      rows =
        session
        |> Latu.read(format: "parquet", path: "/fixtures/people.parquet")
        |> Latu.sort([:id])
        |> Latu.collect!()

      assert rows == @people
    end
  end

  describe "the parsing family" do
    test "from_json round-trips through to_json", %{session: session} do
      {:ok, rows} =
        session
        |> Latu.range(1)
        |> Latu.select(j: F.to_json(F.struct([:id])))
        |> Latu.select(s: F.from_json(:j, "id INT"))
        |> Latu.collect()

      assert rows == [%{s: %{"id" => 0}}]
    end

    test "options reach the server: a custom csv separator", %{session: session} do
      {:ok, rows} =
        session
        |> Latu.range(1)
        |> Latu.select(s: F.schema_of_csv("1;a", sep: ";"))
        |> Latu.collect()

      # with the default separator the whole sample is one column; _c1 proves sep arrived
      assert [%{s: schema}] = rows
      assert schema =~ "_c1"
    end
  end
end
