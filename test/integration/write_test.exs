defmodule Latu.Integration.WriteTest do
  use ExUnit.Case, async: false

  # The dev server runs --master local[1] (docker-compose.yml): concurrent write tasks in the
  # Docker Desktop VM intermittently stalled ~60s in lockstep, PySpark included;
  # docs/decisions.md "The write-stall verdict". If the stall ever returns, dev/probe_writes.exs
  # and dev/probe_writes.py are the instruments.
  #
  # async: false: write jobs are heavyweight, and these tests share the server's /tmp and
  # warehouse.
  @moduletag :integration
  @moduletag :capture_log

  @url "sc://localhost:15002"

  setup do
    session = Latu.connect!(@url)
    on_exit(fn -> Latu.disconnect(session, release: true) end)
    %{session: session}
  end

  # The server's /tmp outlives the BEAM, and `System.unique_integer/1` restarts from a small
  # number in every new VM, so paths carry a run token to stay unique across runs.
  @run System.system_time(:millisecond)

  defp tmp_path, do: "/tmp/latu_test/#{@run}_#{System.unique_integer([:positive])}"

  describe "write/2" do
    test "parquet write → read-back round trip", %{session: session} do
      path = tmp_path()

      assert session |> Latu.range(5) |> Latu.write(format: "parquet", path: path) == :ok

      assert session
             |> Latu.read(format: "parquet", path: path)
             |> Latu.sort([:id])
             |> Latu.collect!() == Enum.map(0..4, &%{id: &1})
    end

    test ":overwrite replaces, :error refuses what exists", %{session: session} do
      path = tmp_path()
      df = Latu.range(session, 3)

      :ok = Latu.write(df, format: "parquet", path: path)
      :ok = Latu.write(df, format: "parquet", path: path, mode: :overwrite)

      assert {:ok, 3} = session |> Latu.read(format: "parquet", path: path) |> Latu.count()

      assert {:error, error} = Latu.write(df, format: "parquet", path: path, mode: :error)
      assert Exception.message(error) =~ ~r/exist/i
    end

    test "writer options reach the server: csv with a header", %{session: session} do
      path = tmp_path()

      :ok = session |> Latu.range(3) |> Latu.write(format: "csv", path: path, header: true)

      assert {:ok, 3} =
               session |> Latu.read(format: "csv", path: path, header: true) |> Latu.count()
    end
  end

  # Unique names: the catalog is in-memory and resets with the server, but the warehouse
  # directory is bind-mounted and persists, so a fixed name with :overwrite hits
  # LOCATION_ALREADY_EXISTS after a server restart — overwrite can only drop what the catalog
  # knows about. `drop_after/1` cleans the table AND its warehouse directory up while the
  # catalog still knows both.
  # cleans the table AND its warehouse directory up while the catalog still knows both.
  test "save_as_table, insert_into, read back through table/2", %{session: session} do
    table = drop_after("latu_tbl_#{System.unique_integer([:positive])}")
    df = Latu.range(session, 5)

    assert Latu.save_as_table(df, table) == :ok
    assert Latu.insert_into(df, table) == :ok
    assert {:ok, 10} = session |> Latu.table(table) |> Latu.count()
  end

  test "write_v2 create and read back", %{session: session} do
    # :create refuses an existing table, so unique here too.
    table = drop_after("latu_v2_#{System.unique_integer([:positive])}")

    assert session |> Latu.range(4) |> Latu.write_v2(table, mode: :create, using: "parquet") ==
             :ok

    assert {:ok, 4} = session |> Latu.table(table) |> Latu.count()
  end

  # A fresh session for the cleanup: `on_exit` runs after the test's own session is torn down.
  # `if_exists:` because a test that failed before writing left nothing to drop.
  defp drop_after(table) do
    on_exit(fn ->
      session = Latu.connect!(@url)
      Latu.Catalog.drop_table(session, table, if_exists: true)
      Latu.disconnect(session, release: true)
    end)

    table
  end
end
