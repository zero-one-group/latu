defmodule Latu.SqlGolden do
  @moduledoc false
  # Result goldens: what the *server* answers, where test/wire holds what Latu sends.
  # Loaded by test/test_helper.exs. Modes, regeneration and why: dev/README.md.

  import ExUnit.Assertions
  import ExUnit.CaptureIO

  @dir "test/sql"

  @doc "Every case name, taken from the `.sql` files on disk."
  @spec cases() :: [String.t()]
  def cases do
    @dir
    |> Path.join("*.sql")
    |> Path.wildcard()
    |> Enum.map(&Path.basename(&1, ".sql"))
    |> Enum.sort()
  end

  @doc """
  `:ok` when every case has a golden checked in, raising by name when one does not.

  Answers `:ok` whenever `LATU_GOLDEN` is set: that run is the one *writing* the goldens, so
  whether they exist yet races the module writing them rather than being an invariant.
  """
  @spec check!() :: :ok
  def check! do
    if System.get_env("LATU_GOLDEN"), do: :ok, else: missing!(cases())
  end

  defp missing!([]) do
    raise "test/sql holds no .sql files, so the result goldens assert nothing"
  end

  defp missing!(cases) do
    case Enum.reject(cases, &File.exists?(Path.join(@dir, "#{&1}.answer"))) do
      [] ->
        :ok

      missing ->
        raise "no golden for #{Enum.join(missing, ", ")}; generate them with " <>
                "LATU_GOLDEN=overwrite mix test --include golden (dev/README.md)"
    end
  end

  @doc "The query one case runs."
  @spec query(String.t()) :: String.t()
  def query(name), do: @dir |> Path.join("#{name}.sql") |> File.read!() |> String.trim()

  @doc """
  Assert a case still answers as its `.answer` file says.

  `LATU_GOLDEN=overwrite` rewrites the file rather than asserting; `report` writes a `.actual`
  beside it and asserts nothing, which is how the newer-server run shows its diff without
  calling a rendering change a failure. dev/README.md.
  """
  @spec assert_answer(Latu.Session.t(), String.t()) :: :ok
  def assert_answer(session, name) do
    actual = answer(session, name)
    path = Path.join(@dir, "#{name}.answer")

    case System.get_env("LATU_GOLDEN") do
      "overwrite" ->
        File.write!(path, actual)

      "report" ->
        File.write!(Path.join(@dir, "#{name}.actual"), actual)

      _assert ->
        assert File.exists?(path),
               "no #{path}; generate it with LATU_GOLDEN=overwrite (dev/README.md)"

        assert actual == File.read!(path)
    end

    :ok
  end

  # The schema as well as the table: `show` renders values, so a column whose *type* changed
  # between servers renders byte-identically. The type line is the half that catches that.
  defp answer(session, name) do
    df = Latu.sql!(session, query(name))
    schema = Enum.map_join(Latu.schema!(df), "\n", &field/1)
    table = capture_io(fn -> Latu.show!(df, truncate: false) end)

    "# schema\n#{schema}\n\n# rows\n#{String.trim_trailing(table)}\n"
  end

  defp field(%{name: name, type: type, nullable: true}), do: "#{name} #{type} nullable"
  defp field(%{name: name, type: type, nullable: false}), do: "#{name} #{type} not null"
end
