defmodule Latu.Fixtures do
  @moduledoc false
  # The integration suite's data files, written by `dev/make_data_fixtures.py`. Generated per
  # checkout rather than committed — dev/README.md says why. Loaded by test/test_helper.exs.

  @dir "fixtures"
  @required ~w(people.csv people.json people.parquet measurements.parquet)

  @doc "Refuse to start an integration run whose data files are missing, naming the command."
  @spec check!(keyword()) :: :ok
  def check!(config) do
    if :integration in Keyword.get(config, :include, []) do
      case Enum.reject(@required, &File.exists?(Path.join(@dir, &1))) do
        [] -> :ok
        missing -> raise missing_message(missing)
      end
    end

    :ok
  end

  defp missing_message(missing) do
    """

    The integration suite reads data files that are not in the repo: they are generated per
    checkout rather than vendored, so the suite stays hermetic and carries no third-party
    data licence. See dev/README.md.

    Missing: #{Enum.map_join(missing, ", ", &Path.join(@dir, &1))}

        mix fixtures

    Offline tests need none of this — `mix check` runs on a fresh clone.
    """
  end
end
