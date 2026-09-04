defmodule Latu.Catalog do
  @moduledoc """
  Spark's catalog: databases, tables, views, caching.

  Session-first and eager — every call answers from the server. Listings come back exactly as
  `Latu.collect/2` returns rows: maps with atom keys spelled the way the server spells its
  columns (`:tableType`, `:isTemporary`).

      Latu.Catalog.list_tables(session, pattern: "latu_*")
      #=> {:ok, [%{name: "latu_tbl_1", tableType: "MANAGED", isTemporary: false, ...}]}

  This is the useful subset of `pyspark.sql.Catalog`, not all of it: catalogs, databases,
  tables, views and the cache. Creating or describing a single object, and the function
  catalog, are left out — each is one `Latu.sql/2` away.
  """

  alias Latu.DataFrame
  alias Latu.Error
  alias Latu.Plan
  alias Latu.Session

  @typep void :: :ok | {:error, Error.t()}
  @typep result(kind) :: {:ok, kind} | {:error, Error.t()}

  # =============================================
  # Catalogs and databases
  # =============================================

  @doc "The current catalog's name."
  @spec current_catalog(Session.t()) :: result(String.t())
  def current_catalog(%Session{} = session), do: scalar(session, Plan.catalog(:current_catalog))

  @doc "Like `current_catalog/1`, raising on failure."
  @spec current_catalog!(Session.t()) :: String.t()
  def current_catalog!(%Session{} = session), do: unwrap!(current_catalog(session))

  @doc "Switch catalogs."
  @spec set_current_catalog(Session.t(), String.t() | atom()) :: void()
  def set_current_catalog(%Session{} = session, name) do
    void(session, Plan.catalog(:set_current_catalog, catalog_name: name(name)))
  end

  @doc "Like `set_current_catalog/2`, raising on failure."
  @spec set_current_catalog!(Session.t(), String.t() | atom()) :: :ok
  def set_current_catalog!(%Session{} = session, name) do
    unwrap!(set_current_catalog(session, name))
  end

  @doc "Every catalog, optionally filtered: `pattern: \"spark*\"` (SQL LIKE, `*` and `|`)."
  @spec list_catalogs(Session.t(), keyword()) :: result([map()])
  def list_catalogs(%Session{} = session, opts \\ []) do
    opts = Keyword.validate!(opts, pattern: nil)
    rows(session, Plan.catalog(:list_catalogs, pattern: opts[:pattern]))
  end

  @doc "Like `list_catalogs/2`, raising on failure."
  @spec list_catalogs!(Session.t(), keyword()) :: [map()]
  def list_catalogs!(%Session{} = session, opts \\ []), do: unwrap!(list_catalogs(session, opts))

  @doc "The current database's name."
  @spec current_database(Session.t()) :: result(String.t())
  def current_database(%Session{} = session) do
    scalar(session, Plan.catalog(:current_database))
  end

  @doc "Like `current_database/1`, raising on failure."
  @spec current_database!(Session.t()) :: String.t()
  def current_database!(%Session{} = session), do: unwrap!(current_database(session))

  @doc "Switch databases."
  @spec set_current_database(Session.t(), String.t() | atom()) :: void()
  def set_current_database(%Session{} = session, name) do
    void(session, Plan.catalog(:set_current_database, db_name: name(name)))
  end

  @doc "Like `set_current_database/2`, raising on failure."
  @spec set_current_database!(Session.t(), String.t() | atom()) :: :ok
  def set_current_database!(%Session{} = session, name) do
    unwrap!(set_current_database(session, name))
  end

  @doc "Every database, optionally filtered by `:pattern`."
  @spec list_databases(Session.t(), keyword()) :: result([map()])
  def list_databases(%Session{} = session, opts \\ []) do
    opts = Keyword.validate!(opts, pattern: nil)
    rows(session, Plan.catalog(:list_databases, pattern: opts[:pattern]))
  end

  @doc "Like `list_databases/2`, raising on failure."
  @spec list_databases!(Session.t(), keyword()) :: [map()]
  def list_databases!(%Session{} = session, opts \\ []) do
    unwrap!(list_databases(session, opts))
  end

  @doc "Whether the database exists."
  @spec database_exists(Session.t(), String.t() | atom()) :: result(boolean())
  def database_exists(%Session{} = session, name) do
    scalar(session, Plan.catalog(:database_exists, db_name: name(name)))
  end

  @doc "Like `database_exists/2`, raising on failure."
  @spec database_exists!(Session.t(), String.t() | atom()) :: boolean()
  def database_exists!(%Session{} = session, name), do: unwrap!(database_exists(session, name))

  # =============================================
  # Tables and views
  # =============================================

  @doc """
  Tables and views, temporary ones included.

  `db_name: "other"` looks elsewhere than the current database; `pattern: "latu_*"` filters.
  """
  @spec list_tables(Session.t(), keyword()) :: result([map()])
  def list_tables(%Session{} = session, opts \\ []) do
    opts = Keyword.validate!(opts, db_name: nil, pattern: nil)
    fields = [db_name: opts[:db_name] && name(opts[:db_name]), pattern: opts[:pattern]]
    rows(session, Plan.catalog(:list_tables, fields))
  end

  @doc "Like `list_tables/2`, raising on failure."
  @spec list_tables!(Session.t(), keyword()) :: [map()]
  def list_tables!(%Session{} = session, opts \\ []), do: unwrap!(list_tables(session, opts))

  @doc "The table's columns: name, dataType, nullable, partition and bucket flags."
  @spec list_columns(Session.t(), String.t() | atom(), keyword()) :: result([map()])
  def list_columns(%Session{} = session, table, opts \\ []) do
    opts = Keyword.validate!(opts, db_name: nil)
    fields = [table_name: name(table), db_name: opts[:db_name] && name(opts[:db_name])]
    rows(session, Plan.catalog(:list_columns, fields))
  end

  @doc "Like `list_columns/3`, raising on failure."
  @spec list_columns!(Session.t(), String.t() | atom(), keyword()) :: [map()]
  def list_columns!(%Session{} = session, table, opts \\ []) do
    unwrap!(list_columns(session, table, opts))
  end

  @doc "Whether the table or view exists. `db_name:` as in `list_tables/2`."
  @spec table_exists(Session.t(), String.t() | atom(), keyword()) :: result(boolean())
  def table_exists(%Session{} = session, table, opts \\ []) do
    opts = Keyword.validate!(opts, db_name: nil)
    fields = [table_name: name(table), db_name: opts[:db_name] && name(opts[:db_name])]
    scalar(session, Plan.catalog(:table_exists, fields))
  end

  @doc "Like `table_exists/3`, raising on failure."
  @spec table_exists!(Session.t(), String.t() | atom(), keyword()) :: boolean()
  def table_exists!(%Session{} = session, table, opts \\ []) do
    unwrap!(table_exists(session, table, opts))
  end

  @doc """
  Drop a table. `if_exists: true` tolerates a missing one; `purge: true` skips the trash.

  The inverse of `Latu.save_as_table/3` — what the write tests clean up with.
  """
  @spec drop_table(Session.t(), String.t() | atom(), keyword()) :: void()
  def drop_table(%Session{} = session, table, opts \\ []) do
    opts = Keyword.validate!(opts, if_exists: false, purge: false)
    fields = [table_name: name(table), if_exists: opts[:if_exists], purge: opts[:purge]]
    void(session, Plan.catalog(:drop_table, fields))
  end

  @doc "Like `drop_table/3`, raising on failure."
  @spec drop_table!(Session.t(), String.t() | atom(), keyword()) :: :ok
  def drop_table!(%Session{} = session, table, opts \\ []) do
    unwrap!(drop_table(session, table, opts))
  end

  @doc "Drop a (non-temporary) view. `if_exists: true` tolerates a missing one."
  @spec drop_view(Session.t(), String.t() | atom(), keyword()) :: void()
  def drop_view(%Session{} = session, view, opts \\ []) do
    opts = Keyword.validate!(opts, if_exists: false)
    void(session, Plan.catalog(:drop_view, view_name: name(view), if_exists: opts[:if_exists]))
  end

  @doc "Like `drop_view/3`, raising on failure."
  @spec drop_view!(Session.t(), String.t() | atom(), keyword()) :: :ok
  def drop_view!(%Session{} = session, view, opts \\ []) do
    unwrap!(drop_view(session, view, opts))
  end

  @doc "Drop a temp view. `{:ok, true}` when it existed — Spark answers rather than raising."
  @spec drop_temp_view(Session.t(), String.t() | atom()) :: result(boolean())
  def drop_temp_view(%Session{} = session, view) do
    scalar(session, Plan.catalog(:drop_temp_view, view_name: name(view)))
  end

  @doc "Like `drop_temp_view/2`, raising on failure."
  @spec drop_temp_view!(Session.t(), String.t() | atom()) :: boolean()
  def drop_temp_view!(%Session{} = session, view), do: unwrap!(drop_temp_view(session, view))

  @doc "Drop a global temp view; answers like `drop_temp_view/2`."
  @spec drop_global_temp_view(Session.t(), String.t() | atom()) :: result(boolean())
  def drop_global_temp_view(%Session{} = session, view) do
    scalar(session, Plan.catalog(:drop_global_temp_view, view_name: name(view)))
  end

  @doc "Like `drop_global_temp_view/2`, raising on failure."
  @spec drop_global_temp_view!(Session.t(), String.t() | atom()) :: boolean()
  def drop_global_temp_view!(%Session{} = session, view) do
    unwrap!(drop_global_temp_view(session, view))
  end

  @doc "Refresh Spark's metadata and cache for a table whose files changed underneath it."
  @spec refresh_table(Session.t(), String.t() | atom()) :: void()
  def refresh_table(%Session{} = session, table) do
    void(session, Plan.catalog(:refresh_table, table_name: name(table)))
  end

  @doc "Like `refresh_table/2`, raising on failure."
  @spec refresh_table!(Session.t(), String.t() | atom()) :: :ok
  def refresh_table!(%Session{} = session, table), do: unwrap!(refresh_table(session, table))

  # =============================================
  # Caching
  # =============================================

  @doc "Cache the table at Spark's default storage level (docs/deviations.md)."
  @spec cache_table(Session.t(), String.t() | atom()) :: void()
  def cache_table(%Session{} = session, table) do
    void(session, Plan.catalog(:cache_table, table_name: name(table)))
  end

  @doc "Like `cache_table/2`, raising on failure."
  @spec cache_table!(Session.t(), String.t() | atom()) :: :ok
  def cache_table!(%Session{} = session, table), do: unwrap!(cache_table(session, table))

  @doc "Drop the table from the cache."
  @spec uncache_table(Session.t(), String.t() | atom()) :: void()
  def uncache_table(%Session{} = session, table) do
    void(session, Plan.catalog(:uncache_table, table_name: name(table)))
  end

  @doc "Like `uncache_table/2`, raising on failure."
  @spec uncache_table!(Session.t(), String.t() | atom()) :: :ok
  def uncache_table!(%Session{} = session, table), do: unwrap!(uncache_table(session, table))

  @doc "Whether the table is cached."
  @spec is_cached(Session.t(), String.t() | atom()) :: result(boolean())
  def is_cached(%Session{} = session, table) do
    scalar(session, Plan.catalog(:is_cached, table_name: name(table)))
  end

  @doc "Like `is_cached/2`, raising on failure."
  @spec is_cached!(Session.t(), String.t() | atom()) :: boolean()
  def is_cached!(%Session{} = session, table), do: unwrap!(is_cached(session, table))

  @doc "Drop every cached table."
  @spec clear_cache(Session.t()) :: void()
  def clear_cache(%Session{} = session), do: void(session, Plan.catalog(:clear_cache))

  @doc "Like `clear_cache/1`, raising on failure."
  @spec clear_cache!(Session.t()) :: :ok
  def clear_cache!(%Session{} = session), do: unwrap!(clear_cache(session))

  # =============================================
  # The shared tails
  # =============================================

  # A catalog operation is a relation that answers eagerly — run it, read the rows.
  defp rows(session, relation) do
    session |> DataFrame.new(relation) |> DataFrame.collect()
  end

  # A one-value answer (a name, a boolean) arrives as one single-column row; PySpark reads
  # `table[0][0]` positionally and so does this.
  defp scalar(session, relation) do
    case rows(session, relation) do
      {:ok, [row]} when map_size(row) == 1 ->
        {:ok, row |> Map.values() |> hd()}

      {:ok, rows} ->
        {:error, Error.new(:decode, "expected one single-column row: #{inspect(rows)}")}

      {:error, _} = error ->
        error
    end
  end

  # Void operations still answer with a (contentless) result; run and discard, like a write.
  defp void(session, relation) do
    with {:ok, _rows} <- rows(session, relation), do: :ok
  end

  defp name(value) when is_binary(value), do: value
  defp name(value) when is_atom(value) and not is_nil(value), do: Atom.to_string(value)

  defp name(value) do
    raise ArgumentError, "a catalog name is a string or an atom, not #{inspect(value)}"
  end

  defp unwrap!(:ok), do: :ok
  defp unwrap!({:ok, value}), do: value
  defp unwrap!({:error, error}), do: raise(error)
end
