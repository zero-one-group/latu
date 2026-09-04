defmodule Latu do
  @moduledoc """
  A native Elixir DataFrame API for Apache Spark, over Spark Connect.

  **Latu adds nothing to your supervision tree.** It defines no GenServer, Agent, Supervisor,
  Registry or pool, and its `mix.exs` declares no application callback module, so adding it
  to your dependencies starts nothing at all. `Latu.Session` is a plain struct; where it
  lives is your application's business, not Latu's.

      {:ok, session} = Latu.connect("sc://localhost:15002")
      Latu.disconnect(session)

  Two things it does hold, both in structs you keep: **the channel is a process** — `connect/2`
  opens it, `disconnect/2` closes it, and an execution's response stream is linked to whoever
  consumes it — and **a checkpoint is a server-side resource**, freed by `release/1` or scoped by
  `with_checkpoint/3`, because there is no finalizer to free it for you. Latu hands out
  resources and never keeps them; nothing is tracked between calls.

  That is the line structured streaming falls the wrong side of, and why it is a separate
  package: a checkpoint is data at rest and can be bracketed, a streaming query is a running
  computation that outlives any bracket — a lifecycle, which wants an owner.
  `docs/decisions.md` has the argument, and why MLlib is separate on different grounds.

  This module is the verbs — plus the few expression builders that take a DataFrame rather than
  a column: `col/2` for a tagged reference, and `scalar/1` and `exists/1` for a subquery over
  another frame. Everything else expression-shaped comes from three modules:

      import Latu.Column              # operators, predicates, casts, sort keys, over/2
      alias Latu.Functions, as: F     # Spark's ~500 functions, under Spark's own names
      alias Latu.Window, as: W        # window specifications

  The catalog — databases, tables and views — lives on `Latu.Catalog`, as it lives on
  `spark.catalog` in PySpark. Caching a *table* by name is there too
  (`Latu.Catalog.cache_table/2`); caching the frame in your hand is `cache/1` here.

  `Latu.Column` is small and gets composed by hand, so it is imported. This module is called
  qualified — `Latu.filter`, `Latu.show` — the way `Enum` is; nothing in it collides with
  `Latu.Column` or with `Kernel`, so `import Latu` compiles if a REPL wants it, but the verbs
  read better with their module on. The other two are aliased: their names collide with the
  verbs on purpose, exactly as Spark's do (`Latu.count/1` and `F.count/1` are different
  things), and `F.` is how you find one of five hundred functions with tab completion.
  """

  alias Latu.Client
  alias Latu.DataFrame
  alias Latu.Error
  alias Latu.ExecutionInfo
  alias Latu.GroupedData
  alias Latu.MergeInto
  alias Latu.Plan
  alias Latu.Session

  # =============================================
  # Session
  # =============================================

  @doc """
  Connect to a Spark Connect server.

  Takes an `sc://` URL or an unconnected `Latu.Session`. With no arguments, reads
  `SPARK_REMOTE`.

      {:ok, session} = Latu.connect("sc://localhost:15002")
      {:ok, session} = Latu.connect("sc://prod:15002/;use_ssl=true", timeout: :infinity)

  ## Options

  These are `Latu.Session.from_url/2`'s, and they override the URL — so they apply to a URL
  and passing them alongside an already-built session raises. Every one is also a field of
  `%Latu.Session{}`, which is where their defaults live.

    * `:session_id` — the id Spark keys server-side state on. A UUID; one is generated.
    * `:user_id`, `:user_name` — the identity the server sees. Also settable in the URL.
    * `:client_type` — the user agent string. Defaults to Latu's own.
    * `:timeout` — per-RPC deadline in milliseconds, or `:infinity`. Defaults to `60_000`.
    * `:connect_timeout` — deadline for establishing the channel. Defaults to `10_000`.
    * `:tags` — execution tags every query inherits, so `interrupt/2` can find them later.
      Defaults to `[]`; `Latu.Session.add_tag/2` adds one afterwards.
    * `:window_size` — HTTP/2 flow-control window in bytes. Defaults to `134_217_728` (128 MiB).
    * `:keepalive` — HTTP/2 ping interval in milliseconds. Defaults to `60_000`.
    * `:keepalive_tolerance` — pings missed before the channel is considered dead. Defaults
      to `2`.
    * `:retry` — a `Latu.Retry` policy every execution retries under. Defaults to `%Latu.Retry{}`.

  Why these numbers, and why the knobs live on the session rather than in application config,
  is in `docs/decisions.md`.
  """
  @spec connect(String.t() | Session.t() | nil, keyword()) ::
          {:ok, Session.t()} | {:error, Error.t()}
  def connect(url_or_session \\ nil, opts \\ [])

  def connect(nil, opts) do
    case System.get_env("SPARK_REMOTE") do
      nil -> {:error, Error.new(:invalid_url, "SPARK_REMOTE is not set; pass a URL instead")}
      url -> connect(url, opts)
    end
  end

  def connect(url, opts) when is_binary(url) do
    with {:ok, session} <- Session.from_url(url, opts), do: Client.connect(session)
  end

  def connect(%Session{} = session, []), do: Client.connect(session)

  def connect(%Session{}, opts) do
    raise ArgumentError,
          "options apply to a URL, not an already-built session; got #{inspect(opts)}"
  end

  @doc "Like `connect/2`, raising on failure."
  @spec connect!(String.t() | Session.t() | nil, keyword()) :: Session.t()
  def connect!(url_or_session \\ nil, opts \\ []) do
    case connect(url_or_session, opts) do
      {:ok, session} -> session
      {:error, error} -> raise error
    end
  end

  @doc """
  The Spark version the server reports, e.g. `"4.2.0"`.

  Also the cheapest way to confirm a session is alive and usable.
  """
  @spec spark_version(Session.t()) :: {:ok, String.t()} | {:error, Error.t()}
  defdelegate spark_version(session), to: Client

  @doc "Like `spark_version/1`, raising on failure."
  @spec spark_version!(Session.t()) :: String.t()
  def spark_version!(session) do
    case spark_version(session) do
      {:ok, version} -> version
      {:error, error} -> raise error
    end
  end

  @doc """
  Close the channel. Idempotent.

  ## Options

    * `:release` — end the session on the server first, rather than leaving Spark to time it
      out. Defaults to `false`; `release_session/2` is the direct call, and `docs/decisions.md`
      has why the default stayed put.

  ## Examples

      {:ok, session} = Latu.disconnect(session)
      {:ok, session} = Latu.disconnect(session, release: true)
  """
  @spec disconnect(Session.t(), keyword()) :: {:ok, Session.t()} | {:error, Error.t()}
  defdelegate disconnect(session, opts \\ []), to: Client

  @doc "Like `disconnect/2`, raising on failure."
  @spec disconnect!(Session.t(), keyword()) :: Session.t()
  def disconnect!(session, opts \\ []) do
    case disconnect(session, opts) do
      {:ok, session} -> session
      {:error, error} -> raise error
    end
  end

  @doc """
  Fill in an error's full server-side cause chain.

      {:error, error} = Latu.collect(df)
      {:ok, error} = Latu.error_details(session, error)

      Enum.map(error.causes, & &1.message)
      #=> ["Job aborted due to stage failure: ...", "/ by zero"]

  **Most of what you want is already on the error**, with no round trip: the Spark error class,
  the SQLSTATE, the JVM class hierarchy, the message parameters and — when the server is
  configured to send one — a stack trace all arrive in the gRPC trailers. See `Latu.Error`.
  This adds the one thing they do not carry: the **chain of causes**, root cause last, each
  with its own frames.

  So it is an explicit call rather than something every failure pays for. PySpark fetches it
  eagerly on every error; a Latu action returns `{:error, _}` for expected refusals too, and
  spending a round trip on each of those would be a poor trade.

  An error with no `error_id` — anything that did not come from the server — comes back
  unchanged rather than as a failure.

  **The server hands an error's detail over exactly once**, and forgets it: its own handler
  invalidates the id as it answers. Latu keeps whatever it already has when a fetch comes back
  empty, so calling this twice is safe and the second call is simply a wasted round trip.
  """
  @spec error_details(Session.t(), Error.t()) :: {:ok, Error.t()} | {:error, Error.t()}
  defdelegate error_details(session, error), to: Client

  @doc "Like `error_details/2`, raising on failure."
  @spec error_details!(Session.t(), Error.t()) :: Error.t()
  def error_details!(%Session{} = session, %Error{} = error) do
    case error_details(session, error) do
      {:ok, filled} -> filled
      {:error, failure} -> raise failure
    end
  end

  @doc """
  Cancel executions on the server, returning the operation ids it interrupted.

      Latu.interrupt(session)                      # everything this session is running
      Latu.interrupt(session, tag: "exploration")  # everything carrying the tag
      Latu.interrupt(session, operation_id: id)    # one execution

  **The process running the query cannot call this**, because it is blocked in the query. That
  is what tags are for: tag the session, run the work from a `Task` or another shell, and
  interrupt by tag from anywhere.

      session = Latu.Session.add_tag(session, "exploration")
      task = Task.async(fn -> session |> Latu.range(5_000_000_000) |> Latu.count() end)
      # ... later, once status/2 shows it running: an interrupt that reaches the server before
      # the ExecutePlan does matches nothing, and {:ok, []} is the answer.
      Latu.interrupt(session, tag: "exploration")
      Task.await(task)

  **This, rather than killing the process.** A Latu execution is reattachable — the promise
  that a client may vanish and come back — so a killed client leaves the query *running on the
  server*, holding cluster resources until the detached timeout expires. Interrupt it, then let
  the task finish: Latu releases the execution as the stream ends, error or not.

  An empty list means nothing matched, which is not an error — an execution that has already
  finished is nothing to cancel. Unlike `status/2` and `clone_session/2`, this works on a
  session the server has never heard of: Spark's interrupt handler creates one rather than
  looking one up, so interrupting a session that has run nothing *makes* it real. Harmless.

  ## Options

  At most one, and it chooses the scope. With neither, the scope is the whole session.

    * `:tag` — cancel every execution carrying this tag. Tags come from `:tags` on
      `connect/2` or from `Latu.Session.add_tag/2`.
    * `:operation_id` — cancel one execution, by the id `status/2` reports.
  """
  @spec interrupt(Session.t(), keyword()) :: {:ok, [String.t()]} | {:error, Error.t()}
  def interrupt(%Session{} = session, opts \\ []) do
    with {:ok, ids, _session} <- Client.interrupt(session, opts), do: {:ok, ids}
  end

  @doc "Like `interrupt/2`, raising on failure."
  @spec interrupt!(Session.t(), keyword()) :: [String.t()]
  def interrupt!(%Session{} = session, opts \\ []) do
    case interrupt(session, opts) do
      {:ok, ids} -> ids
      {:error, error} -> raise error
    end
  end

  @doc """
  What the server is running for this session: one map per operation, with its state.

      Latu.status!(session)
      #=> [%{operation_id: "b3f...", state: :running}]

  States are Spark's own, downcased: `:running`, `:terminating`, `:succeeded`, `:failed`,
  `:cancelled`, `:unknown`, `:unspecified`. Pass a list of operation ids to ask about only
  those; the default asks about all of them, **including ones that have finished** — Spark keeps
  a session's recent operations and reports them here.

  Two things that read wrong until you know them. **`:terminating` does not mean "being
  cancelled"**: Spark maps finished, failed *and* cancelled executions to it, and it means the
  work is over but the server has not cleaned up yet — the outcome shows as `:succeeded`,
  `:failed` or `:cancelled` a moment later. And **the server must already know this session**,
  which it does from the first thing the session runs; before that this returns an error saying
  so, rather than an empty list.
  """
  @spec status(Session.t(), [String.t()]) ::
          {:ok, [%{operation_id: String.t(), state: atom()}]} | {:error, Error.t()}
  def status(%Session{} = session, operation_ids \\ []) do
    with {:ok, statuses, _session} <- Client.status(session, operation_ids), do: {:ok, statuses}
  end

  @doc "Like `status/2`, raising on failure."
  @spec status!(Session.t(), [String.t()]) :: [%{operation_id: String.t(), state: atom()}]
  def status!(%Session{} = session, operation_ids \\ []) do
    case status(session, operation_ids) do
      {:ok, statuses} -> statuses
      {:error, error} -> raise error
    end
  end

  @doc """
  Fork the session on the server, returning the clone.

  The clone starts with the original's configuration and state, and is isolated from it
  afterwards: a temp view registered in one is invisible to the other. Good for a REPL — fork,
  make a mess, throw the fork away. Like `status/2`, it needs a session the server already
  knows — one that has run at least one thing.

      scratch = Latu.clone_session!(session)
      scratch |> Latu.table("orders") |> Latu.create_temp_view!("candidates")
      Latu.release_session!(scratch)

  **The clone shares the channel**, because a Spark Connect session is server-side state keyed
  by an id and not a connection. So `disconnect/2` on either one closes the transport for both;
  `release_session/2` on the clone ends only the clone.

  ## Options

    * `:session_id` — the clone's own id. Must be a UUID, and raises if it is not. Defaults to
      a freshly generated one.
  """
  @spec clone_session(Session.t(), keyword()) :: {:ok, Session.t()} | {:error, Error.t()}
  def clone_session(%Session{} = session, opts \\ []) do
    with {:ok, clone, _session} <- Client.clone_session(session, opts), do: {:ok, clone}
  end

  @doc "Like `clone_session/2`, raising on failure."
  @spec clone_session!(Session.t(), keyword()) :: Session.t()
  def clone_session!(%Session{} = session, opts \\ []) do
    case clone_session(session, opts) do
      {:ok, clone} -> clone
      {:error, error} -> raise error
    end
  end

  @doc """
  End the session on the server, without closing the channel.

  Everything the session held goes with it: temp views, cached frames, artifacts, confs. The
  channel stays open, so this is how you end a clone. `disconnect/2` will call it for you with
  `release: true`, which is not its default.

  ## Options

    * `:allow_reconnect` — let a client reconnect to this session id afterwards. Defaults to
      `false`, which is what PySpark sends and what "I am done with this session" means.
    * `:timeout` — deadline for the call in milliseconds, or `:infinity`. Defaults to the
      session's own `:timeout`.
  """
  @spec release_session(Session.t(), keyword()) :: {:ok, Session.t()} | {:error, Error.t()}
  defdelegate release_session(session, opts \\ []), to: Client

  @doc "Like `release_session/2`, raising on failure."
  @spec release_session!(Session.t(), keyword()) :: Session.t()
  def release_session!(%Session{} = session, opts \\ []) do
    case release_session(session, opts) do
      {:ok, session} -> session
      {:error, error} -> raise error
    end
  end

  # =============================================
  # Configuration
  # =============================================

  @doc """
  A session config value, or nil where Spark has nothing to give.

      Latu.conf!(session, "spark.sql.shuffle.partitions")   #=> "4"
      Latu.conf!(session, "spark.sql.ansi.enabled")         #=> "false", Spark's own default
      Latu.conf!(session, "no.such.conf")                   #=> nil

  A key that is set gives its value; a key Spark knows but nobody has set gives **Spark's own
  default**; a key Spark does not know at all gives nil — `Map.get/2`'s shape. Spark's
  `GetOption` arm, which PySpark never sends. `fetch_conf/2` is the same read with an error for
  the unknown key, and `conf/3` is the one that supplies its own default instead of Spark's.

  Values are strings, as Spark's runtime conf holds them.
  """
  @spec conf(Session.t(), String.t()) :: {:ok, String.t() | nil} | {:error, Error.t()}
  def conf(%Session{} = session, key) do
    session |> Client.config({:get_option, [conf_key!(key)]}) |> only(key)
  end

  @doc "Like `conf/2`, raising on failure."
  @spec conf!(Session.t(), String.t()) :: String.t() | nil
  def conf!(%Session{} = session, key) do
    case conf(session, key) do
      {:ok, value} -> value
      {:error, error} -> raise error
    end
  end

  @doc """
  A session config value, falling back to `default` rather than to Spark's own default.

      Latu.conf!(session, "spark.sql.shuffle.partitions", "200")
      Latu.conf!(session, "my.app.setting", nil)

  **This overrides Spark's default, it does not follow it.** `GetWithDefault` reads only what
  the session has actually set; for anything unset you get `default` back, even where Spark has
  a documented default of its own. That is the point of it — Spark's own comment for this path
  says "useful when the default value defined by Apache Spark is not the desired one" — and it
  is why `conf/2` exists for the other reading.

  A `nil` default is PySpark's `spark.conf.get(key, None)` and comes back as nil. Anything else
  follows `set_conf/3`'s coercion, and Spark type-checks it against the conf it belongs to, so
  a default of `"soon"` for an integer conf is refused rather than returned.
  """
  @spec conf(Session.t(), String.t(), String.t() | number() | boolean() | atom() | nil) ::
          {:ok, String.t() | nil} | {:error, Error.t()}
  def conf(%Session{} = session, key, nil) do
    session |> Client.config({:get_with_default, [{conf_key!(key), nil}]}) |> only(key)
  end

  def conf(%Session{} = session, key, default) do
    pair = {conf_key!(key), conf_value!(key, default)}

    session |> Client.config({:get_with_default, [pair]}) |> only(key)
  end

  @doc "Like `conf/3`, raising on failure."
  @spec conf!(Session.t(), String.t(), String.t() | number() | boolean() | atom() | nil) ::
          String.t() | nil
  def conf!(%Session{} = session, key, default) do
    case conf(session, key, default) do
      {:ok, value} -> value
      {:error, error} -> raise error
    end
  end

  @doc """
  `conf/2` with an error for a key Spark does not know.

      Latu.fetch_conf!(session, "spark.sql.shuffle.partitions")   #=> "4"
      Latu.fetch_conf(session, "no.such.conf")                    #=> {:error, %Latu.Error{}}

  Set value, else Spark's own default, else `SQL_CONF_NOT_FOUND` — Spark's `Get` arm and
  PySpark's `spark.conf.get(key)`. `Map.fetch/2` to `conf/2`'s `Map.get/2`: the read for when a
  typo'd key should say so.
  """
  @spec fetch_conf(Session.t(), String.t()) :: {:ok, String.t()} | {:error, Error.t()}
  def fetch_conf(%Session{} = session, key) do
    session |> Client.config({:get, [conf_key!(key)]}) |> only(key)
  end

  @doc "Like `fetch_conf/2`, raising on failure."
  @spec fetch_conf!(Session.t(), String.t()) :: String.t()
  def fetch_conf!(%Session{} = session, key) do
    case fetch_conf(session, key) do
      {:ok, value} -> value
      {:error, error} -> raise error
    end
  end

  @doc """
  Every config the session has **set**, as a map.

      Latu.confs!(session)
      Latu.confs!(session, prefix: "spark.sql.")

  Not the registry: Spark's `getAll` reads the session's own settings, so a conf sitting at its
  default is absent here even though `conf/2` answers for it. Expect a handful of entries on a
  fresh session — whatever the server was started with — not hundreds.

  ## Options

    * `:prefix` — return only keys starting with this string, filtered on the server. Defaults
      to `nil`, meaning every set conf. Spark strips the prefix off the keys it returns; Latu
      puts it back, so the keys here are always whole keys you can hand to `conf/2`.
  """
  @spec confs(Session.t(), keyword()) ::
          {:ok, %{String.t() => String.t() | nil}} | {:error, Error.t()}
  def confs(%Session{} = session, opts \\ []) do
    opts = Keyword.validate!(opts, prefix: nil)
    prefix = opts[:prefix] && conf_key!(opts[:prefix])

    with {:ok, pairs, _session} <- Client.config(session, {:get_all, prefix}) do
      {:ok, Map.new(pairs, fn {key, value} -> {(prefix || "") <> key, value} end)}
    end
  end

  @doc "Like `confs/2`, raising on failure."
  @spec confs!(Session.t(), keyword()) :: %{String.t() => String.t() | nil}
  def confs!(%Session{} = session, opts \\ []) do
    case confs(session, opts) do
      {:ok, confs} -> confs
      {:error, error} -> raise error
    end
  end

  @doc """
  Set one config on the server.

      :ok = Latu.set_conf(session, "spark.sql.shuffle.partitions", 8)
      Latu.set_conf!(session, "spark.sql.ansi.enabled", true)

  A number, boolean or atom is written as Spark's own string form; a string passes verbatim.
  Spark type-checks the value against the conf, so `"eight"` here is refused, and it refuses a
  **static** conf outright (`CANNOT_MODIFY_VALUE_OF_STATIC_CONFIG`) — see `is_modifiable/2`.
  An unrecognised key is not an error: Spark stores it, which is how a data source's own
  options get set.

  The config lives on the server, not in the struct, so there is nothing to hand back: the same
  session keeps working, and `Latu.clone_session/2` carries the confs into the clone. A
  deprecation warning Spark attaches to a key is logged.
  """
  @spec set_conf(Session.t(), String.t(), String.t() | number() | boolean() | atom()) ::
          :ok | {:error, Error.t()}
  def set_conf(%Session{} = session, key, value), do: set_confs(session, [{key, value}])

  @doc "Like `set_conf/3`, raising on failure."
  @spec set_conf!(Session.t(), String.t(), String.t() | number() | boolean() | atom()) :: :ok
  def set_conf!(%Session{} = session, key, value) do
    case set_conf(session, key, value) do
      :ok -> :ok
      {:error, error} -> raise error
    end
  end

  @doc """
  Set several configs in one round-trip.

      Latu.set_confs(session, %{"spark.sql.ansi.enabled" => true})
      Latu.set_confs(session, [{"spark.sql.shuffle.partitions", 8}, {"a.b", "c"}])

  A map or anything enumerating `{key, value}`. Values follow `set_conf/3`. Spark applies a list
  in order and stops at the first refusal, so one bad key fails the call with the pairs before
  it already applied.
  """
  @spec set_confs(Session.t(), Enumerable.t()) :: :ok | {:error, Error.t()}
  def set_confs(%Session{} = session, pairs) do
    pairs = for {key, value} <- pairs, do: {conf_key!(key), conf_value!(key, value)}

    with {:ok, _pairs, _session} <- Client.config(session, {:set, pairs}), do: :ok
  end

  @doc "Like `set_confs/2`, raising on failure."
  @spec set_confs!(Session.t(), Enumerable.t()) :: :ok
  def set_confs!(%Session{} = session, pairs) do
    case set_confs(session, pairs) do
      :ok -> :ok
      {:error, error} -> raise error
    end
  end

  @doc """
  Put a config back to Spark's default.

      :ok = Latu.unset_conf(session, "spark.sql.shuffle.partitions")

  A key that was never set is not an error. A static conf is refused, as it is by `set_conf/3`.
  """
  @spec unset_conf(Session.t(), String.t()) :: :ok | {:error, Error.t()}
  def unset_conf(%Session{} = session, key) do
    with {:ok, _pairs, _session} <- Client.config(session, {:unset, [conf_key!(key)]}), do: :ok
  end

  @doc "Like `unset_conf/2`, raising on failure."
  @spec unset_conf!(Session.t(), String.t()) :: :ok
  def unset_conf!(%Session{} = session, key) do
    case unset_conf(session, key) do
      :ok -> :ok
      {:error, error} -> raise error
    end
  end

  @doc """
  Whether Spark will let this session change that config.

      Latu.is_modifiable!(session, "spark.sql.ansi.enabled")   #=> true
      Latu.is_modifiable!(session, "spark.sql.warehouse.dir")  #=> false, it is static
      Latu.is_modifiable!(session, "my.app.setting")           #=> false, Spark never heard of it

  **False does not mean `set_conf/3` will fail.** Spark answers true only for a config it
  *defines* and that is not static, so an unregistered key is false and `set_conf/3` will
  happily store it anyway. The one this reliably predicts is the static conf, which is refused.
  Spark's own spelling is `isModifiable`.
  """
  @spec is_modifiable(Session.t(), String.t()) :: {:ok, boolean()} | {:error, Error.t()}
  def is_modifiable(%Session{} = session, key) do
    with {:ok, value} <-
           session |> Client.config({:is_modifiable, [conf_key!(key)]}) |> only(key) do
      case value do
        "true" ->
          {:ok, true}

        "false" ->
          {:ok, false}

        other ->
          {:error,
           Error.new(:protocol, "IsModifiable answered #{inspect(other)}, expected true or false")}
      end
    end
  end

  @doc "Like `is_modifiable/2`, raising on failure."
  @spec is_modifiable!(Session.t(), String.t()) :: boolean()
  def is_modifiable!(%Session{} = session, key) do
    case is_modifiable(session, key) do
      {:ok, modifiable?} -> modifiable?
      {:error, error} -> raise error
    end
  end

  # One key in, one pair out — the handler echoes each key it was asked about. More than one
  # would mean the response does not belong to the request.
  defp only({:ok, [{_echoed, value}], _session}, _key), do: {:ok, value}

  defp only({:ok, pairs, _session}, key) do
    {:error,
     Error.new(:protocol, "Config answered with #{length(pairs)} values for #{key}, wanted 1")}
  end

  defp only({:error, _} = error, _key), do: error

  defp conf_key!(key) when is_binary(key), do: key

  defp conf_key!(key) do
    raise ArgumentError, "a config key is a string, not #{inspect(key)}"
  end

  defp conf_value!(key, nil) do
    raise ArgumentError, "#{key} cannot be set to nil — unset_conf/2 is how a conf goes away"
  end

  defp conf_value!(_key, value) when is_binary(value), do: value
  defp conf_value!(_key, value) when is_boolean(value), do: to_string(value)
  defp conf_value!(_key, value) when is_number(value), do: to_string(value)
  defp conf_value!(_key, value) when is_atom(value), do: Atom.to_string(value)

  defp conf_value!(key, value) do
    raise ArgumentError,
          "#{key} is a string, number, boolean or atom, not #{inspect(value)} — Spark keeps " <>
            "its runtime conf as strings, and Latu converts only those"
  end

  # =============================================
  # Sources
  # =============================================

  @doc """
  A DataFrame of one `id` column of longs, counting up to but not including `stop`.

      Latu.range(session, 5)         # 0, 1, 2, 3, 4
      Latu.range(session, 2, 5)      # 2, 3, 4
      Latu.range(session, 0, 10, 2)  # 0, 2, 4, 6, 8

  Lazy: nothing is sent until an action.

  ## Options

  Every arity takes these last.

    * `:num_partitions` — Spark's own fourth argument to `range`. Defaults to `nil`, which
      leaves the choice to the server: `spark.sql.leafNodeDefaultParallelism` if it is set,
      otherwise its default parallelism. `set_conf/3` sets that for the whole session instead
      of per call.

  A keyword list rather than Spark's fifth positional argument, because
  `Latu.range(session, 0, 10, 2, 4)` gives a reader no way to tell the step from the partitions
  — `docs/deviations.md`.

  ## Examples

      Latu.range(session, 1_000, num_partitions: 4)
      Latu.range(session, 0, 10, 2, num_partitions: 1)
  """
  @spec range(Session.t(), integer()) :: DataFrame.t()
  def range(session, stop), do: DataFrame.range(session, 0, stop, 1)

  @doc "See `range/2`."
  @spec range(Session.t(), integer(), integer()) :: DataFrame.t()
  @spec range(Session.t(), integer(), keyword()) :: DataFrame.t()
  def range(session, stop, opts) when is_list(opts) do
    DataFrame.range(session, 0, stop, 1, opts)
  end

  def range(session, start, stop), do: DataFrame.range(session, start, stop, 1)

  @doc "See `range/2`."
  @spec range(Session.t(), integer(), integer(), integer()) :: DataFrame.t()
  @spec range(Session.t(), integer(), integer(), keyword()) :: DataFrame.t()
  def range(session, start, stop, opts) when is_list(opts) do
    DataFrame.range(session, start, stop, 1, opts)
  end

  def range(session, start, stop, step), do: DataFrame.range(session, start, stop, step)

  @doc "See `range/2`."
  @spec range(Session.t(), integer(), integer(), integer(), keyword()) :: DataFrame.t()
  defdelegate range(session, start, stop, step, opts), to: DataFrame

  @doc """
  Read from a data source.

      Latu.read(session, format: "csv", path: "/data/people.csv",
                schema: "id INT, name STRING", header: true)

  Lazy: nothing is sent until an action. PySpark's builder chain
  (`spark.read.format(...).option(...).load(...)`) is one call here; see `docs/deviations.md`.

  ## Options

  Five keys are Latu's. **Every other key is a reader option** — a snake_case atom becomes
  Spark's camelCase (`infer_schema:` → `"inferSchema"`), a string key passes verbatim, and a
  `nil` value drops its pair.

    * `:format` — the source's short name or class: `"csv"`, `"parquet"`, `"jdbc"`, whatever
      the cluster has. Defaults to `nil`, leaving the server on `spark.sql.sources.default`.
    * `:schema` — a string the server parses: DDL (`"id INT, name STRING"`) or Spark's JSON
      schema form. Defaults to `""`, meaning infer. There is no client-side schema model.
    * `:path` — one path to read, as a string.
    * `:paths` — several, as a list of strings. Defaults to `[]`. Passing both `:path` and
      `:paths` raises; they are two spellings of one thing.

  A reader option whose own name is one of those four has to be written as a **string key**,
  which passes verbatim: `Latu.read(session, [{"path", "s3://bucket/key"}, format: "custom"])`.

  ## Examples

      Latu.read(session, format: "csv", path: "/data/people.csv",
                schema: "id INT, name STRING", header: true)

      Latu.read(session, format: "parquet", paths: ["/data/a", "/data/b"])

      Latu.read(session, format: "jdbc", url: url, dbtable: "people", fetchsize: 1_000)
  """
  @spec read(Session.t(), keyword()) :: DataFrame.t()
  defdelegate read(session, opts), to: DataFrame

  @doc """
  Read a catalog table by name.

      Latu.table(session, "people")

  Options (`table/3`) follow `read/2`'s key and value rules.
  """
  @spec table(Session.t(), String.t() | atom()) :: DataFrame.t()
  def table(session, name), do: DataFrame.table(session, name, [])

  @doc "See `table/2`."
  @spec table(Session.t(), String.t() | atom(), keyword() | map()) :: DataFrame.t()
  defdelegate table(session, name, options), to: DataFrame

  @doc """
  Run SQL. An action: the query executes when called — so DDL works — and the DataFrame that
  comes back queries the *result*, not the query again.

      {:ok, df} = Latu.sql(session, "SELECT * FROM people WHERE age > 30")
      Latu.sql!(session, "DROP TABLE IF EXISTS scratch")

  Parameter markers bind from `args` — a list binds `?` positionally, a map binds `:name` —
  and values are literals, never spliced text:

      Latu.sql(session, "SELECT * FROM people WHERE age > :min", %{min: 30})
      Latu.sql(session, "SELECT ? + ?", [2, 3])

  A DataFrame can be named into the query with `views:`, and the name is yours — write it in
  the SQL and pass the frame:

      Latu.sql(session, "SELECT count(*) FROM orders", views: [orders: df])

  The frame is hoisted into the plan, so nothing is registered on the server and the name is
  gone when the query is. PySpark generates a name and substitutes it into a Python format
  string; Latu takes the name (docs/deviations.md). Bindings can ride along as `args:`.
  """
  @spec sql(Session.t(), String.t(), [term()] | map() | keyword()) ::
          {:ok, DataFrame.t()} | {:error, Error.t()}
  defdelegate sql(session, query, bindings \\ []), to: DataFrame

  @doc "Like `sql/3`, raising on failure."
  @spec sql!(Session.t(), String.t(), [term()] | map() | keyword()) :: DataFrame.t()
  defdelegate sql!(session, query, bindings \\ []), to: DataFrame

  @doc """
  A DataFrame from local data — `collect/2`'s inverse. An action: the data ships to the
  server when called.

  Takes a list of row maps (columns sorted by key, PySpark's rule for dicts), column data (a
  keyword list in declared order, or a map sorted by key), or an `Explorer.DataFrame`. The
  data travels as one Arrow IPC stream inside the plan while it fits the server's
  `localRelationCacheThreshold` (64 MiB by default); past that it is chunked and cached as
  session artifacts, and the plan references the hashes — PySpark's own escalation.

  ## Options

    * `:schema` — a string the server casts the data to: DDL (`"id INT, name STRING"`) or
      Spark's JSON schema form. Defaults to `nil`, meaning infer from the data. **Empty data
      needs one**, since there is nothing to infer from. There is no client-side schema model,
      here or in `read/2`.

  **A schema is applied by position, and row maps are sorted by key** — so
  `[%{id: 1, jan: 10.0, feb: 20.0}]` with `schema: "id INT, jan DOUBLE, feb DOUBLE"` casts the
  *feb* column to `id INT` and says nothing. Use the keyword-list form when both the order and
  the schema matter; it is columns in declared order.

  ## Examples

      {:ok, df} = Latu.create_dataframe(session, [%{id: 1, name: "Ada"}, %{id: 2, name: "Bo"}])

      Latu.create_dataframe!(session, id: [1, 2], name: ["Ada", "Bo"])

      Latu.create_dataframe(session, [%{id: 1}], schema: "id INT")
      Latu.create_dataframe(session, [], schema: "id INT, name STRING")
  """
  @spec create_dataframe(Session.t(), term(), keyword()) ::
          {:ok, DataFrame.t()} | {:error, Error.t()}
  defdelegate create_dataframe(session, data, opts \\ []), to: DataFrame

  @doc "Like `create_dataframe/3`, raising on failure."
  @spec create_dataframe!(Session.t(), term(), keyword()) :: DataFrame.t()
  defdelegate create_dataframe!(session, data, opts \\ []), to: DataFrame

  # =============================================
  # Transformations
  # =============================================

  @doc """
  Keep these columns, in this order.

      Latu.select(df, [:id, "name"])
      Latu.select(df, [:id, doubled: Latu.Column.multiply(:id, 2)])

  See `Latu.DataFrame.select/2`.
  """
  @spec select(DataFrame.t(), term()) :: DataFrame.t()
  defdelegate select(df, columns), to: DataFrame

  @doc """
  Keep these SQL expressions, written as strings.

      Latu.select_expr(df, ["id", "price * 1.1 as with_tax"])

  `select/2` over `Latu.Column.expr/1` for each one, which is exactly PySpark's own
  composition — so anything SQL can say fits here without a wrapper.
  """
  @spec select_expr(DataFrame.t(), [String.t()] | String.t()) :: DataFrame.t()
  defdelegate select_expr(df, expressions), to: DataFrame

  @doc """
  Observe aggregates over a frame without changing what it returns.

  The frame comes back unchanged; the metrics come back from the action, through one of the
  eight `*_with_metrics` twins — `collect_with_metrics/2`, `count_with_metrics/2`,
  `to_explorer_with_metrics/2`, `write_with_metrics/2`, `save_as_table_with_metrics/3`,
  `insert_into_with_metrics/3`, `write_v2_with_metrics/3` and `merge_with_metrics/2` — as a
  `Latu.ExecutionInfo`.

      df = Latu.observe(df, :quality, rows: F.count(:id), worst: F.min(:price))
      {:ok, info} = Latu.write_with_metrics(df, path: "/out")

      info.observed  #=> %{quality: %{rows: 1000, worst: -2.5}}

  Any *other* action still runs and simply does not report — `show/2` on an observed frame
  shows it, `collect/2` collects it, a `join/3` over it is an ordinary join — exactly as
  PySpark does when nobody reads the `Observation`. The one exception is a plain **write**
  (`write/2`, `save_as_table/3`, `insert_into/3`, `write_v2/3`, `merge/2`): a write consumes the
  frame, so its metrics would be produced and dropped with nothing to show for it, and it
  **raises**, naming the twin. `docs/decisions.md` (M11.1, narrowed at M12.6).

  Name each metric with a keyword list: the names are how you read the values back, not
  decoration. Spark requires *aggregate* expressions here and refuses a bare column when it
  analyses the plan.

  PySpark spells this `df.observe(observation, *exprs)`, where the `Observation` is a mutable
  handle the client writes into as responses arrive. Latu holds no processes and its frames are
  inert, so the metrics ride back with the result instead — see `docs/deviations.md`.
  """
  @spec observe(DataFrame.t(), String.t() | atom(), keyword() | [Plan.expression()]) ::
          DataFrame.t()
  defdelegate observe(df, name, metrics), to: DataFrame

  @doc """
  Keep the rows the condition holds for.

      Latu.filter(df, "id > 3")

  See `Latu.DataFrame.filter/2`.
  """
  @spec filter(DataFrame.t(), term()) :: DataFrame.t()
  defdelegate filter(df, condition), to: DataFrame

  @doc "`filter/2`, spelled Spark's other way."
  @spec where(DataFrame.t(), term()) :: DataFrame.t()
  defdelegate where(df, condition), to: DataFrame

  @doc """
  Add or replace columns, keeping the rest.

      Latu.with_columns(df, doubled: Latu.Column.multiply(:id, 2))

  See `Latu.DataFrame.with_columns/2`.
  """
  @spec with_columns(DataFrame.t(), keyword()) :: DataFrame.t()
  defdelegate with_columns(df, columns), to: DataFrame

  @doc """
  Remove columns.

      Latu.drop(df, [:x, "y"])

  See `Latu.DataFrame.drop/2`.
  """
  @spec drop(DataFrame.t(), term()) :: DataFrame.t()
  defdelegate drop(df, columns), to: DataFrame

  @doc """
  Rename columns, by mapping or positionally.

      Latu.rename(df, id: :n)
      Latu.rename(df, [:renamed])

  See `Latu.DataFrame.rename/2`.
  """
  @spec rename(DataFrame.t(), keyword() | map() | [String.t() | atom()]) :: DataFrame.t()
  defdelegate rename(df, names), to: DataFrame

  @doc "Keep at most `count` rows."
  @spec limit(DataFrame.t(), non_neg_integer()) :: DataFrame.t()
  defdelegate limit(df, count), to: DataFrame

  @doc "Skip the first `count` rows."
  @spec offset(DataFrame.t(), non_neg_integer()) :: DataFrame.t()
  defdelegate offset(df, count), to: DataFrame

  @doc """
  Drop duplicate rows, by these columns or by all of them.

      Latu.distinct(df)
      Latu.distinct(df, [:suburb])

  See `Latu.DataFrame.distinct/2`.
  """
  @spec distinct(DataFrame.t(), term()) :: DataFrame.t()
  defdelegate distinct(df, columns \\ []), to: DataFrame

  @doc """
  Sort rows.

      Latu.sort(df, :id)
      Latu.sort(df, [Latu.Column.desc(:price), :id])

  A bare name sorts ascending with nulls first; `Latu.Column.asc/1`, `Latu.Column.desc/1` and
  the four explicit `*_nulls_*` spellings are the alternatives.
  `sort_within_partitions/2` sorts within each partition rather than across the frame, which
  needs no shuffle. See `Latu.DataFrame.sort/2`.
  """
  @spec sort(DataFrame.t(), term()) :: DataFrame.t()
  defdelegate sort(df, columns), to: DataFrame

  @doc "`sort/2`, spelled Spark's other way."
  @spec order_by(DataFrame.t(), term()) :: DataFrame.t()
  defdelegate order_by(df, columns), to: DataFrame

  @doc """
  A random fraction of the rows. Lazy.

  ## Options

    * `:seed` — an integer. Defaults to `nil`, which draws a random one, as in PySpark — so
      the *plan* differs between runs, not just the result.
    * `:with_replacement` — sample with replacement, so a row can appear more than once.
      Defaults to `false`.
    * `:lower_bound` — where the sampled window starts. Defaults to `0.0`. Only
      `random_split/3` sets it, and it is what makes that verb's slices partition the frame;
      you rarely want it by hand.
    * `:deterministic_order` — make the sample stable by forcing a deterministic row order
      first. Defaults to `false`.

  ## Examples

      Latu.sample(df, 0.1)
      Latu.sample(df, 0.1, seed: 42)
      Latu.sample(df, 0.1, with_replacement: true, seed: 42)

  See `Latu.DataFrame.sample/3`.
  """
  @spec sample(DataFrame.t(), number(), keyword()) :: DataFrame.t()
  defdelegate sample(df, fraction, opts \\ []), to: DataFrame

  @doc """
  Split the frame into slices whose sizes are proportional to `weights`.

      [train, test] = Latu.random_split(df, [0.8, 0.2], seed: 42)

  The weights are normalised, so `[8, 2]` and `[0.8, 0.2]` give the same plans. Each slice is
  a `Sample` over a window of `[0.0, 1.0]`, and the windows tile it exactly — which is why
  every slice carries the **same seed** and `deterministic_order`: they only partition the
  frame if each one sees the rows in the same order. Without a `:seed` a random one is drawn
  once and shared, as in PySpark, so the plans differ between runs.

  Nothing runs here: you get a list of lazy frames.

  ## Options

    * `:seed` — an integer shared by every slice. Defaults to `nil`, which draws one random
      seed once and gives it to all of them, as in PySpark.

  ## Examples

      [train, test] = Latu.random_split(df, [0.8, 0.2], seed: 42)
      [a, b, c] = Latu.random_split(df, [1, 1, 1])
  """
  @spec random_split(DataFrame.t(), [number()], keyword()) :: [DataFrame.t()]
  defdelegate random_split(df, weights, opts \\ []), to: DataFrame

  @doc """
  Attach a planner hint.

      Latu.hint(df, "broadcast")
      Latu.hint(df, "merge")
      Latu.hint(df, "repartition", [4, "suburb"])

  Join hints are `BROADCAST`, `MERGE`, `SHUFFLE_HASH` and `SHUFFLE_REPLICATE_NL`; partitioning
  hints are `COALESCE`, `REPARTITION` and `REPARTITION_BY_RANGE`. Spark matches the name
  case-insensitively and **ignores a hint it does not recognise**, so a typo is silent.

  Parameters follow Latu's usual coercion — a binary is a string literal, an atom is a column
  reference — which matches PySpark even though it calls `F.lit` on every parameter, because
  `lit` of a column returns the column unchanged. So `hint(df, "repartition", [4, "suburb"])`
  and `hint(df, "repartition", [4, :suburb])` are different plans, and both are valid.
  """
  @spec hint(DataFrame.t(), String.t() | atom(), [term()]) :: DataFrame.t()
  defdelegate hint(df, name, parameters \\ []), to: DataFrame

  @doc """
  Wide to long: turn a set of columns into two, one holding their names and one their values.

  Spark also calls this `melt`; Latu ships the one name.

  ## Options

    * `:values` — the columns to unpivot, as a list. Defaults to `nil`, which means **every
      column that is not an id**, worked out by the server. `values: []` is not the same
      thing: it is a different message that sends an empty set.
    * `:variable_column_name` — **required.** The name of the column holding the old column
      names.
    * `:value_column_name` — **required.** The name of the column holding their values.

  Both names are required, as in PySpark.

  ## Examples

      Latu.unpivot(df, [:id],
        values: [:jan, :feb, :mar],
        variable_column_name: "month",
        value_column_name: "sales"
      )

      # every non-id column
      Latu.unpivot(df, [:id],
        variable_column_name: "month",
        value_column_name: "sales"
      )
  """
  @spec unpivot(DataFrame.t(), term(), keyword()) :: DataFrame.t()
  defdelegate unpivot(df, ids, opts), to: DataFrame

  @doc """
  Rows to columns.

      Latu.transpose(df)
      Latu.transpose(df, :metric)

  `index_column`'s values become the new column names. Without one Spark uses the first
  column — its rule, not a Latu default. Spark 4 only; the whole frame is collected on the
  driver, so this is for small results.
  """
  @spec transpose(DataFrame.t(), term() | nil) :: DataFrame.t()
  defdelegate transpose(df, index_column \\ nil), to: DataFrame

  @doc """
  Attach metadata to an existing column.

      Latu.with_metadata(df, :id, %{"comment" => "the primary key"})

  A map, encoded to the JSON string the wire carries. The column keeps its name and its
  values; `schema/1` will not show the metadata, because Latu reports a schema as
  `simpleString` and Spark keeps metadata outside it — read it back with `explain/2` or from
  the source that consumes it.
  """
  @spec with_metadata(DataFrame.t(), String.t() | atom(), map()) :: DataFrame.t()
  defdelegate with_metadata(df, name, metadata), to: DataFrame

  @doc """
  Range-partition the frame by these columns.

      Latu.repartition_by_range(df, [:suburb, Latu.Column.desc(:price)])
      Latu.repartition_by_range(df, [:suburb], num_partitions: 8)

  The same relation `repartition/3` builds, carrying **sort orders** instead of bare
  expressions — a range partitioner needs an ordering to cut on. Sort keys, so
  `Latu.Column.desc/1` and friends apply.

  ## Options

    * `:num_partitions` — how many partitions to cut into. Defaults to `nil`, leaving the
      choice to the server, as on `range/2`.
  """
  @spec repartition_by_range(DataFrame.t(), term(), keyword()) :: DataFrame.t()
  defdelegate repartition_by_range(df, columns, opts \\ []), to: DataFrame

  @doc """
  Every pairing of the two frames: `join/3` with `how: :cross` and no condition.

  Spark's own separate method, kept because the intent is worth spelling out — a cross join by
  accident is a very different query from one on purpose.
  """
  @spec cross_join(DataFrame.t(), DataFrame.t()) :: DataFrame.t()
  defdelegate cross_join(df, other), to: DataFrame

  @doc """
  Parse a frame of strings into a structured frame.

  ## Options

    * `:format` — **required.** `:json`, `:csv` or `:xml`. Anything else raises naming the
      three. XML is in Spark 4.2's enum; whether your server implements it is its own question.
    * `:schema` — a type from `parse_ddl_type/2`. Defaults to `nil`, meaning infer.
    * `:options` — the reader options that format takes, as a keyword list. Defaults to `[]`.

  ## Examples

      df |> Latu.parse(format: :json, options: [multiLine: true])
      df |> Latu.parse(format: :csv, schema: Latu.parse_ddl_type!(session, "id INT, name STRING"))

  PySpark spells this `spark.read.json(df)`, overloading the reader on its argument, and parses
  a DDL schema **client-side** to fill the field. Latu names the relation instead and asks the
  server for the type, which is the same route `to/2` takes.
  """
  @spec parse(DataFrame.t(), keyword()) :: DataFrame.t()
  defdelegate parse(df, opts), to: DataFrame

  @doc """
  A table-valued function, as a frame.

      Latu.table_function(session, "explode", [Latu.Column.expr("array(1, 2, 3)")])
      Latu.table_function(session, "sql_keywords")

  One builder for all of them, as `Latu.Column.fun/3` is for scalar functions — PySpark wraps a
  fixed handful under `spark.tvf` and this covers those plus whatever a Spark release adds.

  Arguments are expressions. A **frame** as a table argument is deliberately not offered: the
  protocol's `table_arg` subquery is consumed only by a Python UDTF, which needs a Python worker
  and is out of Latu's scope — see `docs/deviations.md`. Use `Latu.sql/3` and SQL's `TABLE(...)`.
  """
  @spec table_function(Session.t(), String.t() | atom(), [term()]) :: DataFrame.t()
  defdelegate table_function(session, name, arguments \\ []), to: DataFrame

  @doc """
  A table's change feed, as a frame.

  PySpark spells it `spark.read.changes(table)`.

  ## Options

    * `:options` — the CDC window, as a keyword list: `starting_version`, `ending_version`,
      `starting_timestamp`, `ending_timestamp` and the rest, camelCased from snake_case as
      reader options are. Defaults to `[]`.
    * `:is_streaming` — mark it a streaming read. Defaults to `false`.

  ## Examples

      Latu.table_changes(session, "orders", options: [starting_version: 3])
      Latu.table_changes(session, "orders",
        options: [starting_version: 3, ending_version: 9])
  """
  @spec table_changes(Session.t(), String.t() | atom(), keyword()) :: DataFrame.t()
  defdelegate table_changes(session, table, opts \\ []), to: DataFrame

  @doc """
  An as-of join: match each left row with the *nearest* right row instead of an equal one.

  ## Options

    * `:left_as_of` — **required.** The left frame's ordering column: the thing being matched
      *nearest on*, usually a timestamp. A bare name is tagged to the frame it belongs to, so
      the same column name on both sides is unambiguous; pass an expression to build one
      yourself.
    * `:right_as_of` — **required.** The same, on the right frame.
    * `:on` — an equality key applied alongside the as-of match, as `join/3` takes it: names
      become `USING`, an expression becomes a condition. Defaults to `nil`. This is the
      "match within a group" key — symbol, device, account.
    * `:how` — the join type, from `join/3`'s set: `:inner` (the default), `:cross`, `:full`,
      `:left`, `:right`, `:semi`, `:anti`.
    * `:direction` — which way to look for the nearest row. Defaults to `:backward`.
      * `:backward` — the last right row at or before the left one.
      * `:forward` — the first right row at or after it.
      * `:nearest` — whichever is closer in either direction.
    * `:tolerance` — how far a match may be. An **expression**, so an interval is
      `Latu.Column.expr("INTERVAL 1 DAY")` rather than a bare number. Defaults to `nil`,
      meaning no bound.
    * `:allow_exact_matches` — whether an exactly equal row counts as a match. Defaults to
      `true`; `false` makes `:backward` strictly-before and `:forward` strictly-after.

  ## Examples

      # each trade gets the quote in force at the time, within two seconds, per symbol
      Latu.join_as_of(trades, quotes,
        left_as_of: :time,
        right_as_of: :time,
        on: :symbol,
        tolerance: Latu.Column.expr("INTERVAL 2 SECONDS"),
        direction: :backward
      )

      # keep every left row even where nothing matched
      Latu.join_as_of(trades, quotes, left_as_of: :time, right_as_of: :time, how: :left)

  **Joining a frame to itself needs `as/2` on each side.** Both as-of references would
  otherwise carry the same plan id and Spark answers `AMBIGUOUS_COLUMN_REFERENCE` — its own
  rule for any self-join, with aliasing as the documented fix:

      Latu.join_as_of(Latu.as(df, "l"), Latu.as(df, "r"), left_as_of: :t, right_as_of: :t)

  **PySpark keeps this private** as `DataFrame._joinAsOf`, exposed only through
  pandas-on-Spark's `merge_asof`. The relation is part of the protocol, so Latu ships it as a
  verb; `docs/deviations.md`.
  """
  @spec join_as_of(DataFrame.t(), DataFrame.t(), keyword()) :: DataFrame.t()
  defdelegate join_as_of(df, other, opts), to: DataFrame

  @doc """
  Materialise the frame on the server and hand back a frame that reads the result.

      {:ok, base} = Latu.checkpoint(expensive)
      # ... many queries over `base`, each skipping the work above it ...
      :ok = Latu.release(base)

  Cuts the plan: everything above the checkpoint is computed once.

  ## Options

    * `:eager` — do the work now rather than at the next action. Defaults to `true`.
    * `:local` — use executor storage instead of a reliable location. Defaults to `false`.
      Faster and needs no checkpoint directory, but the data dies with the executor.
    * `:storage_level` — a name `storage_level/1` knows: `:none`, `:disk_only`, `:disk_only_2`,
      `:disk_only_3`, `:memory_only`, `:memory_only_2`, `:memory_and_disk`,
      `:memory_and_disk_2`, `:memory_and_disk_deser`, `:off_heap`. Defaults to `nil`.
      **Accepted with `local: true` only** — the server reads the level on that branch alone,
      so passing one without it is refused rather than dropped in silence.

  ## Examples

      {:ok, base} = Latu.checkpoint(expensive)
      {:ok, base} = Latu.checkpoint(expensive, local: true)
      {:ok, base} = Latu.checkpoint(expensive, local: true, storage_level: :memory_only)
      {:ok, base} = Latu.checkpoint(expensive, eager: false)

  **The reliable form needs a checkpoint directory on the server**, and the command carries no
  path: the directory is `spark.checkpoint.dir`, read once at startup (`SparkContext`'s own
  setter is not reachable over Connect). Without it the server answers "Checkpoint directory
  has not been set". `local: true` needs no directory.

  **This is the one resource in Latu with a release call of its own** — `cache/1`, a temp view
  and a clone allocate server state too, but the session's end is the only thing that frees
  them — and it is why `release/1` and `with_checkpoint/3` exist. Latu holds no processes and
  has no finalizer, so nothing frees a checkpoint for you; what does bound it is the session,
  since the server drops its cached relations when the session ends. **Prefer
  `with_checkpoint/3`** unless you need the frame to outlive one function — in a REPL you
  usually do, which is why the plain form exists.
  `docs/decisions.md` has the argument, including why PySpark's finalizer is not available here.
  """
  @spec checkpoint(DataFrame.t(), keyword()) ::
          {:ok, DataFrame.t()} | {:error, Error.t()}
  defdelegate checkpoint(df, opts \\ []), to: DataFrame

  @doc "Like `checkpoint/2`, raising on failure and returning the frame."
  @spec checkpoint!(DataFrame.t(), keyword()) :: DataFrame.t()
  defdelegate checkpoint!(df, opts \\ []), to: DataFrame

  @doc """
  Free a checkpointed frame's server-side storage.

  Only a frame `checkpoint/2` handed back can be released — anything else raises, because
  releasing what you did not allocate is a mistake worth catching client-side.

  **Releasing twice succeeds**, because the server invalidates a cache entry rather than
  looking one up (`handleRemoveCachedRemoteRelationCommand`). *Querying* a released frame is
  what fails, and the error names the id. So a release is safe to repeat and a frame is not
  safe to keep — the opposite pair from what the shape of the API suggests.
  """
  @spec release(DataFrame.t()) :: :ok | {:error, Error.t()}
  defdelegate release(df), to: DataFrame

  @doc "Like `release/1`, raising on failure."
  @spec release!(DataFrame.t()) :: :ok
  defdelegate release!(df), to: DataFrame

  @doc """
  Checkpoint, run your function over the result, and free it on the way out.

      {:ok, counts} =
        Latu.with_checkpoint(expensive, [], fn base ->
          %{all: Latu.count!(base), big: base |> Latu.filter(greater(:n, 100)) |> Latu.count!()}
        end)

  The bracket form, as `File.open/3` is to `File.open/2`. The release happens in an `after`, so
  it runs even when your function raises — which is the whole point, and the reason to reach
  for this over `checkpoint/2` plus `release/1`. A release that itself fails is logged rather
  than raised, so it cannot replace your own exception with a duller one.

  What you cannot do with it is keep the frame: the checkpoint is gone when the function
  returns. Use `checkpoint/2` when you want one to live across REPL prompts.

  ## Options

  `checkpoint/2`'s, in the second argument — `:eager`, `:local`, `:storage_level`. `[]` is the
  common case and is not defaulted, so the function is always the last argument and the call
  stays pipeable.
  """
  @spec with_checkpoint(DataFrame.t(), keyword(), (DataFrame.t() -> result)) ::
          {:ok, result} | {:error, Error.t()}
        when result: term()
  defdelegate with_checkpoint(df, opts, fun), to: DataFrame

  @doc "Like `with_checkpoint/3`, raising on failure."
  @spec with_checkpoint!(DataFrame.t(), keyword(), (DataFrame.t() -> result)) :: result
        when result: term()
  defdelegate with_checkpoint!(df, opts, fun), to: DataFrame

  @doc """
  Add a column of consecutive indices, starting at 0.

      Latu.zip_with_index(df)
      Latu.zip_with_index(df, :row_num)

  Spark has no relation for this and PySpark has no function either: it is a projection of
  every column plus `distributed_sequence_id`, an internal expression registered for
  pandas-on-Spark. Hidden from `DESCRIBE FUNCTION`, so it is not in `Latu.Functions` — this
  verb is the way to reach it.

  The indices are consecutive across the whole frame, not per partition, which is what makes
  it different from `F.monotonically_increasing_id/0`.
  """
  @spec zip_with_index(DataFrame.t(), String.t() | atom()) :: DataFrame.t()
  defdelegate zip_with_index(df, name \\ :index), to: DataFrame

  @doc """
  Start a merge: upsert this frame into a target table.

      source
      |> Latu.as("s")
      |> Latu.merge_into("people", expr("people.id = s.id"))
      |> Latu.when_matched(:update, set: [name: col("s.name")])
      |> Latu.when_not_matched(:insert_all)
      |> Latu.merge()

  The frame is the *source*; `table` is the target, and `condition` is what decides whether a
  source row matches a target row. Both sides are in scope in the condition, so the names need
  qualifying — the target by its table name, the source by `as/2` (`Latu.as(df, "s")`), which
  is what makes `people.id = s.id` resolve.

  Nothing is sent until `merge/2`. This returns a `Latu.MergeInto`, which is inert data: the
  clauses are `when_matched/3`, `when_not_matched/3` and `when_not_matched_by_source/3`, and
  at least one is required.

  ## Options

    * `:schema_evolution` — let the merge add columns the source has and the target does not.
      Defaults to `false`. PySpark's `withSchemaEvolution()`.

  The three clause lists are also keys of the underlying struct, but they are built by the
  `when_*` verbs rather than passed here.

  **The target must be a table that supports row-level operations** — an Iceberg or Delta
  table, not a plain catalog table. Spark's own built-in sources do not, and refuse the merge
  at analysis; the plan is the same either way, which is why Latu ships the verb.
  """
  @spec merge_into(DataFrame.t(), String.t() | atom(), term(), keyword()) :: MergeInto.t()
  defdelegate merge_into(source, table, condition, opts \\ []), to: DataFrame

  @doc """
  Add a `WHEN MATCHED` clause: what to do with a source row that has a match in the target.

      Latu.when_matched(merge, :update_all)
      Latu.when_matched(merge, :update, set: [n: col("s.n")], on: expr("s.op = 'U'"))
      Latu.when_matched(merge, :delete, on: expr("s.op = 'D'"))

  The action is `:update`, `:update_all` or `:delete`; anything else raises naming the three.

  ## Options

    * `:set` — the assignments, as a keyword list of `target_column: expression`. **A key is
      a target column name** and travels as an expression string, so a keyword key and a
      string key are the same bytes; anything that is not a name is refused rather than sent.
    * `:on` — an extra condition narrowing the clause to rows that also satisfy it. Defaults
      to `nil`.

  `:set` is **required** for `:update` and **refused** for the other two — `:update_all` is
  the form that takes the source row whole.

  Clauses apply in the order you add them, and **only the first matching clause runs**, which
  is why an unconditional one belongs last. Spark enforces that ordering rule for SQL text
  only, in its parser, so Latu does not refuse it: the DataFrame path accepts the plan and a
  clause after an unconditional one is simply dead.
  """
  @spec when_matched(MergeInto.t(), atom(), keyword()) :: MergeInto.t()
  defdelegate when_matched(merge, action, opts \\ []), to: MergeInto

  @doc """
  Add a `WHEN NOT MATCHED` clause: what to do with a source row that has no match.

      Latu.when_not_matched(merge, :insert_all)
      Latu.when_not_matched(merge, :insert, set: [id: col("s.id"), name: col("s.name")])

  The action is `:insert` or `:insert_all` only — there is no row in the target to update or
  delete, and Latu refuses those by name rather than letting the server discover it.

  ## Options

    * `:set` — the assignments, as a keyword list of `target_column: expression`. **A key is
      a target column name** and travels as an expression string, so a keyword key and a
      string key are the same bytes; anything that is not a name is refused rather than sent.
    * `:on` — an extra condition narrowing the clause to rows that also satisfy it. Defaults
      to `nil`.

  `:set` is **required** for `:insert` and **refused** for `:insert_all`.
  """
  @spec when_not_matched(MergeInto.t(), atom(), keyword()) :: MergeInto.t()
  defdelegate when_not_matched(merge, action, opts \\ []), to: MergeInto

  @doc """
  Add a `WHEN NOT MATCHED BY SOURCE` clause: what to do with a *target* row that has no match.

      Latu.when_not_matched_by_source(merge, :delete)
      Latu.when_not_matched_by_source(merge, :update, set: [live: false])

  The mirror of `when_not_matched/3`, and the clause that makes a merge able to express a full
  synchronisation. The action is `:update`, `:update_all` or `:delete`, not the inserts.

  ## Options

    * `:set` — the assignments, as a keyword list of `target_column: expression`. **A key is
      a target column name** and travels as an expression string, so a keyword key and a
      string key are the same bytes; anything that is not a name is refused rather than sent.
    * `:on` — an extra condition narrowing the clause to rows that also satisfy it. Defaults
      to `nil`.

  `:set` is **required** for `:update` and **refused** for the other two.
  """
  @spec when_not_matched_by_source(MergeInto.t(), atom(), keyword()) :: MergeInto.t()
  defdelegate when_not_matched_by_source(merge, action, opts \\ []), to: MergeInto

  @doc """
  Run the merge.

  An action: everything before it was inert. Returns `:ok`, since a merge reports no rows —
  `merge_with_metrics/2` is the form that returns what an `observe/3` in the source counted,
  which is how you find out how many rows a merge touched without a second pass.

  ## Options

    * `:progress` — as `collect/2` describes. Defaults to `nil`.
  """
  @spec merge(MergeInto.t(), keyword()) :: :ok | {:error, Error.t()}
  defdelegate merge(merge, opts \\ []), to: DataFrame

  @doc "Like `merge/2`, raising on failure."
  @spec merge!(MergeInto.t(), keyword()) :: :ok
  defdelegate merge!(merge, opts \\ []), to: DataFrame

  @doc """
  `merge/2`, and the metrics an `observe/3` in the source plan asked for.

      {:ok, info} =
        source
        |> Latu.as("s")
        |> Latu.observe(:audit, rows: F.count(lit(1)))
        |> Latu.merge_into("people", expr("people.id = s.id"))
        |> Latu.when_not_matched(:insert_all)
        |> Latu.merge_with_metrics()

      info.observed  #=> %{audit: %{rows: 3}}

  The fifth writer path with a metrics twin, and PySpark threads its observations through this
  one too.

  ## Options

    * `:progress` — as `collect/2` describes. Defaults to `nil`.
  """
  @spec merge_with_metrics(MergeInto.t(), keyword()) ::
          {:ok, ExecutionInfo.t()} | {:error, Error.t()}
  defdelegate merge_with_metrics(merge, opts \\ []), to: DataFrame

  @doc "Like `merge_with_metrics/2`, raising on failure."
  @spec merge_with_metrics!(MergeInto.t(), keyword()) :: ExecutionInfo.t()
  defdelegate merge_with_metrics!(merge, opts \\ []), to: DataFrame

  @doc """
  A lateral join: the right side may reference the left's columns, row by row.

  ## Options

    * `:on` — a **condition** only; this relation has no `USING` form, so a bare column name
      is not accepted here as it is on `join/3`. Defaults to `nil`.
    * `:how` — the join type. Defaults to `:inner`; `:left` and `:cross` are the only others.
      Those three are what Spark's `LateralJoinType` accepts, and anything else — including
      `join/3`'s `:full`, `:semi` and `:anti` — is refused here rather than at the server.

  ## Examples

      Latu.lateral_join(orders, Latu.select_expr(orders, ["explode(items) as item"]))
      Latu.lateral_join(left, right, on: greater(:id, 1), how: :left)
  """
  @spec lateral_join(DataFrame.t(), DataFrame.t(), keyword()) :: DataFrame.t()
  defdelegate lateral_join(df, other, opts \\ []), to: DataFrame

  @doc """
  A nearest-neighbour join: rank the right side per left row and keep the best few.

  The third argument is the **ranking expression** — the distance or similarity being ranked
  on. Both frames are in scope in it, so the names need qualifying with `as/2`.

  ## Options

  Three of the four are required, and all four are validated before anything is sent,
  mirroring Spark's own `NearestByJoinValidation` as PySpark does — so the message reads the
  same wherever it fires.

    * `:num_results` — **required.** How many right rows to keep per left row. An integer from
      1 to 100,000.
    * `:mode` — **required.** How the ranking is computed.
      * `:approx` — an approximate search; cheaper on a large right side.
      * `:exact` — an exhaustive one.
    * `:direction` — **required.** Which end of the ranking is best.
      * `:distance` — smaller ranks better.
      * `:similarity` — larger ranks better.
    * `:how` — the join type. Defaults to `:inner`; `:left` is the only other, and keeps left
      rows that matched nothing.

  ## Examples

      Latu.nearest_by_join(queries, base, Latu.Column.expr("abs(q.x - b.x)"),
        num_results: 3,
        mode: :approx,
        direction: :distance
      )

      Latu.nearest_by_join(queries, base, Latu.Column.expr("cos_sim(q.v, b.v)"),
        num_results: 10,
        mode: :exact,
        direction: :similarity,
        how: :left
      )
  """
  @spec nearest_by_join(DataFrame.t(), DataFrame.t(), term(), keyword()) :: DataFrame.t()
  defdelegate nearest_by_join(df, other, ranking, opts), to: DataFrame

  @doc """
  Shuffle into `count` partitions, or partition by these columns, or both.

      Latu.repartition(df, 4)
      Latu.repartition(df, 4, [:suburb])

  `coalesce/2` is the same Spark relation without the shuffle, so it can only reduce the
  partition count. See `Latu.DataFrame.repartition/2`.
  """
  @spec repartition(DataFrame.t(), pos_integer() | term()) :: DataFrame.t()
  defdelegate repartition(df, count_or_columns), to: DataFrame

  @doc "See `repartition/2`."
  @spec repartition(DataFrame.t(), pos_integer(), term()) :: DataFrame.t()
  defdelegate repartition(df, count, columns), to: DataFrame

  @doc "Sort within each partition, leaving the partitions unordered."
  @spec sort_within_partitions(DataFrame.t(), term()) :: DataFrame.t()
  defdelegate sort_within_partitions(df, columns), to: DataFrame

  @doc "Fewer partitions without a shuffle."
  @spec coalesce(DataFrame.t(), pos_integer()) :: DataFrame.t()
  defdelegate coalesce(df, count), to: DataFrame

  @doc """
  Group rows, giving a `Latu.GroupedData` that `agg/2` turns back into a DataFrame.

      df |> Latu.group_by(:suburb) |> Latu.agg(total: Latu.Column.fun("sum", [:price]))

  See `Latu.DataFrame.group_by/2`.
  """
  @spec group_by(DataFrame.t(), term()) :: GroupedData.t()
  defdelegate group_by(df, columns), to: DataFrame

  @doc """
  Group by an explicit list of grouping sets — SQL's `GROUPING SETS`.

      df
      |> Latu.grouping_sets([[:suburb, :year], [:suburb], []], [:suburb, :year])
      |> Latu.agg(total: F.sum(:price))

  The fifth `Aggregate.GroupType`, and the one `rollup/2` and `cube/2` are special cases of:
  say exactly which combinations you want instead of every prefix or every subset. **An empty
  set is the grand total** and is legal — most of the point of the verb.

  The second argument is the grouping columns, which Spark needs in order to resolve the sets;
  PySpark takes them the same way, as `groupingSets(sets, *cols)`.
  """
  @spec grouping_sets(DataFrame.t(), [[term()]], term()) :: Latu.GroupedData.t()
  defdelegate grouping_sets(df, sets, columns \\ []), to: DataFrame

  @doc "Group by every prefix of these columns, plus the grand total."
  @spec rollup(DataFrame.t(), term()) :: GroupedData.t()
  defdelegate rollup(df, columns), to: DataFrame

  @doc "Group by every combination of these columns."
  @spec cube(DataFrame.t(), term()) :: GroupedData.t()
  defdelegate cube(df, columns), to: DataFrame

  @doc "Pivot a grouped frame on a column. See `Latu.GroupedData.pivot/3`."
  @spec pivot(GroupedData.t(), String.t() | atom(), [term()]) :: GroupedData.t()
  defdelegate pivot(grouped, column, values \\ []), to: GroupedData

  @doc """
  Apply aggregates, giving a DataFrame back.

      df |> Latu.group_by(:suburb) |> Latu.agg(total: F.sum(:price))
      Latu.agg(df, total: F.sum(:price))

  Takes a grouped frame or a plain DataFrame; without grouping it aggregates the whole thing.
  """
  @spec agg(GroupedData.t() | DataFrame.t(), term()) :: DataFrame.t()
  def agg(%GroupedData{} = grouped, aggregates), do: GroupedData.agg(grouped, aggregates)
  def agg(%DataFrame{} = df, aggregates), do: DataFrame.agg(df, aggregates)

  @doc """
  Rows per group (lazy), or how many rows there are (an action).

      df |> Latu.group_by(:suburb) |> Latu.count()   # a DataFrame with a count column
      df |> Latu.count()                             # {:ok, 10}

  Spark's own overloading: `GroupedData.count` is a transformation, `DataFrame.count` runs
  the query. The structs disambiguate.

  ## Options

    * `:progress` — a 1-arity function called with a `Latu.Progress` as the query runs.
      Defaults to `nil`. See `collect/2`.

  On a grouped frame the count is lazy and takes no options at all — passing any raises,
  because there is no execution to watch.

  See `Latu.GroupedData.count/1` and `Latu.DataFrame.count/1`.
  """
  @spec count(GroupedData.t()) :: DataFrame.t()
  @spec count(DataFrame.t(), keyword()) :: {:ok, non_neg_integer()} | {:error, Error.t()}
  def count(df, opts \\ [])
  def count(%GroupedData{} = grouped, []), do: GroupedData.count(grouped)
  def count(%DataFrame{} = df, opts), do: DataFrame.count(df, opts)

  @doc """
  Like `count/2`, raising on failure. On a grouped frame it is `count/1` itself — a lazy count
  cannot fail, and the `!` is accepted so the pair reads the same after a `group_by/2`.
  """
  @spec count!(GroupedData.t()) :: DataFrame.t()
  @spec count!(DataFrame.t(), keyword()) :: non_neg_integer()
  def count!(df, opts \\ [])
  def count!(%GroupedData{} = grouped, []), do: GroupedData.count(grouped)
  def count!(%DataFrame{} = df, opts), do: DataFrame.count!(df, opts)

  @doc """
  All the rows of both, duplicates kept.

  Matches by **position** by default, as Spark's `union` does — the column *names* of the
  second frame are ignored, so two frames whose columns are in different orders union into
  nonsense rather than an error. `by_name: true` is PySpark's `unionByName`.

  ## Options

    * `:all` — keep duplicates. Defaults to `true` here, which is what `union` means; `false`
      is a distinct union.
    * `:by_name` — match columns by name instead of position. Defaults to `false`.
    * `:allow_missing_columns` — let the two frames have different columns, filling the gaps
      with nulls. Defaults to `false`, and raises unless `by_name: true` — as it does in
      PySpark.

  ## Examples

      Latu.union(df, other)
      Latu.union(df, other, by_name: true)
      Latu.union(df, other, by_name: true, allow_missing_columns: true)
      Latu.union(df, other, all: false)

  See `Latu.DataFrame.union/3`.
  """
  @spec union(DataFrame.t(), DataFrame.t(), keyword()) :: DataFrame.t()
  defdelegate union(df, other, opts \\ []), to: DataFrame

  @doc """
  Rows in both, distinct unless `all: true`. Matches by position.

  ## Options

    * `:all` — keep duplicates, pairing them up as `INTERSECT ALL` does. Defaults to `false`,
      which is what `intersect` means. PySpark spells the other one `intersectAll`.
    * `:by_name`, `:allow_missing_columns` — accepted, but setting either to `true` raises:
      they apply to `:union` only, as in PySpark.

  ## Examples

      Latu.intersect(df, other)
      Latu.intersect(df, other, all: true)

  See `Latu.DataFrame.intersect/3`.
  """
  @spec intersect(DataFrame.t(), DataFrame.t(), keyword()) :: DataFrame.t()
  defdelegate intersect(df, other, opts \\ []), to: DataFrame

  @doc """
  Rows in the first and not the second, distinct unless `all: true`. Matches by position.

  ## Options

    * `:all` — keep duplicates, pairing them up as `EXCEPT ALL` does. Defaults to `false`.
    * `:by_name`, `:allow_missing_columns` — accepted, but setting either to `true` raises:
      they apply to `:union` only, as in PySpark.

  ## Examples

      Latu.except(df, other)
      Latu.except(df, other, all: true)

  PySpark spells these `subtract` and `exceptAll`. See `Latu.DataFrame.except/3`.
  """
  @spec except(DataFrame.t(), DataFrame.t(), keyword()) :: DataFrame.t()
  defdelegate except(df, other, opts \\ []), to: DataFrame

  @doc """
  Join two DataFrames.

  Lazy: nothing is sent until an action. Both frames must come from the same session.

  ## Options

    * `:on` — what to join on: a column name, a list of names, or a condition expression.
      Defaults to `nil`. **Names and a condition are not the same join.** Names become Spark's
      `using_columns`, which matches on equality *and collapses the duplicate column*; a
      condition leaves both columns in the result. With no `:on` at all and `how: :cross`, it
      is a cross join; with no `:on` and any other `:how`, Spark decides, which is rarely what
      you meant.
    * `:how` — the join type. Defaults to `:inner`. One of:
      * `:inner` — rows matching on both sides.
      * `:cross` — every pair, so normally with no `:on`.
      * `:full` — every row from both, nulls where there is no match. Spark's `full_outer`.
      * `:left` — every left row. Spark's `left_outer`.
      * `:right` — every right row. Spark's `right_outer`.
      * `:semi` — left rows that have a match, and only the left columns. Spark's `left_semi`.
      * `:anti` — left rows that have **no** match, left columns only. Spark's `left_anti`.

  Spark's aliases (`"outer"`, `"leftouter"`, `"left_anti"`, …) are not accepted: one spelling
  per join type, and an unknown one raises naming the seven. See `docs/deviations.md`.

  ## Examples

      # match on a shared name; one `customer_id` column comes back
      Latu.join(orders, customers, on: :customer_id)

      # several names
      Latu.join(orders, customers, on: [:region, :customer_id])

      # a condition; both id columns survive, so qualify them downstream
      Latu.join(orders, customers, on: Latu.Column.expr("o.id = c.id"), how: :left)

      # orders with no matching customer
      Latu.join(orders, customers, on: :customer_id, how: :anti)

      Latu.join(sizes, colours, how: :cross)

  See `Latu.DataFrame.join/3`.
  """
  @spec join(DataFrame.t(), DataFrame.t(), keyword()) :: DataFrame.t()
  defdelegate join(df, other, opts \\ []), to: DataFrame

  @doc """
  A reference to one of this DataFrame's columns, tagged with its identity.

  See `Latu.DataFrame.col/2`. For an untagged reference, `Latu.Column.col/1`.
  """
  @spec col(DataFrame.t(), String.t() | atom()) :: Latu.Plan.expression()
  defdelegate col(df, name), to: DataFrame

  @doc """
  This DataFrame as a scalar subquery — a single value, hoisted into the plan that uses it.

      Latu.filter(orders, greater(:amount, Latu.scalar(totals)))

  See `Latu.DataFrame.scalar/1`.
  """
  @spec scalar(DataFrame.t()) :: Latu.Plan.expression()
  defdelegate scalar(df), to: DataFrame

  @doc """
  A predicate that holds when this DataFrame has any rows.

      Latu.filter(orders, Latu.exists(open_alerts))

  See `Latu.DataFrame.exists/1`. `Latu.Functions.exists/2` is the array function, not this.
  """
  @spec exists(DataFrame.t()) :: Latu.Plan.expression()
  defdelegate exists(df), to: DataFrame

  @doc """
  Columns whose names match a Java regex.

      Latu.select(df, Latu.col_regex(df, "`.*_id`"))

  Spark wants the pattern in backticks. Always relative to a frame, as PySpark's `colRegex`
  is — the pattern matches that frame's column names, so it takes the frame rather than
  standing alone like `Latu.Column.col/1`.

  Note that a plain `"prefix.*"` is **not** a regex to Spark: `select/2` sends it as an
  ordinary attribute and the analyzer expands it as a qualified star. This is the explicit
  form, and PySpark has no implicit one either.
  """
  @spec col_regex(DataFrame.t(), String.t()) :: Latu.Plan.expression()
  defdelegate col_regex(df, pattern), to: DataFrame

  @doc """
  A hidden metadata column — `_metadata` on a file source, and whatever a source adds.

      Latu.select(df, path: Latu.Column.expr("_metadata.file_path"))
      Latu.select(df, meta: Latu.metadata_column(df, "_metadata"))

  Not in the frame's schema, so `columns/1` does not list it and a bare `col/2` will not
  resolve it; this sets the flag that asks for one.
  """
  @spec metadata_column(DataFrame.t(), String.t() | atom()) :: Latu.Plan.expression()
  defdelegate metadata_column(df, name), to: DataFrame

  @doc """
  Name the DataFrame, so its columns can be qualified as `name.column`.

  Spark calls this `alias`, which Elixir cannot use as a function name.
  """
  @spec as(DataFrame.t(), String.t() | atom()) :: DataFrame.t()
  defdelegate as(df, name), to: DataFrame

  # =============================================
  # Actions
  # =============================================

  @doc """
  Print the table Spark renders, and return `:ok`.

  Byte for byte what PySpark's `df.show()` prints: Spark formats it server-side and Latu
  decodes one string cell.

  ## Options

  All PySpark's, plus `:progress`.

    * `:num_rows` — how many rows to print. Defaults to `20`.
    * `:truncate` — cell width. Defaults to `20`. `true` means 20, `false` means no
      truncation, and an integer is that width.
    * `:vertical` — one field per line instead of a table, which is how a wide frame stays
      readable. Defaults to `false`.
    * `:progress` — as `collect/2` describes. Defaults to `nil`.

  ## Examples

      Latu.range(session, 5) |> Latu.show()
      Latu.show(df, num_rows: 100)
      Latu.show(df, truncate: false)
      Latu.show(df, vertical: true, num_rows: 3)

  `glimpse/2` is usually the better read on a wide frame. See `Latu.DataFrame.show/2`.
  """
  @spec show(DataFrame.t(), keyword()) :: :ok | {:error, Error.t()}
  defdelegate show(df, opts \\ []), to: DataFrame

  @doc "Like `show/2`, raising on failure."
  @spec show!(DataFrame.t(), keyword()) :: :ok
  defdelegate show!(df, opts \\ []), to: DataFrame

  @doc """
  The table `show/2` prints, as an HTML string. Spark's own `_repr_html_`.

  In Livebook a frame renders itself through this — add `:kino` to your dependencies and
  inspect one in a cell; nothing here needs it otherwise.

  ## Options

  `show/2`'s, less `:vertical`, which Spark's `HtmlString` has no field for.

    * `:num_rows` — how many rows. Defaults to `20`.
    * `:truncate` — cell width. Defaults to `20`.
    * `:progress` — as `collect/2` describes. Defaults to `nil`.

  ## Examples

      Latu.range(session, 5) |> Latu.to_html!()
      Latu.to_html(df, num_rows: 5, truncate: false)
  """
  @spec to_html(DataFrame.t(), keyword()) :: {:ok, String.t()} | {:error, Error.t()}
  defdelegate to_html(df, opts \\ []), to: DataFrame

  @doc "Like `to_html/2`, raising on failure."
  @spec to_html!(DataFrame.t(), keyword()) :: String.t()
  defdelegate to_html!(df, opts \\ []), to: DataFrame

  @doc """
  A transposed preview: one line per column, with its type and its first few values.

      Latu.glimpse(df)
      #=> Rows: at least 10
      #=> Columns: 3
      #=> $ id    <bigint> 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
      #=> $ name  <string> "Ada", "Bo", nil, "Grace", nil, "Alan", …
      #=> $ score <double> 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, …

  Reads far better than `show/2` on a wide frame, which is most real frames. Two round trips:
  the schema, then a `limit`ed collect. Values are rendered with `inspect/1`, so a string is
  quoted and a null is `nil`.

  **`Rows:` is exact only when it is free.** A sample that comes back short of `num_rows` has
  proved there was nothing more to give, so the count is known; otherwise it reads
  `at least 10`, because a real count on a Spark frame is a full scan. `count: true` pays for
  the exact number, and is the only unbounded thing here.

  ## Options

    * `:num_rows` — values shown per column. Defaults to `10`.
    * `:width` — where a line is cut. Defaults to `80`; `:infinity` cuts nothing.
    * `:count` — pay for the exact row count. Defaults to `false`.

  There is deliberately no `:progress` — everything glimpse reads is bounded except
  `count: true`, and `count/2` takes a handler if you want to watch that.

  ## Examples

      Latu.glimpse(df)
      Latu.glimpse(df, num_rows: 3, width: :infinity)
      Latu.glimpse(df, count: true)

  Spark has no method like this; the name, the `$` lines and the transposed shape are dplyr's
  and Polars'. `docs/deviations.md`.
  """
  @spec glimpse(DataFrame.t(), keyword()) :: :ok | {:error, Error.t()}
  defdelegate glimpse(df, opts \\ []), to: DataFrame

  @doc "Like `glimpse/2`, raising on failure."
  @spec glimpse!(DataFrame.t(), keyword()) :: :ok
  defdelegate glimpse!(df, opts \\ []), to: DataFrame

  @doc """
  The frame's columns, with Spark's own name for each type. An action: the server analyses
  the plan.

      Latu.schema(df)
      #=> {:ok, [%{name: "id", type: "bigint", nullable: false},
      #=>        %{name: "tags", type: "array<string>", nullable: true}]}

  Nested types render into the type string, so the list stays flat. Latu has no client-side
  type model in either direction: a schema you *send* is a string the server parses
  (`create_dataframe/3`, `read/2`), and one you *read back* is this.
  """
  @spec schema(DataFrame.t()) :: {:ok, [Latu.Result.field()]} | {:error, Error.t()}
  defdelegate schema(df), to: DataFrame

  @doc "Like `schema/1`, raising on failure."
  @spec schema!(DataFrame.t()) :: [Latu.Result.field()]
  defdelegate schema!(df), to: DataFrame

  @doc """
  The column names.

      Latu.columns(df)  #=> {:ok, ["id", "name"]}
  """
  @spec columns(DataFrame.t()) :: {:ok, [String.t()]} | {:error, Error.t()}
  defdelegate columns(df), to: DataFrame

  @doc "Like `columns/1`, raising on failure."
  @spec columns!(DataFrame.t()) :: [String.t()]
  defdelegate columns!(df), to: DataFrame

  @doc """
  Name and type per column, as pairs — PySpark's `df.dtypes`.

      Latu.dtypes(df)  #=> {:ok, [{"id", "bigint"}, {"name", "string"}]}
  """
  @spec dtypes(DataFrame.t()) :: {:ok, [{String.t(), String.t()}]} | {:error, Error.t()}
  defdelegate dtypes(df), to: DataFrame

  @doc "Like `dtypes/1`, raising on failure."
  @spec dtypes!(DataFrame.t()) :: [{String.t(), String.t()}]
  defdelegate dtypes!(df), to: DataFrame

  @doc """
  Print the schema tree Spark renders, and return `:ok`.

      Latu.print_schema!(df)
      # root
      #  |-- id: long (nullable = false)

  Spark does the rendering, so the output is its own — PySpark renders this one client-side,
  which is the difference (`docs/deviations.md`).

  ## Options

    * `:level` — bound the depth of the tree. A positive integer; anything else raises.
      Defaults to `nil`, meaning the whole schema.

  ## Examples

      Latu.print_schema!(df)
      Latu.print_schema!(df, level: 1)
  """
  @spec print_schema(DataFrame.t(), keyword()) :: :ok | {:error, Error.t()}
  defdelegate print_schema(df, opts \\ []), to: DataFrame

  @doc "Like `print_schema/2`, raising on failure."
  @spec print_schema!(DataFrame.t(), keyword()) :: :ok
  defdelegate print_schema!(df, opts \\ []), to: DataFrame

  @doc """
  The schema tree as a string, where `print_schema/2` prints it.

  ## Options

    * `:level` — bound the depth of the tree. A positive integer; anything else raises.
      Defaults to `nil`, meaning the whole schema.
  """
  @spec tree_string(DataFrame.t(), keyword()) :: {:ok, String.t()} | {:error, Error.t()}
  defdelegate tree_string(df, opts \\ []), to: DataFrame

  @doc "Like `tree_string/2`, raising on failure."
  @spec tree_string!(DataFrame.t(), keyword()) :: String.t()
  defdelegate tree_string!(df, opts \\ []), to: DataFrame

  @doc """
  Print the plan Spark would run, and return `:ok`.

      Latu.explain!(df)
      Latu.explain!(df, mode: :formatted)

  ## Options

    * `:mode` — how much plan to print. Defaults to `:simple`.
      * `:simple` — the physical plan alone.
      * `:extended` — parsed, analysed, optimised and physical plans.
      * `:codegen` — the physical plan plus the generated code.
      * `:cost` — the optimised plan with statistics, where they have been computed.
      * `:formatted` — a split view: the plan outline, then per-node detail.

  PySpark spells the same thing two ways — `explain(True)` and `explain(mode="extended")` —
  and refuses both together; there is one spelling here. `explain_string/2` returns it instead
  of printing.
  """
  @spec explain(DataFrame.t(), keyword()) :: :ok | {:error, Error.t()}
  defdelegate explain(df, opts \\ []), to: DataFrame

  @doc "Like `explain/2`, raising on failure."
  @spec explain!(DataFrame.t(), keyword()) :: :ok
  defdelegate explain!(df, opts \\ []), to: DataFrame

  @doc """
  The plan as a string, where `explain/2` prints it.

  ## Options

  `explain/2`'s: `:mode`.
  """
  @spec explain_string(DataFrame.t(), keyword()) :: {:ok, String.t()} | {:error, Error.t()}
  defdelegate explain_string(df, opts \\ []), to: DataFrame

  @doc "Like `explain_string/2`, raising on failure."
  @spec explain_string!(DataFrame.t(), keyword()) :: String.t()
  defdelegate explain_string!(df, opts \\ []), to: DataFrame

  @doc """
  Whether the frame has no rows.

  A one-row limit, counted server-side. PySpark spells it as an empty projection plus
  `take(1)`; the answer is the same (`docs/deviations.md`).
  """
  @spec is_empty(DataFrame.t()) :: {:ok, boolean()} | {:error, Error.t()}
  defdelegate is_empty(df), to: DataFrame

  @doc "Like `is_empty/1`, raising on failure."
  @spec is_empty!(DataFrame.t()) :: boolean()
  defdelegate is_empty!(df), to: DataFrame

  @doc "Whether Spark can run this plan without a cluster — `spark.range(5)` cannot."
  @spec is_local(DataFrame.t()) :: {:ok, boolean()} | {:error, Error.t()}
  defdelegate is_local(df), to: DataFrame

  @doc "Like `is_local/1`, raising on failure."
  @spec is_local!(DataFrame.t()) :: boolean()
  defdelegate is_local!(df), to: DataFrame

  @doc "Whether the frame is a streaming source. Latu does not build one yet; the answer is no."
  @spec is_streaming(DataFrame.t()) :: {:ok, boolean()} | {:error, Error.t()}
  defdelegate is_streaming(df), to: DataFrame

  @doc "Like `is_streaming/1`, raising on failure."
  @spec is_streaming!(DataFrame.t()) :: boolean()
  defdelegate is_streaming!(df), to: DataFrame

  @doc "The files this frame reads, as the server resolved them. Empty for a computed frame."
  @spec input_files(DataFrame.t()) :: {:ok, [String.t()]} | {:error, Error.t()}
  defdelegate input_files(df), to: DataFrame

  @doc "Like `input_files/1`, raising on failure."
  @spec input_files!(DataFrame.t()) :: [String.t()]
  defdelegate input_files!(df), to: DataFrame

  @doc """
  Whether two frames compute the same thing, up to the plan Spark analyses.

  Spelling a query two ways gives one answer; a different literal gives another. Both frames
  must share a session.
  """
  @spec same_semantics(DataFrame.t(), DataFrame.t()) :: {:ok, boolean()} | {:error, Error.t()}
  defdelegate same_semantics(df, other), to: DataFrame

  @doc "Like `same_semantics/2`, raising on failure."
  @spec same_semantics!(DataFrame.t(), DataFrame.t()) :: boolean()
  defdelegate same_semantics!(df, other), to: DataFrame

  @doc "A hash of the analysed plan: equal for frames `same_semantics/2` calls equal."
  @spec semantic_hash(DataFrame.t()) :: {:ok, integer()} | {:error, Error.t()}
  defdelegate semantic_hash(df), to: DataFrame

  @doc "Like `semantic_hash/1`, raising on failure."
  @spec semantic_hash!(DataFrame.t()) :: integer()
  defdelegate semantic_hash!(df), to: DataFrame

  @doc """
  Ask the server to cache this frame, and hand it back.

  Over Connect this is a round trip, where classic Spark's is a driver-local call that cannot
  fail — hence the tuple, with `persist!/2` for the pipe. The caching itself is still lazy: a
  success means the server registered the query, not that anything is materialised.

  ## Options

    * `:level` — one of Spark's own storage levels. Defaults to `:memory_and_disk_deser`, as
      in Scala since 3.0.
      * `:none` — no storage.
      * `:disk_only`, `:disk_only_2`, `:disk_only_3` — disk, at 1, 2 or 3 replicas.
      * `:memory_only`, `:memory_only_2` — memory, serialized, at 1 or 2 replicas.
      * `:memory_and_disk`, `:memory_and_disk_2` — memory, spilling to disk.
      * `:memory_and_disk_deser` — the same, deserialized. The default.
      * `:off_heap` — off-heap memory, spilling to disk.

  ## Examples

      df = Latu.cache!(df)
      df |> Latu.persist!(level: :disk_only) |> Latu.count()
      {:ok, df} = Latu.persist(df, level: :memory_only_2)
  """
  @spec persist(DataFrame.t(), keyword()) :: {:ok, DataFrame.t()} | {:error, Error.t()}
  defdelegate persist(df, opts \\ []), to: DataFrame

  @doc "Like `persist/2`, raising on failure. Returns the DataFrame, so it pipes."
  @spec persist!(DataFrame.t(), keyword()) :: DataFrame.t()
  defdelegate persist!(df, opts \\ []), to: DataFrame

  @doc "`persist/2` at Spark's default level, which is what `cache` means everywhere."
  @spec cache(DataFrame.t()) :: {:ok, DataFrame.t()} | {:error, Error.t()}
  defdelegate cache(df), to: DataFrame

  @doc "Like `cache/1`, raising on failure. Returns the DataFrame, so it pipes."
  @spec cache!(DataFrame.t()) :: DataFrame.t()
  defdelegate cache!(df), to: DataFrame

  @doc """
  Drop the server's cache of this frame, and hand it back.

  ## Options

    * `:blocking` — wait for the blocks to be freed before answering. Defaults to `false`,
      which is PySpark's default too. Sent either way: the field has presence, so an absent
      one would be a different message.

  ## Examples

      df = Latu.unpersist!(df)
      {:ok, df} = Latu.unpersist(df, blocking: true)
  """
  @spec unpersist(DataFrame.t(), keyword()) :: {:ok, DataFrame.t()} | {:error, Error.t()}
  defdelegate unpersist(df, opts \\ []), to: DataFrame

  @doc "Like `unpersist/2`, raising on failure. Returns the DataFrame, so it pipes."
  @spec unpersist!(DataFrame.t(), keyword()) :: DataFrame.t()
  defdelegate unpersist!(df, opts \\ []), to: DataFrame

  @doc """
  How the server is storing this frame, if at all.

      Latu.storage_level!(df)
      #=> %{name: :memory_and_disk_deser, use_disk: true, use_memory: true,
      #=>   use_off_heap: false, deserialized: true, replication: 1}

  `:name` is Spark's name for that combination of flags, or `nil` where it has none. An
  uncached frame is `:none`.
  """
  @spec storage_level(DataFrame.t()) :: {:ok, map()} | {:error, Error.t()}
  defdelegate storage_level(df), to: DataFrame

  @doc "Like `storage_level/1`, raising on failure."
  @spec storage_level!(DataFrame.t()) :: map()
  defdelegate storage_level!(df), to: DataFrame

  @doc """
  What a DDL schema string means to the server, in `schema/1`'s shape.

      Latu.parse_ddl!(session, "id INT, tags ARRAY<STRING>")
      #=> [%{name: "id", type: "int", nullable: true},
      #=>  %{name: "tags", type: "array<string>", nullable: true}]

  Latu sends schemas as strings (`read/2`, `create_dataframe/3`), so this is how you check one
  without running anything. PySpark keeps the same arm private.
  """
  @spec parse_ddl(Session.t(), String.t()) ::
          {:ok, [Latu.Result.field()]} | {:error, Error.t()}
  def parse_ddl(%Session{} = session, ddl) when is_binary(ddl) do
    with {:ok, data_type, _session} <- Client.analyzed(session, Plan.analyze(:ddl_parse, ddl)) do
      Latu.Result.Schema.fields(data_type)
    end
  end

  @doc "Like `parse_ddl/2`, raising on failure."
  @spec parse_ddl!(Session.t(), String.t()) :: [Latu.Result.field()]
  def parse_ddl!(%Session{} = session, ddl) do
    case parse_ddl(session, ddl) do
      {:ok, fields} -> fields
      {:error, error} -> raise error
    end
  end

  @doc """
  The `DataType` message the server parses a DDL string into.

      {:ok, type} = Latu.parse_ddl_type(session, "id BIGINT, name STRING")
      df = Latu.to(df, type)

  `parse_ddl/2` *reports* the same parse as data; this hands back Spark's own type message,
  which is what `to/2` needs. `ToSchema.schema` is a `DataType` with no string alternative:
  on 4.2.0, `DataTypeProtoConverter.toCatalystType` has no `UNPARSED` case, so a DDL string in
  that field is refused.

  This is the one place a generated protobuf struct is part of Latu's public surface, and it is
  deliberate: Latu holds no client-side type model (`docs/decisions.md`, M8.1 and M10.1), so
  the only honest way to name a target schema is to let the server name it. Treat the value as
  opaque — pass it to `to/2`, do not read it.
  """
  @spec parse_ddl_type(Session.t(), String.t()) ::
          {:ok, Plan.data_type()} | {:error, Error.t()}
  def parse_ddl_type(%Session{} = session, ddl) when is_binary(ddl) do
    with {:ok, data_type, _session} <- Client.analyzed(session, Plan.analyze(:ddl_parse, ddl)) do
      {:ok, data_type}
    end
  end

  @doc "Like `parse_ddl_type/2`, raising on failure."
  @spec parse_ddl_type!(Session.t(), String.t()) :: Plan.data_type()
  def parse_ddl_type!(%Session{} = session, ddl) do
    case parse_ddl_type(session, ddl) do
      {:ok, data_type} -> data_type
      {:error, error} -> raise error
    end
  end

  @doc """
  Reconcile a frame to a target schema.

      type = Latu.parse_ddl_type!(session, "name STRING, id BIGINT")
      df = Latu.to(df, type)

  Spark matches columns **by name**, case-insensitively unless `spark.sql.caseSensitive` says
  otherwise, and then:

    * reorders them into the target's order;
    * projects away source columns the target does not name;
    * casts where the types are compatible — numeric to numeric, erroring on overflow, but not
      string to int;
    * fails when a column the target names is missing **and the target field is not nullable**;
    * **fills it with null when the target field is nullable** — which is the default for
      every field a DDL string declares, so `parse_ddl_type!(session, "a INT")` against a frame
      with no `a` gives a column of nulls rather than an error.

  `Dataset.to`'s own scaladoc says missing columns "lead to failures", but `Project.reorderFields`
  fills a nullable one with `Literal.create(null, ...)`. Declare `NOT NULL` in the DDL when a
  missing column should be an error. `docs/deviations.md`.

  Spark's own name for the verb. The schema is a type message rather than a string because the
  wire field has no string form — `parse_ddl_type/2` is how you get one, and its docs say why.
  """
  @spec to(DataFrame.t(), Plan.data_type()) :: DataFrame.t()
  defdelegate to(df, schema), to: DataFrame

  @doc """
  Fill nulls with a value.

  ## Options

    * `:subset` — the columns to fill: one name, or a list of them. Defaults to `[]`, which
      means every column the value's type fits.

  ## Examples

      Latu.fill_na(df, 0)                          # every column the value's type fits
      Latu.fill_na(df, 0, subset: [:score])        # only these
      Latu.fill_na(df, score: 0, team: "unknown")  # a value per column

  **The type is the filter, not an error.** Spark fills only the columns whose type matches the
  value, so filling a string column with a number does nothing at all — quietly. Name the
  columns, or pass pairs, if you want to be sure.
  """
  @spec fill_na(DataFrame.t(), term(), keyword()) :: DataFrame.t()
  defdelegate fill_na(df, value, opts \\ []), to: DataFrame

  @doc """
  Drop rows by how many non-null values they carry.

  ## Options

    * `:how` — `:any` (the default) drops a row with any null in it; `:all` drops only rows
      that are null all the way across. Ignored when `:min_non_nulls` is given.
    * `:min_non_nulls` — keep rows carrying at least this many non-null values, overriding
      `:how`, as PySpark's `thresh` overrides its. Spark's own name for the wire field, where
      PySpark abbreviates (`docs/deviations.md`).
    * `:subset` — the columns to judge on: one name, or a list of them. Defaults to `[]`,
      which means every column.

  ## Examples

      Latu.drop_na(df)                       # any null anywhere
      Latu.drop_na(df, how: :all)            # only rows that are null all the way across
      Latu.drop_na(df, how: :any)            # the default, written out
      Latu.drop_na(df, min_non_nulls: 3)     # keep rows with at least three
      Latu.drop_na(df, subset: [:score])     # judge on these columns only
      Latu.drop_na(df, subset: :score)       # one column needs no list
  """
  @spec drop_na(DataFrame.t(), keyword()) :: DataFrame.t()
  defdelegate drop_na(df, opts \\ []), to: DataFrame

  @doc """
  Replace values with other values, as `{old, new}` pairs.

  ## Options

    * `:subset` — the columns to replace in: one name, or a list of them. Defaults to `[]`,
      which means every column.

  ## Examples

      Latu.replace(df, [{"red", "crimson"}])
      Latu.replace(df, [{"red", "crimson"}, {"blue", "navy"}], subset: [:team])

  Both sides are literals, and as with `fill_na/3` the types have to line up: a pair whose old
  value cannot occur in a column simply never matches there.
  """
  @spec replace(DataFrame.t(), [{term(), term()}], keyword()) :: DataFrame.t()
  defdelegate replace(df, replacements, opts \\ []), to: DataFrame

  @doc """
  Summary statistics: one row per statistic, one column per column Spark can summarise.

      Latu.summary(df)                        # count, mean, stddev, min, 25%, 50%, 75%, max
      Latu.summary(df, ["count", "min", "max"])
      Latu.summary(df, "90%")

  A lazy relation, like every other verb here — nothing runs until you collect or show it.
  Names are Spark's, percentiles included.
  """
  @spec summary(DataFrame.t(), [String.t() | atom()] | String.t() | atom()) :: DataFrame.t()
  defdelegate summary(df, statistics \\ []), to: DataFrame

  @doc """
  `summary/2`'s fixed five — count, mean, stddev, min, max — over the columns named.

      Latu.describe(df)
      Latu.describe(df, [:score, :weight])

  A separate Spark relation from `summary/2`, not an option on it.
  """
  @spec describe(DataFrame.t(), [String.t() | atom()] | String.t() | atom()) :: DataFrame.t()
  defdelegate describe(df, cols \\ []), to: DataFrame

  @doc """
  A contingency table of two columns.

      Latu.crosstab(df, :team, :grade)

  The first column of the result is named `col1_col2` and holds `col1`'s distinct values; the
  remaining columns are `col2`'s distinct values, and the cells are counts. Spark's naming, and
  a null becomes the string `"null"` there.
  """
  @spec crosstab(DataFrame.t(), String.t() | atom(), String.t() | atom()) :: DataFrame.t()
  defdelegate crosstab(df, col1, col2), to: DataFrame

  @doc """
  Frequent items, one array column of candidates per column named.

  The algorithm is Karp, Schenker and Papadimitriou's, so the result may contain false
  positives — Spark says so too.

  ## Options

    * `:support` — the minimum frequency a value must reach to be a candidate, as a fraction
      of the rows. Defaults to `0.01`, as in PySpark.

  ## Examples

      Latu.freq_items(df, [:score, :team])
      Latu.freq_items(df, :score, support: 0.4)
  """
  @spec freq_items(DataFrame.t(), [String.t() | atom()] | String.t() | atom(), keyword()) ::
          DataFrame.t()
  defdelegate freq_items(df, cols, opts \\ []), to: DataFrame

  @doc """
  A stratified sample: a fraction of the rows per stratum.

      Latu.sample_by(df, :team, [{"red", 0.5}, {"blue", 1.0}], seed: 42)

  Strata are **values**, not column names, so they are given as strings or numbers — an atom is
  a column reference everywhere else in Latu and would be ambiguous here. A stratum the map does
  not mention contributes no rows.

  ## Options

    * `:seed` — an integer. Defaults to `nil`, which draws one at random exactly as `sample/3`
      does, so the plan differs between runs. Pass one to make it reproducible.

  ## Examples

      Latu.sample_by(df, :team, [{"red", 0.5}, {"blue", 1.0}], seed: 42)
      Latu.sample_by(df, :team, %{"red" => 0.5, "blue" => 1.0})
  """
  @spec sample_by(DataFrame.t(), term(), [{term(), number()}] | map(), keyword()) ::
          DataFrame.t()
  defdelegate sample_by(df, col, fractions, opts \\ []), to: DataFrame

  @doc """
  Sample covariance of two numeric columns. An action: it runs when called.

      Latu.cov(df, :score, :weight)  #=> {:ok, 12.5}

  **A null counts as zero here; it does not drop the row.** Spark wraps each column in
  `when(isnull(c), 0.0)` before aggregating (`StatFunctions.calculateCovImpl`), so this and
  `F.covar_samp/2` — which ignores a row where either value is null — give **different answers
  on the same data**. Both are Spark's. Use the aggregate if you want the nulls gone.

  ## Options

    * `:progress` — as `collect/2` describes. Defaults to `nil`.
  """
  @spec cov(DataFrame.t(), String.t() | atom(), String.t() | atom(), keyword()) ::
          {:ok, float()} | {:error, Error.t()}
  defdelegate cov(df, col1, col2, opts \\ []), to: DataFrame

  @doc "Like `cov/4`, raising on failure."
  @spec cov!(DataFrame.t(), String.t() | atom(), String.t() | atom(), keyword()) :: float()
  defdelegate cov!(df, col1, col2, opts \\ []), to: DataFrame

  @doc """
  Correlation of two numeric columns. An action: it runs when called.

      Latu.corr(df, :score, :weight)  #=> {:ok, 0.98}

  **A null counts as zero here too**, exactly as in `cov/4`, and for the same reason — so this
  and `F.corr/2` disagree on data with nulls. A correlation Spark cannot compute comes back as
  `NaN` rather than an error.

  ## Options

    * `:method` — the correlation method. Defaults to `:pearson`, and Spark has no other; it
      is an option only because PySpark's signature has one.
    * `:progress` — as `collect/2` describes. Defaults to `nil`.
  """
  @spec corr(DataFrame.t(), String.t() | atom(), String.t() | atom(), keyword()) ::
          {:ok, float()} | {:error, Error.t()}
  defdelegate corr(df, col1, col2, opts \\ []), to: DataFrame

  @doc "Like `corr/4`, raising on failure."
  @spec corr!(DataFrame.t(), String.t() | atom(), String.t() | atom(), keyword()) :: float()
  defdelegate corr!(df, col1, col2, opts \\ []), to: DataFrame

  @doc """
  Approximate quantiles. An action: it runs when called.

      Latu.approx_quantile(df, :score, [0.0, 0.5, 1.0], 0.01)
      #=> {:ok, [10.0, 30.0, 70.0]}

      Latu.approx_quantile(df, [:score, :weight], [0.5], 0.01)
      #=> {:ok, [[30.0], [3.0]]}

  One name gives one flat list, a list of names gives a list per column — PySpark's asymmetry,
  kept because it is the shape callers expect. `relative_error` is the accuracy Spark may trade
  away for speed; 0.0 asks for exact quantiles, which is expensive. **Nulls and NaNs are
  ignored here** — the opposite of `cov/4` and `corr/4`, and Spark's own documented rule — and a
  column with no values left gives an empty list.

  ## Options

    * `:progress` — as `collect/2` describes. Defaults to `nil`.
  """
  @spec approx_quantile(
          DataFrame.t(),
          [String.t() | atom()] | String.t() | atom(),
          [number()],
          number(),
          keyword()
        ) :: {:ok, [float()] | [[float()]]} | {:error, Error.t()}
  defdelegate approx_quantile(df, cols, probabilities, relative_error, opts \\ []),
    to: DataFrame

  @doc "Like `approx_quantile/5`, raising on failure."
  @spec approx_quantile!(
          DataFrame.t(),
          [String.t() | atom()] | String.t() | atom(),
          [number()],
          number(),
          keyword()
        ) :: [float()] | [[float()]]
  defdelegate approx_quantile!(df, cols, probabilities, relative_error, opts \\ []),
    to: DataFrame

  @doc """
  Write to a path. An action: the write runs when called.

      Latu.write(df, format: "parquet", path: "/data/out", mode: :overwrite)
      Latu.write(df, format: "csv", path: "/data/out", header: true,
        partition_by: [:bucket])

  Returns `:ok` — a write has no payload, like `show/2`.

  ## Options

  Seven keys are Latu's. **Every other key is a writer option**, with `read/2`'s key and value
  rules — a snake_case atom becomes camelCase, a string passes verbatim, a `nil` drops its
  pair.

    * `:format` — the sink's short name or class: `"parquet"`, `"csv"`, `"jdbc"`. Defaults to
      `nil`, leaving the server on `spark.sql.sources.default`.
    * `:mode` — what to do when the destination exists. Defaults to `nil`, which sends
      nothing and leaves Spark on its own default of error-if-exists.
      * `:append` — add to what is there.
      * `:overwrite` — replace it.
      * `:error` — fail. Spark's `errorifexists`, and its default.
      * `:ignore` — do nothing and succeed.
    * `:path` — where to write. **Not required** — a JDBC write names its destination in the
      writer options instead. Writing to a *catalog table* is `save_as_table/3` or
      `insert_into/3`, not a key here.
    * `:partition_by` — column names to partition the output by, as a list. Defaults to `[]`.
    * `:sort_by` — column names to sort within each bucket, as a list. Defaults to `[]`.
      Meaningful with `:bucket_by`.
    * `:cluster_by` — column names to cluster by, as a list. Defaults to `[]`.
    * `:bucket_by` — `{buckets, columns}`, e.g. `{8, [:id]}`. Defaults to `nil`; any other
      shape raises.

  A writer option whose own name is one of those seven has to be written as a **string key**,
  which passes verbatim: `Latu.write(df, [{"path", "s3://bucket/key"}, format: "custom"])`.

  ## Examples

      Latu.write(df, format: "parquet", path: "/data/out", mode: :overwrite)

      Latu.write(df, format: "csv", path: "/data/out", header: true, partition_by: [:bucket])

      Latu.write(df, format: "parquet", path: "/data/out",
        bucket_by: {8, [:id]}, sort_by: [:ts])

      # a JDBC write names its table in the writer options
      Latu.write(df, format: "jdbc", url: url, dbtable: "people", mode: :append)
  """
  @spec write(DataFrame.t(), keyword()) :: :ok | {:error, Error.t()}
  defdelegate write(df, opts), to: DataFrame

  @doc "Like `write/2`, raising on failure."
  @spec write!(DataFrame.t(), keyword()) :: :ok
  defdelegate write!(df, opts), to: DataFrame

  @doc """
  Write as a catalog table. An action.

  ## Options

  `write/2`'s six, minus `:path`: `:format`, `:mode`, `:partition_by`, `:sort_by`,
  `:cluster_by` and `:bucket_by`. Every other key is a writer option.

  **`path:` is therefore a writer option here, not a Latu key**, which is how you give a
  managed table an explicit location — Spark's `option("path", ...)` alongside
  `saveAsTable`. The table name is the second argument.

  ## Examples

      Latu.save_as_table(df, "people", mode: :overwrite)
      Latu.save_as_table(df, "people", format: "parquet", partition_by: [:region])
      Latu.save_as_table(df, "people", format: "parquet", path: "/warehouse/people")
  """
  @spec save_as_table(DataFrame.t(), String.t() | atom(), keyword()) ::
          :ok | {:error, Error.t()}
  defdelegate save_as_table(df, name, opts \\ []), to: DataFrame

  @doc "Like `save_as_table/3`, raising on failure."
  @spec save_as_table!(DataFrame.t(), String.t() | atom(), keyword()) :: :ok
  defdelegate save_as_table!(df, name, opts \\ []), to: DataFrame

  @doc """
  Insert into an existing table, by position. An action.

  PySpark's `insertInto`, including its position-based column matching: the frame's column
  *names* are ignored and its column *order* has to match the table's.

  ## Options

    * `:overwrite` — replace the table's contents rather than adding to them. Defaults to
      `nil`, which sends no mode at all and leaves Spark on append; `false` sends append
      explicitly. This is `write/2`'s `:mode` under PySpark's boolean spelling, and the only
      key here — a writer option has nowhere to go on an insert.

  ## Examples

      Latu.insert_into(df, "people")
      Latu.insert_into(df, "people", overwrite: true)
  """
  @spec insert_into(DataFrame.t(), String.t() | atom(), keyword()) ::
          :ok | {:error, Error.t()}
  defdelegate insert_into(df, name, opts \\ []), to: DataFrame

  @doc "Like `insert_into/3`, raising on failure."
  @spec insert_into!(DataFrame.t(), String.t() | atom(), keyword()) :: :ok
  defdelegate insert_into!(df, name, opts \\ []), to: DataFrame

  @doc """
  Write to a table through Spark's v2 API (`df.writeTo` in PySpark). An action.

      Latu.write_v2(df, "people", mode: :create, using: "parquet")
      Latu.write_v2(df, "people", mode: :overwrite, condition: equal(:day, "2026-09-01"))

  ## Options

  **Every other key is a writer option**, with `read/2`'s key and value rules.

    * `:mode` — **required**, one per PySpark terminal method.
      * `:create` — create the table; fail if it exists. `.create()`
      * `:replace` — replace it; fail if it does not exist. `.replace()`
      * `:create_or_replace` — either. `.createOrReplace()`
      * `:append` — add rows. `.append()`
      * `:overwrite` — replace the rows matching `:condition`. `.overwrite(cond)`
      * `:overwrite_partitions` — replace the partitions the data touches.
        `.overwritePartitions()`
    * `:condition` — the overwrite predicate, as an expression. Defaults to `nil`, and pairs
      with `mode: :overwrite` only.
    * `:using` — the provider, e.g. `"parquet"`. Defaults to `nil`.
    * `:partition_by` — **expressions**, not just names, so a transform like
      `Latu.Column.fun("years", [:ts])` works. Defaults to `[]`.
    * `:cluster_by` — column names. Defaults to `[]`.
    * `:table_properties` — a keyword list whose keys pass verbatim. Defaults to `[]`.

  A writer option whose own name is one of those six has to be written as a **string key**,
  which passes verbatim.

  ## Examples

      Latu.write_v2(df, "people", mode: :create, using: "parquet")
      Latu.write_v2(df, "people", mode: :overwrite, condition: equal(:day, "2026-09-01"))
      Latu.write_v2(df, "events", mode: :create,
        partition_by: [Latu.Column.fun("years", [:ts])],
        table_properties: ["write.format.default": "parquet"])
  """
  @spec write_v2(DataFrame.t(), String.t() | atom(), keyword()) :: :ok | {:error, Error.t()}
  defdelegate write_v2(df, table, opts), to: DataFrame

  @doc "Like `write_v2/3`, raising on failure."
  @spec write_v2!(DataFrame.t(), String.t() | atom(), keyword()) :: :ok
  defdelegate write_v2!(df, table, opts), to: DataFrame

  @doc """
  Register the DataFrame as a temporary view, visible to `sql/3`. An action.

      :ok = Latu.create_temp_view(df, "people", replace: true)
      Latu.sql!(session, "SELECT count(*) FROM people")

  ## Options

    * `:replace` — swap an existing view rather than raising. Defaults to `false`.
    * `:global` — register in the `global_temp` database, visible across sessions rather than
      just this one. Defaults to `false`.

  One call for PySpark's four `create*TempView` methods (`docs/deviations.md`).
  `Latu.Catalog.drop_temp_view/2` is the inverse.

  ## Examples

      :ok = Latu.create_temp_view(df, "people")
      :ok = Latu.create_temp_view(df, "people", replace: true)
      :ok = Latu.create_temp_view(df, "people", global: true, replace: true)
  """
  @spec create_temp_view(DataFrame.t(), String.t() | atom(), keyword()) ::
          :ok | {:error, Error.t()}
  defdelegate create_temp_view(df, name, opts \\ []), to: DataFrame

  @doc "Like `create_temp_view/3`, raising on failure."
  @spec create_temp_view!(DataFrame.t(), String.t() | atom(), keyword()) :: :ok
  defdelegate create_temp_view!(df, name, opts \\ []), to: DataFrame

  @doc """
  All the rows, as maps with atom keys.

  An action: the whole result comes to your machine. `stream/2` is the lazy form, and
  `to_explorer/2` the columnar one.

  ## Options

    * `:keys` — how column names come back. Defaults to `:atoms`. Use `:strings` when the
      names come out of dynamic SQL: atom keys are bounded by the columns you have ever
      selected, and an unbounded one is an atom-table leak.
    * `:progress` — a 1-arity function called with a `Latu.Progress` as the query runs.
      Defaults to `nil`. Every action that reaches the server takes it, the `*_with_metrics`
      twins included; `glimpse/2` is the one exception, and says why.

  ## Examples

      Latu.range(session, 2) |> Latu.collect()
      #=> {:ok, [%{id: 0}, %{id: 1}]}

      Latu.collect(df, keys: :strings)
      #=> {:ok, [%{"id" => 0}]}

      Latu.collect(df, progress: &IO.inspect/1)

  See `Latu.DataFrame.collect/2`.
  """
  @spec collect(DataFrame.t(), keyword()) :: {:ok, [map()]} | {:error, Error.t()}
  defdelegate collect(df, opts \\ []), to: DataFrame

  @doc "Like `collect/2`, raising on failure."
  @spec collect!(DataFrame.t(), keyword()) :: [map()]
  defdelegate collect!(df, opts \\ []), to: DataFrame

  @doc """
  The first `count` rows, as maps — `limit/2` then `collect/2`, as in PySpark.

  ## Options

  `collect/2`'s: `:keys` and `:progress`.

  ## Examples

      Latu.take(df, 3)
      Latu.take(df, 3, keys: :strings)
  """
  @spec take(DataFrame.t(), non_neg_integer(), keyword()) :: {:ok, [map()]} | {:error, Error.t()}
  defdelegate take(df, count, opts \\ []), to: DataFrame

  @doc "Like `take/3`, raising on failure."
  @spec take!(DataFrame.t(), non_neg_integer(), keyword()) :: [map()]
  defdelegate take!(df, count, opts \\ []), to: DataFrame

  @doc """
  The last `count` rows, as maps.

  An action, like `take/3`, because Spark's `Tail` relation collects on the driver — which is
  also why it is the one place a large `count` costs driver memory rather than yours.

  ## Options

  `collect/2`'s: `:keys` and `:progress`.

  ## Examples

      Latu.tail(df, 3)
  """
  @spec tail(DataFrame.t(), non_neg_integer(), keyword()) :: {:ok, [map()]} | {:error, Error.t()}
  defdelegate tail(df, count, opts \\ []), to: DataFrame

  @doc "Like `tail/3`, raising on failure."
  @spec tail!(DataFrame.t(), non_neg_integer(), keyword()) :: [map()]
  defdelegate tail!(df, count, opts \\ []), to: DataFrame

  @doc """
  The first row, or nil when there are none.

  ## Options

  `collect/2`'s: `:keys` and `:progress`.

  ## Examples

      Latu.first(df)                  #=> {:ok, %{id: 0}}
      Latu.first(Latu.limit(df, 0))   #=> {:ok, nil}
  """
  @spec first(DataFrame.t(), keyword()) :: {:ok, map() | nil} | {:error, Error.t()}
  defdelegate first(df, opts \\ []), to: DataFrame

  @doc "Like `first/2`, raising on failure."
  @spec first!(DataFrame.t(), keyword()) :: map() | nil
  defdelegate first!(df, opts \\ []), to: DataFrame

  @doc """
  `first/2` under PySpark's other name: one row or nil, not a list. With a count it is
  `take/3`: a list, even for one row. Both shapes are PySpark's.

  ## Options

  `collect/2`'s: `:keys` and `:progress`. They go in the second argument when there is no
  count, and in the third when there is.

  ## Examples

      Latu.head(df)                     #=> {:ok, %{id: 0}}
      Latu.head(df, 2)                  #=> {:ok, [%{id: 0}, %{id: 1}]}
      Latu.head(df, keys: :strings)     #=> {:ok, %{"id" => 0}}
      Latu.head(df, 2, keys: :strings)  #=> {:ok, [%{"id" => 0}, %{"id" => 1}]}
  """
  @spec head(DataFrame.t(), non_neg_integer() | keyword(), keyword()) ::
          {:ok, map() | nil} | {:ok, [map()]} | {:error, Error.t()}
  defdelegate head(df, count_or_opts \\ [], opts \\ []), to: DataFrame

  @doc "Like `head/3`, raising on failure."
  @spec head!(DataFrame.t(), non_neg_integer() | keyword(), keyword()) :: map() | nil | [map()]
  defdelegate head!(df, count_or_opts \\ [], opts \\ []), to: DataFrame

  @doc """
  The result as one `Explorer.DataFrame`.

  Unbounded, like `collect/2` and Spark's own `collect`. Bound the plan rather than the
  action — `limit/2` is the Spark way to ask for part of a result, and `stream/2` is the
  answer for one too large to hold.

  ## Options

    * `:progress` — as `collect/2` describes. Defaults to `nil`.

  ## Examples

      {:ok, frame} = Latu.to_explorer(df)
      {:ok, frame} = df |> Latu.limit(10_000) |> Latu.to_explorer()

  See `Latu.DataFrame.to_explorer/2`.
  """
  @spec to_explorer(DataFrame.t(), keyword()) ::
          {:ok, Explorer.DataFrame.t()} | {:error, Error.t()}
  defdelegate to_explorer(df, opts \\ []), to: DataFrame

  @doc "Like `to_explorer/2`, raising on failure."
  @spec to_explorer!(DataFrame.t(), keyword()) :: Explorer.DataFrame.t()
  defdelegate to_explorer!(df, opts \\ []), to: DataFrame

  @doc """
  `collect/2`, and the metrics `observe/3` asked for.

  ## Options

  `collect/2`'s: `:keys` and `:progress`.

  See `observe/3`.
  """
  @spec collect_with_metrics(DataFrame.t(), keyword()) ::
          {:ok, [map()], ExecutionInfo.t()} | {:error, Error.t()}
  defdelegate collect_with_metrics(df, opts \\ []), to: DataFrame

  @doc "Like `collect_with_metrics/2`, raising on failure and returning `{rows, metrics}`."
  @spec collect_with_metrics!(DataFrame.t(), keyword()) :: {[map()], ExecutionInfo.t()}
  defdelegate collect_with_metrics!(df, opts \\ []), to: DataFrame

  @doc """
  `count/2`, and the metrics `observe/3` asked for.

  ## Options

  `count/2`'s: `:progress`.

  See `observe/3`.
  """
  @spec count_with_metrics(DataFrame.t(), keyword()) ::
          {:ok, non_neg_integer(), ExecutionInfo.t()} | {:error, Error.t()}
  defdelegate count_with_metrics(df, opts \\ []), to: DataFrame

  @doc "Like `count_with_metrics/2`, raising on failure and returning `{count, metrics}`."
  @spec count_with_metrics!(DataFrame.t(), keyword()) :: {non_neg_integer(), ExecutionInfo.t()}
  defdelegate count_with_metrics!(df, opts \\ []), to: DataFrame

  @doc """
  `to_explorer/2`, and the metrics `observe/3` asked for.

  ## Options

  `to_explorer/2`'s: `:limit` and `:progress`.

  See `observe/3`.
  """
  @spec to_explorer_with_metrics(DataFrame.t(), keyword()) ::
          {:ok, Explorer.DataFrame.t(), ExecutionInfo.t()} | {:error, Error.t()}
  defdelegate to_explorer_with_metrics(df, opts \\ []), to: DataFrame

  @doc "Like `to_explorer_with_metrics/2`, raising on failure and returning `{frame, metrics}`."
  @spec to_explorer_with_metrics!(DataFrame.t(), keyword()) ::
          {Explorer.DataFrame.t(), ExecutionInfo.t()}
  defdelegate to_explorer_with_metrics!(df, opts \\ []), to: DataFrame

  @doc "`write/2`, and the metrics `observe/3` asked for. See `observe/3`."
  @spec write_with_metrics(DataFrame.t(), keyword()) ::
          {:ok, ExecutionInfo.t()} | {:error, Error.t()}
  defdelegate write_with_metrics(df, opts), to: DataFrame

  @doc "Like `write_with_metrics/2`, raising on failure."
  @spec write_with_metrics!(DataFrame.t(), keyword()) :: ExecutionInfo.t()
  defdelegate write_with_metrics!(df, opts), to: DataFrame

  @doc "`save_as_table/3`, and the metrics `observe/3` asked for. See `observe/3`."
  @spec save_as_table_with_metrics(DataFrame.t(), String.t() | atom(), keyword()) ::
          {:ok, ExecutionInfo.t()} | {:error, Error.t()}
  defdelegate save_as_table_with_metrics(df, name, opts \\ []), to: DataFrame

  @doc "Like `save_as_table_with_metrics/3`, raising on failure."
  @spec save_as_table_with_metrics!(DataFrame.t(), String.t() | atom(), keyword()) ::
          ExecutionInfo.t()
  defdelegate save_as_table_with_metrics!(df, name, opts \\ []), to: DataFrame

  @doc "`insert_into/3`, and the metrics `observe/3` asked for. See `observe/3`."
  @spec insert_into_with_metrics(DataFrame.t(), String.t() | atom(), keyword()) ::
          {:ok, ExecutionInfo.t()} | {:error, Error.t()}
  defdelegate insert_into_with_metrics(df, name, opts \\ []), to: DataFrame

  @doc "Like `insert_into_with_metrics/3`, raising on failure."
  @spec insert_into_with_metrics!(DataFrame.t(), String.t() | atom(), keyword()) ::
          ExecutionInfo.t()
  defdelegate insert_into_with_metrics!(df, name, opts \\ []), to: DataFrame

  @doc "`write_v2/3`, and the metrics `observe/3` asked for. See `observe/3`."
  @spec write_v2_with_metrics(DataFrame.t(), String.t() | atom(), keyword()) ::
          {:ok, ExecutionInfo.t()} | {:error, Error.t()}
  defdelegate write_v2_with_metrics(df, table, opts), to: DataFrame

  @doc "Like `write_v2_with_metrics/3`, raising on failure."
  @spec write_v2_with_metrics!(DataFrame.t(), String.t() | atom(), keyword()) ::
          ExecutionInfo.t()
  defdelegate write_v2_with_metrics!(df, table, opts), to: DataFrame

  @doc """
  The result as a lazy stream of `Explorer.DataFrame`s, one per Arrow batch.

  For results too large to hold at once; stopping early releases the execution. Raises
  `Latu.Error` on failure, since an enumeration has no way to return one.

  ## Options

    * `:progress` — a 1-arity function called with a `Latu.Progress` as the query runs.
      Defaults to `nil`. See `collect/2`.

  ## Examples

      df |> Latu.stream() |> Stream.map(&Explorer.DataFrame.n_rows/1) |> Enum.sum()
      df |> Latu.stream() |> Enum.each(&handle/1)

  See `Latu.DataFrame.stream/2`.
  """
  @spec stream(DataFrame.t(), keyword()) :: Enumerable.t()
  defdelegate stream(df, opts \\ []), to: DataFrame

  @doc """
  The raw Arrow IPC binaries, one per batch, bypassing Latu's decoder and schema guard.

  Never byte-concatenate them; each is a complete IPC stream.

  ## Options

    * `:progress` — a 1-arity function called with a `Latu.Progress` as the query runs.
      Defaults to `nil`. See `collect/2`.

  ## Examples

      {:ok, batches} = Latu.to_arrow(df)

  See `Latu.DataFrame.to_arrow/2`.
  """
  @spec to_arrow(DataFrame.t(), keyword()) :: {:ok, [binary()]} | {:error, Error.t()}
  defdelegate to_arrow(df, opts \\ []), to: DataFrame

  @doc "Like `to_arrow/2`, raising on failure."
  @spec to_arrow!(DataFrame.t(), keyword()) :: [binary()]
  defdelegate to_arrow!(df, opts \\ []), to: DataFrame
end
