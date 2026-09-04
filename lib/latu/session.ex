defmodule Latu.Session do
  @moduledoc """
  Where the Spark Connect server is, and the identity it keys state on.

  A plain struct, not a process. `Latu.connect/2` builds one; every function that talks to the
  server takes it and lives on `Latu` (`Latu.conf/2`, `Latu.interrupt/2`, `Latu.sql/3`). What
  lives *here* is pure: building a session from a URL, and its tags. That is the whole rule.

  Latu starts and supervises nothing — which makes the session the only place Latu has to keep
  a default, so the knobs live here and travel with it:

    * `:timeout`, `:connect_timeout` — per-RPC and establishment deadlines.
    * `:window_size`, `:keepalive`, `:keepalive_tolerance` — HTTP/2 flow control and liveness.
    * `:retry` — a `Latu.Retry`, the policy every execution retries under.

  Every one is a `Latu.connect/2` option, so none of them needs a struct poke.

  Timeouts are in milliseconds, or `:infinity`.
  """

  alias Latu.Error
  alias Latu.Internal.UUID
  alias Latu.Retry

  @default_port 15002
  @default_timeout 60_000
  @default_connect_timeout 10_000
  @version Mix.Project.config()[:version]

  # Why these numbers, and why they moved here from `Latu.Client`: docs/decisions.md.
  @default_window_size 134_217_728
  @default_keepalive 60_000
  @default_keepalive_tolerance 2

  @url_params ~w(use_ssl token user_id user_agent session_id)
  @overridable [
    :session_id,
    :user_id,
    :user_name,
    :client_type,
    :timeout,
    :connect_timeout,
    :tags,
    :window_size,
    :keepalive,
    :keepalive_tolerance,
    :retry
  ]

  defstruct [
    :channel,
    :host,
    :port,
    :session_id,
    :server_session_id,
    :user_id,
    :user_name,
    :client_type,
    :token,
    headers: [],
    use_ssl: false,
    tags: [],
    timeout: @default_timeout,
    connect_timeout: @default_connect_timeout,
    window_size: @default_window_size,
    keepalive: @default_keepalive,
    keepalive_tolerance: @default_keepalive_tolerance,
    retry: %Retry{}
  ]

  @type t :: %__MODULE__{
          channel: struct() | nil,
          host: String.t(),
          port: :inet.port_number(),
          session_id: String.t(),
          server_session_id: String.t() | nil,
          user_id: String.t(),
          user_name: String.t(),
          client_type: String.t(),
          token: String.t() | nil,
          headers: [{String.t(), String.t()}],
          use_ssl: boolean(),
          tags: [String.t()],
          timeout: timeout(),
          connect_timeout: timeout(),
          window_size: non_neg_integer(),
          keepalive: timeout(),
          keepalive_tolerance: non_neg_integer(),
          retry: Retry.t()
        }

  @doc """
  Parse a Spark Connect URL.

      sc://host[:port][/[;key=value...]]

  Params are semicolon-separated path parameters, not a query string. Recognised: `use_ssl`,
  `token`, `user_id`, `user_agent`, `session_id`. Anything else becomes a gRPC metadata
  header, which is how Databricks-style URLs pass `x-databricks-cluster-id` and friends.
  Values are percent-decoded, so a `=` in a token must be written `%3D`.

  Options override the URL: #{inspect(@overridable)}.

      iex> {:ok, s} = Latu.Session.from_url("sc://localhost:15002")
      iex> {s.host, s.port, s.use_ssl}
      {"localhost", 15002, false}
  """
  @spec from_url(String.t(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def from_url(url, opts \\ []) when is_binary(url) do
    opts = Keyword.validate!(opts, @overridable)

    with {:ok, authority, rest} <- split_scheme(url),
         {:ok, host, port} <- split_host_port(authority),
         {:ok, params, headers} <- parse_params(rest),
         {:ok, use_ssl} <- parse_bool(params, "use_ssl", false),
         {:ok, token} <- token(params),
         {:ok, session_id} <- session_id(params, opts) do
      session = %__MODULE__{
        host: host,
        port: port,
        use_ssl: use_ssl,
        token: token,
        headers: headers,
        session_id: session_id,
        user_id: params["user_id"] || default_user_id(),
        user_name: "",
        client_type: params["user_agent"] || default_client_type()
      }

      opts =
        opts
        |> Keyword.delete(:session_id)
        |> Keyword.replace_lazy(:tags, &valid_tags!/1)
        |> Keyword.replace_lazy(:retry, &Retry.new/1)

      {:ok, struct!(session, opts)}
    end
  end

  @doc "Like `from_url/2`, raising on a malformed URL."
  @spec from_url!(String.t(), keyword()) :: t()
  def from_url!(url, opts \\ []) do
    case from_url(url, opts) do
      {:ok, session} -> session
      {:error, error} -> raise error
    end
  end

  # Transport plumbing: `confirm/3` latches the server's id through this on every response. Not
  # API until something public hands the id back — docs/decisions.md (M12.6).
  @doc false
  @spec pin(t(), String.t()) :: t()
  def pin(%__MODULE__{} = session, server_session_id) when is_binary(server_session_id) do
    %{session | server_session_id: server_session_id}
  end

  # =============================================
  # Tags
  # =============================================

  @doc """
  Add an operation tag, for `Latu.interrupt/2` to match on.

  Every execution this session runs carries its tags, so `Latu.interrupt(session, tag: "etl")`
  cancels them from anywhere — which is the point, since the process running the query is
  blocked in it.

      session = Latu.Session.add_tag(session, "exploration")

  A tag is a string or an atom, cannot be empty, and cannot contain a comma, because Spark
  joins tags with one. Adding a tag twice does nothing. The tags are `session.tags`, and
  `%{session | tags: []}` clears them — a struct, so no getters.
  """
  @spec add_tag(t(), String.t() | atom()) :: t()
  def add_tag(%__MODULE__{} = session, tag) do
    %{session | tags: Enum.uniq(session.tags ++ [valid_tag!(tag)])}
  end

  @doc "Drop a tag. A tag that is not set is not an error."
  @spec remove_tag(t(), String.t() | atom()) :: t()
  def remove_tag(%__MODULE__{} = session, tag) do
    %{session | tags: List.delete(session.tags, valid_tag!(tag))}
  end

  defp valid_tags!(tags) when is_list(tags), do: Enum.uniq(Enum.map(tags, &valid_tag!/1))

  defp valid_tags!(other) do
    raise ArgumentError, "tags is a list of strings, not #{inspect(other)}"
  end

  defp valid_tag!(tag) when is_atom(tag) and not is_nil(tag), do: valid_tag!(Atom.to_string(tag))

  defp valid_tag!(""), do: raise(ArgumentError, "a tag cannot be empty")

  defp valid_tag!(tag) when is_binary(tag) do
    if String.contains?(tag, ",") do
      raise ArgumentError,
            "a tag cannot contain a comma, because Spark joins tags with one; " <>
              "got #{inspect(tag)}"
    end

    tag
  end

  defp valid_tag!(other) do
    raise ArgumentError, "a tag is a string or an atom, not #{inspect(other)}"
  end

  @doc """
  Check that a response belongs to this session, and latch the server's id.

  Every response carries both ids. A mismatched `session_id` means we are reading someone
  else's answer; a changed `server_session_id` on a pinned session means the server restarted
  and our session no longer exists on it. Both are worth catching here rather than as
  inexplicable behaviour later.
  """
  @spec confirm(t(), String.t() | nil, String.t() | nil) :: {:ok, t()} | {:error, Error.t()}
  def confirm(%__MODULE__{} = session, session_id, server_session_id) do
    cond do
      # An absent id is no information, not a mismatch — the same rule this function already
      # applies to `server_session_id` and that `Latu.Client.Execution` applies to operation
      # ids. `FetchErrorDetails` is what made it necessary: its handler is the one in the
      # service that never echoes the session, so every response carries an empty pair.
      session_id in [nil, ""] ->
        {:ok, session}

      session_id != session.session_id ->
        {:error,
         Error.new(
           :session,
           "server answered for session #{session_id}, expected #{session.session_id}"
         )}

      restarted?(session, server_session_id) ->
        {:error, Error.new(:session, "the server restarted; this session no longer exists on it")}

      is_binary(server_session_id) and server_session_id != "" ->
        {:ok, pin(session, server_session_id)}

      true ->
        {:ok, session}
    end
  end

  defp restarted?(%__MODULE__{server_session_id: nil}, _observed), do: false

  defp restarted?(%__MODULE__{server_session_id: pinned}, observed)
       when is_binary(observed) and observed != "",
       do: pinned != observed

  # No id in the response is no information, not a restart.
  defp restarted?(%__MODULE__{}, _observed), do: false

  # =============================================
  # Parsing
  # =============================================

  defp split_scheme("sc://" <> rest) do
    case String.split(rest, "/", parts: 2) do
      [""] -> {:error, invalid("missing host", rest)}
      ["", _] -> {:error, invalid("missing host", rest)}
      [authority] -> check_authority(authority, "")
      [authority, ";" <> params] -> check_authority(authority, params)
      [authority, ""] -> check_authority(authority, "")
      [_, path] -> {:error, invalid("path must be empty; parameters go after \";\"", path)}
    end
  end

  defp split_scheme(url), do: {:error, invalid(~s(must start with "sc://"), url)}

  defp check_authority(authority, params) do
    cond do
      authority == "" ->
        {:error, invalid("missing host", authority)}

      String.contains?(authority, "?") ->
        {:error, invalid("use \";key=value\", not a query string", authority)}

      String.contains?(authority, "#") ->
        {:error, invalid("fragments are not allowed", authority)}

      true ->
        {:ok, authority, params}
    end
  end

  defp split_host_port("[" <> rest) do
    case String.split(rest, "]", parts: 2) do
      ["", _] -> {:error, invalid("missing host", rest)}
      [host, ""] -> {:ok, host, @default_port}
      [host, ":" <> port] -> with {:ok, p} <- parse_port(port), do: {:ok, host, p}
      _ -> {:error, invalid("malformed IPv6 host", rest)}
    end
  end

  defp split_host_port(authority) do
    case String.split(authority, ":") do
      [""] -> {:error, invalid("missing host", authority)}
      [host] -> {:ok, host, @default_port}
      ["", _] -> {:error, invalid("missing host", authority)}
      [host, port] -> with {:ok, p} <- parse_port(port), do: {:ok, host, p}
      _ -> {:error, invalid("too many colons; bracket an IPv6 host", authority)}
    end
  end

  defp parse_port(port) do
    case Integer.parse(port) do
      {n, ""} when n in 1..65_535 -> {:ok, n}
      _ -> {:error, invalid("port must be an integer in 1..65535", port)}
    end
  end

  defp parse_params(""), do: {:ok, %{}, []}

  defp parse_params(rest) do
    rest
    |> String.split(";")
    |> Enum.reject(&(&1 == ""))
    |> Enum.reduce_while({:ok, %{}, []}, fn pair, {:ok, params, headers} ->
      case String.split(pair, "=") do
        [key, value] when key in @url_params ->
          {:cont, {:ok, Map.put(params, key, URI.decode(value)), headers}}

        [key, value] when key != "" ->
          {:cont, {:ok, params, headers ++ [{key, URI.decode(value)}]}}

        _ ->
          {:halt, {:error, invalid("parameter must be key=value", pair)}}
      end
    end)
  end

  defp parse_bool(params, key, default) do
    case Map.fetch(params, key) do
      :error ->
        {:ok, default}

      {:ok, value} ->
        case String.downcase(value) do
          "true" -> {:ok, true}
          "false" -> {:ok, false}
          _ -> {:error, invalid(~s(#{key} must be "true" or "false"), value)}
        end
    end
  end

  defp token(params) do
    case Map.fetch(params, "token") do
      :error -> {:ok, nil}
      {:ok, ""} -> {:error, invalid("token must not be empty", "")}
      {:ok, token} -> {:ok, token}
    end
  end

  defp session_id(params, opts) do
    case opts[:session_id] || params["session_id"] do
      nil ->
        {:ok, UUID.v4()}

      id ->
        if UUID.valid?(String.downcase(id)),
          do: {:ok, id},
          else: {:error, invalid("session_id must be a UUID", id)}
    end
  end

  # =============================================
  # Defaults
  # =============================================

  defp default_user_id, do: System.get_env("USER") || System.get_env("USERNAME") || ""

  defp default_client_type do
    "latu/#{@version} elixir/#{System.version()} otp/#{System.otp_release()}"
  end

  # =============================================
  # Errors
  # =============================================

  defp invalid(reason, got) do
    Error.new(:invalid_url, "invalid Spark Connect URL: #{reason} (got #{inspect(got)})")
  end
end

defimpl Inspect, for: Latu.Session do
  import Inspect.Algebra

  def inspect(session, opts) do
    fields = [
      host: session.host,
      port: session.port,
      use_ssl: session.use_ssl,
      session_id: session.session_id,
      user_id: session.user_id,
      connected: not is_nil(session.channel)
    ]

    # Values are never shown: a token or a header can carry a secret.
    fields = if session.token, do: fields ++ [token: "[REDACTED]"], else: fields

    fields =
      case session.headers do
        [] -> fields
        headers -> fields ++ [headers: Enum.map(headers, &elem(&1, 0))]
      end

    concat(["#Latu.Session<", to_doc(fields, opts), ">"])
  end
end
