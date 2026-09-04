defmodule Latu.Error do
  @moduledoc """
  Anything Latu returns as `{:error, _}`.

  `kind` says which side failed, so callers can match without parsing messages:

    * `:rpc` — the server refused or failed the call; Spark's own detail is below.
    * `:protocol` — the server answered, but not in a shape the protocol allows (a stream that
      ended without completing, a batch out of order, a `Config` reply with the wrong count).
      Latu refused to guess. Not a plan problem, and not something a retry of the plan fixes.
    * `:connect`, `:invalid_url`, `:session` — before or around the call: the channel, the URL,
      the session id.
    * `:decode` — the result arrived and Latu could not turn it into what you asked for; the
      message names the column or the bound.

  ## What a Spark error carries

  For a `:rpc` error, Spark puts structured detail in the gRPC trailers and Latu unpacks it —
  no extra round trip, because it arrives with the failure:

    * `error_class` — Spark's own error class, like `"UNRESOLVED_COLUMN.WITH_SUGGESTION"`. The
      thing Spark's error documentation is indexed by, and the right thing to match on.
    * `sql_state` — the SQLSTATE, like `"42703"`.
    * `classes` — the JVM exception hierarchy, most specific first. `"AnalysisException"` in
      here means the plan was refused rather than the query failed.
    * `parameters` — the message's own parameters, so you can read a value out rather than
      parsing it back out of the sentence.
    * `stacktrace` — the server's stack trace as one string, when the server was configured to
      send one. **Not** in `message/1`: a JVM trace is not what you want in a REPL, and it is
      one field away when you do.
    * `error_id` — the handle `Latu.error_details/2` fetches the full cause chain with.
    * `causes` — only populated by `Latu.error_details/2`. One entry per exception in the
      chain, root cause last.

  `status` and `details` are `GRPC.RPCError`'s, `:rpc` only; `details` keeps the raw trailer
  in case something Latu does not read is in it.
  """

  defexception [
    :kind,
    :message,
    :status,
    :details,
    :error_class,
    :sql_state,
    :stacktrace,
    :error_id,
    classes: [],
    parameters: %{},
    causes: []
  ]

  @typedoc "One exception in a server-side cause chain. See `Latu.error_details/2`."
  @type cause :: %{
          message: String.t(),
          classes: [String.t()],
          error_class: String.t() | nil,
          stacktrace: [String.t()]
        }

  @type t :: %__MODULE__{
          kind: atom(),
          message: String.t(),
          status: non_neg_integer() | nil,
          details: term(),
          error_class: String.t() | nil,
          sql_state: String.t() | nil,
          stacktrace: String.t() | nil,
          error_id: String.t() | nil,
          classes: [String.t()],
          parameters: %{optional(String.t()) => String.t()},
          causes: [cause()]
        }

  @doc false
  @spec new(atom(), String.t(), keyword()) :: t()
  def new(kind, message, opts \\ []) do
    struct!(%__MODULE__{kind: kind, message: message}, opts)
  end

  @doc """
  What a raised `Latu.Error` prints.

  The server's own message, which for a Spark error already reads
  `[ERROR_CLASS] ... SQLSTATE: xxx` — so the class is prefixed only when Spark did not put it
  there itself, and never twice. The stack trace is deliberately absent; read `:stacktrace`.

      iex> Exception.message(%Latu.Error{kind: :decode, message: "expected one column, got 3"})
      "expected one column, got 3"

      iex> error = %Latu.Error{kind: :rpc, error_class: "UNRESOLVED_COLUMN", message: "nope"}
      iex> Exception.message(error)
      "[UNRESOLVED_COLUMN] nope"
  """
  @impl true
  @spec message(t()) :: String.t()
  def message(%__MODULE__{error_class: nil, message: message}), do: message

  def message(%__MODULE__{error_class: class, message: message}) do
    if String.contains?(message, class), do: message, else: "[#{class}] #{message}"
  end
end
