defmodule Latu.Retry do
  @moduledoc """
  When a failed RPC is tried again, and how long Latu waits in between.

  One of these sits on every `%Latu.Session{}`, so the session is where the policy changes:

      Latu.connect!("sc://localhost:15002", retry: [max_retries: 3, max_backoff: 5_000])

  The defaults are PySpark's own `DefaultPolicy`, so Latu retries exactly as PySpark does out
  of the box: 15 attempts, 50ms growing fourfold to a 60s ceiling, jittered once the wait is
  long enough for jitter to matter. The budget covers *consecutive* failures while trying to
  advance a single execution; any response from the server starts a fresh one.

  Times are in milliseconds. `max_retries: 0` turns retrying off, which is what a test that
  wants a failure to surface immediately wants.

  What is retried is not configurable, and deliberately: an `UNAVAILABLE`, a disconnected
  cursor, and a lost handle with nothing received yet.
  """

  @default_max_retries 15
  @default_initial_backoff 50
  @default_max_backoff 60_000
  @default_backoff_multiplier 4.0
  @default_jitter 500
  @default_min_jitter_threshold 2_000

  @counts [:max_retries, :initial_backoff, :max_backoff, :jitter, :min_jitter_threshold]

  defstruct max_retries: @default_max_retries,
            initial_backoff: @default_initial_backoff,
            max_backoff: @default_max_backoff,
            backoff_multiplier: @default_backoff_multiplier,
            jitter: @default_jitter,
            min_jitter_threshold: @default_min_jitter_threshold

  @type t :: %__MODULE__{
          max_retries: non_neg_integer(),
          initial_backoff: non_neg_integer(),
          max_backoff: non_neg_integer(),
          backoff_multiplier: number(),
          jitter: non_neg_integer(),
          min_jitter_threshold: non_neg_integer()
        }

  @doc """
  Build a policy, validating it.

      iex> Latu.Retry.new(max_retries: 3).max_retries
      3

  A `%Latu.Retry{}` passes straight through, so `Latu.connect/2` takes either form.
  """
  @spec new(keyword() | t()) :: t()
  def new(opts \\ [])

  def new(%__MODULE__{} = retry), do: validated!(retry)

  def new(opts) when is_list(opts), do: validated!(struct!(__MODULE__, opts))

  def new(other) do
    raise ArgumentError, "retry is a keyword list or a %Latu.Retry{}, not #{inspect(other)}"
  end

  @doc """
  How long to wait before attempt `attempt`, counting from zero.

      iex> Latu.Retry.wait(Latu.Retry.new(), 0)
      50

  Jitter above `min_jitter_threshold` is the only nondeterminism in the transport, and it is
  bounded by `jitter`.
  """
  @spec wait(t(), non_neg_integer()) :: non_neg_integer()
  def wait(%__MODULE__{} = retry, attempt) when is_integer(attempt) and attempt >= 0 do
    capped = min(retry.initial_backoff * retry.backoff_multiplier ** attempt, retry.max_backoff)

    if capped > retry.min_jitter_threshold,
      do: trunc(capped + :rand.uniform() * retry.jitter),
      else: trunc(capped)
  end

  defp validated!(%__MODULE__{} = retry) do
    Enum.each(@counts, &count!(&1, Map.fetch!(retry, &1)))
    multiplier!(retry.backoff_multiplier)
    retry
  end

  defp count!(_key, value) when is_integer(value) and value >= 0, do: :ok

  defp count!(key, value) do
    raise ArgumentError, ":#{key} is a non-negative integer, not #{inspect(value)}"
  end

  defp multiplier!(value) when is_number(value) and value >= 1, do: :ok

  defp multiplier!(value) do
    raise ArgumentError,
          ":backoff_multiplier is a number of at least 1 — below 1 each wait would be shorter " <>
            "than the one before — not #{inspect(value)}"
  end
end
