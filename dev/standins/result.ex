defmodule Latu.Result do
  @moduledoc """
  Stand-in for `lib/latu/result.ex`, for `dev/check_offline.exs` only.

  The real one decodes Arrow through Explorer, whose NIF this container has no way to build.
  """

  def decode(batches, opts \\ [])
  def decode([], _opts), do: {:error, :stand_in}
  def decode(_batches, _opts), do: {:error, :stand_in}
  def rows(_frame, _keys), do: []
  def only(_frame), do: {:error, :stand_in}
  def only(_frame, _column), do: {:error, :stand_in}
  def from_columns(columns), do: {:frame, columns}
  def size(_frame), do: 0
  def to_ipc(_frame), do: <<>>
  def to_ipc_chunks(_frame, _rows_per_chunk), do: []
end
