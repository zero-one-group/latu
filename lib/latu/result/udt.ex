defmodule Latu.Result.UDT do
  @moduledoc """
  A user-defined type the server sent as a literal, handed back as data.

  Spark serialises a UDT literal as a struct whose type names a class rather than field names,
  so there is nothing to key a map by. `Latu.Result.Literal.value/1` returns this instead: the
  class, and the elements decoded in the order that class defines them. Latu interprets none of
  them; `latu_ml` matches on `:class` for the two `spark.ml` uses.

  `:class` is the JVM class the type declared, and `nil` for a Python UDT; the elements decode
  either way.
  """

  @typedoc "The JVM class the server named, or `nil` for a Python UDT."
  @type class :: String.t() | nil

  @type t :: %__MODULE__{class: class(), elements: [term()]}

  defstruct [:class, elements: []]
end
