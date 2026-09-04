# Dependency stand-ins for the generated messages `lib/latu/result/` pattern-matches, for
# dev/check_offline.exs only — `:protobuf` cannot compile in the container. Fields beyond what
# those modules touch are omitted on purpose, and a message matched as a plain map needs no
# stand-in at all, which is why `Expression.Literal` is the only one here that is not a
# `DataType`.

defmodule Latu.Protocol.Spark.Connect.DataType do
  @moduledoc false
  defstruct [:kind]
end

defmodule Latu.Protocol.Spark.Connect.DataType.Struct do
  @moduledoc false
  defstruct fields: []
end

defmodule Latu.Protocol.Spark.Connect.DataType.StructField do
  @moduledoc false
  defstruct [:name, :data_type, :nullable, :metadata]
end

defmodule Latu.Protocol.Spark.Connect.DataType.Array do
  @moduledoc false
  defstruct [:element_type, :contains_null]
end

defmodule Latu.Protocol.Spark.Connect.DataType.Map do
  @moduledoc false
  defstruct [:key_type, :value_type, :value_contains_null]
end

defmodule Latu.Protocol.Spark.Connect.DataType.UDT do
  @moduledoc false
  defstruct [:type, :jvm_class, :python_class, :serialized_python_class, :sql_type]
end

defmodule Latu.Protocol.Spark.Connect.Expression.Literal do
  @moduledoc false
  defstruct [:literal_type, :data_type]
end
