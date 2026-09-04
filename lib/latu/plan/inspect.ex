defmodule Latu.Plan.Inspect do
  @moduledoc false
  # A raw Proto.Expression inspects as thirty lines of nested structs, which makes the REPL
  # unusable for building expressions. Rendering is best-effort: anything unrecognised falls
  # back to its oneof tag rather than raising, because Inspect must never fail.
  #
  # Reach for Protobuf.Text.encode/2 when the full message is what you want.
  #
  # chain/2 does the same for a relation: the plan's spine, which is what a DataFrame inspects
  # as. Same rule — never fail, never do IO.

  alias Latu.Protocol.Spark.Connect, as: Proto

  @operators ~w(+ - * / % == != <=> < <= > >= and or)

  @directions %{SORT_DIRECTION_ASCENDING: "ASC", SORT_DIRECTION_DESCENDING: "DESC"}
  @nulls %{SORT_NULLS_FIRST: "NULLS FIRST", SORT_NULLS_LAST: "NULLS LAST"}

  def describe(%Proto.Expression{expr_type: expr_type}), do: describe(expr_type)

  def describe(%Proto.Expression.SortOrder{} = order) do
    [describe(order.child), @directions[order.direction], @nulls[order.null_ordering]]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" ")
  end

  # A tagged reference is not the same expression as an untagged one, so show the tag.
  def describe({:unresolved_attribute, %{unparsed_identifier: name, plan_id: nil}}), do: name

  def describe({:unresolved_attribute, %{unparsed_identifier: name, plan_id: id}}) do
    "#{name}@#{id}"
  end

  # A subquery shows what it is and which relation it points at; the relation itself is hoisted
  # by the verb, so it is nowhere in this expression.
  def describe({:subquery_expression, %{subquery_type: type, plan_id: id}}) do
    kind = type |> to_string() |> String.replace_prefix("SUBQUERY_TYPE_", "") |> String.downcase()

    "#{kind}-subquery@#{id}"
  end

  def describe({:unresolved_star, %{unparsed_target: nil}}), do: "*"
  def describe({:unresolved_star, %{unparsed_target: target}}), do: target
  def describe({:expression_string, %{expression: sql}}), do: "`#{sql}`"
  def describe({:literal, %{literal_type: literal_type}}), do: literal(literal_type)
  def describe({:cast, %{expr: expr} = cast}), do: "#{verb(cast)}(#{describe(expr)}#{type(cast)})"
  def describe({:sort_order, order}), do: describe(order)

  def describe({:alias, %{expr: expr, name: names}}) do
    "#{describe(expr)} AS #{Enum.join(names, ", ")}"
  end

  def describe({:unresolved_function, %{function_name: name, arguments: [left, right]}})
      when name in @operators do
    "(#{describe(left)} #{name} #{describe(right)})"
  end

  def describe({:unresolved_function, %{function_name: "not", arguments: [only]}}) do
    "not #{describe(only)}"
  end

  def describe({:unresolved_function, %{function_name: name, arguments: arguments}}) do
    "#{name}(#{Enum.map_join(arguments, ", ", &describe/1)})"
  end

  def describe({tag, _value}), do: to_string(tag)
  def describe(nil), do: "?"
  def describe(other), do: Kernel.inspect(other)

  defp verb(%{eval_mode: :EVAL_MODE_TRY}), do: "try_cast"
  defp verb(_cast), do: "cast"

  defp type(%{cast_to_type: {:type_str, name}}), do: " AS #{name}"
  defp type(%{cast_to_type: {:type, _data_type}}), do: " AS ?"
  defp type(_cast), do: ""

  defp literal({:null, _}), do: "NULL"
  defp literal({:string, value}), do: Kernel.inspect(value)
  defp literal({:decimal, %{value: value}}), do: value
  defp literal({:date, days}), do: Date.to_string(Date.add(~D[1970-01-01], days))

  defp literal({:timestamp, micros}) do
    case DateTime.from_unix(micros, :microsecond) do
      {:ok, instant} -> DateTime.to_string(instant)
      {:error, _} -> "#{micros}us"
    end
  end

  defp literal({:timestamp_ntz, micros}) do
    ~N[1970-01-01 00:00:00]
    |> NaiveDateTime.add(micros, :microsecond)
    |> NaiveDateTime.to_string()
    |> Kernel.<>(" (ntz)")
  end

  defp literal({_tag, value}) when is_number(value) or is_boolean(value), do: to_string(value)
  defp literal({tag, _value}), do: to_string(tag)
  defp literal(nil), do: "?"

  # =============================================
  # Relations
  # =============================================

  # Long enough to see the shape, short enough that inspect output never scrolls.
  @max_links 8

  # A plan's spine, leaf first: `range -> filter -> project(2)`. The spine only: a two-input
  # relation ends it, both sides named and neither followed, because a chain is what you piped
  # and a tree is Latu.tree_string/2's job. Shows the plan rather than the schema PySpark's
  # repr(df) pays a round trip for -- docs/deviations.md.
  def chain(relation, opts \\ %Inspect.Opts{})

  def chain(%Proto.Relation{} = relation, opts) do
    relation |> links([]) |> elide(bound(opts)) |> render()
  end

  def chain(other, _opts), do: Kernel.inspect(other)

  # WithRelations is the wrapper Latu.Plan hoists a subquery's relations into, not something
  # anyone piped. Show what it wraps.
  defp links(%Proto.Relation{rel_type: {:with_relations, message}}, acc) do
    links(message.root, acc)
  end

  defp links(%Proto.Relation{rel_type: {kind, message}}, acc) do
    acc = [label(kind, message) | acc]

    case spine(message) do
      %Proto.Relation{} = input -> links(input, acc)
      nil -> acc
    end
  end

  defp links(_relation, acc), do: acc

  # `input` on 42 of the relations, `root` on WithRelations. Everything else is a leaf or holds
  # two, and both of those end the spine.
  defp spine(%{input: %Proto.Relation{} = input}), do: input
  defp spine(%{root: %Proto.Relation{} = root}), do: root
  defp spine(_message), do: nil

  defp render([]), do: "?"
  defp render(links), do: Enum.join(links, " → ")

  defp bound(%{limit: :infinity}), do: :infinity
  defp bound(%{limit: limit}) when is_integer(limit), do: min(limit, @max_links)
  defp bound(_opts), do: @max_links

  # Keep the ends: the leaf says where the data came from and the last few say what you just
  # did. The middle of a long pipeline is the part nobody reads.
  defp elide(links, max) when is_integer(max) and max >= 3 and length(links) > max do
    [hd(links), "…" | Enum.take(links, 2 - max)]
  end

  defp elide(links, max) when is_integer(max) and length(links) > max do
    ["…" | Enum.take(links, -max)]
  end

  defp elide(links, _max), do: links

  # A parenthesised count is how many things the node names, and only where that is the useful
  # half. Rendering the expressions themselves is describe/1's job and belongs to a Column.
  defp label(:project, %{expressions: expressions}), do: "project(#{length(expressions)})"
  defp label(:sort, %{order: order}), do: "sort(#{length(order)})"
  defp label(:with_columns, %{aliases: aliases}), do: "with_columns(#{length(aliases)})"
  defp label(:aggregate, %{grouping_expressions: by}), do: "aggregate(#{length(by)})"
  defp label(:limit, %{limit: count}), do: "limit(#{count})"
  defp label(:tail, %{limit: count}), do: "tail(#{count})"
  defp label(:offset, %{offset: count}), do: "offset(#{count})"
  defp label(:to_df, %{column_names: names}), do: "to_df(#{length(names)})"
  defp label(:drop, %{column_names: [_ | _] = names}), do: "drop(#{length(names)})"
  defp label(:drop, %{columns: columns}), do: "drop(#{length(columns)})"
  defp label(:subquery_alias, %{alias: name}), do: ~s|as("#{name}")|

  defp label(:read, %{read_type: {:named_table, %{unparsed_identifier: name}}}) do
    ~s|table("#{name}")|
  end

  defp label(:read, %{read_type: {:data_source, %{format: format}}})
       when is_binary(format) and format != "" do
    "read(#{format})"
  end

  defp label(:set_op, %{left_input: left, right_input: right, set_op_type: type}) do
    "#{set_op(type)}(#{kind_of(left)}, #{kind_of(right)})"
  end

  # Every other two-input relation: join, the three Spark 4 joins, and whatever 4.5 adds.
  defp label(kind, %{left: left, right: right}) do
    "#{kind}(#{kind_of(left)}, #{kind_of(right)})"
  end

  defp label(kind, _message), do: to_string(kind)

  defp kind_of(%Proto.Relation{rel_type: {kind, _message}}), do: to_string(kind)
  defp kind_of(_relation), do: "?"

  defp set_op(type) do
    type |> to_string() |> String.replace_prefix("SET_OP_TYPE_", "") |> String.downcase()
  end
end

defimpl Inspect, for: Latu.Protocol.Spark.Connect.Expression do
  import Inspect.Algebra

  def inspect(expression, _opts) do
    concat(["#Latu.Expression<", Latu.Plan.Inspect.describe(expression), ">"])
  end
end

defimpl Inspect, for: Latu.Protocol.Spark.Connect.Expression.SortOrder do
  import Inspect.Algebra

  def inspect(order, _opts) do
    concat(["#Latu.SortOrder<", Latu.Plan.Inspect.describe(order), ">"])
  end
end
