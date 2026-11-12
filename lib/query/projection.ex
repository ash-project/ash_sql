# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.Query.Projection do
  @moduledoc false
  import Ecto.Query, only: [from: 2]

  @clauses [
    :select,
    :wheres,
    :havings,
    :group_bys,
    :order_bys,
    :distinct,
    :windows,
    :limit,
    :offset
  ]

  def reference(acc, bindings, binding, field) do
    with token when not is_nil(token) <- get_in(bindings, [:bindings, binding, :projection]),
         refs when is_map(refs) <- Map.get(acc, :projections, %{}),
         true <- is_atom(field) and not is_nil(field) do
      Map.put(
        acc,
        :projections,
        Map.update(refs, token, MapSet.new([field]), &MapSet.put(&1, field))
      )
    else
      false -> unknown(acc)
      _ -> acc
    end
  end

  def track_source(query, _binding, %{manual: {_, _}}) do
    update_in(query.__ash_bindings__.expression_accumulator, &unknown/1)
  end

  def track_source(query, _binding, %{no_attributes?: true}), do: query

  def track_source(query, binding, %{source_attribute: field}) when not is_nil(field) do
    if get_in(query.__ash_bindings__, [:bindings, binding, :projection]) do
      update_in(query.__ash_bindings__.expression_accumulator, fn acc ->
        reference(acc, query.__ash_bindings__, binding, field)
      end)
    else
      query
    end
  end

  def track_source(query, _binding, _relationship), do: query

  # Unknown dependencies leave the read's original selection intact.
  def unknown(acc), do: Map.put(acc, :projections, :unknown)
  def merge(:unknown, _), do: :unknown
  def merge(_, :unknown), do: :unknown

  def merge(left, right),
    do: Map.merge(left, right, fn _, left, right -> MapSet.union(left, right) end)

  def prune(query) do
    case collect(query, %{}) do
      :unknown -> query
      refs when map_size(refs) == 0 -> query
      refs -> rewrite(query, refs)
    end
  end

  # A read action may change the Ecto query after references have been recorded.
  defp collect(%{__ash_bindings__: %{context: %{action: %{modify_query: modify}}}}, _refs)
       when not is_nil(modify) do
    :unknown
  end

  # Only visit query nodes and their metadata, never expressions or parameter values.
  defp collect(%{__ash_bindings__: %{expression_accumulator: acc}} = query, refs) do
    if query.assocs != [] || query.preloads != [] || query.combinations != [] || query.with_ctes do
      :unknown
    else
      refs = merge(refs, Map.get(acc, :projections, %{}))

      Enum.reduce_while(children(query), refs, fn child, refs ->
        case collect(child, refs) do
          :unknown -> {:halt, :unknown}
          refs -> {:cont, refs}
        end
      end)
    end
  end

  defp collect(_, _), do: :unknown

  defp children(query) do
    [query.from && query.from.source | Enum.flat_map(query.joins, &[&1.source, &1.on])]
    |> Kernel.++(Enum.map(@clauses, &Map.get(query, &1)))
    |> Enum.flat_map(&subqueries/1)
  end

  defp subqueries(%Ecto.SubQuery{query: query}), do: [query]
  defp subqueries(%{from: _, joins: _} = query), do: [query]
  defp subqueries(%{subqueries: subqueries}), do: Enum.flat_map(subqueries, &subqueries/1)
  defp subqueries({_name, value}), do: subqueries(value)
  defp subqueries(list) when is_list(list), do: Enum.flat_map(list, &subqueries/1)
  defp subqueries(_), do: []

  defp rewrite(query, refs) do
    mapper = &map_subqueries(&1, refs)
    from = if query.from, do: %{query.from | source: mapper.(query.from.source)}, else: nil
    joins = Enum.map(query.joins, &%{&1 | source: mapper.(&1.source), on: mapper.(&1.on)})
    query = %{query | from: from, joins: joins}
    query = Enum.reduce(@clauses, query, &Map.update!(&2, &1, mapper))

    case get_in(query, [Access.key(:__ash_bindings__, %{}), :projection]) do
      nil -> query
      token -> restrict(query, Map.get(refs, token, MapSet.new()))
    end
  end

  defp map_subqueries(%Ecto.SubQuery{query: query} = subquery, refs),
    do: %{subquery | query: rewrite(query, refs)}

  defp map_subqueries(%{from: _, joins: _} = query, refs), do: rewrite(query, refs)

  defp map_subqueries(%{subqueries: subqueries} = clause, refs),
    do: %{clause | subqueries: Enum.map(subqueries, &map_subqueries(&1, refs))}

  defp map_subqueries({name, value}, refs), do: {name, map_subqueries(value, refs)}

  defp map_subqueries(list, refs) when is_list(list),
    do: Enum.map(list, &map_subqueries(&1, refs))

  defp map_subqueries(value, _refs), do: value

  defp restrict(
         %{
           select: nil,
           distinct: nil,
           group_bys: [],
           havings: [],
           combinations: [],
           with_ctes: nil
         } = query,
         fields
       ) do
    case query.from.source do
      %Ecto.SubQuery{query: inner} = subquery ->
        %{query | from: %{query.from | source: %{subquery | query: restrict(inner, fields)}}}

      {table, schema} when is_binary(table) and is_atom(schema) and not is_nil(schema) ->
        attributes = schema.__schema__(:fields)
        selected = Enum.filter(attributes, &MapSet.member?(fields, &1))

        if selected != [] && length(selected) == MapSet.size(fields) &&
             length(selected) < length(attributes) do
          from(row in query, select: struct(row, ^selected))
        else
          query
        end

      _ ->
        query
    end
  end

  defp restrict(query, _fields), do: query
end
