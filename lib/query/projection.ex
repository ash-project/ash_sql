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

  # Run at the execution boundary: later calculations can reuse an existing join.
  def prune(query) do
    if candidate?(query) do
      {query, fields} = collect_query(query, [], %{})
      rewrite(query, fields)
    else
      query
    end
  end

  defp candidate?(%Ecto.SubQuery{query: query}), do: candidate?(query)

  defp candidate?(%{from: %{source: source}, joins: joins} = query) do
    Map.get(query, :__ash_bindings__, %{})[:relationship_projection?] ||
      candidate?(source) || Enum.any?(joins, &candidate?(&1.source)) ||
      Enum.any?(@clauses, &candidate?(Map.get(query, &1)))
  end

  defp candidate?(%{subqueries: subqueries}), do: Enum.any?(subqueries, &candidate?/1)
  defp candidate?(list) when is_list(list), do: Enum.any?(list, &candidate?/1)
  defp candidate?({_name, value}), do: candidate?(value)
  defp candidate?(_), do: false

  defp collect_query(query, parents, fields) do
    id = make_ref()
    scope = [{id, query.aliases} | parents]
    query = Map.put(query, :__projection_scope__, id)

    {query, fields} = map_children(query, fields, &collect_query(&1, scope, &2))

    expressions =
      [query.from && query.from.source | Enum.map(@clauses, &Map.get(query, &1))] ++
        Enum.flat_map(query.joins, &[&1.source, &1.on])

    fields = Enum.reduce(expressions, fields, &collect_expr(&1, scope, &2))

    fields =
      if query.assocs != [] || query.preloads != [] || query.combinations != [] || query.with_ctes do
        Enum.reduce(scope, fields, fn {scope_id, aliases}, fields ->
          Enum.reduce(Map.values(aliases), fields, &Map.put(&2, {scope_id, &1}, :all))
        end)
      else
        fields
      end

    {query, fields}
  end

  defp collect_expr({{:., _, [binding, field]}, _, []}, scope, fields) when is_atom(field),
    do: reference(binding, field, scope, fields)

  defp collect_expr({kind, _, [_]} = binding, scope, fields) when kind in [:&, :as, :parent_as],
    do: reference(binding, :all, scope, fields)

  defp collect_expr({:fragment, _, args}, scope, fields) do
    # The CASE fragment emitted by AshSql has no column references in its SQL
    # text. Other fragments may name columns without using a bound expression.
    if Enum.filter(args, &match?({:raw, _}, &1)) ==
         [raw: "(CASE WHEN ", raw: " THEN ", raw: " ELSE ", raw: " END)"] do
      collect_expr(args, scope, fields)
    else
      Enum.reduce(scope, fields, fn {id, aliases}, fields ->
        Enum.reduce(Map.values(aliases), fields, &Map.put(&2, {id, &1}, :all))
      end)
    end
  end

  # Subqueries have already been visited with their own binding scope. Do not walk
  # parameter values or Ash context/actor data while looking for SQL references.
  defp collect_expr(%{expr: expr}, scope, fields), do: collect_expr(expr, scope, fields)

  defp collect_expr(tuple, scope, fields) when is_tuple(tuple),
    do: tuple |> Tuple.to_list() |> collect_expr(scope, fields)

  defp collect_expr(list, scope, fields) when is_list(list),
    do: Enum.reduce(list, fields, &collect_expr(&1, scope, &2))

  defp collect_expr(_, _, fields), do: fields

  defp reference({:&, _, [index]}, field, [{id, _} | _], fields),
    do: add_field(fields, {id, index}, field)

  defp reference({:as, _, [name]}, field, [{id, aliases} | _], fields),
    do: named_field(fields, id, aliases, name, field)

  defp reference({:parent_as, _, [name]}, field, [_ | parents], fields) do
    # Interpolated join queries can be flattened by Ecto. Retaining matches in
    # every enclosing scope is conservative when a binding name is shadowed.
    Enum.reduce(parents, fields, fn {id, aliases}, fields ->
      named_field(fields, id, aliases, name, field)
    end)
  end

  defp reference(_, _, _, fields), do: fields

  defp named_field(fields, id, aliases, name, field) do
    case Map.fetch(aliases, name) do
      {:ok, index} -> add_field(fields, {id, index}, field)
      :error -> fields
    end
  end

  defp add_field(fields, key, field) do
    Map.update(fields, key, if(field == :all, do: :all, else: MapSet.new([field])), fn
      :all -> :all
      _ when field == :all -> :all
      previous -> MapSet.put(previous, field)
    end)
  end

  defp rewrite(query, fields) do
    joins =
      Enum.with_index(query.joins, 1)
      |> Enum.map(fn {join, index} ->
        needed = Map.get(fields, {query.__projection_scope__, index}, MapSet.new())

        source =
          if is_map(join.source) &&
               Map.get(join.source, :__ash_bindings__, %{})[:relationship_projection?] do
            restrict(join.source, needed, fields)
          else
            join.source
          end

        %{join | source: source}
      end)

    {query, _} =
      map_children(%{query | joins: joins}, nil, fn child, _ -> {rewrite(child, fields), nil} end)

    Map.delete(query, :__projection_scope__)
  end

  defp restrict(query, :all, _fields), do: query

  defp restrict(query, needed, fields) do
    if is_nil(query.select) && is_nil(query.distinct) && query.combinations == [] &&
         query.group_bys == [] && query.havings == [] && is_nil(query.with_ctes) do
      case {query.from.source, needed} do
        {%Ecto.SubQuery{query: inner} = subquery, needed} ->
          local = Map.get(fields, {query.__projection_scope__, 0}, MapSet.new())
          needed = if local == :all, do: :all, else: MapSet.union(needed, local)

          %{
            query
            | from: %{query.from | source: %{subquery | query: restrict(inner, needed, fields)}}
          }

        {{table, schema}, needed}
        when is_binary(table) and is_atom(schema) and not is_nil(schema) ->
          attributes = schema.__schema__(:fields)
          selected = Enum.filter(attributes, &MapSet.member?(needed, &1))

          if selected != [] && length(selected) == MapSet.size(needed) &&
               length(selected) < length(attributes) do
            from(row in query, select: struct(row, ^selected))
          else
            query
          end

        _ ->
          query
      end
    else
      query
    end
  end

  defp map_children(query, acc, mapper) do
    {from, acc} =
      if query.from do
        {source, acc} = map_source(query.from.source, acc, mapper)
        {%{query.from | source: source}, acc}
      else
        {nil, acc}
      end

    {joins, acc} =
      Enum.map_reduce(query.joins, acc, fn join, acc ->
        {source, acc} = map_source(join.source, acc, mapper)
        {on, acc} = map_clause(join.on, acc, mapper)
        {%{join | source: source, on: on}, acc}
      end)

    query = %{query | from: from, joins: joins}

    Enum.reduce(@clauses, {query, acc}, fn key, {query, acc} ->
      {value, acc} = map_clause(Map.get(query, key), acc, mapper)
      {Map.put(query, key, value), acc}
    end)
  end

  defp map_source(%Ecto.SubQuery{query: query} = subquery, acc, mapper) do
    {query, acc} = mapper.(query, acc)
    {%{subquery | query: query}, acc}
  end

  defp map_source(%{from: _, joins: _} = query, acc, mapper), do: mapper.(query, acc)
  defp map_source(other, acc, _mapper), do: {other, acc}

  defp map_clause(%{subqueries: subqueries} = clause, acc, mapper) do
    {subqueries, acc} = Enum.map_reduce(subqueries, acc, &map_source(&1, &2, mapper))
    {%{clause | subqueries: subqueries}, acc}
  end

  defp map_clause(list, acc, mapper) when is_list(list),
    do: Enum.map_reduce(list, acc, &map_clause(&1, &2, mapper))

  defp map_clause({name, value}, acc, mapper) do
    {value, acc} = map_clause(value, acc, mapper)
    {{name, value}, acc}
  end

  defp map_clause(other, acc, _mapper), do: {other, acc}
end
