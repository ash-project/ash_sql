# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.Aggregate.Grouped.Query do
  @moduledoc false

  import Ecto.Query, only: [from: 2, subquery: 1]

  @supported_kinds [:count, :first, :sum, :max, :min, :avg, :exists]

  def run_aggregate_query(original_query, aggregates, resource, implementation) do
    aggregates
    |> Enum.reduce_while({:ok, %{}}, fn aggregate, {:ok, acc} ->
      case run_single_aggregate(original_query, aggregate, resource, implementation) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, aggregate.name, value)}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp run_single_aggregate(_original_query, %{kind: kind}, _resource, _implementation)
       when kind not in @supported_kinds do
    {:error, "AshSql grouped query aggregates do not support #{inspect(kind)} aggregates"}
  end

  defp run_single_aggregate(
         _original_query,
         %{relationship_path: [_ | _]} = aggregate,
         _resource,
         _implementation
       ) do
    {:error,
     "AshSql grouped query aggregates do not yet support relationship aggregate #{inspect(aggregate.name)}"}
  end

  defp run_single_aggregate(
         original_query,
         %{kind: :exists} = aggregate,
         resource,
         implementation
       ) do
    with {:ok, query} <- filtered_query(original_query, aggregate, resource) do
      query = aggregate_base_query(query)
      repo = AshSql.dynamic_repo(resource, implementation, query)
      {:ok, repo.exists?(query, AshSql.repo_opts(repo, implementation, nil, nil, resource))}
    end
  end

  defp run_single_aggregate(original_query, %{kind: :first} = aggregate, resource, implementation) do
    with {:ok, query} <- filtered_query(original_query, aggregate, resource),
         query = aggregate_base_query(query),
         {:ok, query, field} <- AshSql.Aggregate.field_expression(query, aggregate, resource),
         {:ok, query} <- sort_first(query, aggregate, resource) do
      query =
        query
        |> Ecto.Query.exclude(:select)
        |> Map.put(:windows, [])
        |> maybe_filter_first_nil_values(aggregate, field)
        |> Ecto.Query.limit(1)
        |> Ecto.Query.select(^field)

      repo = AshSql.dynamic_repo(resource, implementation, query)

      value = repo.one(query, AshSql.repo_opts(repo, implementation, nil, nil, resource))

      {:ok, maybe_default_value(value, aggregate)}
    end
  end

  defp run_single_aggregate(original_query, aggregate, resource, implementation) do
    with {:ok, query} <- filtered_query(original_query, aggregate, resource),
         query = aggregate_base_query(query),
         {:ok, query, dynamic} <- aggregate_dynamic(query, aggregate, resource) do
      query = Ecto.Query.select(query, ^%{aggregate.name => dynamic})

      repo = AshSql.dynamic_repo(resource, implementation, query)

      result =
        query
        |> repo.one(AshSql.repo_opts(repo, implementation, nil, nil, resource))
        |> Map.get(aggregate.name)

      {:ok, result}
    end
  end

  defp filtered_query(original_query, aggregate, resource) do
    case aggregate.query.filter do
      nil -> {:ok, original_query}
      %{expression: nil} -> {:ok, original_query}
      filter -> AshSql.Filter.filter(original_query, filter, resource)
    end
  end

  defp aggregate_base_query(query) do
    if query.distinct || query.limit || query.offset do
      query =
        query
        |> Ecto.Query.exclude(:select)
        |> Map.put(:windows, [])
        |> maybe_add_offset_limit()
        |> maybe_exclude_subquery_order()

      from(row in subquery(query), as: ^query.__ash_bindings__.root_binding)
      |> Map.put(:__ash_bindings__, query.__ash_bindings__)
    else
      query
      |> Ecto.Query.exclude(:select)
      |> Ecto.Query.exclude(:order_by)
      |> Map.put(:windows, [])
    end
  end

  defp aggregate_dynamic(query, %{kind: :count, field: nil, uniq?: true} = aggregate, resource) do
    case Ash.Resource.Info.primary_key(resource) do
      [field] ->
        dynamic =
          Ecto.Query.dynamic(
            count(field(as(^query.__ash_bindings__.root_binding), ^field), :distinct)
          )

        {:ok, query, dynamic}

      [] ->
        {:error,
         "AshSql grouped query aggregate #{inspect(aggregate.name)} requires a single primary key to count distinct records, but #{inspect(resource)} has no primary key"}

      fields ->
        {:error,
         "AshSql grouped query aggregate #{inspect(aggregate.name)} requires a single primary key to count distinct records, but #{inspect(resource)} has composite primary key #{inspect(fields)}"}
    end
  end

  defp aggregate_dynamic(query, %{kind: :count, field: nil}, _resource),
    do: {:ok, query, Ecto.Query.dynamic(count())}

  defp aggregate_dynamic(query, %{kind: :count} = aggregate, resource) do
    with {:ok, query, field} <- AshSql.Aggregate.field_expression(query, aggregate, resource) do
      dynamic =
        if aggregate.uniq? do
          Ecto.Query.dynamic(count(^field, :distinct))
        else
          Ecto.Query.dynamic(count(^field))
        end

      {:ok, query, dynamic}
    end
  end

  defp aggregate_dynamic(query, aggregate, resource)
       when aggregate.kind in [:sum, :max, :min, :avg] do
    with {:ok, query, field} <- AshSql.Aggregate.field_expression(query, aggregate, resource) do
      dynamic =
        case aggregate.kind do
          :sum -> Ecto.Query.dynamic(sum(^field))
          :max -> Ecto.Query.dynamic(max(^field))
          :min -> Ecto.Query.dynamic(min(^field))
          :avg -> Ecto.Query.dynamic(avg(^field))
        end

      {:ok, query,
       maybe_type_dynamic(query, maybe_default_dynamic(dynamic, aggregate), aggregate)}
    end
  end

  defp aggregate_dynamic(_query, aggregate, _resource) do
    {:error, "AshSql grouped query aggregate #{inspect(aggregate.name)} is unsupported"}
  end

  defp maybe_type_dynamic(query, dynamic, aggregate) do
    type =
      AshSql.Expr.parameterized_type(
        query.__ash_bindings__.sql_behaviour,
        aggregate.type,
        aggregate.constraints,
        :aggregate
      )

    if type do
      query.__ash_bindings__.sql_behaviour.type_expr(dynamic, type)
    else
      dynamic
    end
  end

  defp maybe_default_dynamic(dynamic, %{default_value: nil}), do: dynamic

  defp maybe_default_dynamic(dynamic, aggregate) do
    Ecto.Query.dynamic(coalesce(^dynamic, ^aggregate.default_value))
  end

  defp sort_first(query, %{query: %{sort: sort}}, _resource) when sort in [nil, []],
    do: {:ok, query}

  defp sort_first(query, %{query: %{sort: sort}}, resource) do
    AshSql.Sort.sort(
      query,
      List.wrap(sort),
      resource,
      [],
      query.__ash_bindings__.root_binding,
      :direct
    )
  end

  defp maybe_filter_first_nil_values(query, %{include_nil?: true}, _field), do: query

  defp maybe_filter_first_nil_values(query, _aggregate, field) do
    filter = Ecto.Query.dynamic(not is_nil(^field))
    Ecto.Query.where(query, ^filter)
  end

  defp maybe_default_value(nil, %{default_value: default_value}), do: default_value
  defp maybe_default_value(value, _aggregate), do: value

  defp maybe_add_offset_limit(%{limit: nil, offset: offset} = query) when not is_nil(offset),
    do: Ecto.Query.limit(query, -1)

  defp maybe_add_offset_limit(query), do: query

  defp maybe_exclude_subquery_order(%{limit: nil, offset: nil} = query),
    do: Ecto.Query.exclude(query, :order_by)

  defp maybe_exclude_subquery_order(query), do: query
end
