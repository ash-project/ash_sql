# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.Aggregate do
  @moduledoc false

  def add_aggregates(
        query,
        aggregates,
        resource,
        select?,
        source_binding,
        root_data \\ nil
      )

  def add_aggregates(query, [], _resource, _select?, _source_binding, _root_data),
    do: {:ok, query}

  def add_aggregates(query, aggregates, resource, select?, source_binding, root_data) do
    with {:ok, query, aggregates} <-
           AshSql.Aggregate.Common.normalize(query, aggregates, resource, root_data) do
      strategy(query, resource).add_aggregates(
        query,
        aggregates,
        resource,
        select?,
        source_binding,
        root_data
      )
    end
  end

  def extract_shared_filters(aggregates) do
    AshSql.Aggregate.Lateral.extract_shared_filters(aggregates)
  end

  def next_aggregate_name(index) do
    AshSql.Aggregate.Common.next_aggregate_name(index)
  end

  def can_group?(resource, aggregate, query) do
    AshSql.Aggregate.Lateral.can_group?(resource, aggregate, query)
  end

  def optimizable_first_aggregate?(resource, aggregate, query) do
    AshSql.Aggregate.Lateral.optimizable_first_aggregate?(resource, aggregate, query)
  end

  def add_subquery_aggregate_select(
        query,
        relationship_path,
        aggregate,
        resource,
        is_single?,
        first_relationship
      ) do
    AshSql.Aggregate.Lateral.add_subquery_aggregate_select(
      query,
      relationship_path,
      aggregate,
      resource,
      is_single?,
      first_relationship
    )
  end

  def aggregate_field_ref(aggregate, resource, relationship_path, query, first_relationship) do
    AshSql.Aggregate.Lateral.aggregate_field_ref(
      aggregate,
      resource,
      relationship_path,
      query,
      first_relationship
    )
  end

  def aggregate_field(aggregate, resource, query) do
    AshSql.Aggregate.Lateral.aggregate_field(aggregate, resource, query)
  end

  def field_expression(query, aggregate, resource, relationship_path \\ []) do
    ref = aggregate_field_ref(aggregate, resource, relationship_path, query, nil)

    with {:ok, query} <- add_field_dependencies(query, ref, resource) do
      case ref do
        nil ->
          {:ok, query, nil}

        ref ->
          case AshSql.Expr.dynamic_expr(query, ref, query.__ash_bindings__, false) do
            {:error, error} ->
              {:error, error}

            {expression, accumulator} ->
              {:ok, AshSql.Bindings.merge_expr_accumulator(query, accumulator), expression}
          end
      end
    end
  end

  def wrap_in_subquery_for_aggregates(query) do
    AshSql.Aggregate.Lateral.wrap_in_subquery_for_aggregates(query)
  end

  defdelegate limit_one_first_aggregate?(resource, aggregate), to: AshSql.Aggregate.Lateral

  defdelegate resource_aggregate_to_aggregate(resource, aggregate, opts \\ []),
    to: AshSql.Aggregate.Common

  defp add_field_dependencies(query, nil, _resource), do: {:ok, query}

  defp add_field_dependencies(query, ref, resource) do
    with {:ok, query} <- add_field_aggregates(query, ref.attribute, resource) do
      AshSql.Join.join_all_relationships(query, ref)
    end
  end

  defp add_field_aggregates(query, %struct{} = aggregate, resource)
       when struct in [Ash.Query.Aggregate, Ash.Resource.Aggregate] do
    add_aggregates(
      query,
      [aggregate],
      resource,
      false,
      query.__ash_bindings__.root_binding
    )
  end

  defp add_field_aggregates(query, %Ash.Query.Calculation{} = calculation, resource) do
    used_aggregates = Ash.Filter.used_aggregates(calculation, [])

    with {:ok, query} <- AshSql.Join.join_all_relationships(query, calculation, []) do
      add_aggregates(
        query,
        used_aggregates,
        resource,
        false,
        query.__ash_bindings__.root_binding
      )
    end
  end

  defp add_field_aggregates(query, _field, _resource), do: {:ok, query}

  defp strategy(query, resource) do
    case query.__ash_bindings__.sql_behaviour.aggregate_strategy(resource) do
      :lateral -> AshSql.Aggregate.Lateral
      :grouped -> AshSql.Aggregate.Grouped
    end
  end
end
