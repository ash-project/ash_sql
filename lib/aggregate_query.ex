# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.AggregateQuery do
  @moduledoc false

  def run_aggregate_query(original_query, aggregates, resource, implementation) do
    original_query =
      AshSql.Bindings.default_bindings(original_query, resource, implementation)

    strategy(original_query, resource).run_aggregate_query(
      original_query,
      aggregates,
      resource,
      implementation
    )
  end

  def add_single_aggs(result, resource, query, cant_group, implementation) do
    AshSql.Aggregate.Lateral.Query.add_single_aggs(
      result,
      resource,
      query,
      cant_group,
      implementation
    )
  end

  defp strategy(query, resource) do
    case query.__ash_bindings__.sql_behaviour.aggregate_strategy(resource) do
      :lateral -> AshSql.Aggregate.Lateral.Query
      :grouped -> AshSql.Aggregate.Grouped.Query
    end
  end
end
