# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.Filter do
  @moduledoc false

  require Ecto.Query

  def filter(query, filter, resource, opts \\ []) do
    used_aggregates = Ash.Filter.used_aggregates(filter, [])

    query
    |> AshSql.Join.join_all_relationships(filter, opts)
    |> case do
      {:ok, query} ->
        query
        |> AshSql.Aggregate.add_aggregates(
          used_aggregates,
          resource,
          false,
          query.__ash_bindings__.root_binding
        )
        |> case do
          {:ok, query} ->
            {:ok, add_filter_expression(query, filter)}

          {:error, error} ->
            {:error, error}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  def add_filter_expression(query, filter) do
    filter
    |> maybe_wrap_source_exists(query)
    |> AshSql.Expr.split_statements(:and)
    |> Enum.reduce(query, fn filter, query ->
      {dynamic, acc} =
        AshSql.Expr.dynamic_expr(
          query,
          filter,
          Map.put(query.__ash_bindings__, :location, :filter)
        )

      dynamic =
        if is_nil(dynamic) do
          false
        else
          dynamic
        end

      query
      |> Ecto.Query.where([], ^dynamic)
      |> AshSql.Expr.merge_accumulator(acc)
    end)
  end

  defp maybe_wrap_source_exists(filter, query) do
    if query.__ash_bindings__[:source_exists_parent_filter?] &&
         query.__ash_bindings__[:parent_bindings] do
      rewritten = AshSql.Join.rewrite_parent_filter_for_source_exists(filter)
      source = query.__ash_bindings__.parent_bindings.resource

      Map.put(
        %Ash.Query.Exists{
          related?: false,
          resource: source,
          expr: rewritten,
          path: [],
          at_path: []
        },
        :correlate_source_pk,
        true
      )
    else
      filter
    end
  end
end
