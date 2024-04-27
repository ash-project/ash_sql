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
        |> AshSql.Aggregate.add_aggregates(used_aggregates, resource, false, 0)
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
    |> AshSql.Expr.split_statements(:and)
    |> Enum.reduce(query, fn filter, query ->
      {dynamic, acc} = AshSql.Expr.dynamic_expr(query, filter, query.__ash_bindings__)

      if is_nil(dynamic) do
        query
      else
        query
        |> Ecto.Query.where([], ^dynamic)
        |> AshSql.Expr.merge_accumulator(acc)
      end
    end)
  end
end
