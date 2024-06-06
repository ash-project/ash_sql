defmodule AshSql.Query do
  @moduledoc false
  import Ecto.Query, only: [subquery: 1, from: 2]

  def resource_to_query(resource, implementation) do
    from(row in {implementation.table(resource) || "", resource}, [])
  end

  def set_context(resource, data_layer_query, sql_behaviour, context) do
    start_bindings = context[:data_layer][:start_bindings_at] || 0
    data_layer_query = from(row in data_layer_query, as: ^start_bindings)

    data_layer_query =
      if context[:data_layer][:table] do
        %{
          data_layer_query
          | from: %{data_layer_query.from | source: {context[:data_layer][:table], resource}}
        }
      else
        data_layer_query
      end

    data_layer_query =
      if context[:data_layer][:schema] do
        Ecto.Query.put_query_prefix(data_layer_query, to_string(context[:data_layer][:schema]))
      else
        data_layer_query
      end

    data_layer_query =
      data_layer_query
      |> AshSql.Bindings.default_bindings(resource, sql_behaviour, context)
      |> AshSql.Bindings.add_parent_bindings(context)

    data_layer_query =
      case context[:data_layer][:lateral_join_source] do
        {_data, path} ->
          lateral_join_source_query = path |> List.first() |> elem(0)

          lateral_join_source_query.resource
          |> Ash.Query.set_context(%{
            :data_layer => lateral_join_source_query.context[:data_layer]
          })
          |> Ash.Query.set_tenant(lateral_join_source_query.tenant)
          |> set_lateral_join_prefix(data_layer_query)
          |> case do
            %{valid?: true} = query ->
              Ash.Query.data_layer_query(query)

            query ->
              {:error, query}
          end
          |> case do
            {:ok, lateral_join_source_query} ->
              lateral_join_source_query =
                if Enum.count(path) == 2 do
                  Map.update!(lateral_join_source_query, :__ash_bindings__, fn bindings ->
                    bindings
                    |> Map.put(:lateral_join_bindings, [start_bindings + 1])
                    |> Map.update!(:bindings, fn bindings ->
                      Map.put(
                        bindings,
                        start_bindings + 1,
                        %{
                          source: path |> Enum.at(1) |> elem(3) |> Map.get(:source),
                          path: [path |> Enum.at(1) |> elem(3) |> Map.get(:name)],
                          type: :inner
                        }
                      )
                    end)
                  end)
                else
                  lateral_join_source_query
                end

              {:ok,
               Map.update!(data_layer_query, :__ash_bindings__, fn bindings ->
                 Map.put(
                   bindings,
                   :lateral_join_source_query,
                   lateral_join_source_query
                 )
                 |> Map.update!(:current, &(&1 + 1))
               end)}

            {:error, error} ->
              {:error, error}
          end

        _ ->
          {:ok, data_layer_query}
      end

    case data_layer_query do
      {:error, error} ->
        {:error, error}

      {:ok, data_layer_query} ->
        case context[:data_layer][:lateral_join_source] do
          {_, _} ->
            data_layer_query =
              data_layer_query
              |> Map.update!(:__ash_bindings__, &Map.put(&1, :lateral_join?, true))

            {:ok, data_layer_query}

          _ ->
            ash_bindings =
              data_layer_query.__ash_bindings__
              |> Map.put(:lateral_join?, false)

            {:ok, %{data_layer_query | __ash_bindings__: ash_bindings}}
        end
    end
  end

  def return_query(%{__ash_bindings__: %{lateral_join?: true}} = query, resource) do
    query =
      AshSql.Bindings.default_bindings(query, resource, query.__ash_bindings__.sql_behaviour)

    if query.__ash_bindings__[:sort_applied?] do
      {:ok, query}
    else
      AshSql.Sort.apply_sort(
        query,
        query.__ash_bindings__[:sort],
        query.__ash_bindings__.resource
      )
    end
  end

  def return_query(query, resource) do
    query =
      AshSql.Bindings.default_bindings(query, resource, query.__ash_bindings__.sql_behaviour)

    with_sort_applied =
      if query.__ash_bindings__[:sort_applied?] do
        {:ok, query}
      else
        AshSql.Sort.apply_sort(query, query.__ash_bindings__[:sort], resource)
      end

    case with_sort_applied do
      {:error, error} ->
        {:error, error}

      {:ok, query} ->
        if query.__ash_bindings__[:__order__?] && query.windows[:order] do
          if query.distinct do
            query_with_order =
              from(row in query, select_merge: %{__order__: over(row_number(), :order)})

            query_without_limit_and_offset =
              query_with_order
              |> Ecto.Query.exclude(:limit)
              |> Ecto.Query.exclude(:offset)

            {:ok,
             from(row in subquery(query_without_limit_and_offset),
               select: row,
               order_by: row.__order__
             )
             |> Map.put(:limit, query.limit)
             |> Map.put(:offset, query.offset)}
          else
            order_by = %{query.windows[:order] | expr: query.windows[:order].expr[:order_by]}

            {:ok,
             %{
               query
               | windows: Keyword.delete(query.windows, :order),
                 order_bys: [order_by]
             }}
          end
        else
          {:ok, %{query | windows: Keyword.delete(query.windows, :order)}}
        end
    end
  end

  defp set_lateral_join_prefix(ash_query, query) do
    if Ash.Resource.Info.multitenancy_strategy(ash_query.resource) == :context do
      Ash.Query.set_tenant(ash_query, query.prefix)
    else
      ash_query
    end
  end
end
