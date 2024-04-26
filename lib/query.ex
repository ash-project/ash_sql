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

    case context[:data_layer][:lateral_join_source] do
      {_, [{%{resource: resource}, _, _, _} | rest]} ->
        parent =
          resource
          |> resource_to_query(data_layer_query.__ash_bindings__.sql_behaviour)
          |> AshSql.Bindings.default_bindings(resource, sql_behaviour, context)

        parent =
          case rest do
            [{resource, _, _, %{name: join_relationship_name}} | _] ->
              binding_data = %{type: :inner, path: [join_relationship_name], source: resource}
              AshSql.Bindings.add_binding(parent, binding_data)

            _ ->
              parent
          end

        query_with_ash_bindings =
          data_layer_query
          |> AshSql.Bindings.add_parent_bindings(%{
            data_layer: %{parent_bindings: parent.__ash_bindings__}
          })
          |> Map.update!(:__ash_bindings__, &Map.put(&1, :lateral_join?, true))

        {:ok, query_with_ash_bindings}

      _ ->
        ash_bindings =
          data_layer_query.__ash_bindings__
          |> Map.put(:lateral_join?, false)

        {:ok, %{data_layer_query | __ash_bindings__: ash_bindings}}
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
end
