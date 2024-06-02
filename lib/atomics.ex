defmodule AshSql.Atomics do
  require Ecto.Query

  def select_atomics(_resource, query, []) do
    {:ok, query}
  end

  def select_atomics(resource, query, atomics) do
    Enum.reduce_while(atomics, {:ok, query, []}, fn {field, expr}, {:ok, query, dynamics} ->
      attribute = Ash.Resource.Info.attribute(resource, field)

      type =
        query.__ash_bindings__.sql_behaviour.parameterized_type(
          attribute.type,
          attribute.constraints
        )

      case AshSql.Expr.dynamic_expr(
             query,
             expr,
             Map.merge(query.__ash_bindings__, %{
               location: :update,
               updating_field: attribute.name
             }),
             false,
             type
           ) do
        {dynamic, acc} ->
          field = String.to_atom("__new_#{field}")

          {:cont,
           {:ok, AshSql.Expr.merge_accumulator(query, acc), Keyword.put(dynamics, field, dynamic)}}

        other ->
          {:halt, other}
      end
    end)
    |> case do
      {:ok, query, dynamics} ->
        query = Ecto.Query.exclude(query, :select)

        {params, selects, _, query} =
          Enum.reduce(
            dynamics,
            {[], [], 0, query},
            fn {key, value}, {params, select, count, query} ->
              case AshSql.Expr.dynamic_expr(query, value, query.__ash_bindings__) do
                {%Ecto.Query.DynamicExpr{} = dynamic, acc} ->
                  result =
                    Ecto.Query.Builder.Dynamic.partially_expand(
                      :select,
                      query,
                      dynamic,
                      params,
                      count
                    )

                  expr = elem(result, 0)
                  new_params = elem(result, 1)

                  new_count =
                    result |> Tuple.to_list() |> List.last()

                  {new_params, [{key, expr} | select], new_count,
                   AshSql.Expr.merge_accumulator(query, acc)}

                {other, acc} ->
                  {[{other, {0, key}} | params], [{key, {:^, [], [count]}} | select], count + 1,
                   AshSql.Expr.merge_accumulator(query, acc)}
              end
            end
          )

        query =
          Map.put(query, :select, %Ecto.Query.SelectExpr{
            expr: {:%{}, [], Enum.reverse(selects)},
            params: Enum.reverse(params)
          })

        {:ok, query}

      other ->
        other
    end
  end

  def query_with_atomics(
        resource,
        %{__ash_bindings__: %{atomics_in_binding: binding}} = query,
        filter,
        atomics,
        updating_one_changes,
        existing_set
      )
      when updating_one_changes == %{} do
    {:ok, query} =
      if is_nil(filter) do
        {:ok, query}
      else
        AshSql.Filter.filter(query, filter, resource)
      end

    {query, dynamics} =
      Enum.reduce(atomics, {query, []}, fn {field, _expr}, {query, set} ->
        mapped_field = String.to_atom("__new_#{field}")

        {query,
         Keyword.put(set, field, Ecto.Query.dynamic([], field(as(^binding), ^mapped_field)))}
      end)

    {params, set, _, query} =
      Enum.reduce(
        dynamics ++ existing_set,
        {[], [], 0, query},
        fn {key, value}, {params, set, count, query} ->
          case AshSql.Expr.dynamic_expr(query, value, query.__ash_bindings__) do
            {%Ecto.Query.DynamicExpr{} = dynamic, acc} ->
              result =
                Ecto.Query.Builder.Dynamic.partially_expand(
                  :select,
                  query,
                  dynamic,
                  params,
                  count
                )

              expr = elem(result, 0)
              new_params = elem(result, 1)

              new_count =
                result |> Tuple.to_list() |> List.last()

              {new_params, [{key, expr} | set], new_count,
               AshSql.Expr.merge_accumulator(query, acc)}

            {other, acc} ->
              {[{other, {0, key}} | params], [{key, {:^, [], [count]}} | set], count + 1,
               AshSql.Expr.merge_accumulator(query, acc)}
          end
        end
      )

    case set do
      [] ->
        :empty

      set ->
        {:ok,
         Map.put(query, :updates, [
           %Ecto.Query.QueryExpr{
             # why do I have to reverse the `set`???
             # it breaks if I don't
             expr: [set: Enum.reverse(set)],
             params: Enum.reverse(params)
           }
         ])}
    end
  end

  @moduledoc false
  def query_with_atomics(
        resource,
        query,
        filter,
        atomics,
        updating_one_changes,
        existing_set
      ) do
    {:ok, query} =
      if is_nil(filter) do
        {:ok, query}
      else
        AshSql.Filter.filter(query, filter, resource)
      end

    atomics_result =
      Enum.reduce_while(atomics, {:ok, query, []}, fn {field, expr}, {:ok, query, set} ->
        attribute = Ash.Resource.Info.attribute(resource, field)

        type =
          query.__ash_bindings__.sql_behaviour.parameterized_type(
            attribute.type,
            attribute.constraints
          )

        case AshSql.Expr.dynamic_expr(
               query,
               expr,
               Map.merge(query.__ash_bindings__, %{
                 location: :update,
                 updating_field: attribute.name
               }),
               false,
               type
             ) do
          {dynamic, acc} ->
            {:cont,
             {:ok, AshSql.Expr.merge_accumulator(query, acc), Keyword.put(set, field, dynamic)}}

          other ->
            {:halt, other}
        end
      end)

    case atomics_result do
      {:ok, query, dynamics} ->
        {params, set, count} =
          updating_one_changes
          |> Map.to_list()
          |> Enum.reduce({[], [], 0}, fn {key, value}, {params, set, count} ->
            {[{value, {0, key}} | params], [{key, {:^, [], [count]}} | set], count + 1}
          end)

        {params, set, _, query} =
          Enum.reduce(
            dynamics ++ existing_set,
            {params, set, count, query},
            fn {key, value}, {params, set, count, query} ->
              case AshSql.Expr.dynamic_expr(query, value, query.__ash_bindings__) do
                {%Ecto.Query.DynamicExpr{} = dynamic, acc} ->
                  result =
                    Ecto.Query.Builder.Dynamic.partially_expand(
                      :select,
                      query,
                      dynamic,
                      params,
                      count
                    )

                  expr = elem(result, 0)
                  new_params = elem(result, 1)

                  new_count =
                    result |> Tuple.to_list() |> List.last()

                  {new_params, [{key, expr} | set], new_count,
                   AshSql.Expr.merge_accumulator(query, acc)}

                {other, acc} ->
                  {[{other, {0, key}} | params], [{key, {:^, [], [count]}} | set], count + 1,
                   AshSql.Expr.merge_accumulator(query, acc)}
              end
            end
          )

        case set do
          [] ->
            :empty

          set ->
            {:ok,
             Map.put(query, :updates, [
               %Ecto.Query.QueryExpr{
                 # why do I have to reverse the `set`???
                 # it breaks if I don't
                 expr: [set: Enum.reverse(set)],
                 params: Enum.reverse(params)
               }
             ])}
        end

      {:error, error} ->
        {:error, error}
    end
  end
end
