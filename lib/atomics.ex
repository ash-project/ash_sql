defmodule AshSql.Atomics do
  require Ecto.Query

  def select_atomics(_resource, query, []) do
    {:ok, query}
  end

  # sobelow_skip ["DOS.StringToAtom"]
  def select_atomics(resource, query, atomics) do
    atomics = type_atomics(query.__ash_bindings__.sql_behaviour, resource, atomics)

    atomics
    |> Enum.reverse()
    |> Enum.reduce_while({:ok, query, []}, fn {field, expr}, {:ok, query, dynamics} ->
      attribute = Ash.Resource.Info.attribute(resource, field)

      type =
        query.__ash_bindings__.sql_behaviour.storage_type(resource, attribute.name) ||
          query.__ash_bindings__.sql_behaviour.parameterized_type(
            attribute.type,
            attribute.constraints
          )

      case AshSql.Expr.dynamic_expr(
             query,
             expr,
             Map.merge(query.__ash_bindings__, %{
               location: :update
             }),
             false,
             type
           ) do
        {dynamic, acc} ->
          new_field = String.to_atom("__new_#{field}")

          {:cont,
           {:ok, AshSql.Expr.merge_accumulator(query, acc),
            dynamics ++ [{new_field, {dynamic, field}}]}}

        other ->
          {:halt, other}
      end
    end)
    |> case do
      {:ok, query, dynamics} ->
        query = Ecto.Query.exclude(query, :select)

        pkey_dynamics =
          resource
          |> Ash.Resource.Info.primary_key()
          |> Enum.map(fn key ->
            {key, {Ecto.Query.dynamic([row], field(row, ^key)), key}}
          end)

        dynamics = Keyword.merge(dynamics, pkey_dynamics)

        {params, selects, subqueries, _, query} =
          Enum.reduce(
            dynamics,
            {[], [], [], 0, query},
            fn {key, {value, original_field}}, {params, select, subqueries, count, query} ->
              case AshSql.Expr.dynamic_expr(query, value, query.__ash_bindings__) do
                {%Ecto.Query.DynamicExpr{} = dynamic, acc} ->
                  result =
                    Ecto.Query.Builder.Dynamic.partially_expand(
                      query,
                      dynamic,
                      params,
                      subqueries,
                      %{},
                      count
                    )

                  expr = elem(result, 0)
                  new_params = elem(result, 1)
                  new_subqueries = elem(result, 2)

                  new_count =
                    result |> Tuple.to_list() |> List.last()

                  {new_params, [{key, expr} | select], new_subqueries, new_count,
                   AshSql.Expr.merge_accumulator(query, acc)}

                {other, acc} ->
                  {[{other, {0, original_field}} | params], [{key, {:^, [], [count]}} | select],
                   subqueries, count + 1, AshSql.Expr.merge_accumulator(query, acc)}
              end
            end
          )

        query =
          Map.put(query, :select, %Ecto.Query.SelectExpr{
            expr: {:%{}, [], Enum.reverse(selects)},
            subqueries: subqueries,
            params: Enum.reverse(params)
          })

        {:ok, query}

      other ->
        other
    end
  end

  # sobelow_skip ["DOS.StringToAtom"]
  def query_with_atomics(
        resource,
        %{__ash_bindings__: %{atomics_in_binding: binding}} = query,
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

    {query, dynamics} =
      atomics
      |> Enum.reverse()
      |> Enum.reduce({query, []}, fn {field, _expr}, {query, set} ->
        mapped_field = String.to_atom("__new_#{field}")

        {query, [{field, Ecto.Query.dynamic([], field(as(^binding), ^mapped_field))} | set]}
      end)

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
                  :update,
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
    atomics = type_atomics(query.__ash_bindings__.sql_behaviour, resource, atomics)

    {:ok, query} =
      if is_nil(filter) do
        {:ok, query}
      else
        AshSql.Filter.filter(query, filter, resource)
      end

    atomics_result =
      atomics
      |> Enum.reverse()
      |> Enum.reduce_while({:ok, query, []}, fn {field, expr}, {:ok, query, set} ->
        attribute = Ash.Resource.Info.attribute(resource, field)

        type =
          query.__ash_bindings__.sql_behaviour.storage_type(resource, attribute.name) ||
            query.__ash_bindings__.sql_behaviour.parameterized_type(
              attribute.type,
              attribute.constraints
            )

        case AshSql.Expr.dynamic_expr(
               query,
               expr,
               Map.merge(query.__ash_bindings__, %{
                 location: :update
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

  defp type_atomics(sql_behaviour, resource, atomics) do
    Enum.map(atomics, fn {key, expr} ->
      attribute = Ash.Resource.Info.attribute(resource, key)

      expr =
        case sql_behaviour.storage_type(resource, attribute.name) do
          nil ->
            %Ash.Query.Function.Type{arguments: [expr, attribute.type, attribute.constraints]}

          _ ->
            expr
        end

      {key, expr}
    end)
  end
end
