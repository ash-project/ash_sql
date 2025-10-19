# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs.contributors>
#
# SPDX-License-Identifier: MIT

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

      expr =
        case expr do
          %Ash.Query.Function.Type{arguments: [expr | _]} ->
            expr

          %Ash.Query.Call{name: :type, args: [expr | _]} ->
            expr

          _ ->
            expr
        end

      expr =
        if AshSql.Calculation.map_type?(
             attribute.type,
             attribute.constraints || []
           ) do
          expr
        else
          maybe_cast_atomic_expr(
            expr,
            attribute,
            query.__ash_bindings__.sql_behaviour,
            resource
          )
        end

      type =
        case query.__ash_bindings__.sql_behaviour.storage_type(resource, attribute.name) do
          nil -> {attribute.type, attribute.constraints}
          storage_type -> storage_type
        end

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

          dynamic =
            if is_map(dynamic) and not is_struct(dynamic) do
              Ecto.Query.dynamic(type(^dynamic, :map))
            else
              dynamic
            end

          {:cont,
           {:ok, AshSql.Expr.merge_accumulator(query, acc), dynamics ++ [{new_field, dynamic}]}}

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
            {key, Ecto.Query.dynamic([row], field(row, ^key))}
          end)

        dynamics = Map.new(Keyword.merge(dynamics, pkey_dynamics))

        {:ok,
         Ecto.Query.select(query, ^dynamics)
         |> Map.update!(:select, fn select ->
           %{
             select
             | subqueries: Enum.map(select.subqueries || [], &set_subquery_prefix(&1, query))
           }
         end)}

      other ->
        other
    end
  end

  def set_subquery_prefix(sub_query, query) do
    %{
      sub_query
      | query: %{
          sub_query.query
          | prefix:
              subquery_prefix(
                sub_query,
                query,
                sub_query.query.__ash_bindings__.resource
              )
        }
    }
  end

  defp subquery_prefix(sub_query, base_query, resource) do
    if Ash.Resource.Info.multitenancy_strategy(resource) == :context do
      sub_query.query.__ash_bindings__.sql_behaviour.schema(resource) ||
        Map.get(Map.get(base_query, :__ash_bindings__), :tenant) ||
        base_query.prefix ||
        sub_query.query.__ash_bindings__.sql_behaviour.repo(resource, :mutate).config()[
          :default_prefix
        ]
    else
      sub_query.query.__ash_bindings__.sql_behaviour.schema(resource) ||
        sub_query.query.__ash_bindings__.sql_behaviour.repo(resource, :mutate).config()[
          :default_prefix
        ]
    end
  end

  defp maybe_cast_atomic_expr(expr, attribute, sql_behaviour, resource) do
    storage_type = sql_behaviour.storage_type(resource, attribute.name)

    cond do
      is_list(expr) and typed_struct_array_attr_type?(attribute.type) and
          storage_type in [:map, :jsonb, :json] ->
        dump_and_encode_map_array(expr, attribute)

      is_list(expr) and embedded_ash_resource?(Enum.at(expr, 0)) and
          storage_type in [:map, :jsonb, :json] ->
        # Embedded resources with jsonb storage need to be dumped to native format
        dump_and_encode_map_array(expr, attribute)

      is_list(expr) and not embedded_ash_resource?(Enum.at(expr, 0)) ->
        {:ok, casted} =
          Ash.Query.Function.Type.new([expr, attribute.type, attribute.constraints || []])

        casted

      true ->
        expr
    end
  end

  defp typed_struct_array_attr_type?({:array, attr_type}) do
    function_exported?(attr_type, :spark_is, 0) and attr_type.spark_is() == Ash.TypedStruct
  end

  defp typed_struct_array_attr_type?(_attr_type), do: false

  defp embedded_ash_resource?(value) do
    is_struct(value) and Ash.Resource.Info.resource?(value.__struct__) and
      Ash.Resource.Info.embedded?(value.__struct__)
  end

  defp dump_and_encode_map_array(expr, %{type: {:array, inner_type}} = attribute) do
    dumped_list =
      Enum.map(expr, fn item ->
        case Ash.Type.dump_to_native(inner_type, item, attribute.constraints[:items] || []) do
          {:ok, dumped} -> dumped
          :error -> item
        end
      end)

    {:ok, type_expr} =
      Ash.Query.Function.Type.new([
        dumped_list,
        AshSql.TypedStructArrayJsonb,
        attribute.constraints
      ])

    type_expr
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

    Enum.reduce_while(
      dynamics ++ existing_set,
      {:ok, query, Map.to_list(updating_one_changes)},
      fn {key, value}, {:ok, query, set} ->
        case AshSql.Expr.dynamic_expr(query, value, query.__ash_bindings__) do
          {dynamic, acc} ->
            {:cont,
             {:ok, AshSql.Expr.merge_accumulator(query, acc), Keyword.put(set, key, dynamic)}}

          other ->
            {:halt, other}
        end
      end
    )
    |> case do
      {:ok, query, []} ->
        {:empty, query}

      {:ok, query, set} ->
        {:ok, Ecto.Query.update(query, set: ^set)}

      {:error, error} ->
        {:error, error}
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

    atomics
    |> Enum.reduce_while(
      {:ok, query, existing_set ++ Map.to_list(updating_one_changes)},
      fn {field, expr}, {:ok, query, set} ->
        attribute = Ash.Resource.Info.attribute(resource, field)

        expr =
          case expr do
            %Ash.Query.Function.Type{arguments: [expr | _]} ->
              expr

            %Ash.Query.Call{name: :type, args: [expr | _]} ->
              expr

            _ ->
              expr
          end

        expr =
          if AshSql.Calculation.map_type?(
               attribute.type,
               attribute.constraints || []
             ) do
            expr
          else
            maybe_cast_atomic_expr(
              expr,
              attribute,
              query.__ash_bindings__.sql_behaviour,
              resource
            )
          end

        type =
          case query.__ash_bindings__.sql_behaviour.storage_type(resource, attribute.name) do
            nil -> {attribute.type, attribute.constraints}
            storage_type -> storage_type
          end

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
      end
    )
    |> case do
      {:ok, query, []} ->
        {:empty, query}

      {:ok, query, set} ->
        {:ok, Ecto.Query.update(query, set: ^set)}

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
