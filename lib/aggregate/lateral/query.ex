# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.Aggregate.Lateral.Query do
  @moduledoc false
  import Ecto.Query, only: [from: 2, subquery: 1]

  def run_aggregate_query(original_query, aggregates, resource, implementation) do
    {can_group, cant_group} =
      aggregates
      |> Enum.split_with(&AshSql.Aggregate.can_group?(resource, &1, original_query))
      |> case do
        {[one], cant_group} -> {[], [one | cant_group]}
        {can_group, cant_group} -> {can_group, cant_group}
      end

    {global_filter, can_group} =
      AshSql.Aggregate.extract_shared_filters(can_group)

    query =
      case global_filter do
        {:ok, global_filter} ->
          AshSql.Filter.filter(original_query, global_filter, resource)

        :error ->
          {:ok, original_query}
      end

    case query do
      {:error, error} ->
        {:error, error}

      {:ok, query} ->
        query =
          if query.distinct || query.limit do
            query =
              query
              |> Ecto.Query.exclude(:select)
              |> Ecto.Query.exclude(:order_by)
              |> Map.put(:windows, [])

            root_binding = query.__ash_bindings__.root_binding

            # the original query's joins are inside the subquery, so only the root
            # binding can be referenced from the outer query
            from(row in subquery(query), as: ^root_binding, select: %{})
            |> Map.put(:__ash_bindings__, %{
              query.__ash_bindings__
              | bindings: Map.take(query.__ash_bindings__.bindings, [root_binding])
            })
          else
            query
            |> Ecto.Query.exclude(:select)
            |> Ecto.Query.exclude(:order_by)
            |> Map.put(:windows, [])
            |> Ecto.Query.select(%{})
          end

        group_query =
          Enum.reduce_while(
            can_group,
            {:ok, query},
            fn agg, {:ok, query} ->
              first_relationship =
                Ash.Resource.Info.relationship(resource, agg.relationship_path |> Enum.at(0))

              ref =
                AshSql.Aggregate.aggregate_field_ref(
                  agg,
                  resource,
                  agg.relationship_path |> Enum.drop(1),
                  query,
                  first_relationship
                )

              case AshSql.Aggregate.add_field_dependencies(query, ref, resource, left_only?: true) do
                {:ok, query} ->
                  {:cont,
                   {:ok,
                    AshSql.Aggregate.add_subquery_aggregate_select(
                      query,
                      agg.relationship_path |> Enum.drop(1),
                      agg,
                      resource,
                      false,
                      first_relationship
                    )}}

                {:error, error} ->
                  {:halt, {:error, error}}
              end
            end
          )

        with {:ok, group_query} <- group_query do
          result =
            case can_group do
              [] ->
                %{}

              _ ->
                repo = AshSql.dynamic_repo(resource, implementation, query)
                repo.one(group_query, AshSql.repo_opts(repo, implementation, nil, nil, resource))
            end

          {:ok, add_single_aggs(result, resource, query, cant_group, implementation)}
        end
    end
  end

  def add_single_aggs(result, resource, query, cant_group, implementation) do
    Enum.reduce(cant_group, result, fn
      %{kind: :exists} = agg, result ->
        {:ok, filtered} =
          case agg do
            %{query: %{filter: filter}} when not is_nil(filter) ->
              AshSql.Filter.filter(query, filter, resource)

            _ ->
              {:ok, query}
          end

        filtered =
          if filtered.distinct || filtered.limit do
            filtered =
              filtered
              |> Ecto.Query.exclude(:select)
              |> Ecto.Query.exclude(:order_by)
              |> Map.put(:windows, [])

            from(row in subquery(filtered), as: ^query.__ash_bindings__.root_binding, select: %{})
          else
            filtered
            |> Ecto.Query.exclude(:select)
            |> Ecto.Query.exclude(:order_by)
            |> Map.put(:windows, [])
            |> Ecto.Query.select(%{})
          end

        repo = AshSql.dynamic_repo(resource, implementation, filtered)

        Map.put(
          result || %{},
          agg.name,
          repo.exists?(filtered, AshSql.repo_opts(repo, implementation, nil, nil, resource))
        )

      agg, result ->
        {:ok, filtered} =
          case agg do
            %{query: %{filter: filter}} when not is_nil(filter) ->
              AshSql.Filter.filter(query, filter, resource)

            _ ->
              {:ok, query}
          end

        filtered =
          if filtered.distinct do
            in_query = filtered |> Ecto.Query.exclude(:distinct) |> Ecto.Query.exclude(:select)

            dynamic =
              Enum.reduce(Ash.Resource.Info.primary_key(resource), nil, fn key, dynamic ->
                if dynamic do
                  Ecto.Query.dynamic(
                    [row],
                    ^dynamic and
                      field(parent_as(^query.__ash_bindings__.root_binding), ^key) ==
                        field(row, ^key)
                  )
                else
                  Ecto.Query.dynamic(
                    [row],
                    field(parent_as(^query.__ash_bindings__.root_binding), ^key) ==
                      field(row, ^key)
                  )
                end
              end)

            in_query =
              from(row in in_query, where: ^dynamic)

            in_query = Ecto.Query.exclude(in_query, :distinct)

            # `query.from.source` is only `{table, schema}`, so rebuilding from it
            # drops the tenant schema prefix (`strategy(:context)` multitenancy).
            # Restore it so the outer query reads the same schema as the original.
            from(row in query.from.source,
              as: ^query.__ash_bindings__.root_binding,
              where: exists(in_query)
            )
            |> Map.put(:prefix, query.prefix)
            |> Map.update!(:from, &Map.put(&1, :prefix, query.from.prefix))
          else
            filtered
          end

        filtered =
          if filtered.limit do
            filtered =
              filtered
              |> Ecto.Query.exclude(:select)
              |> Ecto.Query.exclude(:order_by)
              |> Map.put(:windows, [])

            from(row in subquery(filtered), as: ^query.__ash_bindings__.root_binding, select: %{})
          else
            filtered
            |> Ecto.Query.exclude(:select)
            |> Ecto.Query.exclude(:order_by)
            |> Map.put(:windows, [])
            |> Ecto.Query.select(%{})
          end

        first_relationship =
          Ash.Resource.Info.relationship(resource, agg.relationship_path |> Enum.at(0))

        filtered = AshSql.Bindings.default_bindings(filtered, resource, implementation)

        ref =
          AshSql.Aggregate.aggregate_field_ref(
            agg,
            Ash.Resource.Info.related(resource, agg.relationship_path),
            agg.relationship_path,
            filtered,
            first_relationship
          )

        {:ok, filtered} = AshSql.Aggregate.add_field_dependencies(filtered, ref, resource)

        query =
          AshSql.Aggregate.add_subquery_aggregate_select(
            filtered,
            agg.relationship_path |> Enum.drop(1),
            %{agg | query: %{agg.query | filter: nil}},
            resource,
            true,
            first_relationship
          )

        repo = AshSql.dynamic_repo(resource, implementation, query)

        Map.merge(
          result || %{},
          repo.one(
            query,
            AshSql.repo_opts(repo, query.__ash_bindings__.sql_behaviour, nil, nil, resource)
          )
        )
    end)
  end
end
