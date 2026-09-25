# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.Aggregate.Grouped do
  @moduledoc false

  import Ecto.Query, only: [from: 2]

  @scalar_aggregate_kinds [:count, :sum, :avg, :max, :min, :exists]
  @window_aggregate_kinds [:first, :list]
  @supported_aggregate_kinds @scalar_aggregate_kinds ++ @window_aggregate_kinds ++ [:custom]
  @window_value_field :__ash_sql_grouped_aggregate_value__
  @window_row_number_field :__ash_sql_grouped_aggregate_row_number__
  @window_count_field :__ash_sql_grouped_aggregate_count__
  @relationship_row_number_field :__ash_sql_grouped_relationship_row_number__
  @unrelated_join_field :__ash_sql_grouped_unrelated_join__

  def add_aggregates(query, aggregates, resource, select?, source_binding, root_data) do
    path = AshSql.Aggregate.Common.attachment_path(root_data)

    do_add_aggregates(query, aggregates, resource, select?, %{
      source_binding: source_binding,
      path: path
    })
  end

  def relationship_filter_uses_parent?(%{filter: nil}), do: false

  def relationship_filter_uses_parent?(%{filter: filter}) do
    filter_uses_parent?(filter)
  end

  defp do_add_aggregates(query, [], _resource, _select?, _context), do: {:ok, query}

  defp do_add_aggregates(query, aggregates, resource, select?, context) do
    primary_key = Ash.Resource.Info.primary_key(resource)

    cond do
      primary_key == [] ->
        {:error, "AshSql cannot load aggregates on resources with no primary key"}

      Enum.any?(aggregates, &(not supported?(&1))) ->
        {:error,
         "AshSql only supports loading related count, sum, avg, min, max, exists, first, list and custom aggregates"}

      true ->
        {already_added, remaining} =
          aggregates
          |> Enum.uniq_by(& &1.name)
          |> Enum.split_with(&already_added?(&1, query.__ash_bindings__, context.path))

        already_added_dynamics =
          if select? do
            Enum.map(
              already_added,
              &existing_aggregate_dynamic(&1, query.__ash_bindings__, context.path)
            )
          else
            []
          end

        remaining
        |> Enum.group_by(&aggregate_group_key/1)
        |> Enum.reduce_while({:ok, query, already_added_dynamics}, fn {relationship_path,
                                                                       aggregates},
                                                                      {:ok, query, dynamics} ->
          case add_aggregate_group(
                 query,
                 resource,
                 aggregate_relationship_path(relationship_path),
                 aggregates,
                 context
               ) do
            {:ok, query, new_dynamics} ->
              {:cont, {:ok, query, new_dynamics ++ dynamics}}

            {:error, error} ->
              {:halt, {:error, error}}
          end
        end)
        |> case do
          {:ok, query, dynamics} ->
            if select? do
              {:ok, select_aggregates(query, dynamics)}
            else
              {:ok, query}
            end

          {:error, error} ->
            {:error, error}
        end
    end
  end

  defp supported?(%{name: name}) when not is_atom(name), do: false

  defp supported?(%{kind: kind, related?: false}) when kind in @supported_aggregate_kinds do
    true
  end

  defp supported?(%{kind: kind, related?: true, relationship_path: path})
       when kind in @supported_aggregate_kinds do
    match?([_ | _], path)
  end

  defp supported?(_), do: false

  defp aggregate_group_key(aggregate) do
    read_action = (aggregate.query.action && aggregate.query.action.name) || aggregate.read_action

    preparation_key =
      {read_action, aggregate.query.arguments, aggregate.query.context, aggregate.query.tenant,
       aggregate.query.domain}

    relationship_key =
      case aggregate do
        %{related?: false, query: %{resource: resource}} -> {:unrelated, resource}
        %{relationship_path: relationship_path} -> {:related, relationship_path}
      end

    {relationship_key, preparation_key, aggregate.join_filters || %{},
     aggregate_filter_group_key(aggregate), aggregate_kind_group_key(aggregate)}
  end

  defp aggregate_relationship_path(
         {{:related, relationship_path}, _preparation, _join_filters, _aggregate_filter_group,
          _kind_group}
       ) do
    relationship_path
  end

  defp aggregate_relationship_path(
         {{:unrelated, _resource}, _preparation, _join_filters, _aggregate_filter_group,
          _kind_group}
       ) do
    []
  end

  defp aggregate_kind_group_key(%{kind: kind, name: name}) when kind in @window_aggregate_kinds do
    {kind, name}
  end

  defp aggregate_kind_group_key(_aggregate), do: :shared

  defp aggregate_filter_group_key(aggregate) do
    if aggregate_filter_uses_relationships?(aggregate) do
      {:filter, aggregate.name}
    else
      :shared
    end
  end

  defp already_added?(aggregate, bindings, path) do
    Enum.any?(bindings.bindings, fn
      {_binding, %{type: :aggregate, aggregates: aggregates, path: ^path}} ->
        aggregate.name in Enum.map(aggregates, & &1.name)

      _binding ->
        false
    end)
  end

  defp existing_aggregate_dynamic(aggregate, bindings, path) do
    {binding, _aggregate_binding} =
      Enum.find(bindings.bindings, fn
        {_binding, %{type: :aggregate, aggregates: aggregates, path: ^path}} ->
          aggregate.name in Enum.map(aggregates, & &1.name)

        _binding ->
          false
      end)

    {aggregate.load, aggregate.name,
     loaded_aggregate_dynamic(aggregate, binding, bindings.sql_behaviour)}
  end

  defp add_aggregate_group(query, _resource, [], aggregates, context) do
    if Enum.all?(aggregates, &(&1.related? == false)) do
      do_add_unrelated_aggregate_group(query, aggregates, context)
    else
      {:error, "AshSql only supports loading unrelated aggregates with no relationship path"}
    end
  end

  defp add_aggregate_group(query, resource, relationship_path, aggregates, context) do
    with {:ok, relationships} <- relationships(resource, relationship_path),
         :ok <- validate_relationships(resource, relationship_path, relationships, aggregates) do
      do_add_aggregate_group(query, relationships, aggregates, context)
    end
  end

  defp relationships(resource, relationship_path) do
    {:ok, AshSql.Join.relationship_path_to_relationships(resource, relationship_path)}
  end

  defp validate_relationships(resource, relationship_path, relationships, aggregates) do
    cond do
      Enum.any?(relationships, &match?(%{manual: {_, _}}, &1)) ->
        {:error, "AshSql does not support loading aggregates over manual relationships"}

      Enum.any?(relationships, &Map.get(&1, :no_attributes?, false)) ->
        {:error, "AshSql does not support loading aggregates over no_attributes? relationships"}

      Enum.any?(relationships, &relationship_filter_uses_parent?/1) ->
        {:error,
         "AshSql does not support loading aggregates over relationships with parent-dependent filters"}

      Enum.any?(relationships, &join_relationship_filter_uses_parent?/1) ->
        {:error,
         "AshSql does not support loading aggregates over many_to_many relationships with parent-dependent join filters"}

      Enum.any?(relationships, &limited_many_to_many?/1) ->
        {:error,
         "AshSql does not support loading aggregates over limited many_to_many relationships"}

      unsupported_multi_hop_many_to_many?(relationships, aggregates) ->
        {:error,
         "AshSql does not support loading aggregates over multi-hop paths that include many_to_many relationships"}

      Enum.empty?(relationships) ->
        {:error,
         "AshSql only supports loading aggregates over a relationship path from #{inspect(resource)}, got: #{inspect(relationship_path)}"}

      true ->
        :ok
    end
  end

  defp limited_many_to_many?(%{type: :many_to_many} = relationship) do
    limited_relationship?(relationship)
  end

  defp limited_many_to_many?(_relationship), do: false

  defp unsupported_multi_hop_many_to_many?(relationships, aggregates) do
    length(relationships) > 1 &&
      Enum.any?(relationships, &(&1.type == :many_to_many)) &&
      !supported_multi_hop_many_to_many?(relationships, aggregates)
  end

  defp supported_multi_hop_many_to_many?(relationships, aggregates) do
    List.last(relationships).type == :many_to_many &&
      Enum.count(relationships, &(&1.type == :many_to_many)) == 1 &&
      Enum.all?(aggregates, &(&1.kind in @scalar_aggregate_kinds))
  end

  defp do_add_unrelated_aggregate_group(query, aggregates, context) do
    binding = query.__ash_bindings__.current

    with :ok <- validate_aggregate_filters(aggregates),
         {:ok, aggregate_query} <- unrelated_aggregate_query(query, aggregates, binding) do
      aggregate_query = Ecto.Query.subquery(aggregate_query)

      query =
        from(_row in query,
          left_join: aggregate in ^aggregate_query,
          as: ^binding,
          on: true
        )

      query =
        AshSql.Bindings.add_binding(query, %{
          type: :aggregate,
          path: context.path,
          aggregates: aggregates
        })

      dynamics =
        Enum.map(aggregates, fn aggregate ->
          {aggregate.load, aggregate.name,
           loaded_aggregate_dynamic(aggregate, binding, query.__ash_bindings__.sql_behaviour)}
        end)

      {:ok, query, dynamics}
    end
  end

  defp do_add_aggregate_group(
         query,
         [first_relationship | _] = relationships,
         aggregates,
         context
       ) do
    binding = query.__ash_bindings__.current
    input_query = aggregate_input_query(query, hd(aggregates))

    with :ok <- validate_aggregate_filters(aggregates),
         {:ok, aggregate_query} <-
           aggregate_query(input_query, relationships, aggregates, binding) do
      aggregate_query = Ecto.Query.subquery(aggregate_query)
      source_binding = context.source_binding

      query =
        from(_row in query,
          left_join: aggregate in ^aggregate_query,
          as: ^binding,
          on:
            field(as(^source_binding), ^first_relationship.source_attribute) ==
              field(aggregate, ^aggregate_join_attribute(first_relationship))
        )

      query =
        AshSql.Bindings.add_binding(query, %{
          type: :aggregate,
          path: context.path,
          aggregates: aggregates
        })

      dynamics =
        Enum.map(aggregates, fn aggregate ->
          {aggregate.load, aggregate.name,
           loaded_aggregate_dynamic(aggregate, binding, query.__ash_bindings__.sql_behaviour)}
        end)

      {:ok, query, dynamics}
    end
  end

  defp aggregate_input_query(query, aggregate) do
    shared_context = aggregate.query.context[:shared] || %{}

    shared_context =
      if AshSql.Join.context_multitenancy(aggregate.query) == :bypass_all do
        # Ash may clear the shared wrapper after merging it into private context.
        Ash.Helpers.deep_merge_maps(shared_context, %{private: %{multitenancy: :bypass_all}})
      else
        shared_context
      end

    # Shared aggregate context applies along the whole path, without changing
    # the parent query or the context used to build sibling aggregates.
    update_in(query.__ash_bindings__.context, fn context ->
      context
      |> Ash.Helpers.deep_merge_maps(%{shared: shared_context})
      |> Ash.Helpers.deep_merge_maps(shared_context)
    end)
  end

  defp aggregate_query(parent_query, [relationship], [%{kind: kind} = aggregate], binding)
       when kind in @window_aggregate_kinds do
    case relationship do
      %{type: :many_to_many} ->
        many_to_many_window_aggregate_query(parent_query, relationship, aggregate, binding)

      relationship ->
        related_window_aggregate_query(parent_query, relationship, aggregate, binding)
    end
  end

  defp aggregate_query(
         parent_query,
         [_ | _] = relationships,
         [%{kind: kind} = aggregate],
         binding
       )
       when kind in @window_aggregate_kinds do
    multi_hop_window_aggregate_query(parent_query, relationships, aggregate, binding)
  end

  defp aggregate_query(parent_query, [relationship], aggregates, binding) do
    case relationship do
      %{type: :many_to_many} ->
        many_to_many_aggregate_query(parent_query, relationship, aggregates, binding)

      relationship ->
        related_aggregate_query(parent_query, relationship, aggregates, binding)
    end
  end

  defp aggregate_query(parent_query, relationships, aggregates, binding) do
    case List.last(relationships) do
      %{type: :many_to_many} ->
        multi_hop_many_to_many_aggregate_query(parent_query, relationships, aggregates, binding)

      _relationship ->
        multi_hop_aggregate_query(parent_query, relationships, aggregates, binding)
    end
  end

  defp unrelated_aggregate_query(parent_query, [%{kind: kind} = aggregate], binding)
       when kind in @window_aggregate_kinds do
    unrelated_window_aggregate_query(parent_query, aggregate, binding)
  end

  defp unrelated_aggregate_query(parent_query, aggregates, binding) do
    with {:ok, query} <- unrelated_query(parent_query, hd(aggregates), binding, filter?: false) do
      root_binding = query.__ash_bindings__.root_binding
      relationship = %{destination: hd(aggregates).query.resource}

      query = from(row in query, select: %{})

      Enum.reduce_while(aggregates, {:ok, query}, fn aggregate, {:ok, query} ->
        case aggregate_dynamic(query, relationship, aggregate, root_binding) do
          {:ok, query, dynamic} ->
            {:cont, {:ok, Ecto.Query.select_merge(query, ^%{aggregate.name => dynamic})}}

          {:error, error} ->
            {:halt, {:error, error}}
        end
      end)
    end
  end

  defp unrelated_window_aggregate_query(parent_query, aggregate, binding) do
    with {:ok, query} <- unrelated_query(parent_query, aggregate, binding, filter?: true) do
      root_binding = query.__ash_bindings__.root_binding

      window_aggregate_query(
        query,
        aggregate,
        @unrelated_join_field,
        nil,
        root_binding,
        %{sort: []}
      )
    end
  end

  defp related_window_aggregate_query(parent_query, relationship, aggregate, binding) do
    with {:ok, query} <-
           related_window_query(parent_query, relationship, aggregate, binding, [
             relationship.name
           ]) do
      root_binding = query.__ash_bindings__.root_binding

      window_aggregate_query(
        query,
        aggregate,
        relationship.destination_attribute,
        root_binding,
        root_binding,
        relationship
      )
    end
  end

  defp many_to_many_window_aggregate_query(parent_query, relationship, aggregate, binding) do
    with {:ok, query} <-
           related_window_query(parent_query, relationship, aggregate, binding, [
             relationship.name
           ]) do
      through_binding = query.__ash_bindings__.current

      with {:ok, through_query} <- through_query(parent_query, relationship, through_binding) do
        root_binding = query.__ash_bindings__.root_binding
        through_query = Ecto.Query.subquery(through_query)

        query =
          from(row in query,
            join: through in ^through_query,
            as: ^through_binding,
            on:
              field(through, ^relationship.destination_attribute_on_join_resource) ==
                field(as(^root_binding), ^relationship.destination_attribute)
          )
          |> AshSql.Bindings.add_binding(%{
            type: :through,
            relationship: relationship
          })

        window_aggregate_query(
          query,
          aggregate,
          relationship.source_attribute_on_join_resource,
          through_binding,
          root_binding,
          relationship
        )
      end
    end
  end

  defp multi_hop_window_aggregate_query(parent_query, relationships, aggregate, binding) do
    final_relationship = List.last(relationships)
    relationship_path = Enum.map(relationships, & &1.name)

    with {:ok, query} <-
           related_window_query(
             parent_query,
             final_relationship,
             aggregate,
             binding,
             relationship_path
           ),
         {:ok, query, first_related_binding} <-
           join_intermediate_relationships(parent_query, query, relationships, aggregate) do
      first_relationship = hd(relationships)
      root_binding = query.__ash_bindings__.root_binding

      window_aggregate_query(
        query,
        aggregate,
        first_relationship.destination_attribute,
        first_related_binding,
        root_binding,
        final_relationship
      )
    end
  end

  defp related_aggregate_query(parent_query, relationship, aggregates, binding) do
    with {:ok, query} <-
           related_query(parent_query, relationship, hd(aggregates), binding, [relationship.name]) do
      root_binding = query.__ash_bindings__.root_binding

      query =
        from(row in query,
          group_by: field(as(^root_binding), ^relationship.destination_attribute),
          select: %{
            ^relationship.destination_attribute =>
              field(as(^root_binding), ^relationship.destination_attribute)
          }
        )

      Enum.reduce_while(aggregates, {:ok, query}, fn aggregate, {:ok, query} ->
        case aggregate_dynamic(query, relationship, aggregate, root_binding) do
          {:ok, query, dynamic} ->
            {:cont, {:ok, Ecto.Query.select_merge(query, ^%{aggregate.name => dynamic})}}

          {:error, error} ->
            {:halt, {:error, error}}
        end
      end)
    end
  end

  defp many_to_many_aggregate_query(parent_query, relationship, aggregates, binding) do
    with {:ok, query} <-
           related_query(parent_query, relationship, hd(aggregates), binding, [relationship.name]) do
      through_binding = query.__ash_bindings__.current

      with {:ok, through_query} <- through_query(parent_query, relationship, through_binding) do
        root_binding = query.__ash_bindings__.root_binding
        ash_bindings = query.__ash_bindings__
        through_query = Ecto.Query.subquery(through_query)

        query =
          from(row in query,
            join: through in ^through_query,
            as: ^through_binding,
            on:
              field(through, ^relationship.destination_attribute_on_join_resource) ==
                field(as(^root_binding), ^relationship.destination_attribute),
            group_by: field(through, ^relationship.source_attribute_on_join_resource),
            select: %{
              ^relationship.source_attribute_on_join_resource =>
                field(through, ^relationship.source_attribute_on_join_resource)
            }
          )
          |> Map.put(:__ash_bindings__, ash_bindings)
          |> AshSql.Bindings.add_binding(%{
            type: :through,
            relationship: relationship
          })

        Enum.reduce_while(aggregates, {:ok, query}, fn aggregate, {:ok, query} ->
          case aggregate_dynamic(query, relationship, aggregate, root_binding) do
            {:ok, query, dynamic} ->
              {:cont, {:ok, Ecto.Query.select_merge(query, ^%{aggregate.name => dynamic})}}

            {:error, error} ->
              {:halt, {:error, error}}
          end
        end)
      end
    end
  end

  defp multi_hop_many_to_many_aggregate_query(parent_query, relationships, aggregates, binding) do
    final_relationship = List.last(relationships)
    relationship_path = Enum.map(relationships, & &1.name)

    with {:ok, query} <-
           related_query(
             parent_query,
             final_relationship,
             hd(aggregates),
             binding,
             relationship_path
           ) do
      through_binding = query.__ash_bindings__.current

      with {:ok, through_query} <-
             through_query(parent_query, final_relationship, through_binding) do
        root_binding = query.__ash_bindings__.root_binding
        through_query = Ecto.Query.subquery(through_query)

        query =
          from(row in query,
            join: through in ^through_query,
            as: ^through_binding,
            on:
              field(through, ^final_relationship.destination_attribute_on_join_resource) ==
                field(as(^root_binding), ^final_relationship.destination_attribute)
          )
          |> AshSql.Bindings.add_binding(%{
            type: :through,
            relationship: final_relationship
          })

        with {:ok, query, first_related_binding} <-
               join_intermediate_relationships(parent_query, query, relationships, hd(aggregates),
                 current_binding: through_binding
               ) do
          first_relationship = hd(relationships)

          query =
            from(row in query,
              group_by:
                field(as(^first_related_binding), ^first_relationship.destination_attribute),
              select: %{
                ^first_relationship.destination_attribute =>
                  field(as(^first_related_binding), ^first_relationship.destination_attribute)
              }
            )

          root_binding = query.__ash_bindings__.root_binding

          Enum.reduce_while(aggregates, {:ok, query}, fn aggregate, {:ok, query} ->
            case aggregate_dynamic(query, final_relationship, aggregate, root_binding) do
              {:ok, query, dynamic} ->
                {:cont, {:ok, Ecto.Query.select_merge(query, ^%{aggregate.name => dynamic})}}

              {:error, error} ->
                {:halt, {:error, error}}
            end
          end)
        end
      end
    end
  end

  defp multi_hop_aggregate_query(parent_query, relationships, aggregates, binding) do
    final_relationship = List.last(relationships)
    relationship_path = Enum.map(relationships, & &1.name)

    with {:ok, query} <-
           related_query(
             parent_query,
             final_relationship,
             hd(aggregates),
             binding,
             relationship_path
           ),
         {:ok, query, first_related_binding} <-
           join_intermediate_relationships(parent_query, query, relationships, hd(aggregates)) do
      first_relationship = hd(relationships)

      query =
        from(row in query,
          group_by: field(as(^first_related_binding), ^first_relationship.destination_attribute),
          select: %{
            ^first_relationship.destination_attribute =>
              field(as(^first_related_binding), ^first_relationship.destination_attribute)
          }
        )

      root_binding = query.__ash_bindings__.root_binding

      Enum.reduce_while(aggregates, {:ok, query}, fn aggregate, {:ok, query} ->
        case aggregate_dynamic(query, final_relationship, aggregate, root_binding) do
          {:ok, query, dynamic} ->
            {:cont, {:ok, Ecto.Query.select_merge(query, ^%{aggregate.name => dynamic})}}

          {:error, error} ->
            {:halt, {:error, error}}
        end
      end)
    end
  end

  defp join_intermediate_relationships(parent_query, query, relationships, aggregate, opts \\ []) do
    current_binding = Keyword.get(opts, :current_binding, query.__ash_bindings__.root_binding)

    relationships
    |> Enum.zip(tl(relationships))
    |> Enum.with_index()
    |> Enum.reverse()
    |> Enum.reduce_while(
      {:ok, query, current_binding, query.__ash_bindings__.current, nil},
      fn {{relationship, next_relationship}, index},
         {:ok, query, current_binding, next_binding, _first_related_binding} ->
        path =
          relationships
          |> Enum.take(index + 1)
          |> Enum.map(& &1.name)

        case intermediate_query(parent_query, relationship, next_binding, aggregate, path) do
          {:ok, related_query} ->
            related_query = Ecto.Query.subquery(related_query)

            on = intermediate_join_on(next_relationship, next_binding, current_binding)

            query =
              from(row in query,
                join: related in ^related_query,
                as: ^next_binding,
                on: ^on
              )

            {:cont, {:ok, query, next_binding, next_binding + 1, next_binding}}

          {:error, error} ->
            {:halt, {:error, error}}
        end
      end
    )
    |> case do
      {:ok, query, _current_binding, _next_binding, first_related_binding}
      when not is_nil(first_related_binding) ->
        {:ok, query, first_related_binding}

      {:ok, _query, _current_binding, _next_binding, nil} ->
        {:error, "AshSql could not build multi-hop aggregate joins"}

      {:error, error} ->
        {:error, error}
    end
  end

  defp intermediate_join_on(
         %{type: :many_to_many} = next_relationship,
         related_binding,
         current_binding
       ) do
    Ecto.Query.dynamic(
      field(as(^related_binding), ^next_relationship.source_attribute) ==
        field(as(^current_binding), ^next_relationship.source_attribute_on_join_resource)
    )
  end

  defp intermediate_join_on(next_relationship, related_binding, current_binding) do
    Ecto.Query.dynamic(
      field(as(^related_binding), ^next_relationship.source_attribute) ==
        field(as(^current_binding), ^next_relationship.destination_attribute)
    )
  end

  defp related_query(parent_query, relationship, aggregate, binding, relationship_path) do
    read_action = (aggregate.query.action && aggregate.query.action.name) || aggregate.read_action

    arguments =
      if relationship.read_action == read_action do
        Map.merge(relationship.read_action_arguments || %{}, aggregate.query.arguments)
      else
        aggregate.query.arguments
      end

    relationship = %{relationship | read_action: read_action, read_action_arguments: arguments}

    with {:ok, query} <-
           prepared_relationship_query(parent_query, relationship, binding,
             source_query: aggregate.query,
             apply_filter: join_filter(aggregate, relationship_path)
           ) do
      limit_relationship_rows(query, relationship)
    end
  end

  defp related_window_query(parent_query, relationship, aggregate, binding, relationship_path) do
    with {:ok, query} <-
           related_query(parent_query, relationship, aggregate, binding, relationship_path) do
      # Aggregate filters apply to the bounded relationship, not to the rows
      # used to choose that relationship's limit/offset window.
      case aggregate.query.filter do
        nil -> {:ok, query}
        %{expression: nil} -> {:ok, query}
        filter -> AshSql.Filter.filter(query, filter, relationship.destination)
      end
    end
  end

  defp unrelated_query(parent_query, aggregate, binding, opts) do
    unset =
      if Keyword.fetch!(opts, :filter?) do
        [:sort, :distinct, :select, :limit, :offset]
      else
        [:filter, :sort, :distinct, :select, :limit, :offset]
      end

    aggregate.query
    |> Ash.Query.unset(unset)
    |> Ash.Query.set_context(%{
      data_layer: %{
        start_bindings_at: binding,
        parent_bindings: parent_query.__ash_bindings__
      }
    })
    |> Ash.Query.data_layer_query(run_return_query?: false)
    |> case do
      {:ok, query} ->
        {:ok,
         query
         |> Ecto.Query.exclude(:select)
         |> Ecto.Query.exclude(:order_by)}

      {:error, error} ->
        {:error, error}
    end
  end

  defp limit_relationship_rows(query, relationship) do
    if limited_relationship?(relationship) do
      root_binding = query.__ash_bindings__.root_binding

      with {:ok, order_by, query} <-
             relationship_limit_order_by(query, relationship, root_binding) do
        partition_by =
          Ecto.Query.dynamic(field(as(^root_binding), ^relationship.destination_attribute))

        ranked_query =
          from(row in query,
            windows: [
              ash_sql_grouped_relationship_window: [
                partition_by: ^partition_by,
                order_by: ^order_by
              ]
            ],
            select_merge: %{
              @relationship_row_number_field =>
                over(row_number(), :ash_sql_grouped_relationship_window)
            }
          )

        filtered_query =
          ranked_query
          |> Ecto.Query.subquery()
          |> relationship_row_filter(relationship, root_binding)

        {:ok, reset_relationship_bindings(filtered_query, query.__ash_bindings__)}
      end
    else
      {:ok, query}
    end
  end

  defp limited_relationship?(relationship) do
    is_integer(Map.get(relationship, :limit)) or
      (Map.get(relationship, :offset) || 0) > 0
  end

  defp relationship_limit_order_by(query, relationship, root_binding) do
    case List.wrap(Map.get(relationship, :sort)) do
      [] ->
        {:ok, [], query}

      sort ->
        AshSql.Sort.sort(
          query,
          sort,
          relationship.destination,
          [],
          root_binding,
          :expressions
        )
    end
  end

  defp relationship_row_filter(query, relationship, root_binding) do
    offset = Map.get(relationship, :offset) || 0
    row_number_field = @relationship_row_number_field

    case Map.get(relationship, :limit) do
      limit when is_integer(limit) ->
        from(row in query,
          as: ^root_binding,
          where:
            field(row, ^row_number_field) > ^offset and
              field(row, ^row_number_field) <= ^(offset + limit)
        )

      _limit ->
        from(row in query,
          as: ^root_binding,
          where: field(row, ^row_number_field) > ^offset
        )
    end
  end

  defp reset_relationship_bindings(query, bindings) do
    root_binding = bindings.root_binding
    root = Map.fetch!(bindings.bindings, root_binding)

    Map.put(query, :__ash_bindings__, %{
      bindings
      | bindings: %{root_binding => root},
        current: root_binding + 1,
        expression_accumulator: %AshSql.Expr.ExprInfo{}
    })
  end

  defp intermediate_query(parent_query, relationship, binding, aggregate, relationship_path) do
    with {:ok, query} <-
           prepared_relationship_query(parent_query, relationship, binding,
             apply_filter: join_filter(aggregate, relationship_path)
           ) do
      limit_relationship_rows(query, relationship)
    end
  end

  defp through_query(parent_query, relationship, binding) do
    join_relationship =
      Ash.Resource.Info.relationship(relationship.source, relationship.join_relationship)

    prepared_relationship_query(parent_query, join_relationship, binding)
  end

  defp prepared_relationship_query(parent_query, relationship, binding, opts \\ []) do
    relationship
    |> AshSql.Join.related_ash_query(parent_query, Keyword.put(opts, :start_bindings_at, binding))
    |> Ash.Query.unset(:sort)
    |> Ash.Query.set_context(%{
      data_layer: %{
        start_bindings_at: binding,
        parent_bindings: parent_query.__ash_bindings__
      }
    })
    |> Ash.Query.data_layer_query(run_return_query?: false)
    |> case do
      {:ok, query} ->
        {:ok,
         query
         |> Ecto.Query.exclude(:select)
         |> Ecto.Query.exclude(:order_by)}

      {:error, error} ->
        {:error, error}
    end
  end

  defp aggregate_join_attribute(%{type: :many_to_many} = relationship) do
    relationship.source_attribute_on_join_resource
  end

  defp aggregate_join_attribute(relationship), do: relationship.destination_attribute

  defp join_relationship_filter_uses_parent?(%{type: :many_to_many} = relationship) do
    relationship.source
    |> Ash.Resource.Info.relationship(relationship.join_relationship)
    |> relationship_filter_uses_parent?()
  end

  defp join_relationship_filter_uses_parent?(_relationship), do: false

  defp join_filter(%{join_filters: join_filters}, relationship_path)
       when is_map(join_filters) do
    Map.get(join_filters, relationship_path)
  end

  defp join_filter(_aggregate, _relationship_path), do: nil

  defp validate_aggregate_filters(aggregates) do
    cond do
      Enum.any?(aggregates, &aggregate_filter_uses_parent?/1) ->
        {:error,
         "AshSql does not support loading aggregates with parent-dependent aggregate filters"}

      Enum.any?(aggregates, &aggregate_filter_uses_parent_dependent_relationship?/1) ->
        {:error,
         "AshSql does not support loading aggregates with filters that reference relationships with parent-dependent filters"}

      Enum.any?(aggregates, &aggregate_filter_uses_aggregates?/1) ->
        {:error,
         "AshSql does not support loading aggregates with aggregate filters that reference other aggregates"}

      Enum.any?(aggregates, &unsupported_to_many_aggregate_filter?/1) ->
        {:error,
         "AshSql does not support loading sum, avg, list, custom, or field-based count aggregates with filters that reference to-many relationships"}

      Enum.any?(aggregates, &join_filters_use_parent?/1) ->
        {:error, "AshSql does not support loading aggregates with parent-dependent join filters"}

      true ->
        :ok
    end
  end

  defp aggregate_filter_uses_parent?(%{query: %{filter: filter}}) do
    filter_uses_parent?(filter)
  end

  defp aggregate_filter_uses_parent_dependent_relationship?(%{
         query: %{filter: filter, resource: resource}
       }) do
    filter
    |> aggregate_filter_relationship_paths()
    |> Enum.any?(&parent_dependent_relationship_path?(resource, &1))
  end

  defp aggregate_filter_uses_parent_dependent_relationship?(_aggregate), do: false

  defp aggregate_filter_uses_relationships?(%{query: %{filter: filter}}) do
    filter
    |> aggregate_filter_relationship_paths()
    |> Enum.any?()
  end

  defp aggregate_filter_uses_relationships?(_aggregate), do: false

  defp aggregate_filter_uses_aggregates?(%{query: %{filter: filter}}) when not is_nil(filter) do
    filter
    |> Ash.Filter.used_aggregates([])
    |> Enum.any?()
  end

  defp aggregate_filter_uses_aggregates?(_aggregate), do: false

  defp unsupported_to_many_aggregate_filter?(%{kind: :count, field: field} = aggregate)
       when not is_nil(field) do
    aggregate_filter_references_to_many_relationship?(aggregate) && !aggregate.uniq?
  end

  defp unsupported_to_many_aggregate_filter?(%{kind: kind} = aggregate)
       when kind in [:sum, :avg, :list, :custom] do
    aggregate_filter_references_to_many_relationship?(aggregate)
  end

  defp unsupported_to_many_aggregate_filter?(_aggregate), do: false

  defp aggregate_filter_references_to_many_relationship?(%{
         query: %{filter: filter, resource: resource}
       }) do
    filter
    |> aggregate_filter_relationship_paths()
    |> Enum.any?(&to_many_relationship_path?(resource, &1))
  end

  defp aggregate_filter_references_to_many_relationship?(_aggregate), do: false

  defp aggregate_filter_relationship_paths(nil), do: []

  defp aggregate_filter_relationship_paths(%{expression: nil}), do: []

  defp aggregate_filter_relationship_paths(filter) do
    Ash.Filter.relationship_paths(filter)
  end

  defp parent_dependent_relationship_path?(_resource, []), do: false

  defp parent_dependent_relationship_path?(resource, [relationship_name | rest]) do
    case Ash.Resource.Info.relationship(resource, relationship_name) do
      nil ->
        false

      relationship ->
        relationship_filter_uses_parent?(relationship) ||
          parent_dependent_relationship_path?(relationship.destination, rest)
    end
  end

  defp to_many_relationship_path?(_resource, []), do: false

  defp to_many_relationship_path?(resource, [relationship_name | rest]) do
    case Ash.Resource.Info.relationship(resource, relationship_name) do
      %{cardinality: :many} ->
        true

      nil ->
        false

      relationship ->
        to_many_relationship_path?(relationship.destination, rest)
    end
  end

  defp join_filters_use_parent?(%{join_filters: join_filters}) when is_map(join_filters) do
    Enum.any?(join_filters, fn {_path, filter} -> filter_uses_parent?(filter) end)
  end

  defp join_filters_use_parent?(_aggregate), do: false

  defp filter_uses_parent?(nil), do: false

  defp filter_uses_parent?(%{expression: nil}), do: false

  defp filter_uses_parent?(filter) do
    Ash.Filter.find(
      filter,
      fn
        %Ash.Query.Parent{} -> true
        %Ash.Query.Call{name: :parent} -> true
        _ -> false
      end,
      true,
      true,
      true
    )
    |> case do
      nil -> false
      _ -> true
    end
  end

  defp window_aggregate_query(
         query,
         aggregate,
         join_attribute,
         partition_binding,
         value_binding,
         relationship
       ) do
    with :ok <- validate_window_aggregate(aggregate),
         {:ok, sort} <- window_aggregate_sort(aggregate, relationship),
         :ok <- validate_window_aggregate_sort(aggregate, sort),
         {:ok, query, value} <-
           AshSql.Aggregate.field_expression(
             query,
             aggregate,
             Map.get(relationship, :destination, aggregate.query.resource)
           ),
         {:ok, order_by, query} <-
           AshSql.Sort.sort(
             query,
             sort,
             Map.get(relationship, :destination, aggregate.query.resource),
             [],
             value_binding,
             :expressions
           ) do
      sql_behaviour = query.__ash_bindings__.sql_behaviour

      with {:ok, aggregate_value} <- window_aggregate_value(sql_behaviour, aggregate) do
        query =
          query
          |> maybe_filter_window_nil_values(aggregate, value)
          |> window_source_query(aggregate, join_attribute, partition_binding, value, order_by)
          |> Ecto.Query.subquery()
          |> window_result_query(aggregate, join_attribute, order_by, aggregate_value)

        {:ok, query}
      end
    end
  end

  defp validate_window_aggregate(%{field: field, kind: kind})
       when kind in @window_aggregate_kinds and not is_nil(field) do
    :ok
  end

  defp validate_window_aggregate(%{name: name, field: field}) do
    {:error,
     "AshSql cannot load first or list aggregate #{inspect(name)} with field #{inspect(field)}"}
  end

  defp validate_window_aggregate_sort(%{kind: :list, uniq?: true, field: field}, sort) do
    if Enum.all?(sort, fn {sort_field, _order} -> sort_field == field end) do
      :ok
    else
      {:error,
       "AshSql only supports uniq list aggregates when sorting by the list aggregate field"}
    end
  end

  defp validate_window_aggregate_sort(_aggregate, _sort), do: :ok

  defp maybe_filter_window_nil_values(query, %{include_nil?: true}, _value), do: query

  defp maybe_filter_window_nil_values(query, _aggregate, value) do
    filter = Ecto.Query.dynamic(not is_nil(^value))
    Ecto.Query.where(query, ^filter)
  end

  defp window_source_query(
         query,
         aggregate,
         join_attribute,
         partition_binding,
         value,
         order_by
       ) do
    sort_selects =
      order_by
      |> Enum.with_index()
      |> Map.new(fn {{_order, expression}, index} ->
        {window_sort_field(index), expression}
      end)

    select =
      Map.merge(
        %{
          join_attribute => window_join_field(partition_binding, join_attribute),
          @window_value_field => value
        },
        sort_selects
      )

    query =
      if aggregate.kind == :list && aggregate.uniq? do
        # This relies on validate_window_aggregate_sort/2 requiring uniq lists to
        # sort by the listed field, so distinct applies to {parent, value}.
        from(row in query, distinct: true)
      else
        query
      end

    from(row in query, select: ^select)
  end

  defp window_result_query(source_query, aggregate, join_attribute, sort, aggregate_value) do
    order_by =
      sort
      |> Enum.with_index()
      |> Enum.map(fn {{order, _expression}, index} ->
        {ecto_sort_order(order), Ecto.Query.dynamic([row], field(row, ^window_sort_field(index)))}
      end)

    partition_by = Ecto.Query.dynamic([row], field(row, ^join_attribute))

    query =
      from(row in source_query,
        windows: [
          ash_sql_grouped_aggregate_window: [
            partition_by: ^partition_by,
            order_by: ^order_by
          ],
          ash_sql_grouped_aggregate_partition_window: [
            partition_by: ^partition_by
          ]
        ],
        select: %{
          ^join_attribute => field(row, ^join_attribute),
          @window_row_number_field => over(row_number(), :ash_sql_grouped_aggregate_window),
          @window_count_field => over(count(), :ash_sql_grouped_aggregate_partition_window)
        }
      )
      |> Ecto.Query.select_merge(^%{aggregate.name => aggregate_value})

    row_filter = window_row_filter(aggregate)

    from(row in Ecto.Query.subquery(query),
      where: ^row_filter,
      select: %{
        ^join_attribute => field(row, ^join_attribute),
        ^aggregate.name => field(row, ^aggregate.name)
      }
    )
  end

  defp window_row_filter(%{kind: :list}) do
    row_number_field = @window_row_number_field
    count_field = @window_count_field

    Ecto.Query.dynamic(
      [row],
      field(row, ^row_number_field) == field(row, ^count_field)
    )
  end

  defp window_row_filter(_aggregate) do
    row_number_field = @window_row_number_field

    Ecto.Query.dynamic([row], field(row, ^row_number_field) == 1)
  end

  defp window_aggregate_value(sql_behaviour, %{kind: :first} = aggregate) do
    value_field = @window_value_field

    value =
      Ecto.Query.dynamic(
        [row],
        over(first_value(field(row, ^value_field)), :ash_sql_grouped_aggregate_window)
      )

    {:ok, maybe_type_dynamic(sql_behaviour, value, aggregate)}
  end

  defp window_aggregate_value(sql_behaviour, %{kind: :list, include_nil?: true} = aggregate) do
    value_field = @window_value_field
    field = Ecto.Query.dynamic([row], field(row, ^value_field))

    grouped_list_aggregate(sql_behaviour, field, aggregate)
  end

  defp window_aggregate_value(sql_behaviour, %{kind: :list} = aggregate) do
    value_field = @window_value_field
    field = Ecto.Query.dynamic([row], field(row, ^value_field))

    grouped_list_aggregate(sql_behaviour, field, aggregate)
  end

  defp grouped_list_aggregate(sql_behaviour, field, aggregate) do
    case sql_behaviour.grouped_list_aggregate(field, aggregate.include_nil?) do
      nil ->
        {:error,
         "#{inspect(sql_behaviour)} does not implement list aggregates for the grouped aggregate strategy"}

      list_aggregate ->
        {:ok, maybe_type_dynamic(sql_behaviour, list_aggregate, aggregate)}
    end
  end

  defp maybe_type_dynamic(_sql_behaviour, dynamic, %{type: nil}), do: dynamic

  defp maybe_type_dynamic(sql_behaviour, dynamic, aggregate) do
    case aggregate_type(sql_behaviour, aggregate) do
      nil -> dynamic
      type -> sql_behaviour.type_expr(dynamic, type)
    end
  end

  defp aggregate_type(sql_behaviour, aggregate) do
    AshSql.Expr.parameterized_type(
      sql_behaviour,
      aggregate.type,
      aggregate.constraints,
      :aggregate
    )
  end

  defp window_aggregate_sort(%{query: %{sort: sort}} = aggregate, relationship) do
    sort =
      cond do
        sort not in [nil, []] ->
          List.wrap(sort)

        relationship.sort not in [nil, []] ->
          List.wrap(relationship.sort)

        true ->
          [{aggregate.field, :asc}]
      end

    sort
    |> Enum.reduce_while({:ok, []}, fn
      {field, order}, {:ok, acc} when is_atom(order) ->
        {:cont, {:ok, [{field, order} | acc]}}

      field, {:ok, acc} when not is_nil(field) ->
        {:cont, {:ok, [{field, :asc} | acc]}}

      sort, _acc ->
        {:halt,
         {:error,
          "AshSql only supports first and list aggregate sorting by related fields, got: #{inspect(sort)}"}}
    end)
    |> case do
      {:ok, sort} -> {:ok, Enum.reverse(sort)}
      {:error, error} -> {:error, error}
    end
  end

  defp window_sort_field(index) do
    :"__ash_sql_grouped_aggregate_sort_#{index}__"
  end

  defp window_join_field(nil, _join_attribute) do
    Ecto.Query.dynamic(fragment("1"))
  end

  defp window_join_field(partition_binding, join_attribute) do
    Ecto.Query.dynamic(field(as(^partition_binding), ^join_attribute))
  end

  defp ecto_sort_order(:asc), do: :asc
  defp ecto_sort_order(:desc), do: :desc
  defp ecto_sort_order(:asc_nils_first), do: :asc_nulls_first
  defp ecto_sort_order(:asc_nils_last), do: :asc_nulls_last
  defp ecto_sort_order(:desc_nils_first), do: :desc_nulls_first
  defp ecto_sort_order(:desc_nils_last), do: :desc_nulls_last
  defp ecto_sort_order(other), do: other

  defp aggregate_dynamic(query, relationship, %{kind: :exists} = aggregate, binding) do
    with {:ok, query, count_dynamic} <- count_dynamic(query, relationship, aggregate, binding),
         {:ok, query, count_dynamic} <-
           maybe_filter_aggregate(query, aggregate, count_dynamic) do
      {:ok, query, Ecto.Query.dynamic(^count_dynamic > 0)}
    end
  end

  defp aggregate_dynamic(query, relationship, %{kind: :count} = aggregate, binding) do
    with {:ok, query, dynamic} <- count_dynamic(query, relationship, aggregate, binding),
         {:ok, query, dynamic} <- maybe_filter_aggregate(query, aggregate, dynamic) do
      {:ok, query, maybe_default_aggregate(query, dynamic, aggregate)}
    end
  end

  defp aggregate_dynamic(query, relationship, aggregate, _binding)
       when aggregate.kind in [:sum, :avg, :max, :min] do
    with {:ok, query, field} <-
           AshSql.Aggregate.field_expression(query, aggregate, relationship.destination),
         dynamic = scalar_aggregate_dynamic(aggregate.kind, field),
         {:ok, query, dynamic} <- maybe_filter_aggregate(query, aggregate, dynamic) do
      {:ok, query, maybe_default_aggregate(query, dynamic, aggregate)}
    end
  end

  defp aggregate_dynamic(query, _relationship, %{kind: :custom} = aggregate, binding) do
    {module, opts} = aggregate.implementation
    dynamic = module.dynamic(opts, binding)

    with {:ok, query, dynamic} <- maybe_filter_aggregate(query, aggregate, dynamic) do
      {:ok, query, maybe_default_aggregate(query, dynamic, aggregate)}
    end
  end

  defp aggregate_dynamic(_query, _relationship, aggregate, _binding) do
    {:error,
     "AshSql cannot load aggregate #{inspect(aggregate.name)} with field #{inspect(aggregate.field)}"}
  end

  defp scalar_aggregate_dynamic(:sum, field), do: Ecto.Query.dynamic(sum(^field))
  defp scalar_aggregate_dynamic(:avg, field), do: Ecto.Query.dynamic(avg(^field))
  defp scalar_aggregate_dynamic(:max, field), do: Ecto.Query.dynamic(max(^field))
  defp scalar_aggregate_dynamic(:min, field), do: Ecto.Query.dynamic(min(^field))

  defp count_dynamic(query, relationship, %{field: nil} = aggregate, binding) do
    if count_distinct?(aggregate) do
      with {:ok, count_field} <- fieldless_count_field(relationship, aggregate) do
        {:ok, query, Ecto.Query.dynamic(count(field(as(^binding), ^count_field), :distinct))}
      end
    else
      {:ok, query, Ecto.Query.dynamic(count())}
    end
  end

  defp count_dynamic(query, relationship, aggregate, _binding) do
    with {:ok, query, field} <-
           AshSql.Aggregate.field_expression(query, aggregate, relationship.destination) do
      dynamic =
        if count_distinct?(aggregate) do
          Ecto.Query.dynamic(count(^field, :distinct))
        else
          Ecto.Query.dynamic(count(^field))
        end

      {:ok, query, dynamic}
    end
  end

  # Distinct records are only identified by a single-column primary key. One
  # column of a composite key, or the join attribute, would undercount.
  defp fieldless_count_field(%{destination: destination}, aggregate) do
    case Ash.Resource.Info.primary_key(destination) do
      [field] ->
        {:ok, field}

      [] ->
        {:error,
         "AshSql grouped aggregate #{inspect(aggregate.name)} requires a single primary key to count distinct records, but #{inspect(destination)} has no primary key"}

      fields ->
        {:error,
         "AshSql grouped aggregate #{inspect(aggregate.name)} requires a single primary key to count distinct records, but #{inspect(destination)} has composite primary key #{inspect(fields)}"}
    end
  end

  # Row multiplication cannot change whether any row matched.
  defp count_distinct?(%{kind: :exists}), do: false
  defp count_distinct?(%{uniq?: true}), do: true

  defp count_distinct?(%{field: nil} = aggregate) do
    aggregate_filter_references_to_many_relationship?(aggregate)
  end

  defp count_distinct?(_aggregate), do: false

  defp maybe_filter_aggregate(query, aggregate, dynamic) do
    case aggregate.query.filter do
      nil ->
        {:ok, query, dynamic}

      %{expression: nil} ->
        {:ok, query, dynamic}

      filter ->
        with {:ok, query} <-
               AshSql.Join.join_all_relationships(
                 query,
                 filter,
                 [],
                 nil,
                 [],
                 nil,
                 true,
                 nil,
                 nil,
                 true
               ) do
          {filter_dynamic, acc} =
            AshSql.Expr.dynamic_expr(
              query,
              filter,
              Map.put(query.__ash_bindings__, :location, :aggregate),
              false
            )

          {:ok, AshSql.Bindings.merge_expr_accumulator(query, acc),
           Ecto.Query.dynamic(filter(^dynamic, ^filter_dynamic))}
        end
    end
  end

  defp maybe_default_aggregate(_query, dynamic, %{default_value: nil}), do: dynamic

  defp maybe_default_aggregate(_query, dynamic, aggregate) do
    Ecto.Query.dynamic(coalesce(^dynamic, ^aggregate.default_value))
  end

  defp loaded_aggregate_dynamic(
         %{kind: :exists, default_value: nil} = aggregate,
         binding,
         _sql_behaviour
       ) do
    aggregate
    |> loaded_aggregate_field(binding)
    |> then(&Ecto.Query.dynamic(coalesce(^&1, false)))
  end

  defp loaded_aggregate_dynamic(%{kind: :list} = aggregate, binding, sql_behaviour) do
    type = aggregate_type(sql_behaviour, aggregate)
    default_value = aggregate.default_value || []

    aggregate
    |> loaded_aggregate_field(binding)
    |> then(fn field ->
      if type do
        default = list_default_expr(default_value, type, sql_behaviour)

        Ecto.Query.dynamic(coalesce(^field, ^default))
        |> sql_behaviour.type_expr(type)
      else
        Ecto.Query.dynamic(coalesce(^field, ^default_value))
      end
    end)
  end

  defp loaded_aggregate_dynamic(aggregate, binding, sql_behaviour) do
    aggregate
    |> loaded_aggregate_field(binding)
    |> maybe_default_loaded_aggregate(aggregate)
    |> then(&maybe_type_dynamic(sql_behaviour, &1, aggregate))
  end

  defp maybe_default_loaded_aggregate(dynamic, %{default_value: nil}), do: dynamic

  defp maybe_default_loaded_aggregate(dynamic, aggregate) do
    Ecto.Query.dynamic(coalesce(^dynamic, ^aggregate.default_value))
  end

  defp list_default_expr(default_value, type, sql_behaviour) when is_list(default_value) do
    default_value = Jason.encode!(default_value)

    Ecto.Query.dynamic(^default_value)
    |> sql_behaviour.type_expr(type)
  end

  defp list_default_expr(default_value, type, sql_behaviour) do
    Ecto.Query.dynamic(^default_value)
    |> sql_behaviour.type_expr(type)
  end

  defp loaded_aggregate_field(aggregate, binding) do
    Ecto.Query.dynamic(field(as(^binding), ^aggregate.name))
  end

  defp select_aggregates(query, dynamics) do
    {in_aggregates, in_body} =
      Enum.split_with(dynamics, fn {load, _name, _dynamic} -> is_nil(load) end)

    aggregates =
      in_body
      |> Map.new(fn {load, _name, dynamic} -> {load, dynamic} end)

    aggregates =
      if Enum.empty?(in_aggregates) do
        aggregates
      else
        Map.put(
          aggregates,
          :aggregates,
          Map.new(in_aggregates, fn {_load, name, dynamic} -> {name, dynamic} end)
        )
      end

    query =
      if query.select do
        query
      else
        from(row in query, select: %{})
      end

    Ecto.Query.select_merge(query, ^aggregates)
  end
end
