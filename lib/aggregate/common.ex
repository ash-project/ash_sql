# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.Aggregate.Common do
  @moduledoc false

  @next_aggregate_names Enum.reduce(0..999, %{}, fn i, acc ->
                          Map.put(acc, :"aggregate_#{i}", :"aggregate_#{i + 1}")
                        end)

  def normalize(query, aggregates, resource, root_data) do
    path = attachment_path(root_data)

    with {:ok, aggregates} <- resource_aggregates_to_aggregates(resource, query, aggregates) do
      {query, aggregates} =
        Enum.reduce(aggregates, {query, []}, fn aggregate, {query, aggregates} ->
          # SQL aliases belong to a definition at an attachment path. The
          # public-name map alone only describes how to return loaded results.
          key = {path, aggregate.resource, aggregate.name}
          registry = Map.get(query.__ash_bindings__, :aggregate_registry, %{})
          definitions = Map.get(registry, key, [])
          existing = Enum.find(definitions, &(not different_queries?(&1.query, aggregate.query)))

          {query, name} =
            cond do
              existing -> {query, existing.name}
              is_atom(aggregate.name) && definitions == [] -> {query, aggregate.name}
              true -> use_aggregate_name(query)
            end

          query =
            if name != aggregate.name && path == [] do
              put_in(query.__ash_bindings__.aggregate_names[aggregate.name], name)
            else
              query
            end

          aggregate = %{aggregate | name: name}

          registry =
            if existing, do: registry, else: Map.put(registry, key, [aggregate | definitions])

          query =
            update_in(query.__ash_bindings__, fn bindings ->
              bindings = Map.put(bindings, :aggregate_registry, registry)

              if path == [] do
                Map.update!(bindings, :aggregate_defs, &Map.put(&1, name, aggregate))
              else
                bindings
              end
            end)

          {query, [aggregate | aggregates]}
        end)

      {:ok, query, aggregates}
    end
  end

  def attachment_path(nil), do: []
  def attachment_path({_, path}), do: path

  def name_for(aggregate, bindings, path) do
    key = {List.wrap(bindings[:refs_at_path]) ++ path, aggregate.resource, aggregate.name}

    bindings
    |> Map.get(:aggregate_registry, %{})
    |> Map.get(key, [])
    |> Enum.find(&(not different_queries?(&1.query, aggregate.query)))
    |> case do
      nil -> bindings.aggregate_names[aggregate.name] || aggregate.name
      existing -> existing.name
    end
  end

  defp use_aggregate_name(query) do
    name = query.__ash_bindings__.current_aggregate_name
    {put_in(query.__ash_bindings__.current_aggregate_name, next_aggregate_name(name)), name}
  end

  defp different_queries?(nil, nil), do: false
  defp different_queries?(nil, _), do: true
  defp different_queries?(_, nil), do: true

  defp different_queries?(query1, query2) do
    # Keep the upstream lateral identity rules when sharing normalization.
    query1.filter != query2.filter || query1.sort != query2.sort
  end

  defp resource_aggregates_to_aggregates(resource, query, aggregates) do
    private_context = query.__ash_bindings__.context[:private]

    Enum.reduce_while(aggregates, {:ok, []}, fn
      %Ash.Query.Aggregate{} = aggregate, {:ok, aggregates} ->
        aggregate =
          Ash.Actions.Read.add_calc_context(
            aggregate,
            private_context[:actor],
            private_context[:authorize?],
            private_context[:tenant],
            private_context[:tracer],
            query.__ash_bindings__[:domain],
            query.__ash_bindings__[:resource],
            parent_stack: query.__ash_bindings__[:parent_resources] || []
          )

        {:cont, {:ok, [aggregate | aggregates]}}

      aggregate, {:ok, aggregates} ->
        resource
        |> resource_aggregate_to_aggregate(aggregate,
          actor: private_context[:actor],
          tenant: private_context[:tenant]
        )
        |> case do
          {:ok, aggregate} ->
            aggregate =
              aggregate
              |> Map.put(:load, aggregate.name)
              |> Ash.Actions.Read.add_calc_context(
                private_context[:actor],
                private_context[:authorize?],
                private_context[:tenant],
                private_context[:tracer],
                query.__ash_bindings__[:domain],
                query.__ash_bindings__[:resource],
                parent_stack: query.__ash_bindings__[:parent_resources] || []
              )

            {:cont, {:ok, [aggregate | aggregates]}}

          {:error, error} ->
            {:halt, {:error, error}}
        end
    end)
  end

  @doc false
  def resource_aggregate_to_aggregate(resource, aggregate, opts \\ []) do
    related = Ash.Resource.Info.related(resource, aggregate.relationship_path)

    read_action =
      aggregate.read_action || Ash.Resource.Info.primary_action!(related, :read).name

    with %{valid?: true} = aggregate_query <-
           Ash.Query.for_read(related, read_action, %{},
             actor: opts[:actor],
             tenant: opts[:tenant]
           ),
         %{valid?: true} = aggregate_query <-
           Ash.Query.build(aggregate_query, filter: aggregate.filter, sort: aggregate.sort) do
      Ash.Query.Aggregate.new(
        resource,
        aggregate.name,
        aggregate.kind,
        path: aggregate.relationship_path,
        query: aggregate_query,
        field: aggregate.field,
        default: aggregate.default,
        filterable?: aggregate.filterable?,
        type: aggregate.type,
        sortable?: aggregate.filterable?,
        include_nil?: aggregate.include_nil?,
        constraints: aggregate.constraints,
        implementation: aggregate.implementation,
        uniq?: aggregate.uniq?,
        read_action: read_action,
        authorize?: aggregate.authorize?
      )
    else
      %{errors: errors} ->
        {:error, errors}
    end
  end

  def next_aggregate_name(i) do
    @next_aggregate_names[i] ||
      raise Ash.Error.Framework.AssumptionFailed,
        message: """
        All 1000 static names for aggregates have been used in a single query.
        Congratulations, this means that you have gone so wildly beyond our imagination
        of how much can fit into a single quer. Please file an issue and we will raise the limit.
        """
  end
end
