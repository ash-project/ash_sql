# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.AggregateTest do
  use ExUnit.Case, async: true

  require Ecto.Query
  require Ash.Query

  defmodule SqlImplementation do
    use AshSql.Implementation
    def table(_), do: "records"
    def schema(_), do: nil
    def repo(_, _), do: AshSql.AggregateTest.SqlRepo
    def parameterized_type(_, _), do: nil
    def determine_types(_, values), do: {Enum.map(values, fn _ -> nil end), nil}
    def manual_relationship_function, do: :ash_sql_test_join
    def manual_relationship_subquery_function, do: :ash_sql_test_subquery
  end

  defmodule SqlRepo do
    def config, do: []
  end

  # Build real SQL joins without requiring a database or a downstream data layer.
  defmodule SqlDataLayer do
    require Ecto.Query
    def can?(_, {:aggregate, :first}), do: true
    def can?(_, {:join, _}), do: true
    def can?(_, {:filter_relationship, _}), do: true
    def can?(_, {:filter_expr, _}), do: true

    def can?(_, feature),
      do:
        feature in [:read, :filter, :boolean_filter, :expression_calculation, :nested_expressions]

    def resource_to_query(_, _), do: Ecto.Query.from(row in "records")

    def set_context(resource, query, context) do
      root = context[:data_layer][:start_bindings_at] || 0
      query = Ecto.Query.from(row in query, as: ^root)
      {:ok, AshSql.Bindings.default_bindings(query, resource, SqlImplementation, context)}
    end

    def filter(query, filter, resource), do: AshSql.Filter.filter(query, filter, resource)
  end

  defmodule Member do
    use Ash.Resource, domain: AshSql.AggregateTest.Domain, data_layer: SqlDataLayer

    attributes do
      integer_primary_key(:id)
      attribute(:name, :string)
      attribute(:hidden, :boolean)
    end

    actions do
      defaults([:read])
    end
  end

  defmodule Ticket do
    use Ash.Resource, domain: AshSql.AggregateTest.Domain, data_layer: SqlDataLayer

    attributes do
      integer_primary_key(:id)
    end

    relationships do
      belongs_to(:assignee, Member, attribute_type: :integer)
      has_one(:peer, __MODULE__, source_attribute: :id, destination_attribute: :id)
    end

    actions do
      defaults([:read])
    end
  end

  defmodule Comment do
    use Ash.Resource, domain: AshSql.AggregateTest.Domain, data_layer: Ash.DataLayer.Ets

    attributes do
      uuid_primary_key(:id)
      attribute(:post_id, :uuid)
      attribute(:score, :integer)
    end

    actions do
      read :read_all do
        primary?(true)
      end
    end
  end

  defmodule Post do
    use Ash.Resource, domain: AshSql.AggregateTest.Domain, data_layer: Ash.DataLayer.Ets

    attributes do
      uuid_primary_key(:id)
    end

    relationships do
      has_many(:comments, Comment, destination_attribute: :post_id)
    end

    aggregates do
      max(:highest_score, :comments, :score)
    end

    actions do
      defaults([:read])
    end
  end

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    authorization do
      require_actor?(true)
    end

    resources do
      resource(Post)
      resource(Comment)
      resource(Member)
      resource(Ticket)
    end
  end

  defp build(opts) do
    AshSql.Aggregate.resource_aggregate_to_aggregate(
      Post,
      Ash.Resource.Info.aggregate(Post, :highest_score),
      opts
    )
  end

  describe "resource_aggregate_to_aggregate/3" do
    test "passes actor and tenant to the related read which may require an actor" do
      actor = %{id: Ash.UUID.generate()}

      assert {:ok, aggregate} = build(actor: actor, tenant: "acme")

      assert aggregate.query.context.private.actor == actor
      assert aggregate.query.tenant == "acme"
    end

    test "reads the related resource through its primary read action" do
      assert {:ok, aggregate} = build(actor: %{id: Ash.UUID.generate()})

      assert aggregate.query.resource == Comment
      assert aggregate.read_action == :read_all
    end
  end

  describe "shared aggregate normalization" do
    test "reuses a string name after a different definition was requested" do
      {:ok, original} = build(actor: %{id: Ash.UUID.generate()})
      original = %{original | name: "highest_score"}
      filtered = %{original | query: Ash.Query.do_filter(original.query, score: 10)}

      {:ok, query, [first]} = normalize([original])
      {:ok, query, [second]} = normalize([filtered], query)
      {:ok, query, [again]} = normalize([original], query)

      assert is_atom(first.name)
      refute first.name == second.name
      assert again.name == first.name
      assert AshSql.Aggregate.Common.name_for(original, query.__ash_bindings__, []) == first.name
      assert AshSql.Aggregate.Common.name_for(filtered, query.__ash_bindings__, []) == second.name
    end

    test "resolves aliases independently for each attachment path" do
      {:ok, original} = build(actor: %{id: Ash.UUID.generate()})
      original = %{original | name: "highest_score"}
      filtered = %{original | query: Ash.Query.do_filter(original.query, score: 10)}

      {:ok, query, [first]} = normalize([original], nil, {Post, [:first]})
      {:ok, query, [second]} = normalize([filtered], query, {Post, [:second]})

      refute first.name == second.name

      assert AshSql.Aggregate.Common.name_for(original, query.__ash_bindings__, [:first]) ==
               first.name

      assert AshSql.Aggregate.Common.name_for(filtered, query.__ash_bindings__, [:second]) ==
               second.name
    end

    test "resource aggregates retain the actor and tenant during normalization" do
      aggregate = Ash.Resource.Info.aggregate(Post, :highest_score)
      actor = %{id: Ash.UUID.generate()}

      query =
        AshSql.Bindings.default_bindings(%Ecto.Query{}, Post, __MODULE__, %{
          private: %{actor: actor, tenant: "acme"}
        })

      assert {:ok, _, [normalized]} = normalize([aggregate], query)
      assert normalized.load == :highest_score
      assert normalized.query.context.private.actor == actor
      assert normalized.query.tenant == "acme"
    end
  end

  describe "lateral aggregate reselection" do
    setup do
      {:ok, aggregate} = Ash.Query.Aggregate.new(Post, :same_name, :count, path: [:comments])

      # Normalization scopes names by attachment path, so the same name can be
      # bound at the root and at a related path in one query.
      query =
        Ecto.Query.from(row in "posts", as: ^0, select: %{})
        |> AshSql.Bindings.default_bindings(Post, __MODULE__)
        |> AshSql.Bindings.add_binding(%{type: :aggregate, path: [], aggregates: [aggregate]})
        |> AshSql.Bindings.add_binding(%{
          type: :aggregate,
          path: [:related],
          aggregates: [aggregate]
        })

      %{aggregate: aggregate, query: query}
    end

    test "selects a root aggregate only from the root binding", context do
      {:ok, query} =
        AshSql.Aggregate.Lateral.add_aggregates(context.query, [context.aggregate], Post, true, 0)

      assert selected_bindings(query) == [1]
    end

    test "selects a related aggregate only from its attachment path", context do
      {:ok, query} =
        AshSql.Aggregate.Lateral.add_aggregates(
          context.query,
          [context.aggregate],
          Post,
          true,
          0,
          {Post, [:related]}
        )

      assert selected_bindings(query) == [2]
    end
  end

  describe "optimized first aggregate references" do
    setup do
      actor = %{id: 1}

      {:ok, query} =
        Ticket |> Ash.Query.for_read(:read, %{}, actor: actor) |> Ash.Query.data_layer_query()

      filter = Ash.Query.filter(Ticket, assignee.name != "nobody" or id > 0).filter
      {:ok, query} = AshSql.Filter.filter(query, filter, Ticket)

      # Read authorization is supplied as a related query filter by Ash.
      related =
        Member
        |> Ash.Query.for_read(:read, %{}, actor: actor)
        |> Ash.Query.filter(hidden == false)

      {:ok, aggregate} =
        Ash.Query.Aggregate.new(Ticket, :assignee_name, :first,
          path: [:assignee],
          field: :name,
          query: related
        )

      {:ok, query} = AshSql.Aggregate.add_aggregates(query, [aggregate], Ticket, false, 0)
      assert [%{as: 1}, %{as: 2, source: authorized}] = query.joins
      assert filtered_join?(authorized)
      assert is_nil(query.select)

      ref = %Ash.Query.Ref{attribute: aggregate, resource: Ticket}
      %{query: query, ref: ref}
    end

    test "aggregate predicates read the authorized join without loading the aggregate", context do
      {value, _} =
        AshSql.Expr.dynamic_expr(context.query, context.ref, context.query.__ash_bindings__)

      predicate = Ecto.Query.dynamic(^value == "Bob")
      query = Ecto.Query.where(context.query, ^predicate)
      assert expression_bindings(List.last(query.wheres).expr) == [2]

      ordinary = %Ash.Query.Ref{
        attribute: Ash.Resource.Info.attribute(Member, :name),
        relationship_path: [:assignee],
        resource: Ticket
      }

      assert AshSql.Expr.ref_binding(ordinary, query.__ash_bindings__) == 1
    end

    test "calculation references use the same authorized value as aggregate selection", context do
      {:ok, calculation} =
        Ash.Query.Calculation.new(
          :aggregate_value,
          Ash.Resource.Calculation.Expression,
          [expr: context.ref],
          :string,
          []
        )

      calculation_ref = %Ash.Query.Ref{attribute: calculation, resource: Ticket}

      {value, _} =
        AshSql.Expr.dynamic_expr(context.query, calculation_ref, context.query.__ash_bindings__)

      query = Ecto.Query.select(context.query, ^value)
      assert expression_bindings(query.select.expr) == [2]
    end

    test "same-named aggregate definitions retain their separate filtered joins", context do
      aggregate = context.ref.attribute
      other = %{aggregate | query: Ash.Query.filter(aggregate.query, id == 2)}
      {:ok, query} = AshSql.Aggregate.add_aggregates(context.query, [other], Ticket, false, 0)

      for {ref, expected} <- [{context.ref, 2}, {%{context.ref | attribute: other}, 3}] do
        {value, _} = AshSql.Expr.dynamic_expr(query, ref, query.__ash_bindings__)
        selected = Ecto.Query.select(query, ^value)
        assert expression_bindings(selected.select.expr) == [expected]
      end
    end

    test "the same aggregate at another attachment path does not replace the root binding",
         context do
      {:ok, query} =
        AshSql.Aggregate.add_aggregates(
          context.query,
          [context.ref.attribute],
          Ticket,
          false,
          0,
          {Ticket, [:peer]}
        )

      for {ref, expected} <- [
            {context.ref, 2},
            {%{context.ref | relationship_path: [:peer]}, 4}
          ] do
        {value, _} = AshSql.Expr.dynamic_expr(query, ref, query.__ash_bindings__)
        selected = Ecto.Query.select(query, ^value)
        assert expression_bindings(selected.select.expr) == [expected]
      end
    end
  end

  defp expression_bindings(expr) do
    expr
    |> Macro.prewalk([], fn
      {:as, _, [binding]} = ast, bindings -> {ast, [binding | bindings]}
      ast, bindings -> {ast, bindings}
    end)
    |> elem(1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp filtered_join?(%Ecto.SubQuery{query: query}), do: filtered_join?(query)
  defp filtered_join?(%{wheres: [_ | _]}), do: true
  defp filtered_join?(%{from: %{source: %Ecto.SubQuery{query: query}}}), do: filtered_join?(query)
  defp filtered_join?(_), do: false

  defp selected_bindings(query) do
    query.select.expr
    |> Macro.prewalk([], fn
      {:as, _, [binding]} = ast, bindings -> {ast, [binding | bindings]}
      ast, bindings -> {ast, bindings}
    end)
    |> elem(1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp normalize(aggregates, query \\ nil, root_data \\ nil) do
    query = query || AshSql.Bindings.default_bindings(%Ecto.Query{}, Post, __MODULE__)
    AshSql.Aggregate.Common.normalize(query, aggregates, Post, root_data)
  end
end
