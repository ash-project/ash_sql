# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.AggregateTest do
  use ExUnit.Case, async: true

  require Ecto.Query

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
