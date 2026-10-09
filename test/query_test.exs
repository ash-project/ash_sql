# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.QueryTest do
  use ExUnit.Case, async: false

  import Ecto.Query

  defmodule SqlImplementation do
    use AshSql.Implementation

    def table(_), do: "posts"
    def schema(_), do: nil
    def repo(_, _), do: nil
    def parameterized_type(type, constraints), do: {:parameterized, {type, constraints}}
    def determine_types(_, values), do: {Enum.map(values, fn _ -> nil end), nil}
    def manual_relationship_function, do: :ash_postgres_join
    def manual_relationship_subquery_function, do: :ash_postgres_subquery
  end

  defmodule RecordSharedContext do
    use Ash.Resource.Preparation

    @impl true
    def prepare(query, _opts, context) do
      send(self(), {:shared_context, context.source_context[:shared]})
      query
    end
  end

  defmodule AuthorPreferences do
    use Ash.Resource, domain: AshSql.QueryTest.Domain

    attributes do
      uuid_primary_key(:id)
      attribute(:favorite_topic_id, :uuid)
    end

    preparations do
      prepare(AshSql.QueryTest.RecordSharedContext)
    end

    actions do
      defaults([:read])
    end
  end

  defmodule Author do
    use Ash.Resource, domain: AshSql.QueryTest.Domain

    attributes do
      uuid_primary_key(:id)
    end

    relationships do
      has_one(:preferences, AshSql.QueryTest.AuthorPreferences,
        destination_attribute: :id,
        source_attribute: :id
      )
    end

    actions do
      defaults([:read])
    end
  end

  defmodule Comment do
    use Ash.Resource, domain: AshSql.QueryTest.Domain

    attributes do
      uuid_primary_key(:id)
      attribute(:post_id, :uuid)
      attribute(:topic_id, :uuid)
    end

    actions do
      defaults([:read])
    end
  end

  defmodule Post do
    use Ash.Resource, domain: AshSql.QueryTest.Domain

    attributes do
      uuid_primary_key(:id)
      attribute(:author_id, :uuid)
    end

    relationships do
      belongs_to(:author, AshSql.QueryTest.Author)

      has_many :comments, AshSql.QueryTest.Comment do
        destination_attribute(:post_id)
        filter(expr(parent(author.preferences.favorite_topic_id) == topic_id))
      end
    end

    actions do
      defaults([:read])
    end
  end

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource(AshSql.QueryTest.Post)
      resource(AshSql.QueryTest.Comment)
      resource(AshSql.QueryTest.Author)
      resource(AshSql.QueryTest.AuthorPreferences)
    end
  end

  test "lateral join source keeps shared context for parent-path preparations" do
    shared = %{current_user_id: Ash.UUID.generate()}

    source_query =
      Post
      |> Ash.Query.new()
      |> Ash.Query.set_context(%{shared: shared, data_layer: %{lateral_join_source: :discarded}})

    rebuilt_query = AshSql.Query.rebuild_lateral_join_source_query(source_query)

    assert rebuilt_query.context[:shared] == shared
    assert rebuilt_query.context[:data_layer][:no_inner_join?]
    refute Map.has_key?(rebuilt_query.context[:data_layer], :lateral_join_source)

    relationship = Ash.Resource.Info.relationship(Post, :comments)

    assert Ash.Filter.find(relationship.filter, fn
             %Ash.Query.Parent{} -> true
             _ -> false
           end)

    Ash.Query.for_read(AuthorPreferences, :read, %{}, context: rebuilt_query.context)

    assert_receive {:shared_context, ^shared}
  end

  describe "distinct pagination ordering" do
    test "distinct on a sort prefix keeps ordering and pagination in the same query" do
      query =
        from(post in "posts", as: ^0, select: %{id: post.id}, limit: ^51, offset: ^50)
        |> distinct_query([author_id: :desc], author_id: :desc, id: :asc)

      assert query.__ash_bindings__.distinct_is_sort_prefix?

      assert {:ok, result} = AshSql.Query.return_query(query, Post)

      refute match?(%Ecto.SubQuery{}, result.from.source)
      assert result.distinct == query.distinct
      assert result.limit == query.limit
      assert result.offset == query.offset
      assert result.windows == []
      assert hd(result.order_bys).expr == query.windows[:order].expr[:order_by]
    end

    test "distinct on the full sort keeps ordering in the same query" do
      query =
        from(post in "posts", as: ^0, select: %{id: post.id})
        |> distinct_query([author_id: :desc, id: :asc], author_id: :desc, id: :asc)

      assert query.__ash_bindings__.distinct_is_sort_prefix?

      assert {:ok, result} = AshSql.Query.return_query(query, Post)
      refute match?(%Ecto.SubQuery{}, result.from.source)
      assert result.windows == []
    end

    test "distinct that is not a sort prefix is not wrapped as a sort prefix" do
      query =
        from(post in "posts", as: ^0, select: %{id: post.id}, limit: ^51)
        |> distinct_query([id: :asc], author_id: :desc, id: :asc)

      refute query.__ash_bindings__[:distinct_is_sort_prefix?]
      assert is_nil(query.distinct)
    end

    test "pre-existing distinct expressions keep the row_number wrapper" do
      query =
        from(post in "posts",
          as: ^0,
          distinct: [asc: post.id],
          select: %{id: post.id},
          limit: ^51
        )
        |> distinct_query([author_id: :desc], author_id: :desc, id: :asc)

      refute query.__ash_bindings__.distinct_is_sort_prefix?

      assert {:ok, result} = AshSql.Query.return_query(query, Post)
      assert %Ecto.SubQuery{query: inner} = result.from.source
      assert inner.limit == nil
      assert result.limit == query.limit
      assert [asc: {{:., _, [_, :__order__]}, _, []}] = hd(result.order_bys).expr
    end
  end

  defp distinct_query(query, distinct, sort) do
    query =
      query
      |> AshSql.Bindings.default_bindings(Post, SqlImplementation)
      |> Map.update!(:__ash_bindings__, &Map.put(&1, :sort, sort))

    {:ok, query} = AshSql.Distinct.distinct(query, distinct, Post)
    query
  end
end
