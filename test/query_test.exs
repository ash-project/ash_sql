# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.QueryTest do
  use ExUnit.Case, async: false

  import Ecto.Query

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
    test "keeps compatible ordering and pagination in the same query" do
      query =
        from(post in "posts",
          as: ^0,
          left_join: comment in "comments",
          on: comment.post_id == post.id,
          where: post.author_id == ^"author",
          distinct: [desc: post.author_id, asc: post.id, asc: post.id],
          select: %{id: post.id, author_id: post.author_id},
          limit: ^51,
          offset: ^50
        )
        |> sorted_query(author_id: :desc, id: :asc)

      assert {:ok, result} = AshSql.Query.return_query(query, Post)

      assert result.from == query.from
      assert result.distinct == query.distinct
      assert result.joins == query.joins
      assert result.wheres == query.wheres
      assert result.select == query.select
      assert result.limit == query.limit
      assert result.offset == query.offset
      assert result.windows == []
      assert hd(result.order_bys).expr == query.windows[:order].expr[:order_by]
    end

    test "keeps tie breakers after a shorter distinct ordering" do
      query =
        from(post in "posts", as: ^0, distinct: [desc: post.author_id], select: post.id)
        |> sorted_query(author_id: :desc, id: :asc)

      assert {:ok, result} = AshSql.Query.return_query(query, Post)
      assert result.from == query.from
      assert hd(result.order_bys).expr == query.windows[:order].expr[:order_by]
      assert result.windows == []
    end

    test "preserves explicit null ordering" do
      query =
        from(post in "posts",
          as: ^0,
          distinct: [desc_nulls_last: post.author_id, asc: post.id],
          select: post.id
        )
        |> sorted_query(author_id: :desc_nils_last, id: :asc)

      assert {:ok, result} = AshSql.Query.return_query(query, Post)
      assert result.from == query.from
      assert hd(result.order_bys).expr == query.windows[:order].expr[:order_by]
    end

    test "recognizes ordering through a nonzero named root binding" do
      query =
        from(post in "posts", as: ^500, distinct: [asc: post.id], select: post.id)
        |> AshSql.Bindings.default_bindings(Post, __MODULE__, %{
          data_layer: %{start_bindings_at: 500}
        })
        |> Map.update!(:__ash_bindings__, &Map.put(&1, :sort, id: :asc))

      assert {:ok, result} = AshSql.Query.return_query(query, Post)
      assert result.from == query.from
      assert result.windows == []
    end

    test "retains the wrapper when distinct would change the requested ordering" do
      for distinct <- [[asc: :id], [asc: :author_id], [desc_nulls_first: :author_id]] do
        query =
          from(post in "posts", as: ^0, distinct: ^distinct, select: %{id: post.id}, limit: ^51)
          |> sorted_query(author_id: :desc, id: :asc)

        assert {:ok, result} = AshSql.Query.return_query(query, Post)
        assert %Ecto.SubQuery{query: inner} = result.from.source
        assert inner.distinct == query.distinct
        assert inner.limit == nil
        assert result.limit == query.limit
        assert [asc: {{:., _, [_, :__order__]}, _, []}] = hd(result.order_bys).expr
      end
    end

    test "retains the wrapper for bound sort expressions" do
      query =
        from(post in "posts",
          as: ^0,
          distinct: [asc: fragment("coalesce(?, ?)", post.author_id, ^"first")],
          select: %{id: post.id},
          windows: [
            order: [order_by: [asc: fragment("coalesce(?, ?)", post.author_id, ^"second")]]
          ]
        )
        |> AshSql.Bindings.default_bindings(Post, __MODULE__)
        |> Map.update!(
          :__ash_bindings__,
          &Map.merge(&1, %{sort_applied?: true, __order__?: true})
        )

      assert {:ok, result} = AshSql.Query.return_query(query, Post)
      assert %Ecto.SubQuery{query: inner} = result.from.source
      assert inner.distinct.params == query.distinct.params
      assert inner.windows[:order].params == query.windows[:order].params
    end

    test "keeps independent subqueries in the distinct and display orders" do
      highest = from(comment in "comments", select: max(comment.score))
      lowest = from(comment in "comments", select: min(comment.score))

      query =
        from(post in "posts",
          as: ^0,
          distinct: [asc: subquery(highest)],
          select: %{id: post.id},
          windows: [order: [order_by: [asc: subquery(lowest)]]]
        )
        |> AshSql.Bindings.default_bindings(Post, __MODULE__)
        |> Map.update!(
          :__ash_bindings__,
          &Map.merge(&1, %{sort_applied?: true, __order__?: true})
        )

      assert {:ok, result} = AshSql.Query.return_query(query, Post)
      assert %Ecto.SubQuery{query: inner} = result.from.source
      assert inner.distinct.subqueries == query.distinct.subqueries
      assert inner.windows[:order].subqueries == query.windows[:order].subqueries
    end
  end

  defp sorted_query(query, sort) do
    query =
      query
      |> AshSql.Bindings.default_bindings(Post, __MODULE__)
      |> Map.update!(:__ash_bindings__, &Map.put(&1, :sort, sort))

    {:ok, query} = AshSql.Sort.apply_sort(query, sort, Post)
    query
  end
end
