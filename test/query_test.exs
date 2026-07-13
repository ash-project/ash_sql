# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.QueryTest do
  use ExUnit.Case, async: false

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
end
