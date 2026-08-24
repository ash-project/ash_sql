# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.AggregateTest do
  use ExUnit.Case, async: true

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
end
