# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.JoinTest do
  use ExUnit.Case, async: true

  alias AshSql.Join

  defmodule Tenanted do
    use Ash.Resource, domain: AshSql.JoinTest.Domain, data_layer: Ash.DataLayer.Ets

    attributes do
      uuid_primary_key(:id)
      attribute(:tenant_id, :string)
    end

    multitenancy do
      strategy(:attribute)
      attribute(:tenant_id)
    end

    actions do
      defaults([:read])

      read :bypass do
        multitenancy(:bypass)
      end
    end
  end

  defmodule Domain do
    use Ash.Domain, validate_config_inclusion?: false

    resources do
      resource(Tenanted)
    end
  end

  describe "handle_attribute_multitenancy/3" do
    # Ash resolves read multitenancy context-first, falling back to the action.
    for {context, action, filtered?} <- [
          {nil, :read, true},
          {nil, :bypass, false},
          {:allow_global, :bypass, true},
          {:enforce, :bypass, true},
          {:bypass_all, :read, false}
        ] do
      test "#{inspect(context)} context with the #{action} action matches Ash" do
        query = tenanted_query(unquote(context), unquote(action))

        {:ok, ash_query} = Ash.Actions.Read.handle_multitenancy(query)
        sql_query = Join.handle_attribute_multitenancy(query, "acme", query.action)

        assert filtered?(sql_query) == unquote(filtered?)
        assert filtered?(sql_query) == filtered?(ash_query)
      end
    end

    test "a bypass_all context applies without a read action" do
      query = tenanted_query(:bypass_all, :read)

      refute filtered?(Join.handle_attribute_multitenancy(query, "acme"))
    end

    test "the tenant filter applies without a read action or context" do
      query = tenanted_query(nil, :read)

      assert filtered?(Join.handle_attribute_multitenancy(query, "acme"))
    end
  end

  defp tenanted_query(context, action) do
    Tenanted
    |> then(fn query ->
      if context do
        Ash.Query.set_context(query, %{private: %{multitenancy: context}})
      else
        Ash.Query.new(query)
      end
    end)
    |> Ash.Query.for_read(action, %{}, tenant: "acme")
  end

  defp filtered?(query), do: not is_nil(query.filter)

  defp query(data_layer_context \\ %{}) do
    %{__ash_bindings__: %{context: %{data_layer: data_layer_context}}}
  end

  describe "left_join_only?/3" do
    test "a select-context join is never inner joined" do
      assert Join.left_join_only?(query(), [left_only?: true], false)
    end

    test "a filter-context join may still be inner joined" do
      refute Join.left_join_only?(query(), [], false)
    end

    test "the data layer context can force left joins" do
      assert Join.left_join_only?(query(%{no_inner_join?: true}), [], false)
    end

    test "an explicit no_inner_join? forces left joins" do
      assert Join.left_join_only?(query(), [], true)
    end

    test "returns a boolean rather than nil" do
      assert Join.left_join_only?(query(), [], false) == false
    end
  end
end
