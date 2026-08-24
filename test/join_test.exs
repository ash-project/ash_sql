# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.JoinTest do
  use ExUnit.Case, async: true

  alias AshSql.Join

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
