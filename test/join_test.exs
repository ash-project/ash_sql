# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs.contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.JoinTest do
  @moduledoc false
  use ExUnit.Case

  import Ash.Expr

  alias AshSql.Join

  describe "extract_parent_referenced_fields/1" do
    test "extracts field from simple parent reference with atom" do
      filter = expr(parent(status) == "active")

      assert Join.extract_parent_referenced_fields(filter) == [:status]
    end

    test "extracts multiple fields from complex filter" do
      filter = expr(parent(status) == "active" and parent(role) == "admin")

      fields = Join.extract_parent_referenced_fields(filter)
      assert Enum.sort(fields) == [:role, :status]
    end

    test "returns empty list when no parent references exist" do
      filter = expr(status == "active")

      assert Join.extract_parent_referenced_fields(filter) == []
    end

    test "extracts unique fields only from duplicate references" do
      filter = expr(parent(status) == "active" or parent(status) == "pending")

      assert Join.extract_parent_referenced_fields(filter) == [:status]
    end

    test "handles nested parent expressions" do
      filter =
        expr((parent(status) == "active" and parent(verified) == true) or parent(admin) == true)

      fields = Join.extract_parent_referenced_fields(filter)
      assert Enum.sort(fields) == [:admin, :status, :verified]
    end

    test "handles parent references in comparison operators" do
      filter = expr(parent(age) > 18 and parent(count) <= 100)

      fields = Join.extract_parent_referenced_fields(filter)
      assert Enum.sort(fields) == [:age, :count]
    end

    test "returns empty list for nil filter" do
      assert Join.extract_parent_referenced_fields(nil) == []
    end

    test "handles parent reference with in operator" do
      filter = expr(parent(status) in ["active", "pending"])

      assert Join.extract_parent_referenced_fields(filter) == [:status]
    end

    test "handles parent reference with is_nil" do
      filter = expr(is_nil(parent(deleted_at)))

      assert Join.extract_parent_referenced_fields(filter) == [:deleted_at]
    end

    test "handles parent reference with not" do
      filter = expr(not parent(archived))

      assert Join.extract_parent_referenced_fields(filter) == [:archived]
    end

    test "handles parent reference with contains" do
      filter = expr(contains(parent(tags), "important"))

      assert Join.extract_parent_referenced_fields(filter) == [:tags]
    end

    test "handles parent reference in mixed conditions with non-parent fields" do
      filter = expr(parent(user_id) == ^123 and status == "pending")

      assert Join.extract_parent_referenced_fields(filter) == [:user_id]
    end

    test "handles parent reference with mathematical operations" do
      filter = expr(parent(price) * 1.1 > 100)

      assert Join.extract_parent_referenced_fields(filter) == [:price]
    end

    test "handles parent reference with string operations" do
      filter = expr(parent(name) <> " Suffix" == "Test Suffix")

      fields = Join.extract_parent_referenced_fields(filter)
      assert :name in fields
    end

    test "handles multiple parent fields in arithmetic expression" do
      filter = expr(parent(quantity) * parent(unit_price) > 1000)

      fields = Join.extract_parent_referenced_fields(filter)
      assert Enum.sort(fields) == [:quantity, :unit_price]
    end
  end
end
