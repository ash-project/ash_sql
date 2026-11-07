# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.Query.ProjectionTest do
  use ExUnit.Case, async: true
  import Ecto.Query
  alias AshSql.Query.Projection

  defmodule Comment do
    use Ecto.Schema

    schema "comments" do
      field(:post_id, :integer)
      field(:title, :string)
      field(:likes, :integer)
    end
  end

  defp related_comment(inner_fun \\ & &1) do
    inner = from(c in Comment, as: :comment, where: c.post_id == parent_as(:post).id, limit: 1)

    from(c in subquery(inner_fun.(inner)), as: :comment)
    |> Map.put(:__ash_bindings__, %{relationship_projection?: true})
  end

  defp posts(source \\ related_comment()) do
    from(p in "posts",
      as: :post,
      left_lateral_join: c in ^source,
      as: :related_comment,
      on: true,
      select: %{id: p.id, title: c.title}
    )
  end

  defp selected_fields(query) do
    query.joins
    |> hd()
    |> Map.fetch!(:source)
    |> Map.fetch!(:from)
    |> Map.fetch!(:source)
    |> Map.fetch!(:query)
    |> Map.fetch!(:select)
  end

  test "retains references from later filters and sorts on a reused join" do
    result =
      posts() |> where([_p, c], c.likes > 0) |> order_by([_p, c], c.id) |> Projection.prune()

    assert selected_fields(result).take == %{0 => {:struct, [:id, :title, :likes]}}
    refute Map.has_key?(result, :__projection_scope__)
  end

  test "retains fields referenced only by a correlated subquery" do
    ratings = from(r in "ratings", where: r.score > parent_as(:related_comment).likes, select: 1)

    result =
      posts() |> select_merge(%{has_rating: exists(subquery(ratings))}) |> Projection.prune()

    assert selected_fields(result).take == %{0 => {:struct, [:title, :likes]}}
  end

  test "retains a wrapper filter's fields below the relationship limit" do
    source = related_comment() |> where([c], c.likes > 0)
    result = source |> posts() |> Projection.prune()
    assert selected_fields(result).take == %{0 => {:struct, [:title, :likes]}}
    assert hd(result.joins).source.wheres == source.wheres
  end

  test "keeps the implicit selection for a whole-row fragment" do
    result =
      posts()
      |> select_merge([_p, c], %{comment: fragment("row_to_json(?)", c)})
      |> Projection.prune()

    assert selected_fields(result) == nil
  end

  test "keeps fields that opaque SQL may reference without a binding" do
    result = posts() |> where(fragment("likes > 0")) |> Projection.prune()
    assert selected_fields(result) == nil
  end

  test "does not change a DISTINCT subquery's implicit selection" do
    result = related_comment(&distinct(&1, true)) |> posts() |> Projection.prune()
    assert selected_fields(result) == nil
  end

  test "does not replace an explicit selection" do
    result = related_comment(&select(&1, [c], %{title: c.title, likes: c.likes})) |> posts()
    assert selected_fields(Projection.prune(result)) == selected_fields(result)
  end
end
