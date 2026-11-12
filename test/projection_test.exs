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

  defp tracked_query(refs_fun \\ & &1) do
    token = make_ref()
    metadata = %{expression_accumulator: %{projections: %{}}}
    inner = from(c in Comment, limit: 1) |> Map.put(:__ash_bindings__, metadata)

    related =
      from(c in subquery(inner))
      |> Map.put(:__ash_bindings__, Map.put(metadata, :projection, token))

    from(p in "posts", left_lateral_join: c in ^related, on: true, select: c.title)
    |> Map.put(:__ash_bindings__, %{
      expression_accumulator: %{projections: refs_fun.(%{token => MapSet.new([:title])})}
    })
  end

  test "uses recorded fields without modifying the input query" do
    query = tracked_query()
    pruned = Projection.prune(query)
    assert hd(pruned.joins).source.from.source.query.select.take == %{0 => {:struct, [:title]}}
    assert hd(query.joins).source.from.source.query.select == nil
  end

  test "unknown references leave the entire query unchanged" do
    query = tracked_query(fn _ -> :unknown end)
    assert Projection.prune(query) == query
  end

  test "queries without tracking metadata pass through" do
    query = tracked_query() |> Map.delete(:__ash_bindings__)
    assert Projection.prune(query) == query
  end

  test "manual relationships preserve columns used by opaque callbacks" do
    query =
      tracked_query()
      |> Projection.track_source(0, %{manual: {__MODULE__, []}, no_attributes?: true})

    assert Projection.prune(query) == query
  end
end
