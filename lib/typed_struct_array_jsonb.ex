# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs.contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.TypedStructArrayJsonb do
  @moduledoc """
  A custom type for handling arrays of typed structs stored as JSONB in Postgres.

  This type behaves similarly to `Ash.Type.Map` but expects a list of maps
  (dumped typed structs) instead of a single map. It's used internally by
  AshSql to properly cast typed struct arrays when stored as JSONB.
  """
  use Ash.Type

  @impl true
  def constraints, do: []

  @impl true
  def storage_type(_), do: :map

  @impl true
  def matches_type?(v, _constraints) do
    is_list(v) && Enum.all?(v, &is_map/1)
  end

  @impl true
  def cast_input(nil, _), do: {:ok, nil}
  def cast_input([], _), do: {:ok, []}

  def cast_input(value, _) when is_list(value) do
    if Enum.all?(value, &is_map/1) do
      {:ok, value}
    else
      :error
    end
  end

  def cast_input(_, _), do: :error

  @impl true
  def cast_stored(nil, _), do: {:ok, nil}
  def cast_stored([], _), do: {:ok, []}

  def cast_stored(value, _) when is_list(value) do
    if Enum.all?(value, &is_map/1) do
      {:ok, value}
    else
      :error
    end
  end

  def cast_stored(_, _), do: :error

  @impl true
  def dump_to_native(nil, _), do: {:ok, nil}
  def dump_to_native([], _), do: {:ok, []}

  def dump_to_native(value, _) when is_list(value) do
    if Enum.all?(value, &is_map/1) do
      {:ok, value}
    else
      :error
    end
  end

  def dump_to_native(_, _), do: :error
end
