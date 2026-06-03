# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql do
  @moduledoc false
  def dynamic_repo(
        resource,
        sql_behaviour,
        %{
          __ash_bindings__: %{context: %{data_layer: %{repo: repo}}}
        } = query
      ) do
    repo || sql_behaviour.repo(resource, repo_type(query))
  end

  def dynamic_repo(
        resource,
        sql_behaviour,
        %_{context: %{data_layer: %{repo: repo}}} = query
      ) do
    repo || sql_behaviour.repo(resource, repo_type(query))
  end

  def dynamic_repo(resource, sql_behaviour, query) do
    sql_behaviour.repo(resource, repo_type(query))
  end

  defp repo_type(%{lock: lock}) when not is_nil(lock), do: :mutate
  defp repo_type(%struct{}), do: struct_to_repo_type(struct)
  defp repo_type(_), do: :read

  def repo_opts(_repo, sql_behaviour, timeout, tenant, resource) do
    if Ash.Resource.Info.multitenancy_strategy(resource) == :context do
      [prefix: tenant]
    else
      if schema = sql_behaviour.schema(resource) do
        [prefix: schema]
      else
        []
      end
    end
    |> add_timeout(timeout)
  end

  defp add_timeout(opts, timeout) when not is_nil(timeout) do
    Keyword.put(opts, :timeout, timeout)
  end

  defp add_timeout(opts, _), do: opts

  defp struct_to_repo_type(struct) do
    case struct do
      Ash.Changeset -> :mutate
      Ash.Query -> :read
      Ecto.Query -> :read
      Ecto.Changeset -> :mutate
    end
  end
end
