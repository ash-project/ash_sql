defmodule AshSql.Implementation do
  @moduledoc false
  @callback table(Ash.Resource.t()) :: String.t()
  @callback schema(Ash.Resource.t()) :: String.t()
  @callback repo(Ash.Resource.t(), :mutate | :read) :: module
  @callback expr(Ecto.Query.t(), Ash.Expr.t(), map, boolean, AshSql.Expr.ExprInfo.t(), term) ::
              {:ok, term, AshSql.Expr.ExprInfo.t()} | {:error, term} | :error
  @callback simple_join_first_aggregates(Ash.Resource.t()) :: list(atom)

  @callback parameterized_type(
              Ash.Type.t() | Ecto.Type.t(),
              constraints :: Keyword.t(),
              no_maps? :: boolean
            ) ::
              term

  @callback parameterized_type(
              Ash.Type.t() | Ecto.Type.t(),
              constraints :: Keyword.t()
            ) ::
              term

  @callback determine_types(module, list(term)) :: list(term)

  @callback list_aggregate(Ash.Resource.t()) :: String.t()

  @callback manual_relationship_function() :: atom
  @callback manual_relationship_subquery_function() :: atom

  defmacro __using__(_) do
    quote do
      @behaviour AshSql.Implementation
    end
  end
end
