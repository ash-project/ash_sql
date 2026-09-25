# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs/contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.Implementation do
  @moduledoc false
  @callback table(Ash.Resource.t()) :: String.t()
  @callback schema(Ash.Resource.t()) :: String.t() | nil
  @callback repo(Ash.Resource.t(), :mutate | :read) :: module
  @callback expr(Ecto.Query.t(), Ash.Expr.t(), map, boolean, AshSql.Expr.ExprInfo.t(), term) ::
              {:ok, term, AshSql.Expr.ExprInfo.t()} | {:error, term} | :error

  @doc """
  Render a list literal that has to be encoded, i.e. one holding maps, lists or expressions.

  `AshSql.Expr` renders those as `ARRAY[...]` (or `array_to_json(ARRAY[...])`), which is
  Postgres syntax. Implementations whose database has no array constructor override this to
  render the list themselves. Returning `:error`, which is the default, keeps the `ARRAY[...]`
  rendering.

  The value is passed after embedded resources have been dumped, so it is a plain list.
  """
  @callback list_expr(Ecto.Query.t(), list(), map, boolean, AshSql.Expr.ExprInfo.t(), term) ::
              {:ok, term, AshSql.Expr.ExprInfo.t()} | {:error, term} | :error

  @callback simple_join_first_aggregates(Ash.Resource.t()) :: list(atom)

  @callback parameterized_type(
              Ash.Type.t() | Ecto.Type.t(),
              constraints :: Keyword.t()
            ) ::
              term

  @callback storage_type(resource :: Ash.Resource.t(), field :: atom()) :: nil | term

  @callback ilike?() :: boolean()
  @callback equals_any?() :: boolean()
  @callback array_overlap_operator?() :: boolean()

  @callback determine_types(module, list(term)) :: {list(term), term} | list(term)
  @callback determine_types(module, list(term), returns :: term) ::
              {list(term), term} | list(term)

  @callback list_aggregate(Ash.Resource.t()) :: String.t() | nil

  @doc """
  Build the value of a `:list` aggregate for the `:grouped` aggregate strategy.

  `field` is a dynamic for the listed value. The grouped strategy selects the returned
  expression from a window partitioned by parent and ordered by the aggregate sort, then
  keeps each partition's last row. The expression must apply that window itself, by name:

      Ecto.Query.dynamic(
        over(fragment("json_group_array(?)", ^field), :ash_sql_grouped_aggregate_window)
      )

  When `include_nil?` is false, nil values have already been removed from the input.

  List defaults are JSON-encoded before they are cast with `type_expr/2`, so the result must
  use a JSON list representation. Returning `nil`, which is the default, makes grouped list
  aggregates return an error.
  """
  @callback grouped_list_aggregate(term, include_nil? :: boolean) :: term | nil

  @callback multicolumn_distinct?() :: boolean

  @callback manual_relationship_function() :: atom
  @callback manual_relationship_subquery_function() :: atom

  @callback require_ash_functions_for_or_and_and?() :: boolean
  @callback require_extension_for_citext() :: {true, String.t()} | false
  @callback strpos_function() :: String.t()
  @callback type_expr(expr :: term, type :: term) :: term
  @callback ref_cast_type(type :: term) :: term

  @doc """
  Choose how aggregates over `resource` are planned.

  `:lateral`, which is the default, loads related aggregates through lateral joins.
  `:grouped` joins grouped and windowed subqueries instead, for databases without lateral
  joins.

  The grouped strategy assumes SQLite-compatible SQL: offset-only query aggregates use
  `LIMIT -1`, and list aggregates use a JSON list representation (see
  `grouped_list_aggregate/2`).
  """
  @callback aggregate_strategy(Ash.Resource.t()) :: :lateral | :grouped

  @optional_callbacks determine_types: 3

  defmacro __using__(_) do
    quote do
      @behaviour AshSql.Implementation
      require Ecto.Query

      def strpos_function, do: "strpos"

      def expr(_, _, _, _, _, _), do: :error
      def list_expr(_, _, _, _, _, _), do: :error
      def simple_join_first_aggregates(_), do: []
      def list_aggregate(_), do: nil
      def grouped_list_aggregate(_, _), do: nil
      def multicolumn_distinct?, do: true
      def require_ash_functions_for_or_and_and?, do: false
      def require_extension_for_citext, do: false
      def array_overlap_operator?, do: true
      def ilike?, do: true
      def equals_any?, do: true
      def storage_type(_, _), do: nil
      def aggregate_strategy(_resource), do: :lateral

      # The cast type to use when casting a bare column reference, as opposed
      # to a value or a computed expression. Implementations can use this to
      # make ref casts match the column's actual DDL type (e.g. `timestamp(0)`
      # instead of `timestamp` on postgres), so the parser can collapse them
      # and partial/expression indexes on the bare column remain usable.
      def ref_cast_type(type), do: type

      def type_expr(expr, type) do
        type =
          if Ash.Type.ash_type?(type) do
            parameterized_type(type, [])
          else
            type
          end

        expr =
          if is_list(expr) and expr != [] and Keyword.keyword?(expr) do
            Map.new(expr)
          else
            expr
          end

        Ecto.Query.dynamic(type(^expr, ^type))
      end

      defoverridable array_overlap_operator?: 0,
                     equals_any?: 0,
                     expr: 6,
                     list_expr: 6,
                     ilike?: 0,
                     strpos_function: 0,
                     require_ash_functions_for_or_and_and?: 0,
                     require_extension_for_citext: 0,
                     simple_join_first_aggregates: 1,
                     aggregate_strategy: 1,
                     type_expr: 2,
                     ref_cast_type: 1,
                     storage_type: 2,
                     list_aggregate: 1,
                     grouped_list_aggregate: 2,
                     multicolumn_distinct?: 0
    end
  end
end
