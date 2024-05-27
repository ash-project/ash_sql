defmodule AshSql.Expr do
  @moduledoc false

  alias Ash.Filter
  alias Ash.Query.{BooleanExpression, Exists, Not, Ref}
  alias Ash.Query.Operator.IsNil

  alias Ash.Query.Function.{
    Ago,
    At,
    CompositeType,
    Contains,
    CountNils,
    DateAdd,
    DateTimeAdd,
    Error,
    Fragment,
    FromNow,
    GetPath,
    If,
    Lazy,
    Length,
    Now,
    Round,
    StringDowncase,
    StringJoin,
    StringLength,
    StringSplit,
    StringTrim,
    Today,
    Type
  }

  require Ecto.Query

  defmodule ExprInfo do
    @moduledoc false
    defstruct has_error?: false

    @type t :: %__MODULE__{}
  end

  def dynamic_expr(query, expr, bindings, embedded? \\ false, type \\ nil, acc \\ %ExprInfo{})

  def dynamic_expr(_query, %Filter{expression: nil}, _bindings, _embedded?, _type, acc) do
    # a nil filter means everything
    {true, acc}
  end

  def dynamic_expr(query, %Filter{expression: expression}, bindings, embedded?, type, acc) do
    dynamic_expr(query, expression, bindings, embedded?, type, acc)
  end

  def dynamic_expr(_, true, _, _, _, acc), do: {true, acc}
  def dynamic_expr(_, false, _, _, _, acc), do: {false, acc}

  def dynamic_expr(query, expression, bindings, embedded?, type, acc) do
    do_dynamic_expr(query, expression, bindings, embedded?, acc, type)
  end

  defp do_dynamic_expr(query, expr, bindings, embedded?, acc, type \\ nil) do
    case bindings.sql_behaviour.expr(query, expr, bindings, embedded?, acc, type) do
      {:ok, expr, acc} -> {expr, acc}
      {:error, error} -> {:error, error}
      :error -> default_dynamic_expr(query, expr, bindings, embedded?, acc, type)
    end
  end

  defp default_dynamic_expr(query, expr, bindings, embedded?, acc, type)

  defp default_dynamic_expr(_, {:embed, other}, _bindings, _true, acc, _type) do
    {other, acc}
  end

  defp default_dynamic_expr(query, %Not{expression: expression}, bindings, embedded?, acc, _type) do
    {new_expression, acc} =
      do_dynamic_expr(query, expression, bindings, embedded?, acc, :boolean)

    {Ecto.Query.dynamic(not (^new_expression)), acc}
  end

  defp default_dynamic_expr(
         query,
         %IsNil{left: left, right: true, embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         _type
       ) do
    {left_expr, acc} = do_dynamic_expr(query, left, bindings, pred_embedded? || embedded?, acc)

    {Ecto.Query.dynamic(is_nil(^left_expr)), acc}
  end

  defp default_dynamic_expr(
         query,
         %IsNil{left: left, right: false, embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         _type
       ) do
    {left_expr, acc} = do_dynamic_expr(query, left, bindings, pred_embedded? || embedded?, acc)

    {Ecto.Query.dynamic(not is_nil(^left_expr)), acc}
  end

  defp default_dynamic_expr(
         query,
         %IsNil{left: left, right: right, embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         _type
       ) do
    {left_expr, acc} = do_dynamic_expr(query, left, bindings, pred_embedded? || embedded?, acc)

    {right_expr, acc} =
      do_dynamic_expr(query, right, bindings, pred_embedded? || embedded?, acc, :boolean)

    {Ecto.Query.dynamic(is_nil(^left_expr) == ^right_expr), acc}
  end

  defp default_dynamic_expr(
         _query,
         %Lazy{arguments: [{m, f, a}]},
         _bindings,
         _embedded?,
         acc,
         _type
       ) do
    {apply(m, f, a), acc}
  end

  defp default_dynamic_expr(
         query,
         %Ago{arguments: [left, right], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         _type
       )
       when is_binary(right) or is_atom(right) do
    {left, acc} =
      do_dynamic_expr(query, left, bindings, pred_embedded? || embedded?, acc, :integer)

    {Ecto.Query.dynamic(
       fragment("(?)", datetime_add(^DateTime.utc_now(), ^left * -1, ^to_string(right)))
     ), acc}
  end

  defp default_dynamic_expr(
         query,
         %At{arguments: [left, right], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         _type
       ) do
    {left, acc} =
      do_dynamic_expr(query, left, bindings, pred_embedded? || embedded?, acc, :integer)

    {right, acc} =
      do_dynamic_expr(query, right, bindings, pred_embedded? || embedded?, acc, :integer)

    expr =
      if is_integer(right) do
        Ecto.Query.dynamic(fragment("(?)[?]", ^left, ^(right + 1)))
      else
        Ecto.Query.dynamic(fragment("(?)[? + 1]", ^left, ^right))
      end

    {expr, acc}
  end

  defp default_dynamic_expr(
         query,
         %FromNow{arguments: [left, right], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         _type
       )
       when is_binary(right) or is_atom(right) do
    {left, acc} =
      do_dynamic_expr(query, left, bindings, pred_embedded? || embedded?, acc, :integer)

    {Ecto.Query.dynamic(
       fragment("(?)", datetime_add(^DateTime.utc_now(), ^left, ^to_string(right)))
     ), acc}
  end

  defp default_dynamic_expr(
         query,
         %DateTimeAdd{arguments: [datetime, amount, interval], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         _type
       )
       when is_binary(interval) or is_atom(interval) do
    {datetime, acc} = do_dynamic_expr(query, datetime, bindings, pred_embedded? || embedded?, acc)

    {amount, acc} =
      do_dynamic_expr(query, amount, bindings, pred_embedded? || embedded?, acc, :integer)

    {Ecto.Query.dynamic(fragment("(?)", datetime_add(^datetime, ^amount, ^to_string(interval)))),
     acc}
  end

  defp default_dynamic_expr(
         query,
         %DateAdd{arguments: [date, amount, interval], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         _type
       )
       when is_binary(interval) or is_atom(interval) do
    {date, acc} = do_dynamic_expr(query, date, bindings, pred_embedded? || embedded?, acc)

    {amount, acc} =
      do_dynamic_expr(query, amount, bindings, pred_embedded? || embedded?, acc, :integer)

    {Ecto.Query.dynamic(fragment("(?)", datetime_add(^date, ^amount, ^to_string(interval)))), acc}
  end

  defp default_dynamic_expr(
         query,
         %GetPath{
           arguments: [
             %Ref{attribute: %Ash.Resource.Aggregate{} = aggregate, resource: resource} = left,
             right
           ],
           embedded?: pred_embedded?
         } = expr,
         bindings,
         embedded?,
         acc,
         type
       )
       when is_list(right) do
    case bindings.sql_behaviour.expr(query, expr, bindings, true, acc, type) do
      {:ok, expr, acc} ->
        {expr, acc}

      :error ->
        attribute =
          case aggregate.field do
            nil ->
              nil

            %{} = field ->
              field

            field ->
              related = Ash.Resource.Info.related(resource, aggregate.relationship_path)
              Ash.Resource.Info.attribute(related, field)
          end

        attribute_type =
          if attribute do
            attribute.type
          end

        attribute_constraints =
          if attribute do
            attribute.constraints
          end

        {:ok, type, constraints} =
          Ash.Query.Aggregate.kind_to_type(aggregate.kind, attribute_type, attribute_constraints)

        type
        |> Ash.Resource.Info.aggregate_type(aggregate)
        |> split_at_paths(constraints, right)
        |> Enum.reduce(do_dynamic_expr(query, left, bindings, embedded?, acc), fn data,
                                                                                  {expr, acc} ->
          do_get_path(query, expr, data, bindings, embedded?, pred_embedded?, acc)
        end)
    end
  end

  defp default_dynamic_expr(
         query,
         %GetPath{
           arguments: [%Ref{attribute: %{type: type, constraints: constraints}} = left, right],
           embedded?: pred_embedded?
         } = expr,
         bindings,
         embedded?,
         acc,
         _
       )
       when is_list(right) do
    case bindings.sql_behaviour.expr(query, expr, bindings, true, acc, type) do
      {:ok, expr, acc} ->
        {expr, acc}

      :error ->
        type
        |> split_at_paths(constraints, right)
        |> Enum.reduce(do_dynamic_expr(query, left, bindings, embedded?, acc), fn data,
                                                                                  {expr, acc} ->
          do_get_path(query, expr, data, bindings, embedded?, pred_embedded?, acc)
        end)
    end
  end

  defp default_dynamic_expr(
         query,
         %Contains{arguments: [left, %Ash.CiString{} = right], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         type
       ) do
    if bindings.sql_behaviour.ilike?() do
      text = escape_contains(right.string)

      {left, acc} =
        AshSql.Expr.dynamic_expr(query, left, bindings, pred_embedded? || embedded?, :string, acc)

      {Ecto.Query.dynamic(ilike(^left, ^text)), acc}
    else
      do_dynamic_expr(
        query,
        %Fragment{
          embedded?: pred_embedded?,
          arguments: [
            raw: "#{bindings.sql_behaviour.strpos_function()}((",
            expr: left,
            raw: "), (",
            expr: right,
            raw: ")) > 0"
          ]
        },
        bindings,
        embedded?,
        acc,
        type
      )
    end
  end

  defp default_dynamic_expr(
         query,
         %CountNils{arguments: [list], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         type
       ) do
    if is_list(list) do
      list =
        Enum.map(list, fn item ->
          %Ash.Query.Operator.IsNil{left: item, right: true}
        end)

      do_dynamic_expr(
        query,
        %Fragment{
          embedded?: pred_embedded?,
          arguments: [
            raw: "SELECT COUNT(*) FROM unnest(",
            expr: list,
            raw: ") AS item WHERE item IS TRUE"
          ]
        },
        bindings,
        embedded?,
        acc,
        type
      )
    else
      do_dynamic_expr(
        query,
        %Fragment{
          embedded?: pred_embedded?,
          arguments: [
            raw: "SELECT COUNT(*) FROM unnest(",
            expr: list,
            raw: ") AS item WHERE item IS NULL"
          ]
        },
        bindings,
        embedded?,
        acc,
        type
      )
    end
  end

  defp default_dynamic_expr(
         query,
         %Contains{arguments: [left, right], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         _type
       )
       when is_binary(right) do
    text = escape_contains(right)

    {left, acc} =
      AshSql.Expr.dynamic_expr(query, left, bindings, pred_embedded? || embedded?, :string, acc)

    {Ecto.Query.dynamic(like(^left, ^text)), acc}
  end

  defp default_dynamic_expr(
         query,
         %Contains{arguments: [left, right], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         type
       ) do
    do_dynamic_expr(
      query,
      %Fragment{
        embedded?: pred_embedded?,
        arguments: [
          raw: "#{bindings.sql_behaviour.strpos_function()}((",
          expr: left,
          raw: "), (",
          expr: right,
          raw: ")) > 0"
        ]
      },
      bindings,
      embedded?,
      acc,
      type
    )
  end

  defp default_dynamic_expr(
         query,
         %Length{arguments: [list], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         type
       ) do
    do_dynamic_expr(
      query,
      %Fragment{
        embedded?: pred_embedded?,
        arguments: [
          raw: "array_length((",
          expr: list,
          raw: "), 1)"
        ]
      },
      bindings,
      embedded?,
      acc,
      type
    )
  end

  defp default_dynamic_expr(
         query,
         %If{arguments: [condition, when_true, when_false], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         type
       ) do
    [condition_type, when_true_type, when_false_type] =
      case bindings.sql_behaviour.determine_types(If, [condition, when_true, when_false]) do
        [condition_type, when_true] ->
          [condition_type, when_true, nil]

        [condition_type, when_true, when_false] ->
          [condition_type, when_true, when_false]
      end
      |> case do
        [condition_type, nil, nil] ->
          [condition_type, type, type]

        [condition_type, when_true, nil] ->
          [condition_type, when_true, type]

        [condition_type, nil, when_false] ->
          [condition_type, type, when_false]

        [condition_type, when_true, when_false] ->
          [condition_type, when_true, when_false]
      end

    when_true_type = when_true_type || when_false_type || type
    when_false_type = when_false_type || when_true_type || type

    {condition, acc} =
      do_dynamic_expr(
        query,
        condition,
        bindings,
        pred_embedded? || embedded?,
        acc,
        condition_type
      )

    {when_true, acc} =
      do_dynamic_expr(
        query,
        when_true,
        bindings,
        pred_embedded? || embedded?,
        acc,
        when_true_type
      )

    {additional_cases, when_false, acc} =
      extract_cases(
        query,
        when_false,
        bindings,
        pred_embedded? || embedded?,
        acc,
        when_false_type
      )

    additional_case_fragments =
      additional_cases
      |> Enum.flat_map(fn {condition, when_true} ->
        [
          raw: " WHEN ",
          casted_expr: condition,
          raw: " THEN ",
          casted_expr: when_true
        ]
      end)

    do_dynamic_expr(
      query,
      %Fragment{
        embedded?: pred_embedded?,
        arguments:
          [
            raw: "CASE WHEN ",
            casted_expr: condition,
            raw: " THEN ",
            casted_expr: when_true
          ] ++
            additional_case_fragments ++
            [
              raw: " ELSE ",
              casted_expr: when_false,
              raw: " END"
            ]
      },
      bindings,
      embedded?,
      acc,
      type
    )
  end

  defp default_dynamic_expr(
         query,
         %StringJoin{arguments: [values, joiner], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         type
       )
       when is_list(values) do
    do_dynamic_expr(
      query,
      %Fragment{
        embedded?: pred_embedded?,
        arguments:
          Enum.reduce(values, [raw: "concat_ws(", expr: joiner], fn value, frag_acc ->
            frag_acc ++ [raw: ", ", expr: value]
          end) ++ [raw: ")"]
      },
      bindings,
      embedded?,
      acc,
      type
    )
  end

  defp default_dynamic_expr(
         query,
         %StringJoin{arguments: [values, joiner], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         type
       ) do
    do_dynamic_expr(
      query,
      %Fragment{
        embedded?: pred_embedded?,
        arguments: [raw: "array_to_string(", expr: values, raw: ", ", expr: joiner, raw: ")"]
      },
      bindings,
      embedded?,
      acc,
      type
    )
  end

  defp default_dynamic_expr(
         query,
         %StringSplit{arguments: [string, delimiter, options], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         type
       ) do
    if options[:trim?] do
      require_ash_functions!(query, "string_split(..., trim?: true)")

      do_dynamic_expr(
        query,
        %Fragment{
          embedded?: pred_embedded?,
          arguments: [
            raw: "ash_trim_whitespace(string_to_array(",
            expr: string,
            raw: ", NULLIF(",
            expr: delimiter,
            raw: ", '')))"
          ]
        },
        bindings,
        embedded?,
        acc,
        type
      )
    else
      do_dynamic_expr(
        query,
        %Fragment{
          embedded?: pred_embedded?,
          arguments: [
            raw: "string_to_array(",
            expr: string,
            raw: ", NULLIF(",
            expr: delimiter,
            raw: ", ''))"
          ]
        },
        bindings,
        embedded?,
        acc,
        type
      )
    end
  end

  defp default_dynamic_expr(
         query,
         %StringJoin{arguments: [values], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         type
       )
       when is_list(values) do
    do_dynamic_expr(
      query,
      %Fragment{
        embedded?: pred_embedded?,
        arguments:
          [raw: "concat("] ++
            (values
             |> Enum.reduce([], fn value, acc ->
               acc ++ [expr: value]
             end)
             |> Enum.intersperse({:raw, ", "})) ++
            [raw: ")"]
      },
      bindings,
      embedded?,
      acc,
      type
    )
  end

  defp default_dynamic_expr(
         query,
         %StringJoin{arguments: [values], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         type
       ) do
    do_dynamic_expr(
      query,
      %StringJoin{arguments: [values, ""], embedded?: pred_embedded?},
      bindings,
      embedded?,
      acc,
      type
    )
  end

  defp default_dynamic_expr(
         query,
         %StringLength{arguments: [value], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         type
       ) do
    do_dynamic_expr(
      query,
      %Fragment{
        embedded?: pred_embedded?,
        arguments: [raw: "length(", expr: value, raw: ")"]
      },
      bindings,
      embedded?,
      acc,
      type
    )
  end

  defp default_dynamic_expr(
         query,
         %StringDowncase{arguments: [value], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         type
       ) do
    do_dynamic_expr(
      query,
      %Fragment{
        embedded?: pred_embedded?,
        arguments: [raw: "lower(", expr: value, raw: ")"]
      },
      bindings,
      embedded?,
      acc,
      type
    )
  end

  defp default_dynamic_expr(
         query,
         %StringTrim{arguments: [value], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         type
       ) do
    do_dynamic_expr(
      query,
      %Fragment{
        embedded?: pred_embedded?,
        arguments: [
          raw: "REGEXP_REPLACE(REGEXP_REPLACE(",
          expr: value,
          raw: ", '\s+$', ''), '^\s+', '')"
        ]
      },
      bindings,
      embedded?,
      acc,
      type
    )
  end

  # Sorry :(
  # This is bad to do, but is the only reasonable way I could find.
  defp default_dynamic_expr(
         query,
         %Fragment{arguments: arguments, embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         _type
       ) do
    arguments =
      case arguments do
        [{:raw, raw} | rest] ->
          [{:raw, "(#{raw}"} | rest]

        arguments ->
          [{:raw, "("} | arguments]
      end

    arguments =
      case List.last(arguments) do
        nil ->
          arguments

        {:raw, _} ->
          List.update_at(arguments, -1, fn {:raw, raw} -> {:raw, "#{raw})"} end)

        _ ->
          arguments ++ [{:raw, ")"}]
      end

    {params, fragment_data, _, acc} =
      Enum.reduce(arguments, {[], [], 0, acc}, fn
        {:raw, str}, {params, fragment_data, count, acc} ->
          {params, [{:raw, str} | fragment_data], count, acc}

        {:casted_expr, dynamic}, {params, fragment_data, count, acc} ->
          {item, params, count} =
            {{:^, [], [count]}, [{dynamic, :any} | params], count + 1}

          {params, [{:expr, item} | fragment_data], count, acc}

        {:expr, expr}, {params, fragment_data, count, acc} ->
          {dynamic, acc} =
            do_dynamic_expr(query, expr, bindings, pred_embedded? || embedded?, acc)

          {item, params, count} =
            {{:^, [], [count]}, [{dynamic, :any} | params], count + 1}

          {params, [{:expr, item} | fragment_data], count, acc}
      end)

    {%Ecto.Query.DynamicExpr{
       fun: fn _query ->
         {{:fragment, [], Enum.reverse(fragment_data)}, Enum.reverse(params), [], %{}}
       end,
       binding: [],
       file: __ENV__.file,
       line: __ENV__.line
     }, acc}
  end

  defp default_dynamic_expr(
         query,
         %BooleanExpression{op: op, left: left, right: right},
         bindings,
         embedded?,
         acc,
         _type
       ) do
    {left_expr, acc} = do_dynamic_expr(query, left, bindings, embedded?, acc, :boolean)
    {right_expr, acc} = do_dynamic_expr(query, right, bindings, embedded?, acc, :boolean)

    expr =
      case op do
        :and ->
          Ecto.Query.dynamic(^left_expr and ^right_expr)

        :or ->
          Ecto.Query.dynamic(^left_expr or ^right_expr)
      end

    {expr, acc}
  end

  defp default_dynamic_expr(
         query,
         %Ash.Query.Function.Minus{arguments: [arg], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         type
       ) do
    [determined_type] = bindings.sql_behaviour.determine_types(Ash.Query.Function.Minus, [arg])

    {expr, acc} =
      do_dynamic_expr(
        query,
        arg,
        bindings,
        pred_embedded? || embedded?,
        acc,
        determined_type || type
      )

    {Ecto.Query.dynamic(-(^expr)), acc}
  end

  # Honestly we need to either 1. not type cast or 2. build in type compatibility concepts
  # instead of `:same` we need an `ANY COMPATIBLE` equivalent.
  @cast_operands_for [:<>]

  defp default_dynamic_expr(
         query,
         %mod{
           __predicate__?: _,
           left: left,
           right: right,
           embedded?: pred_embedded?,
           operator: operator
         },
         bindings,
         embedded?,
         acc,
         type
       ) do
    [left_type, right_type] =
      mod
      |> bindings.sql_behaviour.determine_types([left, right])

    {left_expr, acc} =
      if left_type && operator in @cast_operands_for do
        {left_expr, acc} =
          do_dynamic_expr(query, left, bindings, pred_embedded? || embedded?, acc)

        {query.__ash_bindings__.sql_behaviour.type_expr(left_expr, left_type), acc}
      else
        do_dynamic_expr(query, left, bindings, pred_embedded? || embedded?, acc, left_type)
      end

    {right_expr, acc} =
      if right_type && operator in @cast_operands_for do
        {right_expr, acc} =
          do_dynamic_expr(query, right, bindings, pred_embedded? || embedded?, acc)

        {query.__ash_bindings__.sql_behaviour.type_expr(right_expr, right_type), acc}
      else
        do_dynamic_expr(query, right, bindings, pred_embedded? || embedded?, acc, right_type)
      end

    case operator do
      :== ->
        {Ecto.Query.dynamic(^left_expr == ^right_expr), acc}

      :!= ->
        {Ecto.Query.dynamic(^left_expr != ^right_expr), acc}

      :> ->
        {Ecto.Query.dynamic(^left_expr > ^right_expr), acc}

      :< ->
        {Ecto.Query.dynamic(^left_expr < ^right_expr), acc}

      :>= ->
        {Ecto.Query.dynamic(^left_expr >= ^right_expr), acc}

      :<= ->
        {Ecto.Query.dynamic(^left_expr <= ^right_expr), acc}

      :in ->
        {Ecto.Query.dynamic(^left_expr in ^right_expr), acc}

      :+ ->
        {Ecto.Query.dynamic(^left_expr + ^right_expr), acc}

      :- ->
        {Ecto.Query.dynamic(^left_expr - ^right_expr), acc}

      :/ ->
        typed_left =
          query.__ash_bindings__.sql_behaviour.type_expr(left_expr, :decimal)

        typed_right =
          query.__ash_bindings__.sql_behaviour.type_expr(right_expr, :decimal)

        {Ecto.Query.dynamic(^typed_left / ^typed_right), acc}

      :* ->
        {Ecto.Query.dynamic(^left_expr * ^right_expr), acc}

      :<> ->
        do_dynamic_expr(
          query,
          %Fragment{
            embedded?: pred_embedded?,
            arguments: [
              casted_expr: left_expr,
              raw: " || ",
              casted_expr: right_expr
            ]
          },
          bindings,
          embedded?,
          acc,
          type
        )

      :|| ->
        if "ash-functions" in query.__ash_bindings__.sql_behaviour.repo(
             query.__ash_bindings__.resource,
             :mutate
           ).installed_extensions() do
          do_dynamic_expr(
            query,
            %Fragment{
              embedded?: pred_embedded?,
              arguments: [
                raw: "ash_elixir_or(",
                casted_expr: left_expr,
                raw: ", ",
                casted_expr: right_expr,
                raw: ")"
              ]
            },
            bindings,
            embedded?,
            acc,
            type
          )
        else
          if query.__ash_bindings__.sql_behaviour.require_ash_functions_for_or_and_and?() do
            require_ash_functions!(query, "||")
          end

          do_dynamic_expr(
            query,
            %Ash.Query.Function.Fragment{
              embedded?: pred_embedded?,
              arguments: [
                raw: "CASE WHEN (",
                casted_expr: left_expr,
                raw: " = FALSE OR ",
                casted_expr: left_expr,
                raw: " IS NULL) THEN ",
                casted_expr: right_expr,
                raw: " ELSE ",
                casted_expr: left_expr,
                raw: "END"
              ]
            },
            bindings,
            embedded?,
            acc,
            type
          )
        end

      :&& ->
        if "ash-functions" in query.__ash_bindings__.sql_behaviour.repo(
             query.__ash_bindings__.resource,
             :mutate
           ).installed_extensions() do
          do_dynamic_expr(
            query,
            %Fragment{
              embedded?: pred_embedded?,
              arguments: [
                raw: "ash_elixir_and(",
                casted_expr: left_expr,
                raw: ", ",
                casted_expr: right_expr,
                raw: ")"
              ]
            },
            bindings,
            embedded?,
            acc,
            type
          )
        else
          if query.__ash_bindings__.sql_behaviour.require_ash_functions_for_or_and_and?() do
            require_ash_functions!(query, "&&")
          end

          do_dynamic_expr(
            query,
            %Fragment{
              embedded?: pred_embedded?,
              arguments: [
                raw: "CASE WHEN (",
                casted_expr: left_expr,
                raw: " = FALSE OR ",
                casted_expr: left_expr,
                raw: " IS NULL) THEN ",
                casted_expr: left_expr,
                raw: " ELSE ",
                casted_expr: right_expr,
                raw: "END"
              ]
            },
            bindings,
            embedded?,
            acc,
            type
          )
        end

      other ->
        raise "Operator not implemented #{other}"
    end
  end

  defp default_dynamic_expr(query, %MapSet{} = mapset, bindings, embedded?, acc, type) do
    do_dynamic_expr(query, Enum.to_list(mapset), bindings, embedded?, acc, type)
  end

  defp default_dynamic_expr(
         query,
         %Ash.CiString{string: string} = expression,
         bindings,
         embedded?,
         acc,
         type
       ) do
    case query.__ash_bindings__.sql_behaviour.require_extension_for_citext() do
      {true, extension} ->
        require_extension!(query.__ash_bindings__.resource, extension, expression, query)

        do_dynamic_expr(
          query,
          %Ash.Query.Function.Fragment{arguments: [raw: "", expr: string, raw: "::citext"]},
          bindings,
          embedded?,
          acc,
          type
        )

      false ->
        do_dynamic_expr(
          query,
          %Ash.Query.Function.Type{arguments: [string, Ash.Type.CiString, []]},
          bindings,
          embedded?,
          acc,
          type
        )
    end
  end

  defp default_dynamic_expr(
         query,
         %Ref{
           attribute: %Ash.Query.Calculation{} = calculation,
           relationship_path: relationship_path
         } = type_expr,
         bindings,
         embedded?,
         acc,
         _type
       ) do
    calculation = %{calculation | load: calculation.name}

    type =
      bindings.sql_behaviour.parameterized_type(
        calculation.type,
        Map.get(calculation, :constraints, [])
      )

    validate_type!(query, type, type_expr)
    resource = Ash.Resource.Info.related(bindings.resource, relationship_path)

    case Ash.Filter.hydrate_refs(
           calculation.module.expression(calculation.opts, calculation.context),
           %{
             resource: resource,
             aggregates: %{},
             calculations: %{},
             public?: false
           }
         ) do
      {:ok, expression} ->
        expression =
          Ash.Filter.move_to_relationship_path(
            expression,
            relationship_path
          )

        expression =
          Ash.Actions.Read.add_calc_context_to_filter(
            expression,
            calculation.context.actor,
            calculation.context.authorize?,
            calculation.context.tenant,
            calculation.context.tracer,
            nil
          )

        do_dynamic_expr(
          query,
          expression,
          bindings,
          embedded?,
          acc,
          type
        )

      {:error, error} ->
        raise """
        Failed to hydrate references for resource #{inspect(resource)} in #{inspect(calculation.module.expression(calculation.opts, calculation.context))}

        #{inspect(error)}
        """
    end
  end

  defp default_dynamic_expr(
         query,
         %Ref{
           attribute: %Ash.Query.Aggregate{
             kind: :exists,
             relationship_path: agg_relationship_path,
             query: agg_query,
             join_filters: join_filters
           },
           relationship_path: ref_relationship_path
         },
         bindings,
         embedded?,
         acc,
         type
       ) do
    filter =
      if is_nil(agg_query.filter) do
        true
      else
        agg_query.filter
      end

    do_dynamic_expr(
      query,
      %Ash.Query.Exists{
        path: agg_relationship_path,
        expr: filter,
        at_path: ref_relationship_path
      }
      |> Map.put(:__join_filters__, join_filters),
      bindings,
      embedded?,
      acc,
      type
    )
  end

  defp default_dynamic_expr(
         query,
         %Ref{attribute: %Ash.Query.Aggregate{} = aggregate} = ref,
         bindings,
         _embedded?,
         acc,
         _type
       ) do
    %{attribute: aggregate} =
      ref =
      case bindings.aggregate_names[aggregate.name] do
        nil ->
          ref

        name ->
          %{ref | attribute: %{aggregate | name: name}}
      end

    related = Ash.Resource.Info.related(query.__ash_bindings__.resource, ref.relationship_path)

    first_optimized_aggregate? =
      AshSql.Aggregate.optimizable_first_aggregate?(related, aggregate, query)

    {ref_binding, field_name, value, acc} =
      if first_optimized_aggregate? do
        ref = %{
          ref
          | attribute: %Ash.Resource.Attribute{name: :fake},
            relationship_path: ref.relationship_path ++ aggregate.relationship_path
        }

        ref_binding = ref_binding(ref, bindings)

        if is_nil(ref_binding) do
          raise "Error while building reference: #{inspect(ref)}"
        end

        ref =
          %Ash.Query.Ref{
            attribute:
              AshSql.Aggregate.aggregate_field(
                aggregate,
                Ash.Resource.Info.related(query.__ash_bindings__.resource, ref.relationship_path),
                query
              ),
            relationship_path: ref.relationship_path,
            resource: query.__ash_bindings__.resource
          }

        ref =
          Ash.Actions.Read.add_calc_context_to_filter(
            ref,
            aggregate.context.actor,
            aggregate.context.authorize?,
            aggregate.context.tenant,
            aggregate.context.tracer,
            nil
          )

        {value, acc} = do_dynamic_expr(query, ref, query.__ash_bindings__, false, acc)

        case aggregate.field do
          %{name: name} -> name
          field -> field
        end

        {ref_binding, aggregate.field, value, acc}
      else
        ref_binding = ref_binding(ref, bindings)

        if is_nil(ref_binding) do
          raise "Error while building reference: #{inspect(ref)}"
        end

        {ref_binding, aggregate.name, nil, acc}
      end

    field_name =
      if is_binary(field_name) do
        new_field_name =
          query.__ash_bindings__.aggregate_names[field_name]

        unless new_field_name do
          raise "Unbound aggregate field: #{inspect(field_name)}"
        end

        new_field_name
      else
        field_name
      end

    expr =
      if value do
        value
      else
        if query.__ash_bindings__[:parent?] do
          if rewrite_to_non_parent_binding?(ref_binding, query) do
            Ecto.Query.dynamic(field(as(^ref_binding), ^field_name))
          else
            Ecto.Query.dynamic(field(parent_as(^ref_binding), ^field_name))
          end
        else
          Ecto.Query.dynamic(field(as(^ref_binding), ^field_name))
        end
      end

    type = bindings.sql_behaviour.parameterized_type(aggregate.type, aggregate.constraints)
    validate_type!(query, type, ref)

    type =
      if type && aggregate.kind == :list do
        {:array, type}
      else
        type
      end

    coalesced =
      if is_nil(aggregate.default_value) do
        expr
      else
        if type do
          typed_default =
            query.__ash_bindings__.sql_behaviour.type_expr(aggregate.default_value, type)

          Ecto.Query.dynamic(coalesce(^expr, ^typed_default))
        else
          Ecto.Query.dynamic(coalesce(^expr, ^aggregate.default_value))
        end
      end

    if type do
      {query.__ash_bindings__.sql_behaviour.type_expr(coalesced, type), acc}
    else
      {coalesced, acc}
    end
  end

  defp default_dynamic_expr(
         query,
         %Ash.CustomExpression{expression: expr},
         bindings,
         embedded?,
         acc,
         type
       ) do
    do_dynamic_expr(query, expr, bindings, embedded?, acc, type)
  end

  defp default_dynamic_expr(
         query,
         %Round{arguments: [num | rest], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         _type
       ) do
    precision = Enum.at(rest, 0) || 1

    frag =
      %Fragment{
        embedded?: pred_embedded?,
        arguments: [
          raw: "ROUND(",
          expr: num,
          raw: ", ",
          expr: precision,
          raw: ")"
        ]
      }

    do_dynamic_expr(query, frag, bindings, pred_embedded? || embedded?, acc)
  end

  defp default_dynamic_expr(
         query,
         %Type{
           arguments: [
             %Type{arguments: [_, type, constraints]} = nested_call,
             type,
             constraints
           ]
         },
         bindings,
         embedded?,
         acc,
         type
       ) do
    do_dynamic_expr(query, nested_call, bindings, embedded?, acc, type)
  end

  defp default_dynamic_expr(
         query,
         %Type{arguments: [arg1, arg2, constraints]},
         bindings,
         embedded?,
         acc,
         _type
       ) do
    arg2 = Ash.Type.get_type(arg2)
    arg1 = maybe_uuid_to_binary(arg2, arg1, arg1)
    type = bindings.sql_behaviour.parameterized_type(arg2, constraints)

    if type do
      {expr, acc} = do_dynamic_expr(query, arg1, bindings, embedded?, acc, type)

      case {type, expr} do
        {{:parameterized, Ash.Type.Map.EctoType, []}, %Ecto.Query.DynamicExpr{}} ->
          {expr, acc}

        {{:parameterized, {Ash.Type.Map.EctoType, []}}, %Ecto.Query.DynamicExpr{}} ->
          {expr, acc}

        _ ->
          {query.__ash_bindings__.sql_behaviour.type_expr(expr, type), acc}
      end
    else
      do_dynamic_expr(query, arg1, bindings, embedded?, acc, type)
    end
  end

  defp default_dynamic_expr(
         query,
         %CompositeType{arguments: [arg1, arg2, constraints], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         _type
       )
       when is_map(arg1) do
    type = Ash.Type.get_type(arg2)

    composite_keys = Ash.Type.composite_types(type, constraints)

    type = bindings.sql_behaviour.parameterized_type(type, constraints)

    values =
      composite_keys
      |> Enum.map(fn config ->
        key = elem(config, 0)
        {:expr, Map.get(arg1, key)}
      end)
      |> Enum.intersperse({:raw, ","})

    frag =
      %Fragment{
        embedded?: pred_embedded?,
        arguments:
          [
            raw: "ROW("
          ] ++
            values ++
            [
              raw: ")"
            ]
      }

    {frag, acc} =
      do_dynamic_expr(query, frag, bindings, embedded?, acc)

    {query.__ash_bindings__.sql_behaviour.type_expr(frag, type), acc}
  end

  defp default_dynamic_expr(
         query,
         %Now{embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         type
       ) do
    do_dynamic_expr(
      query,
      %Ash.Query.Function.Type{arguments: [DateTime.utc_now(), type || :datetime, []]},
      bindings,
      embedded? || pred_embedded?,
      acc,
      type
    )
  end

  defp default_dynamic_expr(
         query,
         %Today{embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         type
       ) do
    do_dynamic_expr(
      query,
      Date.utc_today(),
      bindings,
      embedded? || pred_embedded?,
      acc,
      type
    )
  end

  defp default_dynamic_expr(
         query,
         %Ash.Query.Parent{expr: expr},
         bindings,
         embedded?,
         acc,
         type
       ) do
    parent? = Map.get(bindings.parent_bindings, :parent_is_parent_as?, true)
    new_bindings = Map.put(bindings.parent_bindings, :parent?, parent?)

    do_dynamic_expr(
      %{
        query
        | __ash_bindings__: new_bindings
      },
      expr,
      new_bindings,
      embedded?,
      acc,
      type
    )
  end

  defp default_dynamic_expr(
         query,
         %Error{arguments: [exception, input]} = value,
         bindings,
         embedded?,
         acc,
         type
       ) do
    require_ash_functions!(query, "error/2")

    acc = %{acc | has_error?: true}

    unless Keyword.keyword?(input) || is_map(input) do
      raise "Input expression to `error` must be a map or keyword list"
    end

    {encoded, acc} =
      if Ash.Expr.expr?(input) do
        frag_parts =
          Enum.flat_map(input, fn {key, value} ->
            if Ash.Expr.expr?(value) do
              [
                expr: to_string(key),
                raw: "::text, ",
                expr: value,
                raw: ", "
              ]
            else
              [
                expr: to_string(key),
                raw: "::text, ",
                expr: value,
                raw: "::jsonb, "
              ]
            end
          end)

        frag_parts =
          List.update_at(frag_parts, -1, fn {:raw, text} ->
            {:raw, String.trim_trailing(text, ", ") <> "))"}
          end)

        do_dynamic_expr(
          query,
          %Fragment{
            embedded?: false,
            arguments:
              [
                raw: "jsonb_build_object('exception', ",
                expr: inspect(exception),
                raw: "::text, 'input', jsonb_build_object("
              ] ++
                frag_parts
          },
          bindings,
          embedded?,
          acc
        )
      else
        {Jason.encode!(%{exception: inspect(exception), input: Map.new(input)}), acc}
      end

    if type do
      # This is a type hint, if we're raising an error, we tell it what the value
      # type *would* be in this expression so that we can return a "NULL" of that type
      # its weird, but there isn't any other way that I can tell :)
      validate_type!(query, type, value)

      field_ref =
        case bindings[:updating_field] do
          nil ->
            nil

          ref ->
            Ecto.Query.dynamic([row], field(row, ^ref))
        end

      dynamic =
        query.__ash_bindings__.sql_behaviour.type_expr(field_ref, type)

      {Ecto.Query.dynamic(fragment("ash_raise_error(?::jsonb, ?)", ^encoded, ^dynamic)), acc}
    else
      if bindings[:updating_field] do
        field_ref = Ecto.Query.dynamic([row], field(row, ^bindings[:updating_field]))
        {Ecto.Query.dynamic(fragment("ash_raise_error(?::jsonb, ?)", ^encoded, ^field_ref)), acc}
      else
        {Ecto.Query.dynamic(fragment("ash_raise_error(?::jsonb)", ^encoded)), acc}
      end
    end
  end

  defp default_dynamic_expr(
         query,
         %Exists{at_path: at_path, path: [first | rest], expr: expr} = exists,
         bindings,
         _embedded?,
         acc,
         _type
       ) do
    resource = Ash.Resource.Info.related(bindings.resource, at_path)
    first_relationship = Ash.Resource.Info.relationship(resource, first)

    filter = Ash.Filter.move_to_relationship_path(expr, rest)

    filter =
      exists
      |> Map.get(:__join_filters__, %{})
      |> Map.fetch([first_relationship.name])
      |> case do
        {:ok, join_filter} ->
          Ash.Query.BooleanExpression.optimized_new(
            :and,
            filter,
            Ash.Filter.move_to_relationship_path(
              join_filter,
              rest ++ [first_relationship.name]
            )
          )

        :error ->
          filter
      end

    filter =
      exists
      |> Map.get(:__join_filters__, %{})
      |> Map.delete([first_relationship.name])
      |> Enum.reduce(filter, fn {path, path_filter}, filter ->
        path = Enum.drop(path, 1)
        parent_path = :lists.droplast(path)

        Ash.Query.BooleanExpression.optimized_new(
          :and,
          filter,
          Ash.Filter.move_to_relationship_path(path_filter, path)
        )
        |> Ash.Filter.map(fn
          %Ash.Query.Parent{expr: expr} ->
            {:halt, Ash.Filter.move_to_relationship_path(expr, parent_path)}

          other ->
            other
        end)
      end)

    {:ok, subquery} =
      AshSql.Join.related_subquery(first_relationship, query,
        filter: filter,
        filter_subquery?: true,
        sort?: Map.get(first_relationship, :from_many?),
        parent_resources: [
          query.__ash_bindings__.resource
          | query.__ash_bindings__[:parent_resources] || []
        ],
        return_subquery?: true,
        on_subquery: fn subquery ->
          subquery =
            Ecto.Query.from(row in subquery, select: row)
            |> Map.put(:__ash_bindings__, subquery.__ash_bindings__)

          cond do
            Map.get(first_relationship, :manual) ->
              {module, opts} = first_relationship.manual

              source_binding =
                ref_binding(
                  %Ref{
                    attribute:
                      Ash.Resource.Info.attribute(resource, first_relationship.source_attribute),
                    relationship_path: at_path,
                    resource: resource
                  },
                  bindings
                )

              {:ok, subquery} =
                apply(
                  module,
                  query.__ash_bindings__.sql_behaviour.manual_relationship_subquery_function,
                  [
                    opts,
                    source_binding,
                    0,
                    subquery
                  ]
                )

              subquery

            Map.get(first_relationship, :no_attributes?) ->
              subquery

            first_relationship.type == :many_to_many ->
              source_ref =
                ref_binding(
                  %Ref{
                    attribute:
                      Ash.Resource.Info.attribute(resource, first_relationship.source_attribute),
                    relationship_path: at_path,
                    resource: resource
                  },
                  bindings
                )

              through_relationship =
                Ash.Resource.Info.relationship(resource, first_relationship.join_relationship)

              {:ok, through} =
                AshSql.Join.related_subquery(through_relationship, query)

              Ecto.Query.from(destination in subquery,
                join: through in ^through,
                as: ^subquery.__ash_bindings__.current,
                on:
                  field(through, ^first_relationship.destination_attribute_on_join_resource) ==
                    field(destination, ^first_relationship.destination_attribute),
                on:
                  field(parent_as(^source_ref), ^first_relationship.source_attribute) ==
                    field(through, ^first_relationship.source_attribute_on_join_resource)
              )

            true ->
              source_ref =
                ref_binding(
                  %Ref{
                    attribute:
                      Ash.Resource.Info.attribute(resource, first_relationship.source_attribute),
                    relationship_path: at_path,
                    resource: resource
                  },
                  bindings
                )

              Ecto.Query.from(destination in subquery,
                where:
                  field(parent_as(^source_ref), ^first_relationship.source_attribute) ==
                    field(destination, ^first_relationship.destination_attribute)
              )
          end
        end
      )

    {Ecto.Query.dynamic(exists(subquery)), acc}
  end

  defp default_dynamic_expr(
         query,
         %Ref{
           attribute: %Ash.Resource.Attribute{
             name: name,
             type: attr_type,
             constraints: constraints
           }
         } = ref,
         bindings,
         _embedded?,
         acc,
         expr_type
       ) do
    ref_binding = ref_binding(ref, bindings)

    if is_nil(ref_binding) do
      raise "Error while building reference: #{inspect(ref)}"
    end

    constraints =
      if attr_type do
        constraints
      end

    expr =
      case bindings.sql_behaviour.parameterized_type(attr_type || expr_type, constraints) do
        nil ->
          if query.__ash_bindings__[:parent?] do
            if rewrite_to_non_parent_binding?(ref_binding, query) do
              Ecto.Query.dynamic(field(as(^ref_binding), ^name))
            else
              Ecto.Query.dynamic(field(parent_as(^ref_binding), ^name))
            end
          else
            Ecto.Query.dynamic(field(as(^ref_binding), ^name))
          end

        type ->
          validate_type!(query, type, ref)

          ref_dynamic =
            if query.__ash_bindings__[:parent?] do
              if rewrite_to_non_parent_binding?(ref_binding, query) do
                Ecto.Query.dynamic(field(as(^ref_binding), ^name))
              else
                Ecto.Query.dynamic(field(parent_as(^ref_binding), ^name))
              end
            else
              Ecto.Query.dynamic(field(as(^ref_binding), ^name))
            end

          query.__ash_bindings__.sql_behaviour.type_expr(ref_dynamic, type)
      end

    {expr, acc}
  end

  defp default_dynamic_expr(
         query,
         %Ref{attribute: %Ash.Resource.Aggregate{name: name}} = ref,
         bindings,
         _embedded?,
         acc,
         _expr_type
       ) do
    ref_binding = ref_binding(ref, bindings)

    if is_nil(ref_binding) do
      raise "Error while building reference: #{inspect(ref)}"
    end

    expr =
      if query.__ash_bindings__[:parent?] do
        if rewrite_to_non_parent_binding?(ref_binding, query) do
          Ecto.Query.dynamic(field(as(^ref_binding), ^name))
        else
          Ecto.Query.dynamic(field(parent_as(^ref_binding), ^name))
        end
      else
        Ecto.Query.dynamic(field(as(^ref_binding), ^name))
      end

    {expr, acc}
  end

  defp default_dynamic_expr(_query, %Ash.Vector{} = value, _bindings, _embedded?, acc, _type) do
    {value, acc}
  end

  defp default_dynamic_expr(query, value, bindings, embedded?, acc, type)
       when is_map(value) and not is_struct(value) do
    if bindings[:location] == :update do
      elements =
        value
        |> Enum.flat_map(fn {key, list_item} ->
          list_item = reverse_engineer_type(list_item)

          if is_atom(key) do
            [
              {:expr, %Ash.Query.Function.Type{arguments: [key, :atom, []]}},
              [
                {:raw, "("},
                {:expr, list_item},
                {:raw, ")"}
              ]
            ]
          else
            [
              {:expr, %Ash.Query.Function.Type{arguments: [key, :string, []]}},
              [{:raw, "("}, {:expr, list_item}, {:raw, ")"}]
            ]
          end
        end)
        |> Enum.intersperse({:raw, ","})
        |> List.flatten()

      do_dynamic_expr(
        query,
        %Fragment{
          embedded?: embedded?,
          arguments:
            squash_raw(
              [
                raw: "jsonb_build_object("
              ] ++ elements ++ [raw: ")"]
            )
        },
        bindings,
        embedded?,
        acc,
        type
      )
    else
      {value, acc} =
        Enum.reduce(value, {%{}, acc}, fn {key, value}, {map, acc} ->
          {value, acc} = do_dynamic_expr(query, value, bindings, embedded?, acc)

          {Map.put(map, key, value), acc}
        end)

      if embedded? do
        {query.__ash_bindings__.sql_behaviour.type_expr(value, :map), acc}
      else
        {value, acc}
      end
    end
  end

  defp default_dynamic_expr(query, other, bindings, true, acc, type) do
    if other && is_atom(other) && !is_boolean(other) do
      {to_string(other), acc}
    else
      if Ash.Expr.expr?(other) do
        if is_list(other) do
          list_expr(query, other, bindings, true, acc, type)
        else
          case bindings.sql_behaviour.expr(query, other, bindings, true, acc, type) do
            {:ok, expr, acc} ->
              {expr, acc}

            {:error, error} ->
              raise "Error while building expression: #{error}"

            :error ->
              raise "Unsupported expression in #{bindings.sql_behaviour} query: #{inspect(other, structs: false)}"
          end
        end
      else
        maybe_sanitize_list(query, other, bindings, true, acc, type)
      end
    end
  end

  defp default_dynamic_expr(query, value, bindings, embedded?, acc, {:in, type})
       when is_list(value) do
    list_expr(query, value, bindings, embedded?, acc, {:array, type})
  end

  defp default_dynamic_expr(query, value, bindings, embedded?, acc, type)
       when not is_nil(value) and is_atom(value) and not is_boolean(value) do
    do_dynamic_expr(query, to_string(value), bindings, embedded?, acc, type)
  end

  defp default_dynamic_expr(query, value, bindings, false, acc, type)
       when type == nil or type == :any do
    if is_list(value) do
      list_expr(query, value, bindings, false, acc, type)
    else
      maybe_sanitize_list(query, value, bindings, true, acc, type)
    end
  end

  defp default_dynamic_expr(query, value, bindings, false, acc, type) do
    if Ash.Expr.expr?(value) do
      if is_list(value) do
        list_expr(query, value, bindings, false, acc, type)
      else
        case bindings.sql_behaviour.expr(query, value, bindings, false, acc, type) do
          {:ok, expr, acc} ->
            {expr, acc}

          {:error, error} ->
            raise "Error while building expression: #{error}"

          :error ->
            raise "Unsupported expression in #{bindings.sql_behaviour} query: #{inspect(value, structs: false)}"
        end
      end
    else
      case maybe_sanitize_list(query, value, bindings, true, acc, type) do
        {^value, acc} ->
          if type do
            validate_type!(query, type, value)

            {query.__ash_bindings__.sql_behaviour.type_expr(value, type), acc}
          else
            {value, acc}
          end

        {value, acc} ->
          {value, acc}
      end
    end
  end

  defp squash_raw(list, trail \\ [])
  defp squash_raw([], trail), do: Enum.reverse(trail)

  defp squash_raw([{:raw, left}, {:raw, right} | rest], trail) do
    squash_raw([{:raw, left <> right} | rest], trail)
  end

  defp squash_raw([other | rest], trail), do: squash_raw(rest, [other | trail])

  # I literally hate this
  defp reverse_engineer_type(value) do
    case value do
      list_item when is_integer(list_item) ->
        %Type{arguments: [list_item, :integer, []]}

      list_item when is_float(list_item) ->
        %Type{arguments: [list_item, :float, []]}

      list_item when is_boolean(list_item) ->
        %Type{arguments: [list_item, :boolean, []]}

      list_item when is_binary(list_item) ->
        %Type{arguments: [list_item, :string, []]}

      list_item when is_atom(list_item) ->
        %Type{arguments: [list_item, :atom, []]}

      [] ->
        %Type{arguments: [[], {:array, :string}, []]}

      [item | _] = list_item ->
        %{arguments: [_, type, constraints]} = type_func = reverse_engineer_type(item)
        %{type_func | arguments: [list_item, {:array, type}, [items: constraints || []]]}

      %{} = list_item ->
        %Type{arguments: [list_item, :map, []]}

      %Decimal{} = list_item ->
        %Type{arguments: [list_item, :decimal, []]}

      list_item ->
        list_item
    end
  end

  defp extract_cases(
         query,
         expr,
         bindings,
         embedded?,
         acc,
         type,
         list_acc \\ []
       )

  defp extract_cases(
         query,
         %If{arguments: [condition, when_true, when_false], embedded?: pred_embedded?},
         bindings,
         embedded?,
         acc,
         type,
         list_acc
       ) do
    [condition_type, when_true_type, when_false_type] =
      case bindings.sql_behaviour.determine_types(If, [condition, when_true, when_false]) do
        [condition_type, when_true] ->
          [condition_type, when_true, nil]

        [condition_type, when_true, when_false] ->
          [condition_type, when_true, when_false]
      end
      |> case do
        [condition_type, nil, nil] ->
          [condition_type, type, type]

        [condition_type, when_true, nil] ->
          [condition_type, when_true, type]

        [condition_type, nil, when_false] ->
          [condition_type, type, when_false]

        [condition_type, when_true, when_false] ->
          [condition_type, when_true, when_false]
      end

    {condition, acc} =
      do_dynamic_expr(
        query,
        condition,
        bindings,
        pred_embedded? || embedded?,
        acc,
        condition_type
      )

    {when_true, acc} =
      do_dynamic_expr(
        query,
        when_true,
        bindings,
        pred_embedded? || embedded?,
        acc,
        when_true_type
      )

    extract_cases(
      query,
      when_false,
      bindings,
      embedded?,
      acc,
      when_false_type,
      [{condition, when_true} | list_acc]
    )
  end

  defp extract_cases(
         query,
         other,
         bindings,
         embedded?,
         acc,
         type,
         list_acc
       ) do
    {expr, acc} =
      do_dynamic_expr(
        query,
        other,
        bindings,
        embedded?,
        acc,
        type
      )

    {Enum.reverse(list_acc), expr, acc}
  end

  defp split_at_paths(type, constraints, next, acc \\ [{:bracket, [], nil, nil}])

  defp split_at_paths(_type, _constraints, [], acc) do
    acc
  end

  defp split_at_paths({:array, type}, constraints, [next | rest], [first_acc | rest_acc])
       when is_integer(next) do
    case first_acc do
      {:bracket, path, nil, nil} ->
        split_at_paths(type, constraints[:items] || [], rest, [
          {:bracket, [next | path], type, constraints}
          | rest_acc
        ])

      {:dot, _field, _, _} ->
        split_at_paths(type, constraints[:items] || [], rest, [
          {:bracket, [next], type, constraints},
          first_acc
          | rest_acc
        ])
    end
  end

  defp split_at_paths(type, constraints, [next | rest], [first_acc | rest_acc])
       when is_atom(next) or is_binary(next) do
    bracket_or_dot =
      if type && Ash.Type.composite?(type, constraints) do
        :dot
      else
        :bracket
      end

    {next, type, constraints} =
      cond do
        type && Ash.Type.embedded_type?(type) ->
          type =
            if Ash.Type.NewType.new_type?(type) do
              Ash.Type.NewType.subtype_of(type)
            else
              type
            end

          %{type: type, constraints: constraints} = Ash.Resource.Info.attribute(type, next)
          {next, type, constraints}

        type && Ash.Type.composite?(type, constraints) ->
          condition =
            if is_binary(next) do
              fn {name, _type, _constraints} ->
                to_string(name) == next
              end
            else
              fn {name, _type, _constraints} ->
                name == next
              end
            end

          case Enum.find(Ash.Type.composite_types(type, constraints), condition) do
            nil ->
              {next, nil, nil}

            {_, aliased_as, type, constraints} ->
              {aliased_as, type, constraints}

            {name, type, constraints} ->
              {name, type, constraints}
          end

        true ->
          {next, nil, nil}
      end

    case bracket_or_dot do
      :dot ->
        case first_acc do
          {:bracket, [], _, _} ->
            split_at_paths(type, constraints, rest, [
              {bracket_or_dot, [next], type, constraints} | rest_acc
            ])

          {:bracket, path, nil, nil} ->
            split_at_paths(type, constraints, rest, [
              {bracket_or_dot, [next], type, constraints},
              {:bracket, path, nil, nil}
              | rest_acc
            ])

          {:dot, _path, _, _} ->
            split_at_paths(type, constraints, rest, [
              {bracket_or_dot, [next], nil, nil},
              first_acc | rest_acc
            ])
        end

      :bracket ->
        case first_acc do
          {:bracket, path, nil, nil} ->
            split_at_paths(type, constraints, rest, [
              {bracket_or_dot, [next | path], type, constraints}
              | rest_acc
            ])

          {:dot, _path, _, _} ->
            split_at_paths(type, constraints, rest, [
              {bracket_or_dot, [next], nil, nil},
              first_acc | rest_acc
            ])
        end
    end
  end

  defp list_requires_encoding?(value) do
    !Enum.empty?(value) &&
      Enum.any?(value, fn value ->
        Ash.Expr.expr?(value) || is_map(value) || is_list(value)
      end)
  end

  defp encode_list(query, value, bindings, embedded?, acc, type) do
    type =
      case type do
        {:array, type} -> type
        {:in, type} -> type
        _ -> nil
      end

    elements =
      Enum.map(value, fn list_item ->
        if type do
          {:expr, %Ash.Query.Function.Type{arguments: [list_item, type, []]}}
        else
          {:expr, list_item}
        end
      end)
      |> Enum.intersperse({:raw, ","})

    do_dynamic_expr(
      query,
      %Fragment{
        embedded?: embedded?,
        arguments:
          [
            raw: "ARRAY["
          ] ++ elements ++ [raw: "]"]
      },
      bindings,
      embedded?,
      acc,
      type
    )
  end

  defp list_expr(query, value, bindings, embedded?, acc, type) do
    if list_requires_encoding?(value) do
      encode_list(query, value, bindings, embedded?, acc, type)
    else
      type =
        case type do
          {:array, type} -> type
          {:in, type} -> type
          _ -> nil
        end

      {params, exprs, _, acc} =
        Enum.reduce(value, {[], [], 0, acc}, fn value, {params, data, count, acc} ->
          case do_dynamic_expr(query, value, bindings, embedded?, acc, type) do
            {%Ecto.Query.DynamicExpr{} = dynamic, acc} ->
              result =
                Ecto.Query.Builder.Dynamic.partially_expand(
                  :select,
                  query,
                  dynamic,
                  params,
                  count
                )

              expr = elem(result, 0)
              new_params = elem(result, 1)
              new_count = result |> Tuple.to_list() |> List.last()

              {new_params, [expr | data], new_count, acc}

            {other, acc} ->
              {params, [other | data], count, acc}
          end
        end)

      {%Ecto.Query.DynamicExpr{
         fun: fn _query ->
           {Enum.reverse(exprs), Enum.reverse(params), [], []}
         end,
         binding: [],
         file: __ENV__.file,
         line: __ENV__.line
       }, acc}
    end
  end

  defp maybe_uuid_to_binary({:array, type}, value, _original_value) when is_list(value) do
    Enum.map(value, &maybe_uuid_to_binary(type, &1, &1))
  end

  defp maybe_uuid_to_binary(type, value, original_value)
       when type in [
              Ash.Type.UUID.EctoType,
              :uuid
            ] and is_binary(value) do
    case Ecto.UUID.dump(value) do
      {:ok, encoded} -> encoded
      _ -> original_value
    end
  end

  defp maybe_uuid_to_binary(_type, _value, original_value), do: original_value

  @doc false
  def validate_type!(%{__ash_bindings__: %{resource: resource}} = query, type, context) do
    validate_type!(resource, type, context, query)
  end

  defp validate_type!(resource, type, context, query) do
    case query.__ash_bindings__.sql_behaviour.require_extension_for_citext() do
      {true, extension} ->
        case type do
          {:parameterized, AshSql.Type.CiString, _} ->
            require_extension!(resource, extension, context, query)

          {:parameterized, {AshSql.Type.CiString, _}} ->
            require_extension!(resource, extension, context, query)

          :ci_string ->
            require_extension!(resource, extension, context, query)

          :citext ->
            require_extension!(resource, extension, context, query)

          _ ->
            :ok
        end

      false ->
        :ok
    end
  end

  defp maybe_sanitize_list(query, value, bindings, embedded?, acc, type) do
    if is_list(value) do
      if list_requires_encoding?(value) do
        encode_list(query, value, bindings, embedded?, acc, type)
      else
        value
        |> Enum.reduce({[], acc}, fn item, {list, acc} ->
          {new_item, acc} = do_dynamic_expr(query, item, bindings, embedded?, acc, type)

          {[new_item | list], acc}
        end)
        |> then(fn {list, acc} ->
          {Enum.reverse(list), acc}
        end)
      end
    else
      case bindings.sql_behaviour.expr(query, value, bindings, true, acc, type) do
        {:ok, expr, acc} ->
          {expr, acc}

        {:error, error} ->
          raise "Error while building expression: #{error}"

        :error ->
          {value, acc}
      end
    end
  end

  defp ref_binding(
         %{attribute: %Ash.Query.Aggregate{name: name}, relationship_path: relationship_path},
         bindings
       ) do
    Enum.find_value(bindings.bindings, fn {binding, data} ->
      data.type == :aggregate &&
        data.path == relationship_path &&
        Enum.any?(data.aggregates, &(&1.name == name)) && binding
    end) ||
      Enum.find_value(bindings.bindings, fn {binding, data} ->
        data.type in [:inner, :left, :root] &&
          Ash.SatSolver.synonymous_relationship_paths?(
            bindings.resource,
            data.path,
            relationship_path
          ) && binding
      end)
  end

  defp ref_binding(
         %{
           attribute: %Ash.Resource.Aggregate{name: name},
           relationship_path: relationship_path
         },
         bindings
       ) do
    Enum.find_value(bindings.bindings, fn {binding, data} ->
      data.type == :aggregate &&
        data.path == relationship_path &&
        Enum.any?(data.aggregates, &(&1.name == name)) && binding
    end)
  end

  defp ref_binding(%{attribute: %Ash.Resource.Attribute{}} = ref, bindings) do
    Enum.find_value(bindings.bindings, fn {binding, data} ->
      data.type in [:inner, :left, :root] &&
        Ash.SatSolver.synonymous_relationship_paths?(
          bindings.resource,
          data.path,
          ref.relationship_path
        ) && binding
    end)
  end

  defp do_get_path(
         query,
         expr,
         {:bracket, path, type, constraints},
         bindings,
         embedded?,
         pred_embedded?,
         acc
       ) do
    type = bindings.sql_behaviour.parameterized_type(type, constraints)
    path = path |> Enum.reverse() |> Enum.map(&to_string/1)

    path_frags =
      path
      |> Enum.flat_map(fn item ->
        [expr: item, raw: "::text,"]
      end)
      |> :lists.droplast()
      |> Enum.concat(raw: "::text)")

    {expr, acc} =
      do_dynamic_expr(
        query,
        %Fragment{
          embedded?: pred_embedded?,
          arguments:
            [
              raw: "jsonb_extract_path_text(",
              expr: expr,
              raw: "::jsonb,"
            ] ++ path_frags
        },
        bindings,
        embedded?,
        acc
      )

    if type do
      {query.__ash_bindings__.sql_behaviour.type_expr(expr, type), acc}
    else
      {expr, acc}
    end
  end

  defp do_get_path(
         query,
         expr,
         {:dot, [field], type, constraints},
         bindings,
         embedded?,
         pred_embedded?,
         acc
       )
       when is_atom(field) do
    type = bindings.sql_behaviour.parameterized_type(type, constraints)

    {expr, acc} =
      do_dynamic_expr(
        query,
        %Fragment{
          embedded?: pred_embedded?,
          arguments: [
            raw: "(",
            expr: expr,
            raw: ").#{field}"
          ]
        },
        bindings,
        embedded?,
        acc
      )

    if type do
      {query.__ash_bindings__.sql_behaviour.type_expr(expr, type), acc}
    else
      {expr, acc}
    end
  end

  defp require_ash_functions!(query, operator) do
    installed_extensions =
      query.__ash_bindings__.sql_behaviour.repo(query.__ash_bindings__.resource, :mutate).installed_extensions()

    unless "ash-functions" in installed_extensions do
      raise """
      Cannot use `#{operator}` without adding the extension `ash-functions` to your repo.

      Add it to the list in `installed_extensions/0` and generate migrations.
      """
    end
  end

  defp require_extension!(resource, extension, context, query) do
    repo = query.__ash_bindings__.sql_behaviour.repo(resource, :mutate)

    if extension not in repo.installed_extensions() do
      raise Ash.Error.Query.InvalidExpression,
        expression: context,
        message:
          "The #{extension} extension needs to be installed before #{inspect(context)} can be used. Please add \"#{extension}\" to the list of installed_extensions in #{inspect(repo)}."
    end
  end

  @doc false
  def set_parent_path(query, parent, parent_is_parent_as? \\ true) do
    # This is a stupid name. Its actually the path we *remove* when stepping up a level. I.e the child's path
    Map.update!(query, :__ash_bindings__, fn ash_bindings ->
      ash_bindings
      |> Map.put(
        :parent_bindings,
        parent.__ash_bindings__ |> Map.put(:parent_is_parent_as?, parent_is_parent_as?)
      )
      |> Map.put(:parent_resources, [
        parent.__ash_bindings__.resource | parent.__ash_bindings__[:parent_resources] || []
      ])
    end)
  end

  @doc false
  def merge_accumulator(
        %{__ash_bindings__: %{expression_accumulator: expression_accumulator}} = query,
        right
      ) do
    %{
      query
      | __ash_bindings__: %{
          query.__ash_bindings__
          | expression_accumulator: merge_accumulator(expression_accumulator, right)
        }
    }
  end

  @doc false
  def merge_accumulator(%ExprInfo{has_error?: left_has_error?}, %ExprInfo{
        has_error?: right_has_error?
      }) do
    %ExprInfo{has_error?: left_has_error? || right_has_error?}
  end

  @doc false
  def split_statements(%Ash.Filter{expression: expression}, op) do
    split_statements(expression, op)
  end

  def split_statements(
        %Not{
          expression: %BooleanExpression{op: :or, left: left, right: right}
        },
        op
      ) do
    split_statements(
      %BooleanExpression{
        op: :and,
        left: %Not{expression: left},
        right: %Not{expression: right}
      },
      op
    )
  end

  def split_statements(%Not{expression: %Not{expression: expression}}, op) do
    split_statements(expression, op)
  end

  def split_statements(%BooleanExpression{op: op, left: left, right: right}, op) do
    split_statements(left, op) ++ split_statements(right, op)
  end

  def split_statements(other, _op), do: [other]

  defp escape_contains(text) do
    "%" <> String.replace(text, ~r/([\%_])/u, "\\\\\\0") <> "%"
  end

  defp rewrite_to_non_parent_binding?(ref_binding, query) do
    with {_, [_, last_rel]} <- query.__ash_bindings__.context[:data_layer][:lateral_join_source],
         [maybe_join_rel] <- query.__ash_bindings__.bindings[ref_binding][:path],
         true <- elem(last_rel, 3).name == maybe_join_rel do
      true
    else
      _ ->
        false
    end
  end
end
