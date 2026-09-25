<!--
SPDX-FileCopyrightText: 2020 Zach Daniel

SPDX-License-Identifier: MIT
-->
![Elixir CI](https://github.com/ash-project/ash_sql/workflows/CI/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Hex version badge](https://img.shields.io/hexpm/v/ash_sql.svg)](https://hex.pm/packages/ash_sql)
[![Hexdocs badge](https://img.shields.io/badge/docs-hexdocs-purple)](https://hexdocs.pm/ash_sql)
[![REUSE status](https://api.reuse.software/badge/github.com/ash-project/ash_sql)](https://api.reuse.software/info/github.com/ash-project/ash_sql)


# AshSql

Shared functionality for ecto-based sql data layers.

## Installation

```elixir
def deps do
  [
    {:ash_sql, "~> 0.7.6"}
  ]
end
```

## Aggregate Strategies

`AshSql.Implementation` defaults aggregate planning to `:lateral`. SQL data
layers can override `aggregate_strategy/1` with `:grouped` when they need the
SQLite-style grouped aggregate implementation.

The grouped strategy uses adapter-provided list aggregation. Implementations
that select `:grouped` must implement `grouped_list_aggregate/2` and return a
list expression over the `:ash_sql_grouped_aggregate_window` window. The
grouped strategy assumes SQLite-compatible SQL: list defaults are JSON-encoded,
so list values must use a JSON list representation, and offset-only query
aggregates use `LIMIT -1`. AshSQLite uses SQLite's JSON list representation
for this callback.

The aggregate facade normalizes resource aggregates and SQL aliases before
dispatch. Both strategies preserve the source binding and attachment path, so
aggregates referenced through joined relationships attach to the related row.
Alias reuse is scoped to that path and follows the existing filter/sort identity
rules. Public aggregate names, including strings, are retained in results.
