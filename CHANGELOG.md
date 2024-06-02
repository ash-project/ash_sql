# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](Https://conventionalcommits.org) for commit guidelines.

<!-- changelog -->

## [v0.2.1](https://github.com/ash-project/ash_sql/compare/v0.2.0...v0.2.1) (2024-06-02)




### Improvements:

* select dynamics uses `__new_` prefix

## [v0.2.0](https://github.com/ash-project/ash_sql/compare/v0.1.3...v0.2.0) (2024-05-29)




### Features:

* add auto dispatch of dynamic_expr calls to behaviour module (#33)

* add auto dispatch of dynamic_expr calls to behaviour module

### Bug Fixes:

* match on new & old parameterized types

### Improvements:

* support selecting atomic results into a subquery, and using those as the atomic values

## [v0.1.3](https://github.com/ash-project/ash_sql/compare/v0.1.2...v0.1.3) (2024-05-22)




### Bug Fixes:

* handle anonymous sorting aggregates

* properly set aggregate source binding when adding aggregate calculations

* use period notation to access aggregate context fields (#30)

* use SQL standard = instead of non standard == (#28)

## [v0.1.2](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.20...v0.1.2) (2024-05-10)




## [v0.1.1-rc.20](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.19...v0.1.1-rc.20) (2024-05-08)




### Bug Fixes:

* don't use `fragment("1")` because ecto requires a proper select

## [v0.1.1-rc.19](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.18...v0.1.1-rc.19) (2024-05-05)




### Bug Fixes:

* use calculation context, and set calculation constraints for aggregates

## [v0.1.1-rc.18](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.17...v0.1.1-rc.18) (2024-05-05)




### Bug Fixes:

* don't use `ilike` if its not supported

* use type for now expr if available

## [v0.1.1-rc.17](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.16...v0.1.1-rc.17) (2024-05-02)




### Bug Fixes:

* use manual relationship impl for exists subqueries

## [v0.1.1-rc.16](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.15...v0.1.1-rc.16) (2024-05-01)




### Bug Fixes:

* hydrate & fill template for related queries

## [v0.1.1-rc.15](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.14...v0.1.1-rc.15) (2024-04-29)




### Bug Fixes:

* properly support custom expressions

## [v0.1.1-rc.14](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.13...v0.1.1-rc.14) (2024-04-29)




### Bug Fixes:

* fix argument order in AshSql.Bindings.default_bindings/4

* query_with_atomics pattern matching error

* fix argument order in AshSql.Bindings.default_bindings/4

## [v0.1.1-rc.13](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.12...v0.1.1-rc.13) (2024-04-27)




### Improvements:

* better inner-join-ability detection

## [v0.1.1-rc.12](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.11...v0.1.1-rc.12) (2024-04-26)




### Improvements:

* better type casting logic

## [v0.1.1-rc.11](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.10...v0.1.1-rc.11) (2024-04-23)




### Bug Fixes:

* ensure tenant is properly set in aggregates

* properly pass context through when expanding calculations in aggregates

## [v0.1.1-rc.10](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.9...v0.1.1-rc.10) (2024-04-22)




### Improvements:

* optimize `contains` when used with literal strings

## [v0.1.1-rc.9](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.8...v0.1.1-rc.9) (2024-04-22)




### Bug Fixes:

* make `strpos_function` overridable (sqlite uses `instr`)

## [v0.1.1-rc.8](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.7...v0.1.1-rc.8) (2024-04-22)




### Bug Fixes:

* handle non-literal lists in string join

## [v0.1.1-rc.7](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.6...v0.1.1-rc.7) (2024-04-20)




### Bug Fixes:

* ensure that `from_many?` is properly honored

* ensure applied query gets joined

* apply related filter inside of related subquery

## [v0.1.1-rc.6](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.5...v0.1.1-rc.6) (2024-04-12)




### Improvements:

* apply aggregate filters on first join aggregates

## [v0.1.1-rc.5](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.4...v0.1.1-rc.5) (2024-04-11)




### Bug Fixes:

* don't use to_tenant

* loosen elixir requirements

### Improvements:

* automatically wrap fragments in parenthesis

* remove unnecessary parenthesis from builtin fragments

## [v0.1.1-rc.4](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.3...v0.1.1-rc.4) (2024-04-05)




### Improvements:

* loosen ash rc restriction

## [v0.1.1-rc.3](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.2...v0.1.1-rc.3) (2024-04-01)




### Bug Fixes:

* fixes for `ash_postgres`

## [v0.1.1-rc.2](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.1...v0.1.1-rc.2) (2024-04-01)




### Improvements:

* refactoring out and parameterization to support ash_sqlite

## [v0.1.1-rc.1](https://github.com/ash-project/ash_sql/compare/v0.1.1-rc.0...v0.1.1-rc.1) (2024-04-01)




### Improvements:

* remove postgres-specific copy-pasta

## [v0.1.1-rc.0](https://github.com/ash-project/ash_sql/compare/v0.1.0...v0.1.1-rc.0) (2024-04-01)




## [v0.1.0](https://github.com/ash-project/ash_sql/compare/v0.1.0...v0.1.0) (2024-04-01)




### Improvements:

* extract a bunch of things out of AshPostgres
