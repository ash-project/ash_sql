# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](Https://conventionalcommits.org) for commit guidelines.

<!-- changelog -->

## [v0.2.36](https://github.com/ash-project/ash_sql/compare/v0.2.35...v0.2.36) (2024-10-08)




### Bug Fixes:

* properly handle parent bindings in aggregate references

## [v0.2.35](https://github.com/ash-project/ash_sql/compare/v0.2.34...v0.2.35) (2024-10-07)




### Bug Fixes:

* properly group aggregates together

* don't attempt to add multiple filter statements to a single aggregate

## [v0.2.34](https://github.com/ash-project/ash_sql/compare/v0.2.33...v0.2.34) (2024-09-27)




### Bug Fixes:

* use `NULL` for cases where we get `nil` values. We actually want `nil` here.

## [v0.2.33](https://github.com/ash-project/ash_sql/compare/v0.2.32...v0.2.33) (2024-09-26)




### Bug Fixes:

* don't reorder selects when modifying for subquery presence

## [v0.2.25](https://github.com/ash-project/ash_sql/compare/v0.2.24...v0.2.25) (2024-07-22)




### Bug Fixes:

* track subqueries while selecting atomics

## [v0.2.24](https://github.com/ash-project/ash_sql/compare/v0.2.23...v0.2.24) (2024-07-17)




### Bug Fixes:

* properly determine `parent_as` bindings for nested joins

## [v0.2.23](https://github.com/ash-project/ash_sql/compare/v0.2.22...v0.2.23) (2024-07-17)




### Bug Fixes:

* fix build

* properly expand resource calculation/aggregates in fields

## [v0.2.22](https://github.com/ash-project/ash_sql/compare/v0.2.21...v0.2.22) (2024-07-16)




### Bug Fixes:

* properly adjust calculation expressions before adding to query

* properly traverse nested maps in non-select contexts

* pass correct resource down when adding calculation fields

## [v0.2.21](https://github.com/ash-project/ash_sql/compare/v0.2.20...v0.2.21) (2024-07-16)




### Bug Fixes:

* move `FILTER` outside of `array_agg` aggregation

* properly honor `include_nil?` option on sorted first aggregates

## [v0.2.20](https://github.com/ash-project/ash_sql/compare/v0.2.19...v0.2.20) (2024-07-15)




### Bug Fixes:

* don't set load on anonymous aggregates

## [v0.2.19](https://github.com/ash-project/ash_sql/compare/v0.2.18...v0.2.19) (2024-07-15)




### Bug Fixes:

* match on old return types for `determine_types` code

## [v0.2.18](https://github.com/ash-project/ash_sql/compare/v0.2.17...v0.2.18) (2024-07-15)




### Bug Fixes:

* properly set aggregate query context

## [v0.2.17](https://github.com/ash-project/ash_sql/compare/v0.2.16...v0.2.17) (2024-07-14)




### Improvements:

* use `determine_types/3` in callback

## [v0.2.16](https://github.com/ash-project/ash_sql/compare/v0.2.15...v0.2.16) (2024-07-14)




### Bug Fixes:

* cast atomics when creating expressions

### Improvements:

* support latest format for type determination type

## [v0.2.15](https://github.com/ash-project/ash_sql/compare/v0.2.14...v0.2.15) (2024-07-13)




### Bug Fixes:

* use original field for type signal when selecting atomics

## [v0.2.14](https://github.com/ash-project/ash_sql/compare/v0.2.13...v0.2.14) (2024-07-13)




### Improvements:

* use explicit `NULL` fragment in error type hints

## [v0.2.13](https://github.com/ash-project/ash_sql/compare/v0.2.12...v0.2.13) (2024-07-12)



### Bug Fixes:

* use expression type for atomic updates

## [v0.2.12](https://github.com/ash-project/ash_sql/compare/v0.2.11...v0.2.12) (2024-07-12)




### Bug Fixes:

* properly handle nested `nil` filters in boolean statements

## [v0.2.11](https://github.com/ash-project/ash_sql/compare/v0.2.10...v0.2.11) (2024-07-11)




### Improvements:

* select pkey so data layers don't have to

## [v0.2.10](https://github.com/ash-project/ash_sql/compare/v0.2.9...v0.2.10) (2024-07-08)




### Bug Fixes:

* ensure selected atomics are also reversed

## [v0.2.9](https://github.com/ash-project/ash_sql/compare/v0.2.8...v0.2.9) (2024-07-08)




### Bug Fixes:

* retain original order for atomics statements

## [v0.2.8](https://github.com/ash-project/ash_sql/compare/v0.2.7...v0.2.8) (2024-07-06)




### Improvements:

* handle `{:array, :map}` stored as `:map`

## [v0.2.7](https://github.com/ash-project/ash_sql/compare/v0.2.6...v0.2.7) (2024-06-27)




### Bug Fixes:

* prefer resource's static prefix over current query's prefix

## [v0.2.6](https://github.com/ash-project/ash_sql/compare/v0.2.5...v0.2.6) (2024-06-18)




### Bug Fixes:

* ensure we always honor `atomics_at_binding` option from data layer

## [v0.2.5](https://github.com/ash-project/ash_sql/compare/v0.2.4...v0.2.5) (2024-06-13)




### Bug Fixes:

* properly remap selects on nested subqueries

## [v0.2.4](https://github.com/ash-project/ash_sql/compare/v0.2.3...v0.2.4) (2024-06-13)




### Bug Fixes:

* remap nested selects when sort requires a subquery

* don't create dynamics for map atomics where there are no expressions

### Improvements:

* only use `jsonb_build_object` for expressions, not literals

## [v0.2.3](https://github.com/ash-project/ash_sql/compare/v0.2.2...v0.2.3) (2024-06-06)




### Bug Fixes:

* various fixes to retain lateral join context

## [v0.2.2](https://github.com/ash-project/ash_sql/compare/v0.2.1...v0.2.2) (2024-06-05)




### Bug Fixes:

* carry over tenant in joined queries

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
