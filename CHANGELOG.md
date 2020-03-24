# Changelog

## 0.4.1 (2020-03-24)

**Breaking change**

**Safe for most production environments**

- Queries are now being modified on-the-fly to facilitate full forking of data, either with chaining views (when running with `--test`) or to build data in a staging environment (when running with `--staging`)
- DML statements from comments, as well as parsing semicolons from comments is no longer an issue. Comments from statements are being ignored unless they're functional
- Configuration for `--test` and `--staging` has changed. Explicit `schema_prefix` is no longer used.
- `--test` no longer performs `v` action, but rather `mock`, which, in the most part, is the same as `v`, but will be overridden in the future for Snowflake for example, to allow chaining mock structures, as views cause compilation memory issues on Snowflake.
- New `--except-locally-independent` option for `test` runs, that base new views on test views that are being created in this run, otherwise on original data structures. This allows running tests on a limited data model part, when the other mock structures don't exist or aren't updated.
- New `--cold-run` option, forces the sqlrunner to initiate mock database connections that only output every statement. No actual connection is made. This is done purely for debugging purposes.
- Queries now reveal modifiable dependencies, and this functionality can be further extended.
- Cleanup is now happening only on newly created schemata.

## 0.3.0 (2020-01-15)
**Might be a breaking change**
- Rebrand to _DEPT SQL Runner_.  
  LEROI is now part of DEPT
- Add Google BigQuery support
- Enable calling the runner with function arguments
- Refactor dependency detection:
  - Switch from regular expressions to query tokenizer and state-machine-based parsing
  - Make the functionality overrideable from the DB-specific modules
- Rename `src` to `sql_runner` to make it less generic in the directory structure when installed using `pip`

## 0.2.9 (2019-11-21)
- Fix file decoding with explicit optional `encoding` option in config, that defaults to `utf-8`

## 0.2.8 (2019-11-19)
- Streamline dependencies (remove superfluous version restrictions and redundant dependencies)
- Update documentation

## 0.2.7 (2019-11-18)
- Return "legacy" CLI entry point `sqlrunner` which is the same as `runner`

## 0.2.6 (2019-11-14)
- Fix bugs with view creation on Snowflake / RedShift / Postgresql (`DROP TABLE IF EXISTS`)
- Update snowflake connector
- Separate snowflake from Azure BLOB
- Add Azure BLOB as an optional dependency

## 0.2.5 (2019-11-01)
- Add data testing feature

## 0.2.4 (2019-10-31)
- Dependency-aware ordered statement execution

## 0.2.3 (2019-10-21)
- Add interactive runner `run_sql path/to/config.json`

## 0.2.2 (2019-09-23)
- Fix Python 3.6 compatibility

## 0.2.1 (2019-09-20)
- Merge pull request that adds support for staging runs
- Fix dependency viewer for Azure Data Warehouse

## 0.2.0 (2019-09-20)
**Might be a breaking change**
- Add Azure Data Warehouse support
- Add setup.py - based installer
- Split database-specific code into separate files
  - Also split dependencies into optional features
- Update help to highlight the new executable name, setup process, and more

## 0.1.0
- Legacy version
