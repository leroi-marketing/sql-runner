# Changelog

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
