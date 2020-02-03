import os
import re
from textwrap import dedent
from types import SimpleNamespace, FunctionType
from typing import List, Dict, Union, Tuple, Iterator
from functools import partial, lru_cache
import csv, io
from sql_runner import tests, parsing
import sqlparse


class Query(object):

    default_schema_prefix = 'zz_'
    default_schema_suffix = '_mat'

    def __init__(self, config: SimpleNamespace, schema_name: str, table_name: str, action: str):
        self.config: SimpleNamespace = config
        self.schema_name: str = schema_name.strip()
        # zz_ when running --test
        self.schema_prefix: str = ''
        # _mat when building "materialized" views
        self.schema_suffix = Query.default_schema_suffix

        self.table_name: str = table_name.strip()
        self.action: str = action.strip()
        self.sql_path: str = config.sql_path
        self.full_table_name: str = self.schema_name + '.' + self.table_name

        path = f'{self.sql_path}/{self.schema_name}/{self.table_name}.sql'
        self.path: str = os.path.abspath(os.path.normpath(path))
        if not os.path.isfile(self.path):
            raise ValueError(f'file {self.path} does not exist')
        with open(self.path, 'r', encoding=getattr(self.config, 'encoding', 'utf-8')) as f:
            self.query: str = f.read()

        self.managed_statements: Iterator[parsing.Query] = parsing.Query.get_queries(self.query)

    def __repr__(self):
        return f'{self.schema_prefix}{self.schema_name}.{self.table_name} > {self.action}'

    @property
    def assertion(self) -> FunctionType:
        match = re.match(r'\/\*\s*(assert_[a-z_]+)\s*(.*?)\s*\*\/', self.query, re.DOTALL)
        if match:
            check = match.group(1)
            if not hasattr(tests, check):
                raise Exception(f"Check '{check}' is not defined")
            # Arguments can be passed after this. CSV-style comma-separated values with strings being put in
            # mandatory quotes
            args = match.group(2)
            if args:
                args = next(csv.reader(io.StringIO(args), quoting=csv.QUOTE_NONNUMERIC))
            check_fun = getattr(tests, check)
            return partial(check_fun, *args)
        return None

    def preprocess_names(self, name_components: Union[parsing.Source, SimpleNamespace]):
        """ Modifies name components in-place, in accordance with "staging" configuration
        """
        if getattr(self.config, "explicit_database", False):
            name_components.database = self.config.auth["database"]

        # Process Staging definition. What to add, or replace, and to which component of the name
        if hasattr(self.config, "staging"):
            staging_config = self.config.staging
            except_rules = getattr(staging_config, "except", {})
            # What not to override
            except_matches = False
            if except_rules:
                # Check every exception from the rule, ex. "schema": "^x_"
                for except_component, regex in except_rules.items():
                    if re.search(regex, getattr(name_components, except_component, ""), re.IGNORECASE):
                        except_matches = True
                        break

            if not except_matches:
                override = staging_config["override"]
                for override_component, override_directives in staging_config["override"].items():
                    existing_value = getattr(name_components, override_component, "")
                    for directive, value in override_directives.items():
                        if directive == 'suffix':
                            setattr(name_components, override_component, existing_value + value)
                        elif directive == 'prefix':
                            setattr(name_components, override_component, value + existing_value)
                        elif directive == 'regex':
                            new_value = re.sub(value["pattern"], value["replace"], existing_value)
                            setattr(name_components, override_component, new_value)

    @property
    @lru_cache(maxsize=1)
    def name_components(self) -> SimpleNamespace:
        """ Name components for an element
        """
        components = SimpleNamespace(
            database=None,
            schema=f"{self.schema_prefix}{self.schema_name}",
            relation=self.table_name
        )
        self.preprocess_names(components)
        return components

    @property
    def name(self) -> str:
        """ Full Table name
        """
        components = self.name_components
        # Gather non-empty components
        existing_components = (c for c in (
                components.database,
                components.schema,
                components.relation
            ) if c)
        return '.'.join(existing_components)

    @property
    def name_mat(self) -> str:
        """ Full Table name for materialized view back-end
        """
        components = self.name_components
        # Gather non-empty components
        existing_components = (c for c in (
                components.database,
                components.schema + self.schema_suffix,
                components.relation
            ) if c)
        return '.'.join(existing_components)

    @property
    def schema(self) -> str:
        """ Full Schema name
        """
        components = self.name_components
        # Gather non-empty components
        existing_components = (c for c in (
                components.database,
                components.schema
            ) if c)
        return '.'.join(existing_components)

    @property
    def schema_mat(self) -> str:
        """ Full Schema name for materialized view back-end
        """
        components = self.name_components
        # Gather non-empty components
        existing_components = (c for c in (
                components.database,
                components.schema + self.schema_suffix
            ) if c)
        return '.'.join(existing_components)

    @property
    def select_stmt(self) -> Union[str, None]:
        """ Query that has DML, stripped of DDL
        """
        for stmt in self.managed_statements:
            if stmt.has_dml:
                if stmt.has_ddl:
                    dml = stmt.without_ddl
                    break
                dml = stmt
                break
        for source in dml.sources:
            self.preprocess_names(source)
        return str(dml)

    @property
    def unique_keys(self) -> List[str]:
        """ Unique key list (key1, key2), parsed out of `UNIQUE KEY <key1, key2>`
        """
        match = re.search(r'/\*.*unique key\s*\(([^\()]*)\).*\**/', self.query,
                          re.DOTALL | re.IGNORECASE)
        if match is not None:
            unique_keys = [k.strip() for k in match.group(1).split(',')]
        else:
            unique_keys = []
        return unique_keys

    @property
    def create_view_stmt(self) -> str:
        """ Statement that creates a view out of `select_stmt`
        """
        return dedent(f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema};
        DROP VIEW IF EXISTS {self.name} CASCADE;
        CREATE VIEW {self.name}
        AS
        {self.select_stmt};
        """)

    @property
    def create_table_stmt(self) -> str:
        """ Statement that creates a table out of `select_stmt`
        """
        return dedent(f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema};
        DROP TABLE IF EXISTS {self.name} CASCADE;
        CREATE TABLE {self.name}
        AS
        {self.select_stmt};
        """)

    @property
    def materialize_view_stmt(self) -> str:
        """ Statement that creates a "materialized" view, or equivalent, out of a `select_stmt`
        """
        return dedent(f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema_mat};
        DROP TABLE IF EXISTS {self.name_mat} CASCADE;
        CREATE TABLE {self.name_mat}
        AS
        {self.select_stmt};
        DROP VIEW IF EXISTS {self.name} CASCADE;
        CREATE VIEW {self.name}
        AS
        SELECT * FROM {self.name_mat};
        """)

    @property
    def run_check_stmt(self) -> str:
        return dedent(self.select_stmt)

    @property
    def skip(self):
        """ Empty statement, skip
        """
        return ""


class DB():
    def __init__(self, config: SimpleNamespace):
        self.cursor = None
    
    def execute(self, stmt: str, query: Query = None):
        """Execute statement using DB-specific connector
        """
        raise Exception(f"`execute()` not implemented for type {type(self)}")

    def clean_schemas(self, prefix: str):
        """ Drop schemata that have a specific name prefix
        """
        raise Exception(f"`clean_schemas()` not implemented for type {type(self)}")
    
    def save(self, monitor_schema: str, dependencies: List[Dict]):
        """ Save dependencies list in the database in the `monitor_schema` schema
        """
        template = "('{source_schema}','{source_table}','{dependent_schema}','{dependent_table}')"
        values = ',\n'.join(template.format(**item) for item in dependencies)

        self.execute(f'CREATE SCHEMA IF NOT EXISTS {monitor_schema};')
        self.execute(f"""
            CREATE TABLE IF NOT EXISTS {monitor_schema}.table_deps 
            (
            source_schema    VARCHAR,
            source_table     VARCHAR,
            dependent_schema VARCHAR,
            dependent_table  VARCHAR
            );"""
        )
        self.execute(f'TRUNCATE {monitor_schema}.table_deps;')
        insert_stmt = f"""
            INSERT INTO {monitor_schema}.table_deps
            VALUES
            {values}"""
        self.execute(insert_stmt)

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchmany(self):
        return self.cursor.fetchmany()

    def fetchall(self):
        return self.cursor.fetchall()


def get_db_and_query_classes(config: SimpleNamespace) -> Tuple[DB, Query]:
    """Returns database specific connector and query class
    """
    # If you know of an easier to write method that's also easy to debug, and easy enough for anyone to understand,
    # please change this
    if config.database_type == 'postgres':
        from sql_runner.db.postgres import PostgresQuery as _Query, PostgresDB as _DB
    elif config.database_type == 'redshift':
        from sql_runner.db.redshift import RedshiftQuery as _Query, RedshiftDB as _DB
    elif config.database_type == 'snowflake':
        from sql_runner.db.snowflake import SnowflakeQuery as _Query, SnowflakeDB as _DB
    elif config.database_type == 'azuredwh':
        from sql_runner.db.azuredwh import AzureDwhQuery as _Query, AzureDwhDB as _DB
    elif config.database_type == 'bigquery':
        from sql_runner.db.bigquery import BigQueryQuery as _Query, BigQueryDB as _DB
    else:
        raise Exception(f"Unknown database type: {config.database_type}")
    return _DB, _Query
