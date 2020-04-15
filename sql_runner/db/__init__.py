import os
import re
from types import SimpleNamespace, FunctionType
from typing import List, Dict, Union, Tuple, Set, Iterator, Iterable, Callable, Any
from functools import partial, lru_cache
import csv
import io
import sqlparse

from sql_runner import tests, parsing, ExecutionType


class FakeCursor:
    def execute(self, statement: str):
        print("\n>>>>>>>>>> BEGIN STATEMENT >>>>>>>>>")
        print(statement)
        print(">>>>>>>>>>> END STATEMENT >>>>>>>>>>\n")

    def fetchall(self):
        return []

    def fetchmany(self, *args, **kwargs):
        return []

    def fetchone(self):
        return None


class Query(object):

    default_schema_suffix = '_mat'

    def __init__(self, config: SimpleNamespace, args: SimpleNamespace, all_created_entities: Set[Tuple[str, str]],
                 execution_type: ExecutionType, schema_name: str, table_name: str, action: str):
        self.config: SimpleNamespace = config
        self.args: SimpleNamespace = args
        self.all_created_entities: Set[Tuple[str, str]] = all_created_entities
        self.execution_type: ExecutionType = execution_type
        self.schema_name: str = schema_name.strip()
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

        self.__managed_statements: Iterator[parsing.Query] = parsing.Query.get_queries(self.query)

    def __repr__(self):
        return f'{self.name} > {self.action}'

    def get_statement_generator(self, statement_type: str) -> Callable[[ExecutionType], Iterable[str]]:
        return getattr(self, statement_type)

    @property
    @lru_cache(maxsize=1)
    def managed_statements(self) -> List[parsing.Query]:
        return list(self.__managed_statements)

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
        """ Modifies name components in-place, in accordance with "staging" or "test" configuration
        """
        # Only modify names that at least have a schema (not CTEs)
        if name_components.schema is None:
            return
        # Only modify DB if explicit database is required and at least a schema is defined
        if getattr(self.config, "explicit_database", False) and name_components.schema is not None:
            if name_components.database is None:
                name_components.database = self.config.auth["database"]

        # Process Staging definition. What to add, or replace, and to which component of the name
        if self.execution_type == ExecutionType.staging and hasattr(self.config, "staging") \
                or self.execution_type == ExecutionType.test and hasattr(self.config, "test"):
            if self.execution_type == ExecutionType.staging:
                override_config: Dict[str, Dict] = self.config.staging
            else:
                override_config: Dict[str, Dict] = self.config.test

            perform_replace = True
            if self.args.except_locally_independent \
                    and (name_components.schema, name_components.relation) not in self.all_created_entities:
                perform_replace = False

            # What not to override
            if perform_replace and "except" in override_config:
                perform_replace = not eval(
                    override_config["except"],
                    {
                        "re": re
                    },
                    {
                        "database": name_components.database,
                        "schema": name_components.schema,
                        "relation": name_components.relation
                    }
                )

            if perform_replace:
                override = override_config["override"]
                for override_component, override_directives in override.items():
                    existing_value = getattr(name_components, override_component, "")
                    for directive, value in override_directives.items():
                        if directive == 'suffix':
                            setattr(name_components, override_component, existing_value + value)
                        elif directive == 'prefix':
                            setattr(name_components, override_component, value + existing_value)
                        elif directive == 'regex':
                            new_value = re.sub(value["pattern"], value["replace"], existing_value)
                            setattr(name_components, override_component, new_value)

    def limit_0(self, dml: parsing.Query) -> None:
        tokens_as_str = dml.tokens_as_str()
        # Does it already have a LIMIT clause?
        re_result = re.search(r"l[\s-]+#[\s-]*[);]?[\s-]*$", tokens_as_str)
        if re_result:
            limit_start = re_result.span()[0]
            limit_clause = re_result.group(0)
            limit_index = limit_clause.find("#") + limit_start
            dml.tokens[limit_index].value = "0"
        else:
            # No limit clause exist. Find end of query and insert one
            re_result = re.search(r"[;)]?[\s-]*$", tokens_as_str)
            if re_result:
                insert_position = re_result.span()[0]
                tokens_to_insert = [
                    sqlparse.sql.Token(sqlparse.tokens.Whitespace,             "\n"),
                    sqlparse.sql.Token(sqlparse.tokens.Keyword,                "LIMIT"),
                    sqlparse.sql.Token(sqlparse.tokens.Whitespace,             " "),
                    sqlparse.sql.Token(sqlparse.tokens.Literal.Number.Integer, "0")
                ]
                # insert in reverse order to preserve insertion point index
                for token in tokens_to_insert[::-1]:
                    dml.tokens.insert(insert_position, token)
                # Notify the query that it has a different token list now
                dml.clear_caches()
            else:
                raise Exception("Could not find end of query to insert LIMIT statement.")

    @property
    @lru_cache(maxsize=1)
    def name_components(self) -> SimpleNamespace:
        """ Name components for an element
        """
        components = SimpleNamespace(
            database=None,
            schema=f"{self.schema_name}",
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

    def execute_stmt(self):
        """ Literal statement to execute
        """
        # Statement splitting, or any parsing whatsoever, is not being done due to this being preserved as an option
        # to make literal DB queries, no matter how complex they are for `sqlparse`.
        return (self.query,)

    def select_stmt(self,
                    extra_manipulations: Union[None, Callable[[parsing.Query], None]] = None) -> Union[str, None]:
        """ Query that has DML, stripped of DDL
        """
        dml: Union[None, parsing.Query] = None
        for stmt in self.managed_statements:
            if stmt.has_dml():
                dml = stmt.without_ddl()
                break
        if not dml:
            return None
        for source in dml.sources():
            self.preprocess_names(source)
        if extra_manipulations:
            extra_manipulations(dml)
        return str(dml)

    def create_view_stmt(self) -> Iterable[str]:
        """ Statement that creates a view out of `select_stmt`
        """
        return (f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema}
        """,
        f"""
        DROP VIEW IF EXISTS {self.name} CASCADE
        """,
        f"""
        CREATE VIEW {self.name}
        AS
        {self.select_stmt()}
        """)

    def create_mock_relation_stmt(self) -> Iterable[str]:
        """ Statement that creates a mock relation out of `select_stmt`
        """
        return (f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema}
        """,
        f"""
        DROP TABLE IF EXISTS {self.name} CASCADE
        """,
        f"""
        CREATE TABLE {self.name}
        AS
        {self.select_stmt(self.limit_0)}
        """)

    def create_table_stmt(self) -> Iterable[str]:
        """ Statement that creates a table out of `select_stmt`
        """
        return (f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema}
        """,
        f"""
        DROP TABLE IF EXISTS {self.name} CASCADE
        """,
        f"""
        CREATE TABLE {self.name}
        AS
        {self.select_stmt()}
        """)

    def materialize_view_stmt(self) -> Iterable[str]:
        """ Statement that creates a "materialized" view, or equivalent, out of a `select_stmt`
        """
        return (f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema_mat}
        """,
        f"""
        DROP TABLE IF EXISTS {self.name_mat} CASCADE
        """,
        f"""
        CREATE TABLE {self.name_mat}
        AS
        {self.select_stmt()}
        """,
        f"""
        DROP VIEW IF EXISTS {self.name} CASCADE
        """,
        f"""
        CREATE VIEW {self.name}
        AS
        SELECT * FROM {self.name_mat}
        """)

    def run_check_stmt(self) -> Iterable[str]:
        return self.select_stmt(),

    def skip(self) -> Iterable[str]:
        """ Empty statement, skip
        """
        return ()


class DB:
    def __init__(self, config: SimpleNamespace, cold_run: bool):
        self.cursor = None
        self.cold_run: bool = cold_run

    def execute(self, stmt: str, query: Query = None):
        """Execute statement using DB-specific connector
        """
        raise Exception(f"`execute()` not implemented for type {type(self)}")

    def clean_specific_schemas(self, schemata: Iterable[str]):
        """ Drop a specific list of schemata
        """
        raise Exception(f"`clean_specific_schemas()` not implemented for type {type(self)}")

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
        if self.cold_run:
            return None
        return self.cursor.fetchone()

    def fetchmany(self):
        if self.cold_run:
            return []
        return self.cursor.fetchmany()

    def fetchall(self):
        if self.cold_run:
            return []
        return self.cursor.fetchall()


def get_db_and_query_classes(config: SimpleNamespace) -> Tuple[Callable[[Any, bool], DB], Callable[[], Query]]:
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
