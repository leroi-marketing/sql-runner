import os
import re
from textwrap import dedent
from types import SimpleNamespace
from typing import List, Union


class Query(object):

    default_schema_prefix = 'zz_'
    default_schema_suffix = '_mat'

    def __init__(self, config: SimpleNamespace, schema_name: str, table_name: str, action: str):
        self.config: SimpleNamespace = config
        self.schema_name: str = schema_name.strip()
        self.schema_prefix: str = ''
        self.schema_suffix = Query.default_schema_suffix
        self.table_name: str = table_name.strip()
        self.action: str = action.strip()
        self.sql_path: str = config.sql_path
        self.full_table_name: str = self.schema_name + '.' + self.table_name

        path = f'{self.sql_path}/{self.schema_name}/{self.table_name}.sql'
        self.path: str = os.path.abspath(os.path.normpath(path))
        if not os.path.isfile(self.path):
            raise ValueError(f'file {self.path} does not exist')
        with open(self.path, 'r') as f:
            self.query: str = f.read()

    def __repr__(self):
        return f'{self.schema_prefix}{self.schema_name}.{self.table_name} > {self.action}'

    def check_uniqueness(self, cursor: Union["psycopg2.extensions.cursor", "snowflake.connector.SnowflakeCursor"]):
        """ Check if unique keys for current table make sense, and prints offending keys
        """
        if len(self.unique_keys) > 0:
            print(f'check uniqueness for {self.name}')
            for key in self.unique_keys:
                select_key_stmt = f"""
                SELECT '{key}' AS offending_key, COUNT(*)
                FROM (
                  SELECT
                    {key}
                  FROM {self.name}
                  WHERE {key} IS NOT NULL
                  GROUP BY 1
                  HAVING COUNT(*) > 1
                ) AS tmp
                GROUP BY 1;
                """
                cursor.execute(select_key_stmt)
                for line in cursor:
                    print(line)

    @property
    def name(self) -> str:
        """ Table name
        """
        return f'{self.schema_prefix}{self.schema_name}.{self.table_name}'

    @property
    def table_dependencies(self) -> List[str]:
        """ List of tables mentioned in the SQL query
        """
        #TODO: This can be done more reliably with sqlparse
        return [str(match) for match in re.findall(r'(?:FROM|JOIN)\s*([a-zA-Z0-9_]*\.[a-zA-Z0-9_]*)(?:\s|;)',
                                                   self.query, re.DOTALL)]

    @property
    def select_stmt(self) -> Union[str, None]:
        """ Parses out something that looks like a `SELECT` statement from the query
        """
        # Anything that starts with `SELECT` or `WITH`, and ends with a semicolon or end of file
        match = re.search(r'((SELECT|WITH)(\'.*\'|[^;])*)(;|$)', self.query, re.DOTALL)
        if match is not None:
            select_stmt = match.group(1).strip()
            # Rudimentary validation of parentheses + a simple attempt to fix a very specific instance of bad SQL code
            if select_stmt[-1] == ')' and select_stmt.count(')') > select_stmt.count('('):
                select_stmt = select_stmt[:-1]
        # No SQL Statement found, return None
        else:
            select_stmt = None
        return select_stmt

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
        CREATE SCHEMA IF NOT EXISTS {self.schema_name}{self.schema_suffix};
        DROP TABLE IF EXISTS {self.name} CASCADE;
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
        CREATE SCHEMA IF NOT EXISTS {self.schema_prefix}{self.schema_name}{self.schema_suffix};
        DROP TABLE IF EXISTS {self.schema_prefix}{self.schema_name}{self.schema_suffix}.{self.table_name} CASCADE;
        CREATE TABLE {self.schema_prefix}{self.schema_name}{self.schema_suffix}.{self.table_name}
        AS
        {self.select_stmt};
        DROP VIEW IF EXISTS {self.name} CASCADE;
        CREATE VIEW {self.name}
        AS
        SELECT * FROM {self.schema_prefix}{self.schema_name}{self.schema_suffix}.{self.table_name};
        """)

    def skip(self):
        """ Empty statement, skip
        """
        return ""
