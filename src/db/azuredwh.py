import pyodbc
import traceback
import sys
import re
from types import SimpleNamespace
from textwrap import dedent

from src.db import Query, DB


class AzureDwhDB(DB):
    def __init__(self, config):
        server = f'tcp:yourserver.database.windows.net'
        database = 'mydb'
        username = 'myuser'
        password = 'mypass'
        conn = pyodbc.connect(
            'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=tcp:{server};DATABASE={database};UID={username};PWD={password}'.format(
                    **config.auth
                )
            )
        self.cursor = conn.cursor()


    def execute(self, stmt: str, query: Query = None):
        """Execute statement using DB-specific connector
        """
        try:
            self.cursor.execute(stmt)
        except pyodbc.Error:
            msg = ""
            if query:
                msg = dedent(f'''
                    ERROR: executing '{query.name}':
                    SQL path "{query.path}"'''
                )
            msg += dedent(f"""
                {stmt}\n{traceback.format_exc()}\n""")
            sys.stderr.write(msg)
            exit(1)


class AzureDwhQuery(Query):
    @property
    def distribution(self) -> str:
        """ Distribution statement, parsed out of `DISTRIBUTION = HASH (<column>)`.
        """
        match = re.search(r'/\*.*(DISTRIBUTION\s*=\s*HASH\s*\([^\()]*\)).*\**/', self.query, re.DOTALL | re.IGNORECASE)
        if match is not None:
            distribution_stmt = f'DISTRIBUTION = HASH ({match.group(1)})'
        else:
            distribution_stmt = 'DISTRIBUTION = ROUND_ROBIN'
        return distribution_stmt

    @property
    def create_table_stmt(self) -> str:
        """ Statement that creates a table out of `select_stmt`
        """
        # https://docs.microsoft.com/en-us/sql/t-sql/statements/create-table-as-select-azure-sql-data-warehouse?view=azure-sqldw-latest
        return dedent(f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema_name}{self.schema_suffix};
        DROP TABLE IF EXISTS {self.schema_name}{self.schema_suffix}.{self.table_name} CASCADE;
        DROP TABLE IF EXISTS {self.name} CASCADE;
        CREATE TABLE {self.name}
        WITH ( {self.distribution} )
        AS
        {self.select_stmt};
        """)

    @property
    def materialize_view_stmt(self) -> str:
        """ Statement that creates a materialized view, out of a `select_stmt`
        """
        return dedent(f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema_prefix}{self.schema_name}{self.schema_suffix};
        DROP VIEW IF EXISTS {self.schema_prefix}{self.schema_name}{self.schema_suffix}.{self.table_name} CASCADE;
        CREATE MATERIALIZED VIEW {self.schema_prefix}{self.schema_name}{self.schema_suffix}.{self.table_name}
        WITH ( {self.distribution} )
        AS
        {self.select_stmt};
        """)
