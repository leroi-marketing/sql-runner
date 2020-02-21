import snowflake.connector
import traceback
import sys
from types import SimpleNamespace
from typing import List, Iterable
from textwrap import dedent

from sql_runner.db import Query, DB, FakeCursor


class SnowflakeQuery(Query):
    def create_mock_relation_stmt(self) -> Iterable[str]:
        """ Statement that creates a mock relation out of `select_stmt`
        """
        # Snowflake doesn't care about `LIMIT 0` when selecting - it still scans and computes everything
        # So the first tables that get created from original data take forever
        # This has to be views
        return self.create_view_stmt()


class SnowflakeDB(DB):
    def __init__(self, config: SimpleNamespace, cold_run: bool):
        super().__init__(config, cold_run)
        if cold_run:
            self.cursor = FakeCursor()
        else:
            connection = snowflake.connector.connect(**config.auth)
            self.cursor = connection.cursor()
        self.cursor.execute(f'USE DATABASE {config.auth["database"]}')

    def execute(self, stmt: str, query: SnowflakeQuery = None):
        """Execute statement using DB-specific connector
        """
        try:
            self.cursor.execute(stmt)
        except snowflake.connector.errors.ProgrammingError:
            msg = ""
            if query:
                msg = dedent(f'''
                    ERROR: executing '{query.name}':
                    SQL path "{query.path}"'''
                )
            else:
                msg = "ERROR: executing query:\n\n"
            msg += f"\n\n{stmt}\n\n{traceback.format_exc()}\n"
            sys.stderr.write(msg)
            exit(1)

    def clean_specific_schemas(self, schemata: List[str]):
        """ Drop a specific list of schemata
        """
        for schema in schemata:
            self.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")

    def clean_schemas(self, prefix: str):
        """ Drop schemata that have a specific name prefix
        """
        prefix = prefix.upper()
        cmd = f"""
        WITH filter_ AS
        (
            SELECT DISTINCT table_schema AS table_schema
            FROM information_schema.tables
        )
        SELECT DISTINCT schema_name AS schema_name_
        FROM information_schema.schemata schemata
            LEFT JOIN filter_ ON filter_.table_schema = schema_name
        WHERE regexp_like(schema_name_,'^{prefix}.*')
        OR (filter_.table_schema IS NULL AND regexp_like(schema_name_,'.*_MAT$'));"""

        self.execute(cmd)
        for schema_name in self.cursor.fetchall():
            self.execute(f"DROP SCHEMA {schema_name[0]} CASCADE;")
