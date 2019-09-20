import snowflake.connector
import traceback
import sys
from types import SimpleNamespace
from textwrap import dedent

from src.db import Query, DB

class SnowflakeQuery(Query):
    pass


class SnowflakeDB(DB):
    def __init__(self, config: SimpleNamespace):
        connection = snowflake.connector.connect(**config.auth)
        cursor = connection.cursor()
        cursor.execute(f'USE DATABASE {config.auth.database}')
        self.cursor = cursor

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
