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
