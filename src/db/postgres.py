import psycopg2
import traceback
import sys
from types import SimpleNamespace
from textwrap import dedent
from src.db import Query, DB


class PostgresDB(DB):
    def __init__(self, config: SimpleNamespace) -> psycopg2.extensions.cursor:
        connection = psycopg2.connect(**config.auth, connect_timeout=3)
        connection.autocommit = True
        self.cursor = connection.cursor()

    def execute(self, stmt: str, query: Query = None):
        """Execute statement using DB-specific connector
        """
        try:
            self.cursor.execute(stmt)
        except (psycopg2.ProgrammingError, psycopg2.InternalError):
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


class PostgresQuery(Query):
    pass
