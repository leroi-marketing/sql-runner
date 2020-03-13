import re
from textwrap import dedent
from typing import Iterable
from sql_runner.db.postgres import PostgresQuery, PostgresDB


class RedshiftQuery(PostgresQuery):
    @property
    def distkey_stmt(self) -> str:
        """ Distribution key statement, parsed out of `DISTKEY (<list>)`.
        """
        match = re.search(r'/\*.*(distkey\s*\([^\()]*\)).*\**/', self.query, re.DOTALL | re.IGNORECASE)
        if match is not None:
            if match.group(1) == 'DISTKEY ()':
                distkey_stmt = 'DISTSTYLE ALL'
            else:
                distkey_stmt = 'DISTSTYLE KEY ' + match.group(1)
        else:
            distkey_stmt = 'DISTSTYLE EVEN'
        return distkey_stmt

    @property
    def sortkey_stmt(self) -> str:
        """ Sort key statement, parsed out of `[INTERLEAVED|COMPOUND] SORTKEY <key>`
        """
        match = re.search(r'/\*.*((compound\s*sortkey|interleaved\s*sortkey)\s*\([^\()]*\)).*\**/', self.query,
                            re.DOTALL | re.IGNORECASE)
        if match is None:
            match = re.search(r'/\*.*(sortkey\s*\([^\()]*\)).*\**/', self.query,
                                re.DOTALL | re.IGNORECASE)
        if match is not None:
            sortkey_stmt = match.group(1)
        else:
            sortkey_stmt = ''
        return sortkey_stmt

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
        CREATE TABLE {self.name} {self.distkey_stmt} {self.sortkey_stmt}
        AS
        {self.select_stmt()}
        """,
        f"""
        ANALYZE {self.name}
        """)

    def create_mock_relation_stmt(self) -> Iterable[str]:
        """ Statement that creates a mock relation out of `select_stmt`
        """
        # Redshift handles views well. It doesn't make sense to use anything else for this purpose
        return self.create_view_stmt()

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
        CREATE TABLE {self.name_mat} {self.distkey_stmt} {self.sortkey_stmt}
        AS
        {self.select_stmt()}
        """,
        f"""
        ANALYZE {self.name_mat}
        """,
        f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema}
        """,
        f"""
        DROP VIEW IF EXISTS {self.name} CASCADE
        """,
        f"""
        CREATE VIEW {self.name}
        AS
        SELECT * FROM {self.name_mat}
        """)


class RedshiftDB(PostgresDB):
    pass
