import re
from textwrap import dedent
from src.db.postgres import PostgresQuery, PostgresDB


class RedshiftQuery(PostgresqlQuery):
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

    @property
    def create_table_stmt(self) -> str:
        """ Statement that creates a table out of `select_stmt`
        """
        return dedent(f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema_name}{self.schema_suffix};
        DROP TABLE IF EXISTS {self.schema_name}{self.schema_suffix}.{self.table_name} CASCADE;
        DROP TABLE IF EXISTS {self.name} CASCADE;
        CREATE TABLE {self.name} {self.distkey_stmt} {self.sortkey_stmt}
        AS
        {self.select_stmt};
        ANALYZE {self.name};
        """)

    @property
    def materialize_view_stmt(self) -> str:
        """ Statement that creates a "materialized" view, or equivalent, out of a `select_stmt`
        """
        return dedent(f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema_prefix}{self.schema_name}{self.schema_suffix};
        DROP TABLE IF EXISTS {self.schema_prefix}{self.schema_name}{self.schema_suffix}.{self.table_name} CASCADE;
        CREATE TABLE {self.schema_prefix}{self.schema_name}{self.schema_suffix}.{self.table_name} {self.distkey_stmt} {self.sortkey_stmt}
        AS
        {self.select_stmt};
        ANALYZE {self.schema_prefix}{self.schema_name}{self.schema_suffix}.{self.table_name};
        DROP VIEW IF EXISTS {self.name} CASCADE;
        CREATE VIEW {self.name}
        AS
        SELECT * FROM {self.schema_prefix}{self.schema_name}{self.schema_suffix}.{self.table_name};
        """)


class RedshiftDB(PostgresDB):
    pass
