import pyodbc
import traceback
import sys
import re
from types import SimpleNamespace
from textwrap import dedent
from typing import List, Dict, Iterable

from sql_runner.db import Query, DB, FakeCursor


class AzureDwhQuery(Query):
    @property
    def distribution(self) -> str:
        """ Distribution statement, parsed out of `DISTRIBUTION = HASH (<column>)`.
        """
        match = re.search(r'/\*.*(?:DISTRIBUTION\s*=\s*HASH\s*\(([^\()]*)\)).*\**/', self.query, re.DOTALL | re.IGNORECASE)
        if match is not None:
            distribution_stmt = f'DISTRIBUTION = HASH ({match.group(1)})'
        else:
            distribution_stmt = 'DISTRIBUTION = ROUND_ROBIN'
        return distribution_stmt

    def object_exists_stmt(self, schema_name: str, table: bool = False, view: bool = False):
        """ Returns statement that, when executed, returns TRUE when the current object (of specified type) exists
        """
        return AzureDwhDB.object_exists_stmt(schema_name, self.name_components.relation, table=table, view=view)

    def schema_exists_stmt(self, schema_name: str) -> str:
        """ Returns statement that, when executed, returns TRUE when the schema exists
        """
        return self.object_exists_stmt(schema_name)

    def table_exists_stmt(self, schema_name: str) -> str:
        """ Returns statement that, when executed, returns TRUE when the table exists
        """
        return self.object_exists_stmt(schema_name, table=True)

    def view_exists_stmt(self, schema_name: str) -> str:
        """ Returns statement that, when executed, returns TRUE when the view exists
        """
        return self.object_exists_stmt(schema_name, view=True)

    def create_table_stmt(self) -> Iterable[str]:
        """ Statement that creates a table out of `select_stmt`
        """
        # https://docs.microsoft.com/en-us/sql/t-sql/statements/create-table-as-select-azure-sql-data-warehouse?view=azure-sqldw-latest
        return (f"""
        IF NOT {self.schema_exists_stmt(self.schema)}
            EXEC('CREATE SCHEMA {self.schema}')
        """,
        f"""
        IF {self.table_exists_stmt(self.schema)}
            DROP TABLE {self.name}
        """,
        f"""
        IF {self.view_exists_stmt(self.schema)}
            DROP VIEW {self.name}
        """,
        f"""
        CREATE TABLE {self.name}
        WITH ( {self.distribution} )
        AS
        {self.select_stmt()}
        """
        )

    def create_mock_relation_stmt(self) -> Iterable[str]:
        """ Statement that creates a mock relation out of `select_stmt`
        """
        # For now, make it a view
        # TODO: For the future it might make sense to modify the select_stmt to insert the `TOP 0` after `SELECT`
        return self.create_view_stmt()

    def create_view_stmt(self) -> Iterable[str]:
        """ Statement that creates a view out of `select_stmt`
        """
        return (f"""
        IF NOT {self.schema_exists_stmt(self.schema)}
            EXEC('CREATE SCHEMA {self.schema}')
        """,
        f"""
        IF {self.view_exists_stmt(self.schema)}
            DROP VIEW {self.name}
        """,
        f"""
        IF {self.table_exists_stmt(self.schema)}
            DROP TABLE {self.name}
        """,
        f"""
        CREATE VIEW {self.name}
        AS
        {self.select_stmt()};
        """
        )

    def materialize_view_stmt(self) -> Iterable[str]:
        """ Statement that creates a materialized view, out of a `select_stmt`
        """
        table_schema=f"{self.schema_prefix}{self.schema_name}{self.schema_suffix}"
        view_schema=f"{self.schema_prefix}{self.schema_name}"

        return (f"""
        IF NOT {self.schema_exists_stmt(self.schema_mat)}
            EXEC('CREATE SCHEMA {self.schema_mat}')
        """,
        f"""
        IF {self.table_exists_stmt(self.schema_mat)}
            DROP TABLE {self.name_mat}
        """,
        f"""
        IF {self.view_exists_stmt(self.schema)}
            DROP VIEW {self.name}
        """,
        f"""
        IF {self.table_exists_stmt(view_schema)}
            DROP TABLE {self.name}
        """,
        f"""
        CREATE TABLE {self.name_mat}
        WITH (
            {self.distribution}
        )
        AS
        {self.select_stmt()}
        """,
        f"""
        IF {self.view_exists_stmt(self.schema)}
            DROP VIEW {self.name}
        """,
        f"""
        CREATE VIEW {self.name}
        AS
        SELECT * FROM {self.name_mat}
        """)


class AzureDwhDB(DB):
    def __init__(self, config, cold_run: bool):
        super().__init__(config, cold_run)

        if cold_run:
            self.cursor = FakeCursor()
        else:
            conn = pyodbc.connect(
                'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=tcp:{server};DATABASE={database};UID={username};PWD={password}'.format(
                        **config.auth
                    )
                )
            conn.autocommit = True
            self.cursor = conn.cursor()

    def drop_object_cascade(self, type_desc: str, schema_name: str, object_name: str, object_id: int):
        self.execute(f"""
        SELECT
            obj.type_desc,
            s.name AS schema_name,
            obj.name AS object_name,
            obj.object_id
        FROM sys.sql_expression_dependencies dep
        JOIN sys.all_objects obj ON obj.object_id=dep.referencing_id
        JOIN sys.schemas s ON s.schema_id = obj.schema_id
        WHERE referenced_id = {object_id}
        """)

        for obj in self.cursor.fetchall():
            self.drop_object_cascade(*obj)

        if type_desc == 'USER_TABLE':
            type_desc = 'TABLE'
        
        self.execute(f"""
        IF OBJECT_ID('{schema_name}.{object_name}') IS NOT NULL
            DROP {type_desc} {schema_name}.{object_name}
        """)

    def drop_schema_cascade(self, schema: str):
        self.execute(f"""
        SELECT obj.type_desc, s.name AS schema_name, obj.name AS object_name, obj.object_id
        FROM sys.all_objects obj
        JOIN sys.schemas s ON s.schema_id = obj.schema_id
        WHERE s.name='{schema}'
        """)

        for obj in self.cursor.fetchall():
            self.drop_object_cascade(*obj)

        self.execute(f"SELECT 1 FROM sys.schemas WHERE name='{schema}'")
        if self.cursor.fetchone():
            self.execute(f"DROP SCHEMA {schema}")

    def drop_schema_cascade_replacement(self, stmt: str) -> str:
        """ If the statement has `DROP SCHEMA x CASCADE`, do this in Python and remove the statement
        """
        def replace(match) -> str:
            schema = match.groups()[0]
            self.drop_schema_cascade(schema)
            # Return a placeholder to show what's happening
            return f"SELECT '-- Replacement for: DROP SCHEMA IF EXISTS {schema} CASCADE'"

        return re.sub(
            r'(?<!\w)DROP\s+SCHEMA\s+(?:IF\s+EXISTS\s+)?(\w+)\s+CASCADE(?:;|$)',
            replace,
            stmt,
            flags=re.IGNORECASE | re.DOTALL
        )

    @staticmethod
    def object_exists_stmt(schema_name: str, child_name='', table: bool = False, view: bool = False):
        """ Returns statement that, when executed, returns TRUE when the current object (of specified type) exists
        """
        if table or view:
            if table:
                sql_runner = 'tables'
            elif view:
                sql_runner = 'views'
            join = f"JOIN sys.{sql_runner} o ON o.schema_id = s.schema_id AND o.name='{child_name}'"
        else:
            join = ''
        return f"EXISTS (SELECT 1 FROM sys.schemas s {join} WHERE s.name='{schema_name}')"

    def clean_specific_schemas(self, schemata: List[str]):
        """ Drop a specific list of schemata
        """
        for schema in schemata:
            self.drop_schema_cascade(schema)

    def clean_schemas(self, prefix: str):
        """ Drop schemata that have a specific name prefix
        """
        cmd = f"""
        SELECT
            name
        FROM sys.schemas
        WHERE
            name LIKE '{prefix}%'
            OR schema_id NOT IN (SELECT schema_id FROM sys.tables)
            AND name LIKE '%_mat'"""

        self.execute(cmd)
        for schema_name in self.cursor.fetchall():
            self.drop_schema_cascade(schema_name[0])

    def save(self, monitor_schema: str, dependencies: List[Dict]):
        """ Save dependencies list in the database in the `monitor_schema` schema
        """
        template = f"""
            INSERT INTO {monitor_schema}.table_deps
            VALUES ('{{source_schema}}','{{source_table}}','{{dependent_schema}}','{{dependent_table}}');"""
        inserts = '\n'.join(template.format(**item) for item in dependencies)

        self.execute(f"""
        IF NOT {AzureDwhDB.object_exists_stmt(monitor_schema)}
            EXEC('CREATE SCHEMA {monitor_schema}');""")

        self.execute(f"""
            IF {AzureDwhDB.object_exists_stmt(monitor_schema, 'table_deps', table=True)}
                DROP TABLE {monitor_schema}.table_deps;

            CREATE TABLE {monitor_schema}.table_deps
            (
            source_schema    NVARCHAR(2000),
            source_table     NVARCHAR(2000),
            dependent_schema NVARCHAR(2000),
            dependent_table  NVARCHAR(2000)
            );"""
        )

        self.execute(inserts)

    def execute(self, stmt: str, query: AzureDwhQuery = None):
        """Execute statement using DB-specific connector
        """
        try:
            stmt = self.drop_schema_cascade_replacement(stmt)
            self.cursor.execute(stmt)
        except (pyodbc.Error, pyodbc.ProgrammingError) as ex:
            msg = ""
            if query:
                msg = dedent(f'''
                    ERROR: executing '{query.name}':
                    SQL path "{query.path}"\n\n'''
                )
            else:
                msg = "ERROR: executing query:\n\n"
            
            msg += f"{dedent(stmt)}\n\n{ex.args[1]}\n{''.join(traceback.format_stack(limit=3)[:-1])}\n"
            sys.stderr.write(msg)
            exit(1)
