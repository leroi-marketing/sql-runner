import re
from textwrap import dedent
import traceback
import os
import sys
from types import SimpleNamespace
from google.cloud import bigquery
from google.api_core import exceptions
from google.cloud.bigquery.job import QueryJob
from sql_runner.db import Query, DB, FakeCursor
from typing import List, Dict, Iterable

'''
  ▄████  ▒█████   ▒█████    ▄████  ██▓    ▓█████     ▄▄▄▄    ██▓  ▄████   █████   █    ██ ▓█████  ██▀███ ▓██   ██▓
 ██▒ ▀█▒▒██▒  ██▒▒██▒  ██▒ ██▒ ▀█▒▓██▒    ▓█   ▀    ▓█████▄ ▓██▒ ██▒ ▀█▒▒██▓  ██▒ ██  ▓██▒▓█   ▀ ▓██ ▒ ██▒▒██  ██▒
▒██░▄▄▄░▒██░  ██▒▒██░  ██▒▒██░▄▄▄░▒██░    ▒███      ▒██▒ ▄██▒██▒▒██░▄▄▄░▒██▒  ██░▓██  ▒██░▒███   ▓██ ░▄█ ▒ ▒██ ██░
░▓█  ██▓▒██   ██░▒██   ██░░▓█  ██▓▒██░    ▒▓█  ▄    ▒██░█▀  ░██░░▓█  ██▓░██  █▀ ░▓▓█  ░██░▒▓█  ▄ ▒██▀▀█▄   ░ ▐██▓░
░▒▓███▀▒░ ████▓▒░░ ████▓▒░░▒▓███▀▒░██████▒░▒████▒   ░▓█  ▀█▓░██░░▒▓███▀▒░▒███▒█▄ ▒▒█████▓ ░▒████▒░██▓ ▒██▒ ░ ██▒▓░
 ░▒   ▒ ░ ▒░▒░▒░ ░ ▒░▒░▒░  ░▒   ▒ ░ ▒░▓  ░░░ ▒░ ░   ░▒▓███▀▒░▓   ░▒   ▒ ░░ ▒▒░ ▒ ░▒▓▒ ▒ ▒ ░░ ▒░ ░░ ▒▓ ░▒▓░  ██▒▒▒ 
  ░   ░   ░ ▒ ▒░   ░ ▒ ▒░   ░   ░ ░ ░ ▒  ░ ░ ░  ░   ▒░▒   ░  ▒ ░  ░   ░  ░ ▒░  ░ ░░▒░ ░ ░  ░ ░  ░  ░▒ ░ ▒░▓██ ░▒░ 
░ ░   ░ ░ ░ ░ ▒  ░ ░ ░ ▒  ░ ░   ░   ░ ░      ░       ░    ░  ▒ ░░ ░   ░    ░   ░  ░░░ ░ ░    ░     ░░   ░ ▒ ▒ ░░  
      ░     ░ ░      ░ ░        ░     ░  ░   ░  ░    ░       ░        ░     ░       ░        ░  ░   ░     ░ ░     
                                                          ░                                               ░ ░     
'''


class FakeClient:
    def __init__(self):
        self.fake_cursor = FakeCursor()

    def query(self, statement):
        self.fake_cursor.execute(statement)
        return SimpleNamespace(result=lambda: iter([]))


class BigQueryQuery(Query):
    def __init__(self, config: SimpleNamespace, args: SimpleNamespace, all_created_entities: Set[Tuple[str, str]],
                 schema_name: str, table_name: str, action: str):
        # BigQuery requires explicit database
        config.explicit_database = True
        super().__init__(config, args, all_created_entities, schema_name, table_name, action)
        self.database = config.auth["database"]

    @property
    def partition_by_stmt(self) -> str:
        """ Partition key statement, parsed out of `PARTITION BY <field>`.
        """
        match = re.search(r'/\*.*(partition\s+by\s+[^\s]*).*\**/', self.query, re.DOTALL | re.IGNORECASE)
        if match is not None:
            return match.group(1)
        else:
            return ''

    @property
    def options_stmt(self) -> str:
        """ Options statement, parsed out of `OPTIONS (<list>)`
        """
        match = re.search(r'/\*.*(options\s*\([^\()]*\)).*\**/', self.query, re.DOTALL | re.IGNORECASE)
        if match is not None:
            return match.group(1)
        else:
            return ''

    def create_table_stmt(self) -> Iterable[str]:
        """ Statement that creates a table out of `select_stmt`
        """
        return (f"""
        CREATE SCHEMA IF NOT EXISTS `{self.schema}`
        """,
        f"""
        CREATE OR REPLACE TABLE `{self.name}` {self.partition_by_stmt} {self.options_stmt}
        AS
        {self.select_stmt()}
        """)

    def create_mock_relation_stmt(self) -> Iterable[str]:
        """ Statement that creates a mock relation out of `select_stmt`
        """
        # BigQuery charges per table scan, regardless of LIMIT clause. Cost-wise it makes more sense to make it a view
        return self.create_view_stmt()

    def create_view_stmt(self) -> Iterable[str]:
        """ Statement that creates a view out of `select_stmt`
        """
        return (f"""
        CREATE SCHEMA IF NOT EXISTS `{self.schema}`
        """,
        f"""
        CREATE OR REPLACE VIEW `{self.name}`
        AS
        {self.select_stmt()}
        """)

    def materialize_view_stmt(self) -> Iterable[str]:
        """ Statement that creates a "materialized" view, or equivalent, out of a `select_stmt`
        """
        return (f"""
        CREATE SCHEMA IF NOT EXISTS `{self.schema_mat}`
        """,
        f"""
        CREATE OR REPLACE TABLE `{self.name_mat}` {self.partition_by_stmt} {self.options_stmt}
        AS
        {self.select_stmt()}
        """,
        f"""
        DROP VIEW IF EXISTS `{self.name}`
        """,
        f"""
        CREATE VIEW `{self.name}`
        AS
        SELECT * FROM `{self.name_mat}`;
        """)


class BigQueryDB(DB):
    def __init__(self, config: SimpleNamespace, cold_run: bool):
        super().__init__(config, cold_run)
        if 'credentials_path' in config.auth:
            credentials_path = os.path.abspath(config.auth['credentials_path'])
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

        self.database = config.auth["database"]
        if cold_run:
            self.client = FakeClient()
        else:
            self.client = bigquery.Client(project=self.database)
        self.result = None

    def create_schema(self, schema: str):
        self.client.create_dataset(schema)

    def create_schema_replacement(self, stmt: str) -> str:
        """ If the statement has `CREATE SCHEMA x`, do this in Python and remove the statement
        """
        def replace(match) -> str:
            schema = match.groups()[0]
            try:
                self.create_schema(schema)
            except exceptions.Conflict:
                pass
            return ''

        return re.sub(
            r'(?<!\w)CREATE\s+SCHEMA\s+(?:IF\s+NOT\s+EXISTS\s+)?`([^`]+)`(?:;|$)',
            replace,
            stmt,
            flags=re.IGNORECASE | re.DOTALL
        )

    def drop_schema(self, schema: str, cascade: bool):
        self.client.delete_dataset(schema, delete_contents=cascade)

    def drop_schema_replacement(self, stmt: str) -> str:
        """ If the statement has `DROP SCHEMA x`, do this in Python and remove the statement
        """
        def replace(match) -> str:
            schema = match.groups()[0]
            cascade = match.groups()[1]
            try:
                self.drop_schema(schema, cascade!='')
            except exceptions.NotFound:
                pass
            return ''

        return re.sub(
            r'(?<!\w)DROP\s+SCHEMA\s+(?:IF\s+EXISTS\s+)?`([^`]+)`(\s+CASCADE)?(?:;|$)',
            replace,
            stmt,
            flags=re.IGNORECASE | re.DOTALL
        )

    def execute(self, stmt: str, query: BigQueryQuery = None):
        """Execute statement using DB-specific connector
        """
        stmt = self.drop_schema_replacement(stmt)
        stmt = self.create_schema_replacement(stmt)
        if stmt.strip().strip(';') == '':
            return
        try:
            self.result: QueryJob = self.client.query(stmt).result()
        except Exception:
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
            self.client.delete_dataset(schema)

    def clean_schemas(self, prefix: str):
        """ Drop schemata that have a specific name prefix
        """
        datasets = list(self.client.list_datasets())
        for ds in datasets:
            if ds.dataset_id.startswith(prefix) \
                or len(list(self.client.list_tables(ds.dataset_id))) == 0 and not ds.dataset_id.endswith('_mat'):
                self.client.delete_dataset(ds.dataset_id)

    def fetchone(self):
        return next(self.result)

    def fetchmany(self):
        return list(self.result)

    def fetchall(self):
        return list(self.result)

    def save(self, monitor_schema: str, dependencies: List[Dict]):
        print("Saving dependencies is not supported on BigQuery")
