import re
from textwrap import dedent
import traceback
import os
import sys
from types import SimpleNamespace
from google.cloud import bigquery
from google.api_core import exceptions
from sql_runner.db import Query, DB

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

class BigQueryQuery(Query):
    def __init__(self, config: SimpleNamespace, schema_name: str, table_name: str, action: str):
        super().__init__(config, schema_name, table_name, action)
        self.database = config.auth["database"]


    @property
    def name(self) -> str:
        """ Full Table name
        """
        return f'{self.database}.{self.schema_prefix}{self.schema_name}.{self.table_name}'

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

    @property
    def create_table_stmt(self) -> str:
        """ Statement that creates a table out of `select_stmt`
        """
        return dedent(f"""
        CREATE SCHEMA IF NOT EXISTS `{self.database}.{self.schema_name}{self.schema_suffix}`;
        CREATE OR REPLACE TABLE `{self.name}` {self.partition_by_stmt} {self.options_stmt}
        AS
        {self.select_stmt};
        """)

    @property
    def create_view_stmt(self) -> str:
        """ Statement that creates a view out of `select_stmt`
        """
        return dedent(f"""
        CREATE SCHEMA IF NOT EXISTS `{self.database}.{self.schema_name}{self.schema_suffix}`;
        CREATE OR REPLACE VIEW `{self.name}`
        AS
        {self.select_stmt};
        """)

    @property
    def materialize_view_stmt(self) -> str:
        """ Statement that creates a "materialized" view, or equivalent, out of a `select_stmt`
        """
        return dedent(f"""
        CREATE SCHEMA IF NOT EXISTS `{self.database}.{self.schema_prefix}{self.schema_name}{self.schema_suffix}`;
        CREATE OR REPLACE TABLE `{self.schema_prefix}{self.schema_name}{self.schema_suffix}.{self.table_name}` {self.partition_by_stmt} {self.options_stmt}
        AS
        {self.select_stmt};
        DROP VIEW IF EXISTS `{self.name}`;
        CREATE VIEW `{self.name}`
        AS
        SELECT * FROM `{self.database}.{self.schema_prefix}{self.schema_name}{self.schema_suffix}.{self.table_name}`;
        """)


class BigQueryDB(DB):
    def __init__(self, config: SimpleNamespace):
        if 'credentials_path' in config.auth:
            credentials_path = os.path.abspath(config.auth['credentials_path'])
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

        self.database = config.auth["database"]
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
            self.result: google.cloud.bigquery.job.QueryJob = self.client.query(stmt).result()
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
