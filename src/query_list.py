import csv
import io
import os
import sys
import re
import datetime
import traceback
import psycopg2
import snowflake.connector
from textwrap import dedent
from types import SimpleNamespace
from typing import Union, Any, Dict, List

from src.query import Query


def get_connection(config: SimpleNamespace) -> Union[snowflake.connector.SnowflakeConnection, psycopg2.extensions.connection]:
    """ Get database connection object
    """
    if config.database_type == 'snowflake':
        connection = snowflake.connector.connect(**config.auth)
        connection.cursor().execute(f'USE DATABASE {config.auth.database}')
    elif config.database_type == 'redshift' or config.database_type == 'postgres':
        connection = psycopg2.connect(**config.auth, connect_timeout=3)
    else:
        raise ValueError(f'Invalid database: {config.database}! Value must be snowflake, redshift or postgres')
    connection.autocommit = True
    return connection


class QueryList(list):

    actions: Dict[str, str] = {
        'e': 'query',
        't': 'create_table_stmt',
        'v': 'create_view_stmt',
        'm': 'materialize_view_stmt',
        's': 'skip'
    }

    def __init__(self, config: SimpleNamespace, csv_string: str):
        self.cursor = get_connection(config).cursor()
        self.config = config
        for query in csv.DictReader(io.StringIO(csv_string.strip()), delimiter=';'):
            if not query['schema_name'].startswith('#'):
                self.append(Query(config, **query))

    @staticmethod
    def from_csv_files(config: SimpleNamespace, csv_files: List[str]) -> "QueryList":
        """ Creates a query list from a list of CSV file names, passed in as Command Line Arguments
        """
        if not isinstance(csv_files, list):
            csv_files = [csv_files]
        print('read query lists from: {}'.format(', '.join(csv_files)))
        csv_string = ['schema_name;table_name;action']
        for file in csv_files:
            file_path = f'{config.sql_path}/{file}.csv'
            with open(file_path, 'r') as f:
                csv_string.append(f.read().strip())
        return QueryList(config, '\n'.join(csv_string))

    def test(self):
        """ Test exeution of query list, if `test` is passed into the arguments
        """
        if hasattr(self.config, 'test_schema_prefix'):
            schema_prefix = self.config.test_schema_prefix
        else:
            schema_prefix = Query.default_schema_prefix
        schema_names = set(query.schema_name for query in self)
        full_table_names = set(query.full_table_name for query in self)
        # Recreate schemata
        for schema_name in schema_names:
            statements = f"""
            DROP SCHEMA IF EXISTS {schema_prefix}{schema_name} CASCADE;
            CREATE SCHEMA {schema_prefix}{schema_name}
            """
            for stmt in statements.split(';'):
                self.cursor.execute(stmt)
        
        # Change all queries to use schema_prefix
        # Plus change query actions:
        # query => s (skip)
        # *     => view
        for query in self:
            query.schema_prefix = schema_prefix
            for full_table_name in full_table_names:
                query.query = query.query.replace(' ' + full_table_name, ' ' + schema_prefix + full_table_name)
            if query.action == 'e':
                query.action = 's'
            else:
                query.action = 'v'
        self.execute()

    def execute(self):
        """ Execute every statement from every query
        """
        run_start = datetime.datetime.now()
        for query in self:
            start = datetime.datetime.now()
            print(query)
            if query.action in QueryList.actions:
                # Any of 'query', 'create_table_stmt', 'create_view_stmt', 'materialize_view_stmt'
                stmt_type = QueryList.actions[query.action]
                # Get list of individual specific statements and process them
                for stmt in getattr(query, stmt_type).split(';'):
                    if stmt.strip():
                        try:
                            self.cursor.execute(stmt)
                        except (
                                psycopg2.ProgrammingError, psycopg2.InternalError,
                                snowflake.connector.errors.ProgrammingError):
                            msg = dedent(f"""
                            ERROR: executing '{query.name}':
                            SQL path "{query.path}"
                            {stmt}\n{traceback.format_exc()}
                            """)
                            sys.stderr.write(msg)
                            exit(1)
            print(datetime.datetime.now() - start)
        print('Run finished in {}'.format(datetime.datetime.now() - run_start))
