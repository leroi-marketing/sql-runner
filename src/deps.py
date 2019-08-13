import os
import re

import networkx as nx
from src.db import get_db_and_query_classes, DB
from types import SimpleNamespace
from typing import Set, List, Dict


class Dependencies:
    def __init__(self, config: SimpleNamespace):
        self.config = config
        dbclass, _ = get_db_and_query_classes(config)
        self.db: DB = dbclass(config)

        def get_query_sources(select_stmt: str) -> Set[str]:
            """ Get set of tables mentioned in the select statement
            """
            regex_schema_table = r'(?:from|join)\s*([a-z0-9_]*\.[a-z0-9_]*)(?:\s|;|,|$)'
            regex_db_schema_table = r'(?:from|join)\s*([a-z0-9_]*\.[a-z0-9_]*\.[a-z0-9_]*)(?:\s|;|,|$)'
            schema_table = set(str(match) for match in re.findall(regex_schema_table, select_stmt.lower(), re.DOTALL))
            db_schema_table = set(
                str(match) for match in re.findall(regex_db_schema_table, select_stmt.lower(), re.DOTALL))
            return schema_table | db_schema_table

        self.dependencies: List[Dict[str, str]] = []
        for root, _, file_names in os.walk(config.sql_path):
            if not root.endswith(tuple(config.exclude_dependencies)):
                for file_name in file_names:
                    if file_name[-4:] == '.sql':
                        file_path = os.path.normpath(os.path.join(root, file_name))
                        with open(file_path, 'rb') as sql_file:
                            select_stmt = sql_file.read().decode('utf-8')
                            if select_stmt != '':
                                dependent_schema = os.path.basename(os.path.normpath(root))
                                dependent_table = file_name[:-4]
                                for source in get_query_sources(select_stmt):
                                    source_schema = source.split('.')[-2]
                                    source_table = source.split('.')[-1]
                                    self.dependencies.append({
                                        'source_schema': source_schema,
                                        'source_table': source_table,
                                        'dependent_schema': dependent_schema,
                                        'dependent_table': dependent_table
                                    })

    def clean_schemas(self, prefix: str):
        """ Drop schemata that have a specific name prefix
        """
        if self.config.database_type == 'redshift' or self.config.database_type == 'postgres':
            cmd = f"""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name ~ '^{prefix}.*'
            OR    schema_name NOT IN (SELECT table_schema FROM information_schema.tables)
            AND   schema_name ~ '.*_mat$';"""

        elif self.config.database_type == 'snowflake':
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

        self.db.execute(cmd)
        for schema_name in self.db.cursor.fetchall():
            self.db.execute(f"DROP SCHEMA {schema_name[0]} CASCADE;")

    def save(self, monitor_schema: str):
        """ Save dependencies list in the database in the `monitor_schema` schema
        """
        if not self.dependencies:
            return

        template = "('{source_schema}','{source_table}','{dependent_schema}','{dependent_table}')"
        values = ',\n'.join(template.format(**item) for item in self.dependencies)
        self.db.execute(f'CREATE SCHEMA IF NOT EXISTS {monitor_schema};')
        self.db.execute(f"""
            CREATE TABLE IF NOT EXISTS {monitor_schema}.table_deps 
            (
            source_schema    VARCHAR,
            source_table     VARCHAR,
            dependent_schema VARCHAR,
            dependent_table  VARCHAR
            );"""
        )
        self.db.execute(f'TRUNCATE {monitor_schema}.table_deps;')
        insert_stmt = f"""
            INSERT INTO {monitor_schema}.table_deps
            VALUES
            {values}"""
        self.db.execute(insert_stmt)

    def viz(self):
        def lookup(node_name: str, config_attr: str) -> str:
            """ Look up `config_attr` value for the specified `node_name`
            """
            # defaults
            value = {
                'colors': 'white',
                'shapes': 'oval'
            }[config_attr]
            if hasattr(self.config, config_attr):
                for prefix, config_val in getattr(self.config, config_attr).items():
                    if node_name.startswith(prefix):
                        value = config_val
                        break
            return value

        dependency_tuples = [(f'{item.source_schema}.{item.source_table}',
                              f'{item.dependent_schema}.{item.dependent_table}'
                              ) for item in self.dependencies]
        g = nx.MultiDiGraph()
        edges = [(from_, to_, {'fontsize': 10.0, 'penwidth': 1}) for from_, to_ in dependency_tuples]
        g.add_edges_from(edges)
        for node in g.nodes:
            g.node[node].update({
                'fillcolor': lookup(node, 'colors'),
                'shape': lookup(node, 'shapes'),
                'style': 'filled'
            })
        os.environ["PATH"] += os.pathsep + self.config.graphviz_path
        nx.drawing.nx_pydot.to_pydot(g).write_svg('dependencies.svg')
        if self.config.s3_bucket:
            import boto3
            s3 = boto3.resource('s3')
            body = open('dependencies.svg', 'rb')
            key = f'{self.config.s3_folder}/dependencies.svg'
            s3.Bucket(self.config.s3_bucket).put_object(Key=key, Body=body)
