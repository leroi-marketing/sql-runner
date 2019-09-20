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
        self.db.clean_schemas(prefix)

    def save(self, monitor_schema: str):
        """ Save dependencies list in the database in the `monitor_schema` schema
        """
        if not self.dependencies:
            return
        self.db.save(monitor_schema, self.dependencies)

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
