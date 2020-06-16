import os
import re

import networkx as nx
import csv
import json
from hashlib import md5
from collections import defaultdict, namedtuple
from glob import glob
from sql_runner.db import get_db_and_query_classes, DB
from sql_runner import parsing
from types import SimpleNamespace
from typing import Set, List, Dict, Tuple
from functools import lru_cache


Dependency = namedtuple("Dependency", ['md5', 'source_schema', 'source_table', 'dependent_schema', 'dependent_table'])


class Dependencies:
    # Version used for dependency caching invalidation
    # Increment version when you make changes that impact detected dependencies
    VERSION = b"2"

    def __init__(self, config: SimpleNamespace):
        self.config = config
        print("Parsing queries to determine dependencies")
        cached_dependencies: List[Dict[str, str]] = self.load_cache()
        dependency_cache: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        for d in cached_dependencies:
            dependency_cache[d['md5']].append(d)

        self.dependencies: List[Dict[str, str]] = []
        # To make sure dependencies are unique
        dependencies_set = set()
        for file_path in glob(config.sql_path + '/*/*.sql'):
            base_dir_name = os.path.basename(os.path.dirname(file_path))
            file_name = os.path.basename(file_path)
            if base_dir_name in config.exclude_dependencies:
                continue

            with open(file_path, 'r', encoding=getattr(self.config, 'encoding', 'utf-8')) as sql_file:
                select_stmt = sql_file.read()
                hash_md5 = md5()
                hash_md5.update(Dependencies.VERSION)
                hash_md5.update(f"{base_dir_name}/{file_name}".encode('utf-8'))
                hash_md5.update(select_stmt.encode("utf-8"))
                checksum = hash_md5.hexdigest()
                if select_stmt == '':
                    continue

            dependent_schema = base_dir_name
            dependent_table = file_name[:-4]

            cache_key = checksum
            if cache_key in dependency_cache:
                for dep in dependency_cache[cache_key]:
                    dependencies_set.add(Dependency(
                        dep['md5'],
                        dep['source_schema'],
                        dep['source_table'],
                        dep['dependent_schema'],
                        dep['dependent_table']
                    ))
                continue

            # deduplicate sources
            sources = set()
            has_explicit_dependencies = False
            for query in parsing.Query.get_queries(select_stmt):
                ignored_dependencies = set()
                override_dependencies = None
                additional_dependencies = set()

                # first retrieve any functional comments that have information about dependencies
                for comment in query.comment_contents():
                    functional_comment = None
                    try:
                        functional_comment = json.loads(comment)
                    except:
                        continue

                    if 'node_id' in functional_comment:
                        # Bug when reading dependencies
                        dependent_schema, dependent_table = functional_comment['node_id']
                    if 'override_dependencies' in functional_comment:
                        sources = set()
                        for schema, table in functional_comment['override_dependencies']:
                            sources.add((schema, table))
                        has_explicit_dependencies = True
                    if 'ignore_dependencies' in functional_comment:
                        for schema, table in functional_comment['ignore_dependencies']:
                            ignored_dependencies.add((schema, table))
                    if 'additional_dependencies' in functional_comment:
                        for schema, table in functional_comment['additional_dependencies']:
                            additional_dependencies.add((schema, table))

                # If there aren't explicit dependencies, get them from query sources.
                if not has_explicit_dependencies:
                    for source in query.sources():
                        # Ignore sources without a specified schema
                        if source.schema:
                            source_schema = source.schema.lower()
                            source_table = source.relation.lower()
                            sources.add((source_schema, source_table))

                    # Add / remove dependencies depending on functional comments
                    sources.update(additional_dependencies)
                    sources.difference_update(ignored_dependencies)

            for source_schema, source_table in sources:
                # Doing it with a set, eliminates the bug where multiple files with the same name, parent directory
                # and content hash, contribute to duplication of dependencies after each run
                dependencies_set.add(
                    Dependency(checksum, source_schema, source_table, dependent_schema, dependent_table)
                )
        self.dependencies = list(dep._asdict() for dep in dependencies_set)
        self.save_cache()

    def load_cache(self) -> List[Dict[str, str]]:
        if hasattr(self.config, 'deps_cache'):
            cache_config = self.config.deps_cache
            if cache_config['type'] == 'filesystem':
                if os.path.exists(cache_config['location']):
                    with open(cache_config['location'], 'r') as fp:
                        return list(csv.DictReader(fp))
        return []

    def save_cache(self):
        if not self.dependencies or not hasattr(self.config, 'deps_cache'):
            return
        cache_config = self.config.deps_cache
        if cache_config['type'] == 'filesystem':
            os.makedirs(os.path.dirname(cache_config['location']), exist_ok=True)
            with open(cache_config['location'], 'w') as fp:
                writer = csv.DictWriter(fp, self.dependencies[0].keys())
                writer.writeheader()
                writer.writerows(self.dependencies)

    @property
    @lru_cache(maxsize=1)
    def db(self) -> DB:
        dbclass, _ = get_db_and_query_classes(self.config)
        return dbclass(self.config, cold_run=False)

    @property
    @lru_cache(maxsize=1)
    def dag(self) -> nx.MultiDiGraph:
        """Computes a DAG using networkx. Each node is a (schema, table) tuple.
        """
        dependency_tuples = [(f'{item["source_schema"]}.{item["source_table"]}',
                              f'{item["dependent_schema"]}.{item["dependent_table"]}'
                              ) for item in self.dependencies]
        dg = nx.MultiDiGraph()
        edges = [(from_, to_, {'fontsize': 10.0, 'penwidth': 1}) for from_, to_ in dependency_tuples]
        dg.add_edges_from(edges)
        return dg

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

        dag = self.dag
        for node in dag.nodes:
            dag.node[node].update({
                'fillcolor': lookup(node, 'colors'),
                'shape': lookup(node, 'shapes'),
                'style': 'filled'
            })
        os.environ["PATH"] += os.pathsep + self.config.graphviz_path
        nx.drawing.nx_pydot.to_pydot(dag).write_svg('dependencies.svg')
        if getattr(self.config, 's3_bucket', False):
            import boto3
            s3 = boto3.resource('s3')
            body = open('dependencies.svg', 'rb')
            key = f'{self.config.s3_folder}/dependencies.svg'
            s3.Bucket(self.config.s3_bucket).put_object(Key=key, Body=body)
