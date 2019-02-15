import os
import re

import networkx as nx
import query_list
import boto3


class Dependencies:

    def __init__(self, config):
        self.config = config
        self.cursor = query_list.get_connection(config).cursor()

        def get_query_sources(select_stmt):
            regex_schema_table = r'(?:from|join)\s*([a-z0-9_]*\.[a-z0-9_]*)(?:\s|;|,|$)'
            regex_db_schema_table = r'(?:from|join)\s*([a-z0-9_]*\.[a-z0-9_]*\.[a-z0-9_]*)(?:\s|;|,|$)'
            schema_table = set(str(match) for match in re.findall(regex_schema_table, select_stmt.lower(), re.DOTALL))
            db_schema_table = set(str(match) for match in re.findall(regex_db_schema_table, select_stmt.lower(), re.DOTALL))
            return schema_table | db_schema_table

        self.values = []
        for root, _, file_names in os.walk(config.sql_path):
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
                                self.values.append({
                                    'source_schema': source_schema,
                                    'source_table': source_table,
                                    'dependent_schema': dependent_schema,
                                    'dependent_table': dependent_table
                                })

    def clean_schemas(self, prefix):
        cursor = query_list.get_connection(self.config).cursor()
        if self.config.database_type == 'redshift' or self.config.database_type == 'postgres':
            cmd = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name ~ '^{prefix}.*';""".format(prefix=prefix)
        elif self.config.database_type == 'snowflake':
            cmd = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE regexp_like(schema_name, '^{prefix}.*');""".format(prefix=prefix.upper())
        cursor.execute(cmd)
        for schema_name in cursor.fetchall():
            cursor.execute("DROP SCHEMA {} CASCADE;".format(schema_name[0]))

    def save(self, monitor_schema):
        if not self.values:
            return

        template = "('{source_schema}','{source_table}','{dependent_schema}','{dependent_table}')"
        values = ',\n'.join(template.format(**item) for item in self.values)
        self.cursor.execute('CREATE SCHEMA IF NOT EXISTS {};'.format(monitor_schema))
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS {}.table_deps 
        (
          source_schema    VARCHAR,
          source_table     VARCHAR,
          dependent_schema VARCHAR,
          dependent_table  VARCHAR
        );""".format(monitor_schema))
        self.cursor.execute('TRUNCATE {}.table_deps;'.format(monitor_schema))
        insert_stmt = """
        INSERT INTO {schema}.table_deps
        VALUES
        {values}""".format(schema=monitor_schema, values=values)
        self.cursor.execute(insert_stmt)

    def viz(self):
        def key(s):
            s = s.split('.')[0]
            pos = s.find('_') + 1
            return (s, s[:pos])[pos > 0]

        os.environ["PATH"] += os.pathsep + self.config.graphviz_path
        cmd = """
        SELECT DISTINCT schema_name, table_name, file_path
        FROM {schema}.table_deps
        WHERE file_path !~ '({schema}|admin)';
        """.format(schema=self.config.deps_schema)
        self.cursor.execute(cmd)
        results = [('{}.{}'.format(*line[0:2]),
                    line[2].split('.')[0].replace('/', '.'))
                   for line in self.cursor.fetchall()]
        g = nx.MultiDiGraph()
        nodes = [(from_, {}) for from_, _ in results]
        g.add_nodes_from(nodes)
        edges = [(from_, to_, {'fontsize': 10.0, 'penwidth': 1}) for from_, to_ in results]
        g.add_edges_from(edges)
        for node in g.nodes:
            g.node[node].update({
                'fillcolor': self.config.colors.get(key(node), 'white'),
                'shape': self.config.shapes.get(key(node), 'oval'),
                'style': 'filled'
            })
        nx.drawing.nx_pydot.to_pydot(g).write_svg('dependencies.svg')
        if self.config.s3_bucket:
            s3 = boto3.resource('s3')
            data = open('dependencies.svg', 'rb')
            s3.Bucket(self.config.s3_bucket).put_object(Key='{}/dependencies.svg'.format(self.config.s3_folder), Body=data)
