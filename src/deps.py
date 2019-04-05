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
            db_schema_table = set(
                str(match) for match in re.findall(regex_db_schema_table, select_stmt.lower(), re.DOTALL))
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
            WHERE schema_name ~ '^{prefix}.*'
            OR    schema_name NOT IN (SELECT table_schema FROM information_schema.tables)
            AND   schema_name ~ '.*_mat$';""".format(prefix=prefix)

        elif self.config.database_type == 'snowflake':
            prefix = prefix.upper()
            cmd = """
            WITH filter_ AS
            (
              SELECT DISTINCT table_schema AS table_schema
              FROM information_schema.tables
            )
            SELECT DISTINCT schema_name AS schema_name_
            FROM information_schema.schemata schemata
              LEFT JOIN filter_ ON filter_.table_schema = schema_name
            WHERE regexp_like(schema_name_,'^{prefix}.*')
            OR (filter_.table_schema IS NULL AND regexp_like(schema_name_,'.*_MAT$'));""".format(prefix=prefix)

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
        def lookup(s, attr):
            value = {
                'colors': 'white',
                'shapes': 'oval'
            }[attr]
            if hasattr(self.config, attr):
                for k, v in getattr(self.config, attr).items():
                    if s.startswith(k):
                        value = v
            return value

        results = [('{source_schema}.{source_table}'.format(**item),
                    '{dependent_schema}.{dependent_table}'.format(**item)
                    ) for item in self.values]
        g = nx.MultiDiGraph()
        edges = [(from_, to_, {'fontsize': 10.0, 'penwidth': 1}) for from_, to_ in results]
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
            s3 = boto3.resource('s3')
            body = open('dependencies.svg', 'rb')
            key = '{}/dependencies.svg'.format(self.config.s3_folder)
            s3.Bucket(self.config.s3_bucket).put_object(Key=key, Body=body)
