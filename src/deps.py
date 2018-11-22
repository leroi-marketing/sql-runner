import itertools
import os
import re

import networkx as nx

from utils import config, get_connection


class Deps:

    def __init__(self):
        self.cursor = get_connection().cursor()

    def get_tables(self):
        cmd = """
        SELECT LOWER(table_schema),
               LOWER(table_name),
               CASE table_type
                   WHEN 'BASE TABLE' THEN 'table'
                   WHEN 'VIEW' THEN 'view'
               END
        FROM information_schema.tables
        WHERE table_schema NOT IN {exclude}
        ORDER BY table_schema,
                 table_name;""".format(exclude=config.exclude_dependencies)
        self.cursor.execute(cmd)
        return self.cursor.fetchall()

    def clean_schemas(self):
        if config.database_type == 'redshift' or config.database_type == 'postgres':
            cmd = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name ~ '^zz_.*';"""
        elif config.database_type == 'snowflake':
            cmd = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE regexp_like(schema_name, '^ZZ_.*');"""
        self.cursor.execute(cmd)
        for schema_name in self.cursor.fetchall():
            self.cursor.execute("DROP SCHEMA {} CASCADE;".format(schema_name[0]))

    def get_column_dict(self):
        column_dict = {}
        rows = []
        offset = 0
        while True:
            self.cursor.execute("""
            SELECT table_schema,
                   table_name,
                   column_name
            FROM information_schema.COLUMNS
            WHERE table_schema NOT IN {exclude}
            ORDER BY table_schema,
                     table_name,
                     ordinal_position LIMIT 1000 OFFSET {offset};
            """.format(offset=offset, exclude=config.exclude_dependencies))
            chunk = self.cursor.fetchall()
            offset += 1000
            if chunk:
                rows.extend(chunk)
            else:
                break
        for key, iter in itertools.groupby(rows, lambda row: '%s.%s' % row[0:2]):
            column_dict[key.lower()] = [row[2].lower() for row in iter]
        return column_dict

    def save_deps(self, path, schema):
        def get_query_dependencies(select_stmt):
            return set(str(match) for match in
                       re.findall(r'(?:from|join)\s*([a-z0-9_]*\.[a-z0-9_]*)(?:\s|;|$)', select_stmt.lower(),
                                  re.DOTALL))

        def has_column_name(column_name, select_stmt):
            return re.search(r'((=|\s|\(|\.|"){}(=|\s|\)|\.|")|select\s+\*)'.format(column_name), select_stmt.lower())

        column_dict = self.get_column_dict()
        self.cursor.execute('CREATE SCHEMA IF NOT EXISTS {};'.format(schema))
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS {}.table_deps 
        (
          schema_name   VARCHAR,
          table_name    VARCHAR,
          column_name   VARCHAR,
          file_path     VARCHAR
        );""".format(schema))
        self.cursor.execute('TRUNCATE {}.table_deps;'.format(schema))
        values = []
        template = "('{schema_name}','{table_name}',{column_name},'{rel_file_path}')"
        for root, _, file_names in os.walk(path):
            for file_name in file_names:
                if file_name[-4:] in ('.sql', '.ddl'):
                    file_path = os.path.normpath(os.path.join(root, file_name))
                    with open(file_path, 'rb') as sql_file:
                        select_stmt = sql_file.read().decode('utf-8')
                        if select_stmt != '':
                            for dep_table in get_query_dependencies(select_stmt):
                                schema_name, table_name = dep_table.split('.')
                                column_names = ['NULL']
                                dep_table = '{}.{}'.format(schema_name, table_name)
                                try:
                                    column_names = [
                                        "'{}'".format(column_name) for column_name in column_dict[dep_table] if
                                        has_column_name(column_name, select_stmt)]
                                except KeyError:
                                    print("{} contains unknown table '{}'".format(file_path, dep_table))
                                rel_file_path = file_path.replace('\\', '/').split(config.sql_path)[1]
                                for column_name in column_names:
                                    values.append(template.format(**locals()))
        if values:
            insert_stmt = """
            INSERT INTO {schema}.table_deps
            VALUES
            {values}""".format(schema=schema, values=',\n'.join(values))
            self.cursor.execute(insert_stmt)

    def viz_deps(self, schema):
        os.environ["PATH"] += os.pathsep + config.graphviz_path
        cmd = """
        SELECT DISTINCT schema_name, table_name, file_path
        FROM {schema}.table_deps
        WHERE file_path NOT LIKE '%{schema}%';
        """.format(schema=schema)
        self.cursor.execute(cmd)
        results = [('{}.{}'.format(*line[0:2]),
                    line[2].split('.')[0].replace('/', '.'))
                   for line in self.cursor.fetchall()]
        g = nx.MultiDiGraph()
        nodes = [(from_, {'shape': 'box', 'color': 'blue'}) for from_, _ in results if from_.startswith('dv')]
        g.add_nodes_from(nodes)
        edges = [(from_, to_, {'fontsize': 10.0, 'penwidth': 1}) for from_, to_ in results]
        g.add_edges_from(edges)
        nx.drawing.nx_pydot.to_pydot(g).write_svg('{path}/map.svg'.format(path=config.sql_path))
