import itertools
import os
import re

import networkx as nx
import sqlparse

import sqlrunner.conn

graphviz_path = 'C:/Program Files (x86)/Graphviz2.38/bin/'


class Deps:

    def __init__(self):
        self.cursor = sqlrunner.conn.get_connection().cursor()

    def create_table_stmt(self, schema, table):
        cmd = """
            SELECT ddl
            FROM lr_monitor.generate_tbl_ddl
            WHERE schemaname='{schema}'
            AND tablename='{table}'
            ORDER BY seq""".format(**locals())
        self.cursor.execute(cmd)
        stmt = '\n'.join(row[0] for row in self.cursor).replace('\n\t,', ',\n\t')
        return stmt

    def create_view_stmt(self, schema, table):
        cmd = """
            SELECT ddl
            FROM lr_monitor.generate_view_ddl
            WHERE schemaname='{schema}'
            AND viewname='{table}'""".format(**locals())
        self.cursor.execute(cmd)
        stmt = sqlparse.format(self.cursor.fetchall()[0][0].replace('`', ''),
                               reindent=True, keyword_case='upper')
        return stmt

    def get_tables(self):
        cmd = """
        SELECT LOWER(table_schema),
               LOWER(table_name),
               CASE table_type
                   WHEN 'BASE TABLE' THEN 'table'
                   WHEN 'VIEW' THEN 'view'
               END
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'looker_scratch', 'information_schema')
        ORDER BY table_schema,
                 table_name;"""
        self.cursor.execute(cmd)
        return self.cursor.fetchall()

    def clean_schemas(self):
        cmd = """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name ~ '^zz_.*';"""
        self.cursor.execute(cmd)
        for schema_name in self.cursor.fetchall():
            self.cursor.execute("DROP SCHEMA {} CASCADE;".format(schema_name[0]))

    def get_column_dict(self):
        column_dict = {}
        self.cursor.execute("""
        SELECT table_schema,
               table_name,
               column_name
        FROM information_schema.COLUMNS
        WHERE table_schema NOT IN ('pg_catalog', 'looker_scratch', 'information_schema')
        ORDER BY table_schema,
                 table_name,
                 ordinal_position
        """)
        for key, iter in itertools.groupby(self.cursor.fetchall(), lambda row: '%s.%s' % row[0:2]):
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
                                rel_file_path = file_path.replace('\\', '/').split('/sql/')[1]
                                for column_name in column_names:
                                    values.append(template.format(**locals()))
        if values:
            insert_stmt = """
            INSERT INTO {schema}.table_deps
            VALUES
            {values}""".format(schema=schema, values=',\n'.join(values))
            self.cursor.execute(insert_stmt)

    def viz_deps(self, schema):
        os.environ["PATH"] += os.pathsep + graphviz_path
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
        nx.drawing.nx_pydot.to_pydot(g).write_svg('output/sqlrunner/map.svg')
