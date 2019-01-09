import itertools
import os
import re

import networkx as nx

from utils import config, get_connection


class Dependencies:

    def __init__(self, path):
        self.path = path
        self.cursor = get_connection().cursor()

        def get_query_sources(select_stmt):
            return set(str(match) for match in
                       re.findall(r'(?:from|join)\s*([a-z0-9_]*\.[a-z0-9_]*)(?:\s|;|$)', select_stmt.lower(),
                                  re.DOTALL))

        self.values = []
        for root, _, file_names in os.walk(path):
            for file_name in file_names:
                if file_name[-4:] == '.sql':
                    file_path = os.path.normpath(os.path.join(root, file_name))
                    with open(file_path, 'rb') as sql_file:
                        select_stmt = sql_file.read().decode('utf-8')
                        if select_stmt != '':
                            dependent_schema = os.path.basename(os.path.normpath(root))
                            dependent_table = file_name[:-4]
                            for source in get_query_sources(select_stmt):
                                source_schema, source_table = source.split('.')
                                self.values.append({
                                    'source_schema': source_schema,
                                    'source_table': source_table,
                                    'dependent_schema': dependent_schema,
                                    'dependent_table': dependent_table
                                })

    def clean_schemas(self):
        if config.database_type == 'redshift' or config.database_type == 'postgres':
            cmd = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name ~ '^{prefix}.*';""".format(prefix=config.test_schema_prefix)
        elif config.database_type == 'snowflake':
            cmd = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE regexp_like(schema_name, '^{prefix}.*');""".format(prefix=config.test_schema_prefix.upper())
        self.cursor.execute(cmd)
        for schema_name in self.cursor.fetchall():
            self.cursor.execute("DROP SCHEMA {} CASCADE;".format(schema_name[0]))

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
        results = [('{source_schema}.{source_table}'.format(**item),
                    '{dependent_schema}.{dependent_table}'.format(**item)
                    ) for item in self.values]
        g = nx.MultiDiGraph()
        edges = [(from_, to_, {'fontsize': 10.0, 'penwidth': 1}) for from_, to_ in results]
        g.add_edges_from(edges)
        nx.drawing.nx_pydot.to_pydot(g).write_svg('{path}/map.svg'.format(path=config.sql_path))

    #
    # The following column related code is not used and might be re-activated
    #

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

    def get_columns(self, dep_table, select_stmt, file_path):
        template = "('{schema_name}','{table_name}',{column_name},'{rel_file_path}')"
        column_dict = self.get_column_dict()

        def has_column_name(column_name, select_stmt):
            return re.search(r'((=|\s|\(|\.|"){}(=|\s|\)|\.|")|select\s+\*)'.format(column_name), select_stmt.lower())

        try:
            column_names = [
                "'{}'".format(column_name) for column_name in column_dict[dep_table] if
                has_column_name(column_name, select_stmt)]
        except KeyError:
            print("{} contains unknown table '{}'".format(file_path, dep_table))
        rel_file_path = file_path.replace('\\', '/').split(config.sql_path)[1]
        return [template.format(**locals()) for column_name in column_names]
