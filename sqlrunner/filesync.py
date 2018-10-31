import os

import apiclient.discovery
import httplib2
import oauth2client.service_account

import sqlrunner.conn


def complete_ddl_files(db, path):
    table_keys = set()
    for schema, table, ddl_type in db.get_tables():
        file_dir = os.path.join(os.path.normpath(path), schema.lower())
        if ddl_type == 'table':
            table_keys.add(schema.lower() + '.' + table.lower())
        file_name = '{table}.{suffix}'.format(table=table.lower(), suffix=['ddl', 'sql'][ddl_type == 'view'])
        file_path = os.path.join(file_dir, file_name)
        if not os.path.isfile(file_path):
            if ddl_type == 'table':
                ddl_stmt = db.create_table_stmt(schema, table)
            elif ddl_type == 'view':
                ddl_stmt = db.create_view_stmt(schema, table)
            else:
                raise ValueError('ddl type {} unknown'.format(ddl_type))
            print('write %s ' % file_path)
            if not os.path.exists(file_dir):
                os.makedirs(file_dir)
            with open(file_path, 'wb') as f:
                f.write(ddl_stmt.encode('utf-8'))

    for root, _, files in os.walk(os.path.normpath(path)):
        for file_name in files:
            key = '%s.%s' % (root.replace('\\', '/').strip('/').split('/')[-1], file_name[:-4])
            if file_name[-4:] == 'ddl' and key not in table_keys:
                full_path = os.path.normpath(os.path.join(os.getcwd(), root, file_name))
                print("found '%s' but no table '%s'" % (full_path, key))
                os.unlink(full_path)