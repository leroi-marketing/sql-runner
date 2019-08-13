import argparse
import json
import os
from types import SimpleNamespace

def main():
    parser = argparse.ArgumentParser(description='Parse arguments SQL runner.')
    parser.add_argument(
        '--config',
        help='Path to the config file',
        nargs='?',
        default='auth/config.json'
    )

    command_group = parser.add_mutually_exclusive_group(required=True)

    command_group.add_argument(
        '--execute',
        metavar='csv_file',
        help='Execute statements based on the provided list of CSV command files',
        nargs='*'
    )
    command_group.add_argument(
        '--test',
        metavar='test_csv_file',
        help='Test execution of statements based on the provided list of CSV command files',
        nargs='*'
    )
    command_group.add_argument(
        '--deps',
        help='View dependencies graph',
        action='store_const',
        const=True,
        default=False
    )
    command_group.add_argument(
        '--clean',
        help='Schemata prefix to clean up',
        nargs='?',
        default='test_'
    )

    parser.add_argument(
        '--database',
        help='Database name',
        nargs='?',
        default=False
    )
    args = parser.parse_args()


    from src import deps, query_list, db

    with open(args.config) as f:
        config = SimpleNamespace(**json.load(f))
        os.environ["PATH"] += os.pathsep + config.graphviz_path

    if hasattr(config, 'test_schema_prefix'):
        schema_prefix = config.test_schema_prefix
    else:
        schema_prefix = db.Query.default_schema_prefix

    if args.database:
        config.auth['database'] = args.database
        config.sql_path = config.sql_path + args.database

    if args.execute:
        query_list.QueryList.from_csv_files(config, args.execute).execute()
        deps.Dependencies(config).clean_schemas(schema_prefix)

    elif args.test:
        query_list.QueryList.from_csv_files(config, args.test).test()
        deps.Dependencies(config).clean_schemas(schema_prefix)

    elif args.deps:
        schema = config.deps_schema
        d = deps.Dependencies(config)
        d.save(schema)
        d.viz()

    elif args.clean:
        deps.Dependencies(config).clean_schemas(args.clean)


if __name__ == '__main__':
    main()
