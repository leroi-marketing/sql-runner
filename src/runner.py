import argparse
from src import deps, query_list
import json
import os
from types import SimpleNamespace

def main():
    parser = argparse.ArgumentParser(description='Parse arguments SQL runner.')
    parser.add_argument('--config', nargs='?', default='auth/config.json')
    parser.add_argument('--execute', nargs='*')
    parser.add_argument('--test', nargs='*')
    parser.add_argument('--deps', action='store_const', const=True, default=False)
    parser.add_argument('--database', nargs='?', default=False)
    parser.add_argument('--clean', nargs='?', default='test_')
    args = parser.parse_args()

    with open(args.config) as f:
        config = SimpleNamespace(**json.load(f))
        os.environ["PATH"] += os.pathsep + config.graphviz_path

    if hasattr(config, 'test_schema_prefix'):
        schema_prefix = config.test_schema_prefix
    else:
        schema_prefix = query_list.Query.default_schema_prefix

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
