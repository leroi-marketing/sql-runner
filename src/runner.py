import argparse
import query_list
import deps
import json
import os
from types import SimpleNamespace

if __name__ == '__main__':
    """
    TODO: split auth and config to have config in git
    TODO: move config into src and complete custom config with default values
    TODO: check whether move from SimpleNamespace back to dict for config
    """
    parser = argparse.ArgumentParser(description='Parse arguments SQL runner.')
    parser.add_argument('--config', nargs='?', default='auth/config.json')
    parser.add_argument('--execute', nargs='*')
    parser.add_argument('--test', nargs='*')
    parser.add_argument('--deps', action='store_const', const=True, default=False)
    parser.add_argument('--staging', nargs='*')
    parser.add_argument('--database', nargs='?', default=False)
    parser.add_argument('--clean', nargs='?', default='test_')
    args = parser.parse_args()

    with open(args.config) as f:
        config = SimpleNamespace(**json.load(f))
        os.environ["PATH"] += os.pathsep + config.graphviz_path

    if args.database:
        config.database = args.database
        config.sql_path = config.sql_path + args.database

    if args.execute:
        query_list.QueryList.from_csv_files(config, args.execute).execute()
    elif args.test:
        query_list.QueryList.from_csv_files(config, args.test).test()
        if hasattr(config, 'test_schema_prefix'):
            schema_prefix = config.test_schema_prefix
        else:
            schema_prefix = query_list.Query.default_schema_prefix
        deps.Dependencies(config).clean_schemas(schema_prefix)
    elif args.deps:
        schema = config.deps_schema
        d = deps.Dependencies(config)
        d.save(schema)
        d.viz()
    elif args.staging:
        query_list.QueryList.from_csv_files(config, args.staging).stage()
    elif args.clean:
        deps.Dependencies(config).clean_schemas(args.clean)

