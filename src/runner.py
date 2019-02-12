import argparse
import query_list
import deps
import json
import os
from types import SimpleNamespace

if __name__ == '__main__':
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
        deps.Dependencies(config).clean_schemas(config.test_schema_prefix)
    elif args.deps:
        schema = config.deps_schema
        d = deps.Dependencies(config)
        d.save(schema)
        d.viz()
    elif args.staging:
        query_list.QueryList.from_csv_files(config, args.staging).stage()
    elif args.clean:
        deps.Dependencies(config).clean_schemas(args.clean)

