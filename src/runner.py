import argparse
import query_list
import deps
import json
from types import SimpleNamespace

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse arguments SQL runner.')
    parser.add_argument('--config', nargs='?', default='auth/config.json')
    parser.add_argument('--execute', nargs='*')
    parser.add_argument('--test', nargs='*')
    parser.add_argument('--deps', action='store_const', const=True, default=False)
    args = parser.parse_args()

    with open(args.config) as f:
        config = SimpleNamespace(**json.load(f))

    if args.execute:
        query_list.QueryList.from_csv_files(config, args.execute).execute()
    elif args.test:
        query_list.QueryList.from_csv_files(config, args.test).test()
        deps.Dependencies(config).clean_schemas()
    elif args.deps:
        schema = config.deps_schema
        d = deps.Dependencies(config)
        d.save(schema)
        d.viz()
