import argparse
import query_list
import deps
from utils import config

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse arguments SQL runner.')
    parser.add_argument('--execute', nargs='*')
    parser.add_argument('--test', nargs='*')
    parser.add_argument('--deps', action='store_const', const=True, default=False)
    args = parser.parse_args()

    if args.execute:
        query_list.QueryList.from_csv_files(config.sql_path, args.execute).execute()
    elif args.test:
        query_list.QueryList.from_csv_files(config.sql_path, args.test).test()
        deps.Dependencies.clean_schemas()
    elif args.deps:
        schema = config.deps_schema
        d = deps.Dependencies(config.sql_path)
        d.save(schema)
        d.viz()
