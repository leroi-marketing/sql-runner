import argparse
import sqlrunner.query_list
import sqlrunner.deps
import sqlrunner.lookml

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse arguments SQL runner.')
    parser.add_argument('--execute', nargs='*')
    parser.add_argument('--test', nargs='*')
    parser.add_argument('--deps', action='store_const', const=True, default=False)
    parser.add_argument('--clean', action='store_const', const=True, default=False)
    parser.add_argument('--docs', action='store_const', const=True, default=False)
    args = parser.parse_args()

    path = '../sql'
    if args.execute:
        sqlrunner.query_list.QueryList.from_csv_files(path, args.execute).execute()
    elif args.test:
        sqlrunner.query_list.QueryList.from_csv_files(path, args.test).test()
    elif args.clean:
        sqlrunner.deps.Deps().clean_schemas()
    elif args.docs:
        sqlrunner.lookml.parse_lookml('../../Ergo_Direkt_MDH_Lookml')
    elif args.deps:
        schema = 'lr_monitor'
        d = sqlrunner.deps.Deps()
        d.save_deps(path, schema)
        d.viz_deps(schema)
