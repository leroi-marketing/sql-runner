import argparse
import json
import os
from types import SimpleNamespace


def main():
    args = parse_args()
    run(args)


def parse_args():
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
        '--staging',
        metavar='csv_file',
        help='Execute statements based on the provided list of CSV command files, in staging schemata',
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
        '-i',
        '--except-locally-independent',
        help='When testing or staging, don\'t modify DML for locally independent nodes for this execution',
        action="store_true",
        default=False
    )

    parser.add_argument(
        '--database',
        help='Database name to override for config. Will also change source directory for sql files.',
        nargs='?',
        default=False
    )

    parser.add_argument(
        '--cold-run',
        help="Doesn't do any changes to the database. Just outputs the commands it would have run.",
        default=False,
        action="store_true"
    )
    args = parser.parse_args()

    return args


def run(args):
    from sql_runner import deps, query_list, db, ExecutionType

    with open(args.config) as f:
        config = SimpleNamespace(**json.load(f))
        os.environ["PATH"] += os.pathsep + config.graphviz_path

    if args.database:
        config.auth['database'] = args.database
        config.sql_path = config.sql_path + args.database

    dependencies = deps.Dependencies(config)

    execution_type: ExecutionType = ExecutionType.none
    execution_list: list = []
    if args.execute:
        execution_type = ExecutionType.execute
        execution_list = args.execute
    elif args.staging:
        execution_type = ExecutionType.staging
        execution_list = args.staging
    elif args.test:
        execution_type = ExecutionType.test
        execution_list = args.test

    if execution_type != ExecutionType.none:
        qlist = query_list.QueryList.from_csv_files(config, args, execution_list, dependencies.dependencies,
                                                    execution_type)
        qlist.run()

    elif args.deps:
        schema = config.deps_schema
        dependencies.save(schema)
        dependencies.viz()
    elif args.clean:
        dependencies.clean_schemas(args.clean)


if __name__ == '__main__':
    main()
