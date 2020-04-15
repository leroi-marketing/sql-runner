import csv
import datetime
import io
from collections import defaultdict
from types import SimpleNamespace
from typing import Dict, List, Callable, Iterable

from sql_runner import ExecutionType
from sql_runner.db import DB, get_db_and_query_classes


class QueryList(list):

    actions: Dict[str, str] = {
        'e': 'execute_stmt',
        't': 'create_table_stmt',
        # For testing whether the query works
        'mock': 'create_mock_relation_stmt',
        'v': 'create_view_stmt',
        'm': 'materialize_view_stmt',
        'check': 'run_check_stmt',
        's': 'skip'
    }

    def __init__(self, config: SimpleNamespace, args: SimpleNamespace, csv_string: str,
                 dependencies: List[Dict], execution_type: ExecutionType):
        super().__init__()
        self.execution_type = execution_type
        DBClass, QueryClass = get_db_and_query_classes(config)
        self.config = config
        self.cold_run = args.cold_run
        self.db: DB = DBClass(config, args.cold_run)
        given_order = []
        requested_queries_dict = {}
        for query in csv.DictReader(io.StringIO(csv_string.strip()), delimiter=';'):
            if not query['schema_name'].startswith('#'):
                given_order.append(query)
                requested_queries_dict[(query['schema_name'], query['table_name'])] = query

        entities_to_be_created_set = set(requested_queries_dict.keys())

        indexed_dependencies = defaultdict(list)
        for d in dependencies:
            indexed_dependencies[(d['dependent_schema'], d['dependent_table'])].append(
                (d['source_schema'], d['source_table'])
            )

        added_entities_set = set()

        def add_query(schema, table) -> int:
            """ Adds a query that creates schema.table, but first it adds its dependencies recursively
            """
            query_key = (schema, table)
            is_top_node = True
            # if this query depends on other queries, add the dependencies first
            if query_key in indexed_dependencies:
                for dep in indexed_dependencies[query_key]:
                    if add_query(*dep):
                        is_top_node = False

            if query_key in requested_queries_dict:
                # Only if this query is requested
                if query_key not in added_entities_set:
                    self.append(
                        QueryClass(config, args, entities_to_be_created_set, execution_type,
                                   **requested_queries_dict[query_key])
                    )
                    added_entities_set.add(query_key)
                # Tell that this query was added, now or before
                return True
            return False

        for query in given_order:
            add_query(query['schema_name'], query['table_name'])

    @staticmethod
    def from_csv_files(config: SimpleNamespace, args: SimpleNamespace, csv_files: List[str],
                       dependencies: List[Dict], execution_type: ExecutionType) -> "QueryList":
        """ Creates a query list from a list of CSV file names, passed in as Command Line Arguments
        """
        if not isinstance(csv_files, list):
            csv_files = [csv_files]
        print('read query lists from: {}'.format(', '.join(csv_files)))
        csv_string = ['schema_name;table_name;action']
        for file in csv_files:
            file_path = f'{config.sql_path}/{file}.csv'
            with open(file_path, 'r', encoding=getattr(config, 'encoding', 'utf-8')) as f:
                csv_string.append(f.read().strip())
        return QueryList(config, args, '\n'.join(csv_string), dependencies, execution_type)

    def run(self):
        """ Execute every statement from every query
        """
        run_start = datetime.datetime.now()
        created_schemata = set()
        for query in self:
            start = datetime.datetime.now()
            if self.execution_type == ExecutionType.test:
                # Just validate syntax
                if query.action in {'e', 'check'}:
                    query.action = 's'
                else:
                    query.action = 'mock'
            print(query)
            if query.action in QueryList.actions:
                # Any of 'query', 'create_table_stmt', 'create_view_stmt', 'materialize_view_stmt', 'run_check'
                stmt_type = QueryList.actions[query.action]
                statement_generator: Callable[[], Iterable[str]] = query.get_statement_generator(stmt_type)
                # Get list of individual specific statements and process them
                for stmt in statement_generator():
                    self.db.execute(stmt, query)

                    if self.execution_type in (ExecutionType.execute, ExecutionType.staging) and not self.cold_run:
                        # Validate data only when data is computed properly
                        assertion = query.assertion
                        if assertion:
                            assertion(rows=self.db.fetchall())
                # Keep track of what gets created in the test
                if self.execution_type == ExecutionType.test and query.action == 'mock':
                    created_schemata.add(query.schema)
            print(datetime.datetime.now() - start)

        if self.execution_type == ExecutionType.test:
            # Clean up the temporary views
            self.db.clean_specific_schemas(created_schemata)
        print('Run finished in {}'.format(datetime.datetime.now() - run_start))
