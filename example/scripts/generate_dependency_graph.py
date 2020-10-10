import graphviz
import csv
import logging
import argparse


LOGGER = logging.getLogger(__name__)


def generate_graph(table: str, downstream_tables: dict, upstream_tables: dict, target_folder: str) -> None:
    LOGGER.info(f'start generating graph for {table}.')

    graph = graphviz.Digraph(table)
    graph.node(table, style="filled", color="purple")

    # dfs for downstream deps of table
    visited = {table}
    stack = [table]
    while stack:
        cur_table = stack.pop()
        for child_table in downstream_tables[cur_table]:
            if child_table not in visited:
                visited.add(child_table)
                stack.append(child_table)
                # create the node for 'child_table'
                graph.node(child_table)
                # create an edge from 'cur_table' to 'child_table'
                graph.edge(cur_table, child_table, 'downstream')

    # dfs for upstream deps of the table
    stack = [table]
    while stack:
        cur_table = stack.pop()
        for child_table in upstream_tables[cur_table]:
            if child_table not in visited:
                visited.add(child_table)
                stack.append(child_table)
                # create the node for 'child_table'
                graph.node(child_table)
                # create an edge from 'child_table' to 'cur_table' since 'child_table' is the upstream for 'cur_table'
                graph.edge(child_table, cur_table, 'downstream')

    graph.render(directory=target_folder, cleanup=True, format='png')
    LOGGER.info(f'successfully generated graph for {table}.')


def main(source_csv: str, target_folder: str) -> None:
    downstream_tables = {}
    upstream_tables = {}
    with open(source_csv, newline='', encoding='utf-8-sig') as f:
        csv_reader = csv.DictReader(f, delimiter=',')
        for row in csv_reader:
            table_uri = f'{row["CLUSTER"]}.{row["SCHEMA"]}.{row["TABLE_NAME"]}'
            if table_uri not in downstream_tables:
                downstream_tables[table_uri] = []
            if table_uri not in upstream_tables:
                upstream_tables[table_uri] = []
            if not row['DOWNSTREAM_DEPS']:
                LOGGER.info(f'{table_uri} has no downstream deps.')
                continue
            downstream_deps = row['DOWNSTREAM_DEPS'].split(',')
            downstream_tables[table_uri].extend(downstream_deps)

            for dep in downstream_deps:
                if dep not in downstream_tables:
                    downstream_tables[dep] = []
                if dep not in upstream_tables:
                    upstream_tables[dep] = []
                upstream_tables[dep].append(table_uri)

    # generate graph file for every table
    for table in downstream_tables:
        generate_graph(table, downstream_tables, upstream_tables, target_folder)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate dependencies for tables.')
    parser.add_argument('--source-csv', type=str, required=True, help='path to the source csv files containing the '
                                                                      'downstream deps for each table')
    parser.add_argument('--target-folder', type=str, required=True, help='path to the folder to save the '
                                                                         'generated graphs.')
    args = parser.parse_args()
    main(args.source_csv, args.target_folder)
