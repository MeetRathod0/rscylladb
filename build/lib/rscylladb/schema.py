from .logger import get_logger
from .connect import get_connection
from cassandra.query import SimpleStatement
import pandas as pd
import os

_dict_cql_to_python_type = {
    'NULL': None,
    'boolean': pd.BooleanDtype(),
    'float': pd.Float64Dtype(),
    'double': pd.Float64Dtype(),
    'int': pd.Int64Dtype(),
    'smallint': pd.Int16Dtype(),
    'tinyint': pd.Int64Dtype(),
    'counter': pd.Int64Dtype(),
    'varint': pd.Int64Dtype(),
    'bigint': pd.Int64Dtype(),
    'decimal': pd.Float64Dtype(),
    'ascii': str,
    'varchar': str,
    'text': str,
    'blob': str,
    'date': str,
    'timestamp': str,
    'time': str,
    'list': list,
    'set': set,
    'map': dict,
    'timeuuid,': None,
    'uuid': None
}

logger = get_logger(os.path.basename(__file__)+"_get_schema_cols")


def get_schema_cols(hosts: list, port: int, keyspace: str, table_name: str, username: str, password: str):
    cluster, session = get_connection(
        hosts, port, keyspace, username, password)
    statment = SimpleStatement(
        "SELECT column_name,type FROM system_schema.columns WHERE keyspace_name=%s AND table_name=%s")
    logger.info(f"Start fetching columns from the {table_name}")
    data = session.execute(statment, (keyspace, table_name))
    list_table_cols = []
    dict_csv_cols = {}
    for row in data:
        dict_csv_cols[row.column_name] = _dict_cql_to_python_type[row.type]
        list_table_cols.append(row.column_name)
    session.shutdown()
    cluster.shutdown()
    logger.debug("Successfully shutdown session.")
    return list_table_cols, dict_csv_cols


__all__ = ["get_schema_cols", "cql_to_python_type"]
