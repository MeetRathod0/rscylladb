import threading
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from collections import namedtuple
import pandas as pd
import numpy as np
import time
import logging as lg
from .logger import create_logger

lg.getLogger("cassandra").setLevel('ERROR')

logger = create_logger("thread.AscyllaDB")


class AScyllaDB(threading.Thread):
    def __init__(self, host_list: list, port: int, keyspace: str, table_name: str, username: str, password: str, file_name='data.csv', chunks=10000, workers=4) -> None:
        self.start_time = time.time()
        super().__init__()
        auth = PlainTextAuthProvider(username, password)
        cluster = Cluster(host_list,
                          port, auth_provider=auth)
        logger.debug(
            f"Connection started... >> {str(host_list)}:{port} | keyspace: {keyspace} | table: {table_name}.")
        session = cluster.connect(keyspace)
        logger.debug(f"{keyspace} keyspace is connected.")

        Conf = namedtuple(
            "Conf", ["cluster", "session", "keyspace", "table_name", "file_name", "chunks", "workers"])
        self.conf = Conf(cluster, session, keyspace,
                         table_name, file_name, chunks, workers)

    def init_insert(self):
        data_frames = pd.read_csv(
            self.conf.file_name, chunksize=self.conf.chunks, dtype=self.dict_table_types)
        logger.debug("File is successfully loaded.")
        threads = list(range(self.conf.workers))
        logger.debug(f"{self.conf.workers} workers are ready.")

        def execute_insert(df: pd.DataFrame) -> None:
            # make lower columns name for cql
            df.columns = map(str.lower, df.columns)
            # handle null for numeric type default 0 add
            df[df.select_dtypes(include=np.number).columns] = df.select_dtypes(
                include=np.number).fillna(0).astype(int)
            # fill boolean
            df[df.select_dtypes(include=bool).columns] = df.select_dtypes(
                include=bool).fillna(False)
            # handle null for object and string types
            df[df.select_dtypes(exclude=np.number).columns] = df.select_dtypes(
                exclude=np.number).fillna('')
            # empty list
            futures = []
            # excute async & store future object
            for row in df.to_dict(orient='records'):  # iterate each records
                futures.append(self.conf.session.execute_async(
                    self.insert__statement, row))

            # wait for complete async
            for future in futures:
                future.result()  # wait until insert queries

        flag = 1
        count = 0
        while True:
            try:
                # start thread
                for i in range(self.conf.workers):
                    # fetch next dataframe
                    df = next(data_frames)
                    count += len(df)
                    df.columns = map(str.lower, df.columns)
                    if len(self.list_columns) == 0 and flag == 1:
                        self.list_columns = df.columns.to_list()
                        logger.warn("Assign column from the file.")
                        param_keys = ",".join(self.list_columns)
                        param_values = ",".join(
                            ['%('+k+')s' for k in self.list_columns])
                        self.insert__statement = "INSERT INTO "+self.conf.table_name + \
                            " ("+param_keys+") VALUES ("+param_values+")"
                        logger.debug(f"{self.insert} statement is generated.")
                        flag = 0
                    threads[i] = threading.Thread(
                        target=execute_insert, args=(df,))
                    threads[i].start()
                # wait until complete task
                for i in range(self.conf.workers):
                    threads[i].join()
            # on execption break
            except Exception as e:
                print(e)
                break
        duration_minutes = (time.time() - self.start_time) / 60
        logger.debug(
            f"DONE! {count} Records Transferred In {duration_minutes:.2f} Minutes.")

    def set_columns(self):
        logger.debug(f"Start fetch columns from {self.conf.table_name}.")
        table_schema = self.conf.session.execute(
            "SELECT column_name,type FROM system_schema.columns WHERE keyspace_name=%s AND table_name=%s", [self.conf.keyspace, self.conf.table_name])
        # has cql to python data type
        self.cql_to_python_type = {
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

        self.list_columns = []
        self.dict_table_types = {}
        for row in table_schema:
            self.dict_table_types[row.column_name] = self.cql_to_python_type[row.type]
            self.list_columns.append(row.column_name)

        if len(self.list_columns) != 0:
            param_keys = ",".join(self.list_columns)
            param_values = ",".join(['%('+k+')s' for k in self.list_columns])
            self.insert__statement = "INSERT INTO "+self.conf.table_name + \
                " ("+param_keys+") VALUES ("+param_values+")"
            n = len(self.list_columns)
            logger.debug(
                f"{n} columns are fetched from {self.conf.table_name}.")
        else:
            logger.warning(
                f"No columns found from table {self.conf.table_name}.")

    def run(self):
        self.set_columns()
        self.init_insert()
