from .logger import get_logger
from .connect import get_connection
from .schema import get_schema_cols
import pandas as pd
import os
import sys
import numpy as np
import threading
import re
import time
logger = get_logger(os.path.basename(__file__)+"_insert")


def cdc_insert(hosts: list, port: int = 9042, keyspace_name: str = '', username: str = 'cassandra', password: str = 'cassandra', file_name: str = 'data.csv', chunks: int = 10000, workers: int = 4) -> None:
    try:
        start_time = time.time()
        data_frames = pd.read_csv(file_name, chunksize=chunks)
        # find schema & table name from metadata column
        schema = [i.split("=")[1] for i in re.findall(
            "(?:schema=[A-z0-9\-_]+)|table=[A-z0-9\-_]+", next(data_frames).loc[0, "metadata"])]
        table_name = schema[0]

        cluster, session = get_connection(
            hosts, port, keyspace_name, username, password)
        list_columns, csv_dtypes = get_schema_cols(
            hosts, port, keyspace_name, table_name, username, password)

        if len(list_columns) <= 0:
            df = next(data_frames)
            df = df.drop('metadata', axis=1)  # drop metadata column
            list_columns = map(str.lower, df.columns.to_list())
            logger.warning(f"Unable to fetch columns from {table_name}")
            logger.info("Columns fetched from the CSV file")
        else:
            data_frames.close()
            data_frames = pd.read_csv(
                file_name, chunksize=chunks, dtype=csv_dtypes)
            logger.info(f"Columns successfully fetched from the {table_name}")

        insert_statement = "INSERT INTO "+table_name + \
            " (" + ",".join(list_columns) + ") VALUES (" + \
            ",".join(['%('+k+')s' for k in list_columns])+")"

        def execute_insert(df: pd.DataFrame, session) -> None:
            # convert lower names
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
                futures.append(session.execute_async(insert_statement, row))

            # wait for complete async
            for future in futures:
                future.result()  # wait until insert queries

        threads = list(range(workers))
        logger.debug(f"{workers} workers are ready!")
        count = 0
        while True:
            try:
                # start thread
                for i in range(workers):
                    # fetch next dataframe
                    df = next(data_frames)
                    count += len(df)
                    try:
                        threads[i] = threading.Thread(
                            target=execute_insert, args=(df, session,))
                        threads[i].start()
                    except Exception as e:
                        logger.error("\n{}\nERROR".format('-'*30), exc_info=e)
                # wait until complete task
                for i in range(workers):
                    threads[i].join()
            # on execption break
            except Exception as e:
                break

        duration_minutes = (time.time() - start_time) / 60
        logger.debug(
            f"DONE! {count} Records Transferred In {duration_minutes:.2f} Minutes.")
    except Exception as e:
        logger.error("\n{}\nERROR".format('-'*30), exc_info=e)
        exit(1)


__all__ = ["cdc_insert"]
