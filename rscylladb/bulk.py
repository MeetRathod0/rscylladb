from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import numpy as np
import threading
import time
import sys
import logging as lg
from logger import create_logger

logger = create_logger("bulk_insert")

def insert(hosts:list,port:int,table_name="",user="cassandra",password="cassandra",file_name="data.csv",workers=4,chuncks=4):
    
    if len(sys.argv)<=1: 
        if table_name=="": raise Exception("table_name is required!")
    else:
        table_name=sys.argv[1] 

    start_time = time.time() 
    # get keyspace & table name
    if len(table_name.split("."))!=2: raise Exception("Table name must be! keyspace.table_name")
    # keyspace
    keyspace_name = table_name.split(".")[0]
    # table nme
    table_name = table_name.split(".")[1]

    logger.debug(f"keyspace >> {keyspace_name}")
    logger.debug(f"table    >> {table_name}") 
    # set authenticator
    auth = PlainTextAuthProvider(username=user,password=password)
    # set cluster
    cluster = Cluster(contact_points=hosts,port=port,auth_provider=auth)
    # set keyspace
    session = cluster.connect(keyspace_name)
    logger.debug("Connected to %s keyspace..."%(keyspace_name))
    stmt = SimpleStatement("SELECT column_name,type FROM system_schema.columns WHERE keyspace_name=%(key)s AND table_name=%(tbl)s")
    cql_columns = []
    cql_types = {}
    # has cql to python data type
    cql_to_python_type = { 
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
        'ascii':str,
        'varchar':str,
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
    # get table's all columns and data type
    table_schema = session.execute_async(stmt,{ "key":keyspace_name,"tbl":table_name})
    for row in table_schema.result():
        cql_types[row.column_name] = cql_to_python_type[row.type]
        cql_columns.append(row.column_name)

    insert__statement = ""
    if len(cql_columns)>=1:
        param_keys = ",".join(cql_columns) 
        param_values = ",".join([ '%('+k+')s' for k in cql_columns])
        insert__statement = "INSERT INTO "+table_name+" ("+param_keys+") VALUES ("+param_values+")"
    else:
        logger.debug("No columns found in table! columns fetch from csv.")

    def execute_insert(df:pd.DataFrame,session)->None:
        # handle null for numeric type default 0 add
        df[df.select_dtypes(include=np.number).columns] = df.select_dtypes(include=np.number).fillna(0).astype(int) 
        # fill boolean
        df[df.select_dtypes(include=bool).columns] = df.select_dtypes(include=bool).fillna(False)
        # handle null for object and string types
        df[df.select_dtypes(exclude=np.number).columns] = df.select_dtypes(exclude=np.number).fillna('') 
        
        # empty list
        futures = []
        # excute async & store future object
        for row in df.to_dict(orient='records'): # iterate each records
            futures.append(session.execute_async(insert__statement,row))

        # wait for complete async 
        for future in futures:
            future.result() # wait until insert queries
            
    data_frames = pd.read_csv(file_name,chunksize=chuncks)
    threads = list(range(workers))
    count = 0
    while True:
        try:
            # start thread
            for i in range(workers):
                # fetch next dataframe
                df = next(data_frames)
                count+=len(df)
                df.columns = map(str.lower,df.columns)
                if len(cql_columns)==0:
                    cql_columns = df.columns.to_list()
                    param_keys = ",".join(cql_columns) 
                    param_values = ",".join([ '%('+k+')s' for k in cql_columns])
                    insert__statement = "INSERT INTO "+table_name+" ("+param_keys+") VALUES ("+param_values+")"
                threads[i] = threading.Thread(target=execute_insert,args=(df,session,))
                threads[i].start()
            
            # wait until complete task
            for i in range(workers):
                threads[i].join()
        # on execption break
        except Exception as e:
            print(e)
            break

    duration_minutes = (time.time() - start_time) / 60
    logger.debug(
            f"DONE! {count} Records Transferred In {duration_minutes:.2f} Minutes.")
