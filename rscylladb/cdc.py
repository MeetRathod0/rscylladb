from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import numpy as np
import threading
import time
import re
import logging as lg

lg.getLogger("cassandra").setLevel('ERROR')
formatter = lg.Formatter(
    '%(asctime)s  %(levelname)s  %(message)s')
console_handler = lg.StreamHandler()
console_handler.setLevel(lg.DEBUG)
console_handler.setFormatter(formatter)
logger = lg.getLogger()
logger.addHandler(console_handler)
logger.setLevel(lg.DEBUG)
logger.propagate = False

def insert(hosts:list,port:int,keyspace_name:str,user="cassandra",password="cassandra",file_name="data.csv",workers=4,chuncks=4):
    start_time = time.time()
    dfs = pd.read_csv("data.csv",chunksize=10000)
    # find schema & table name from metadata column
    schema = [ i.split("=")[1] for i in re.findall("(?:schema=[A-z0-9\-_]+)|table=[A-z0-9\-_]+",next(dfs).loc[0,"metadata"]) ] 
    table_name = schema[0]

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


    is_col_null = True
    if len(cql_columns)>=1:
        is_col_null = False
        
    param_keys = ",".join(cql_columns) 
    param_values = ",".join([ '%('+k+')s' for k in cql_columns])
    insert__statement = "INSERT INTO "+table_name+" ("+param_keys+") VALUES ("+param_values+")"


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
    count=0
    while True:
        try:
            # start thread
            for i in range(workers):
                # fetch next dataframe
                df = next(data_frames)
                df.columns = map(str.lower,df.columns)
                count+=len(df)
                if is_col_null:
                    df = df.drop('metadata',axis=1)
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
            f"DONE! {table_name} >> {count} Records Transferred In {duration_minutes:.2f} Minutes.")

