from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import numpy as np
import threading
import time
import logging as lg
from logger import create_logger

lg.getLogger("cassandra").setLevel('ERROR')

logger = create_logger("cql.ScyllaDB")

class ScyllaDB:
    def __init__(self,hosts:list,port:int,table_name:str,user:str,password:str) -> None:
        self.start_time = time.time() # for completion the time show
        # get keyspace & table name
        if len(table_name.split("."))!=2: raise Exception("Table name must be! keyspace.table_name")
        # keyspace
        self.keyspace_name = table_name.split(".")[0]
        # table nme
        self.table_name = table_name.split(".")[1]

        logger.debug(f"keyspace >> {self.keyspace_name}")
        logger.debug(f"table    >> {self.table_name}") 
        # set authenticator
        auth = PlainTextAuthProvider(username=user,password=password)
        # set cluster
        self.cluster = Cluster(contact_points=hosts,port=port,auth_provider=auth)
        # set keyspace
        self.session = self.cluster.connect(self.keyspace_name)
        logger.debug(f"connected to %s keyspace..."%(self.keyspace_name))
        stmt = SimpleStatement("SELECT column_name,type FROM system_schema.columns WHERE keyspace_name=%(key)s AND table_name=%(tbl)s")
        # get table's all columns and data type
        table_schema = self.session.execute(stmt,{ "key":self.keyspace_name,"tbl":self.table_name})
        logger.debug("%s columns fetched..."%(str(len([ i.column_name for i in table_schema]))))
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
        
        self.cql_columns = []
        self.cql_types = {}
        # create dictonary for data frame: column_name:python_datatype
        for row in table_schema:
            self.cql_types[row.column_name] = self.cql_to_python_type[row.type]
            self.cql_columns.append(row.column_name)
		
        if len(self.cql_columns)!=0:
            param_keys = ",".join(self.cql_columns) 
            param_values = ",".join([ '%('+k+')s' for k in self.cql_columns])
            self.insert__statement = "INSERT INTO "+self.table_name+" ("+param_keys+") VALUES ("+param_values+")"
        else:
            logger.debug("columns set using file.")

    def execute_insert(self,df:pd.DataFrame)->None:
        
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
            futures.append(self.session.execute_async(self.insert__statement,row))

        # wait for complete async 
        for future in futures:
            future.result() # wait until insert queries
        
    def insert(self,file_name:str,chuncks=10000,workers=4)->None:
        
        # supported files
        file_ext = file_name.split(".")[-1] # get extension
        if file_ext=="csv": # if csv then read csv with chuncks
            data_frames = pd.read_csv(file_name,chunksize=chuncks)
        elif file_ext=="json":
            data_frames = pd.read_json(file_name,chunksize=chuncks,lines=True)
        elif file_ext in ["xls","xlsx"]:
            data_frames = pd.read_excel(file_name,chunksize=chuncks)
        else:
            raise Exception("%s is not supported!"%(file_name))

        # empty list number of threads
        threads = list(range(workers))
        while True:
            try:
                # start thread
                for i in range(workers):
                    # fetch next dataframe
                    df = next(data_frames)
                    df.columns = map(str.lower,df.columns)
                    if len(self.cql_columns)==0:
                        self.cql_columns= df.columns.to_list()
                        param_keys = ",".join(self.cql_columns) 
                        param_values = ",".join([ '%('+k+')s' for k in self.cql_columns])
                        self.insert__statement = "INSERT INTO "+self.table_name+" ("+param_keys+") VALUES ("+param_values+")"
                    threads[i] = threading.Thread(target=self.execute_insert,args=(df,))
                    threads[i].start()  
                
                # wait until complete task
                for i in range(workers):
                    threads[i].join()
            # on execption break
            except Exception as e:
                print(e)
                break
        duration_minutes = (time.time() - self.start_time) / 60
        logger.debug(f"Completed in : {duration_minutes:.2f} minutes")
