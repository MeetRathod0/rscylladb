from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import numpy as np
import threading
import time

class ScyllaDB:
    def __init__(self,hosts:list,port:int,table_name:str,user:str,password:str) -> None:
        self.start_time = time.time() # for completion the time show
        # get keyspace & table name
        if len(table_name.split("."))!=2: raise Exception("Table name must be! keyspace.table_name")
        # keyspace
        self.keyspace_name = table_name.split(".")[0]
        # table nme
        self.table_name = table_name.split(".")[1]
        # set authenticator
        auth = PlainTextAuthProvider(username=user,password=password)
        # set cluster
        self.cluster = Cluster(contact_points=hosts,port=port,auth_provider=auth)
        # set keyspace
        self.session = self.cluster.connect(self.keyspace_name)

    
    def execute_insert(self,df:pd.DataFrame)->None:
        # make lower columns name for cql
        df.columns = map(str.lower,df.columns)
        # handle null for numeric type default 0 add
        df[df.select_dtypes(include=np.number).columns] = df.select_dtypes(include=np.number).fillna(0) 
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
        # get table's all columns and data type
        table_schema = self.session.execute("SELECT column_name,type FROM system_schema.columns WHERE keyspace_name='%s' AND table_name='%s'"%(self.keyspace_name,self.table_name))
        # has cql to python data type
        self.cql_to_python_type = { 
            'NULL': None,
            'boolean': bool,
            'float': float,
            'double': float,
            'int':int,
            'smallint':int, 
            'tinyint':int,
            'counter':int,
            'varint':int,
            'bigint': int,
            'decimal': float,
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
        
        self.cql_types = {}
        self.cql_columns=[]
        # create dictonary for data frame: column_name:python_datatype
        for row in table_schema:
            self.cql_types[row.column_name] = self.cql_to_python_type[row.type]
            self.cql_columns.append(row.column_name)

        # if table no columns table not exists
        if len(self.cql_columns)==0: raise Exception("%s table not exists!"%(self.table_name))
        param_keys = ",".join(self.cql_columns) 
        param_values = ",".join([ '%('+k+')s' for k in self.cql_columns])
        self.insert__statement = "INSERT INTO "+self.table_name+" ("+param_keys+") VALUES ("+param_values+")"

        # supported files
        file_ext = file_name.split(".")[-1] # get extension
        if file_ext=="csv": # if csv then read csv with chuncks
            data_frames = pd.read_csv(file_name,chunksize=chuncks,dtype=self.cql_types)
        elif file_ext=="json":
            data_frames = pd.read_json(file_name,chunksize=chuncks,lines=True,dtype=self.cql_types)
        elif file_ext in ["xls","xlsx"]:
            data_frames = pd.read_excel(file_name,chunksize=chuncks,dtype=self.cql_types)
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
        print(f"Completed in : {duration_minutes:.2f} minutes")

