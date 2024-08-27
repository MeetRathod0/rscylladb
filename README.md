<p align="center">
  <a href="https://skillicons.dev">
    <img src="https://skillicons.dev/icons?i=git,cassandra,python" />
  </a>
</p>

## Bulk Installer ScyllaDB/Cassandra ![my badge](https://badgen.net/badge/version/0.0.1/red?icon=git)

Insert over 20Million records within minutes.

### Install using
```shell
pip install rscylla
```
### Usage
```python
from rscylla.cql import Cql
# hostslist = ['127.0.0.1']
# port = 9042
# table_name = keyspace.table_name [must required]
# username = cassandra
# password  = cassandra
obj = Cql(hostslist,port,table_name,username,password)
# default chunks = 10,000
# default workers = 4
obj.insert(filename,chuncks,workers) # filename or filepath
```
#### Example:
```python
from rscylla.cql import Cql
obj = Cql(["localhost"],9042,"cassandra","cassandra")
obj.insert("data.csv") # optional : chuncks and workers
# way 2
obj.insert("data.csv",chunks=1000,workers=2)
```
### Result
- Tested on 4GB RAM with i3 processor.

| Rows | Duration |
|:-----|:---------|
|10,000|0.03s|
|3,00,000|2m|
|2,00,00,000|66m|
**Note: Depends on you hardware capability!**

# Authors :
- ### 🙋‍♀️ [Reeya Patel](https://github.com/ReeyaPatel06)
- ### 🙋‍♂️ [Meet Rathod](https://github.com/MeetRathod0)


