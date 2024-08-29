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
#### Example: 
``` python
from rscylladb.bulk import bulk_insert
bulk_insert(['192.168.10.129'],9042,username='cassandra',password='cassandra',file_name='file.csv') 
```

#### Example: If you use CDC with Kestra
```python
from rscylladb.cdc import cdc_insert
cdc_insert(['192.168.10.129'],9042,'test',username='cassandra',password='cassandra',file_name='file.csv') 
```

### Result
- Tested on 4GB RAM with i3 5th Gen processor.

| Rows | Duration |
|:-----|:---------|
|10,000|0.03s|
|3,00,000|2m|
|2,00,00,000|66m|
**Note: Depends on you hardware capability!**

# Author :
- ### 🙋‍♂️ [Meet Rathod](https://github.com/MeetRathod0)


