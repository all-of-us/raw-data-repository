# Set up for local testing

## Install SDK for App Engine:

Requires: Python 2.7

From https://cloud.google.com/appengine/docs/python/download follow "Download and Install" steps

## Install mysql and create schema

Follow setup at https://cloud.google.com/appengine/docs/python/cloud-sql/ in order to:

    sudo apt-get install mysql-server
    pip install --upgrade google-api-python-client
    sudo apt-get install python-mysqldb

Create a ~/.my.cnf file with:

```
[client]
user=root
password={{your localhost mysql password}}
```

echo "CREATE DATABASE pmi_rdr" | mysql -u root -p
mysql -u root -p  pmi_rdr < schema.sql
