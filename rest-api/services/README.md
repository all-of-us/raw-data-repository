# System Services for RDR 

## Service scripts to run directly or as a system service

### gcp_db_daemon.py

Starts a background GCP cloud_sql_proxy process that keeps persistent proxy tunnel connections to MySql instances.

Notes: Can be run using Python 3.x or Python 2.7

Display command line help
```
python gcp_db_daemon.py --help
```

Start, restart or stop service with only connections to Prod, Stable and Staging
```
python gcp_db_daemon.py --account=<email>@pmi-ops.org [start|restart|stop]
```
