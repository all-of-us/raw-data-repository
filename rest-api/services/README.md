# System Services for RDR 

## Google Cloud Platform Helper Functions

The file `gcp_utils.py` has many python helper functions to manage and call GCP command line tools.

For example use of these GCP helper functions, please look at `rest-api/tools/__main__.py` and tool files in `rest-api/tools/tool_libs`.

## Service scripts to run directly or as a system service

### gcp_db_daemon.py

Starts a background GCP cloud_sql_proxy process that keeps persistent proxy tunnel connections to database instances.

##### Notes: ##### 

* Can be run using Python 3.x or Python 2.7.

* The service log file is stored in ~/.local/log/gcp_db_daemon.log

Display command line help
```
python gcp_db_daemon.py --help
```

Start, restart or stop service with only connections to Prod, Stable and Staging. Use --debug flag to show instance
connection information.
```
python gcp_db_daemon.py --account=<email>@pmi-ops.org [start|restart|stop]
```
