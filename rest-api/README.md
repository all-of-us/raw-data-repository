# The RDR API

## Cloud projects for testing

This api is implemented using Google Cloud Endpoints. They are served by a
AppEngine instance.

The App Engine project used for testing is pmi-rdr-api-test (Note that this is
not the same project set up! Creating an app engine instance requires creating a
new project.)

-   Currently doesn't have billing (should be fine for a while, but will need to
    be eventually set up.)

The cloud SQL instance is under the original pmi-drc-api-test project. The
instance is named 'pmi-rdr'

The GCS bucket for biobank manifest testing is pmi-drc-biobank-test.

## Configuring a Ubuntu workstation for API development:

You can either use the Cloud SQL proxy or run a MySQL instance locally.

### Use the Cloud SQL Proxy

[Install the proxy](https://cloud.google.com/sql/docs/external#proxy) (make sure
to set up a service account)

Next, run the proxy:

```Shell
sudo ${HOME}/bin/cloud_sql_proxy -dir=/cloudsql \
-instances=pmi-drc-api-test:us-central1:pmi-rdr -credential_file \ <path to
credential file> &
```

### Run a MySQL instance locally

TODO: try this and document it.

When setting up a database, create the database manually "pmi_rdr".

Run schema.sql to create the table(s).

Create the "api" user manually. Then `GRANT ALL on pmi_rdr.* to 'api';`

### Configuring your instance

When the instance comes up for the first time, it will check for the existance
of the configuration values in the datastore.  If there are none found, then
development defaults will be written. (The instance may need to try to service
its first request before it does this)

In order to modify the configuration:

If running a local dev_appserver, navigate to the
[datastore viewer](http://localhost:8000/datastore?kind=Config).
You should be able to modify config settings using the fancy UI.

If running in produciton, go to the
[cloud console](https://console.cloud.google.com).  Select the app engine
project and then click on "datastore" in the left hand navigation bar.

### Setting up a new DB
```Sql
mysql> create database pmi_rdr;
mysql> use pmi_rdr;
mysql> create user api identified by '<password>';
mysql> grant all privileges on pmi_rdr.* to api;
mysql> source schema.sql
```


