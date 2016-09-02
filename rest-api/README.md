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
