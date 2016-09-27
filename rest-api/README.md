# The RDR API

## Cloud projects for testing

This api is implemented using Flask. They are served by a AppEngine instance.

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

### Installing dependencies
From the rest-api directory, run
'''Shell
pip install -r requirements.txt -t lib/
'''
This will install all the needed dependencies in the 'lib' directory.

### Configuring your instance

When the instance comes up for the first time, it will check for the existance
of the configuration values in the datastore.  If there are none found, then
development defaults will be written. (The instance may need to try to service
its first request before it does this)

In order to modify the configuration:

If running a local dev_appserver, navigate to the
[datastore viewer](http://localhost:8000/datastore?kind=Config).
You should be able to modify config settings using the fancy UI.

For local development, add an "allowed_user" entry with a value of
"example@example.com".  This is what a oauth user appears as under the
dev_appserver.

If running in produciton, go to the
[cloud console](https://console.cloud.google.com).  Select the app engine
project and then click on "datastore" in the left hand navigation bar.



