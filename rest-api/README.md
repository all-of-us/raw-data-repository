# The RDR API

## Cloud projects for testing

This api is implemented using Flask. They are served by a AppEngine instance.

The App Engine project used for testing is pmi-drc-api-test, billed to Vanderbilt. 

The GCS bucket for biobank manifest testing is pmi-drc-biobank-test.

## Configuring a Ubuntu workstation for API development:

Follow the instructions in the client directory first to set up a
virtual Python environment, then follow the instructions here.

### Installing dependencies

Make sure that you have google
[cloud SDK](https://cloud.google.com/sdk/downloads) installed.

From the rest-api directory, run:

* tools/setup_env.sh (get libs and Cloud SQL Proxy)
* sudo apt-get install mysql-server libmysqlclient-dev (to install MySQL server and client)
* dev_appserver.py test.yaml --require_indexes (to run your local server)
* tools/setup_local_database.sh (to create a database in MySQL and put the config for it in Datastore)
* tools/upgrade_database.sh (to update your database to the latest schema)

### Running the development app server

From the rest-api directory, you can in general run your local server with:

```Shell
dev_appserver.py test.yaml --require_indexes &
```

This runs a local server with both API and offline services (suitable for local
development as well as running client tests).

### Configuring your instance

When the instance comes up for the first time, it will have no configuration, and be generally useless.

The best way to get set up for development is to install the dev config:

```Shell
tools/install_config.sh --config config/config_dev.json --update
```
The server should be now be good to go!

In order to modify the configuration manually:

(Note: For local development we need an `user_info` map entry with a key of
`example@example.com`.  This is what a oauth user appears as under the
dev_appserver.)


If running a local dev_appserver, navigate to the
[datastore viewer](http://localhost:8000/datastore?kind=Config).
You should be able to modify config settings using the fancy UI.

If running in production, go to the
[cloud console](https://console.cloud.google.com).  Select the app engine
project and then click on "datastore" in the left hand navigation bar.

### Running the tests against the local appserver
Make sure that the dev appserver is running, then from the rest-api directory run:
```Shell
test/run_tests.sh -g $sdk_dir
```

This will run both the unit tests and the client tests. See below if what you want to do is to run 

If you want to be super slick, and have the tests run every time you change a
source file, you can do this.

(You will have to install ack-grep and entr if you haven't already.)

```Shell
until ack-grep -f --python | entr -r test/run_tests.sh -g $sdk_dir unit;do sleep 1; done
```

### Adding fake participants to local appserver

See `rest-api-client/README.md` for instructions.

Your `config_dev.json` loaded earlier should include a Config entity with
`config_key=allow_fake_history_dates` and `value=True`. You can check the
current config by running `tools/install_config.sh` with no arguments.

## Deploying to test server

To deploy to the test server, `https://pmi-drc-api-test.appspot.com/`, first get your
Git repo into the desired state, then run the following from the rest-api directory:

```Shell
gcloud config set project pmi-drc-api-test
gcloud app deploy app.yaml offline.yaml
```

If you've changed other files you may need to deploy them as well, for instance the cron config:
```Shell
gcloud app deploy cron.yaml
```

After uploading a new version you should run the metrics cron job on the
appengine server: from the AppEngine console select the Task queues panel, and
then the Cron Jobs tab.  Click the "Run now" button for the MetricsRecalculate
cron job.  (Note: if there is a stale MetricsVersion with `in_progress=true`, the
MetricsRecalculate will report that a pipeline is already running.  To fix this,
use the Datastore viewer to manually edit the MetricsVersion to set
`in_progress=false` and then try again.

### Running client tests against test server

From the rest-api directory, run:

```Shell
test/test_server.sh
```

This will execute all the client tests in turn against the test server, stopping
if any one of them fails. To run a particular test use the -r flag as for
run_tests.  Use the -i flag to override the default instance.

## Auth Model

The RDR has separate permissions management for gcloud project administration,
RDR's custom config updates, and general API endpoints.

### Cloud Project Admin Permissions

[Cloud Platform Admin settings](https://console.cloud.google.com/iam-admin/serviceaccounts/project?project=all-of-us-rdr-staging)
control which people can administer the project. Service accounts (and their
keys) are also created here.

Admin accounts must be pmi-ops accounts using two-factor auth. For prod, only
these accounts are allowed; for development environments, additional accounts
may have access for convenience.

### Config Updates

The `/config` endpoint uses separate auth from any other endpoint. It depends on
the hardcoded values in `rest-api/config/config_admins.json`. If an app ID is
listed in `config_admins.json`, only the service account that it's mapped to
may make `/config` requests. Otherwise, a default
`configurator@$APPID.iam.gserviceaccount.com` has permission.

Config updates happen automatically on deploy for some environments, controlled
by `circle.yml`. To manually update configs, download the appropriate service
account's private key in JSON format (or generate a new key which you can revoke
after use), and pass it to `install_config.sh`.

## API Endpoints

All endpoints except `/config` are authenticated using service account
credentials in oauth request headers. The config loaded into an app's datastore
(from `config/config_$ENV.json`) maps service accounts to roles, and the
`auth_*` decorators (from `api_util.py`) assign permissible roles to endpoints.

The config may also specify that a service account is only authorized from
certain IP ranges, or from specific appids (for AppEngine-to-AppEngine
requests), as second verification of the service account's auth.

### Deploying to staging

* Go to https://github.com/vanderbilt/pmi-data/releases/new
* Enter a tag name of the form vX-Y-rcZZ -- e.g. v0-1-rc14
* Unless this is intended to be pushed to prod eventually, check the "This is a pre-release" box.
* Submit.

CircleCI should automatically push to staging, based on logic found in
https://github.com/vanderbilt/pmi-data/blob/master/circle.yml

If you are adding new indexes, the tests may fail when they aren't ready yet; use Rebuild in
CircleCI to retry.

### Tools

Please see the [Tools README](tools/README.md) for more information on command line tools.
