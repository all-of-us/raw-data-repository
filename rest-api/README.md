# The RDR API

## Cloud projects for testing

This api is implemented using Flask. They are served by a AppEngine instance.

The App Engine project used for testing is pmi-drc-api-test, billed to Vanderbilt. 

The GCS bucket for biobank manifest testing is pmi-drc-biobank-test.

## Configuring a Ubuntu workstation for API development:

Follow the instructions in the client directory first to set up a
virtual Python environment, then follow the instructions here.

### Installing dependencies
From the rest-api directory, run
```Shell
pip install -r requirements.txt -t lib/

git submodule update --init
```
This will install all the needed dependencies in the `lib` directory.

### Running the development app server
Make sure that you have google [cloud SDK](https://cloud.google.com/sdk/downloads) installed.

From the rest-api directory, run:

```Shell
dev_appserver.py . &
```

### Configuring your instance

When the instance comes up for the first time, it will check for the existance
of the configuration values in the datastore.  If there are none found, then
development defaults will be written. (The instance may need to try to service
its first request before it does this)

In order to modify the configuration:

If running a local dev_appserver, navigate to the
[datastore viewer](http://localhost:8000/datastore?kind=Config).
You should be able to modify config settings using the fancy UI.

For local development, add an `allowed_user` entry with a value of
`example@example.com`.  This is what a oauth user appears as under the
dev_appserver.

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

Use the [local datastore viewer](http://localhost:8000/datastore?kind=Config) 
to create a Config entity with
`config_key=allow_fake_history_dates` and `value=True`.

Then execute the following from the rest-api-client directory:
```Shell
python load_fake_participants.py --instance=http://localhost:8080 --count=10
```
Running it repeatedly just adds more fake participants.

## Deploying to test server

To deploy to the test server, `https://pmi-drc-api-test.appspot.com/`, first get your
Git repo into the desired state, then run the following from the rest-api directory:

```Shell
gcloud config set project pmi-drc-api-test
gcloud app deploy app.yaml
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
