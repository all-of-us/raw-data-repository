# The RDR API

## Cloud projects for testing

This api is implemented using Flask. They are served by a AppEngine instance.

The App Engine project used for testing is pmi-rdr-api-test (Note that this is
not the same project set up! Creating an app engine instance requires creating a
new project.)

-   Currently doesn't have billing (should be fine for a while, but will need to
    be eventually set up.)

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

### Running the tests
Make sure that the dev_appserver is running.

Run 
```Shell
test/run_tests.sh $sdk_dir
```

If you want to be super slick, and have the tests run every time you change a
source file, you can do this.

(You will have to install ack-grep and entr if you haven't already.)

```Shell
until ack-grep -f --python | entr -r test/run_tests.sh $sdk_dir unit;do sleep 1; done
```
