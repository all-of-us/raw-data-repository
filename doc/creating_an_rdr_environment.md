# Creating an RDR Environment

As of now, `raw-data-repository` (RDR) is a Google App Engine application that must be run in App Engine Standard Python 2.7.

These are the steps to stand up a new RDR environment:

0. Create a Google Cloud Project for the environment
0. Create required service accounts
    * `configurator@<PROJECT>.iam.gserviceaccount.com`
        * `Storage Admin`
        * `Cloud SQL Viewer`
        * `Cloud SQL Client`
    * `exporter@<PROJECT>.iam.gserviceaccount.com`
        * `DLP Administrator`
        * `DLP Jobs Editor`
        * `Storage Admin`
0. Create dedicated cloud storage buckets for the environment (as needed)
    * biobank samples
    * ghost id
    * consent pdf
0. Update config and tooling files
    0. Create `rest-api/config/config_<PROJECT>.json` for the new project
    0. Create `rest-api/cron_<PROJECT>.yaml` to override any settings from `cron_default.yaml` (if needed)
    0. Update `rest-api/tools/auth_setup.sh` to know how to handle the new project
    0. Update `rest-api/tools/deploy_app.sh` to know how to handle the new project
    0. Update `rest-api/tools/build_cron_yaml.py` to know how to handle the new project
0. From Google Cloud Console, Create new App Engine application
0. (?) run `tools/deploy_app.sh --target app --project <PROJECT> --account <USER>@pmi-ops.org`
0. (?) run `tools/setup_database.sh --create_instance --project <PROJECT> --account <USER>@pmi-ops.org`
0. (?) run `tools/deploy_app.sh --target all --project <PROJECT> --account <USER>@pmi-ops.org`
0. (?) run `tools/import_data.sh --project <PROJECT> --account <USER>@pmi-ops.org`
0. set up circle-ci deployment (if needed)
    0. use dark magics
    0. ...
    0. profit!
