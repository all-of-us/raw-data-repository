# Creating an RDR Environment

As of now, `raw-data-repository` (RDR) is a Google App Engine application that must be run in App Engine Standard Python 2.7.

These are the steps to stand up a new RDR environment:

0. Create a Google Cloud Project (`PROJECT`) for the environment
0. Enable `Cloud SQL Admin API`
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
0. Create BigQuery `rdr_ops_data_view` dataset
    * either via the web interface or with `bq mk --dataset rdr_ops_data_view`
0. Update config and tooling files
    * Create `rest-api/config/config_<PROJECT>.json` for the new project
    * Create `rest-api/cron_<PROJECT>.yaml` to override any settings from `cron_default.yaml` (if needed)
    * Update `rest-api/tools/auth_setup.sh` to know how to handle the new project
    * Update `rest-api/tools/deploy_app.sh` to know how to handle the new project
    * Update `rest-api/tools/build_cron_yaml.py` to know how to handle the new project
    * Update `rest-api/services/gcp_config.py` to know how to handle the new project
0. commit changes and create local tag (`TAG`) `<PROJECT>-initial`. _Do **not** push this tag to github._
0. From Google Cloud Console, Create new App Engine application (just enable it for the project)
0. run `tools/deploy_app.sh --target app --version <TAG> --project <PROJECT> --account <USER>@pmi-ops.org`
0. run `tools/setup_database.sh --create_instance --project <PROJECT> --account <USER>@pmi-ops.org`
    * If this command fails because the database operations take too long:
        * wait until the database is available in the Google Cloud Console web interface
        * then run `tools/setup_database.sh --continue_creating_instance --project <PROJECT> --account <USER>@pmi-ops.org`
0. run `tools/setup_database.sh --project <PROJECT> --account <USER>@pmi-ops.org`
0. run `tools/deploy_app.sh --target all --version <TAG> --project <PROJECT> --account <USER>@pmi-ops.org`
0. run `python -m tools migrate-bq --project all-of-us-rdr-xxxx --dataset rdr_ops_data_view`
0. run `tools/import_data.sh --project <PROJECT> --account <USER>@pmi-ops.org`
0. set up circle-ci deployment (if needed)
