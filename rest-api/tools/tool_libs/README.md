# Command Line Tools

## Overview

This section describes getting started with using the new style Python tool scripts.

The new style Python tools have Bash Completion support, which allow single and double clicking the tab key to complete an argument or show possible arguments. Use the following command to add Bash Completion to your Bash session;

```
. tools/tool_libs/tools.bash
```

The tools are invoked by calling Python with the module argument (-m). To show all available tools and a short description, use the following command line;

```
python -m tools --help
```

You should see out like the following;

```
usage: python -m tools command [-h|--help] [args]

available commands:
  migrate-bq       : bigquery schema migration tool
  oauth-token      : get oauth token for account or service account
  sync-consents    : manually sync consent files to sites
  verify           : test local environment configuration
```

To view the help for a specific command, add the command name and then the help argument;

```
python -m tools verify --help 

usage: verify [-h] [--debug] [--log-file] [--project PROJECT]
              [--account ACCOUNT] [--service-account SERVICE_ACCOUNT]

test local environment configuration

optional arguments:
  -h, --help            show this help message and exit
  --debug               Enable debug output
  --log-file            write output to a log file
  --project PROJECT     gcp project name
  --account ACCOUNT     pmi-ops account
  --service-account SERVICE_ACCOUNT
                        gcp iam service account

```
##### Notes

All tools that deal with GCP projects will have the `--project`, `--account` and `--service-account` arguments.  If a service account argument is used, a service account key will automatically be generated and stored in a key index file located in `~/.rdr/service-keys`.  Keys are automatically deleted when the tool completes. 

If the tool crashes, orphaned keys will usually be cleaned up and deleted on the next run of the tool.

If the `--debug` argument is used, extra information will be printed from the tool to aid with diagnosing problems.

When the `--log-file` argument is used, the output of the tool will go to a file in the current directory named for the tool with a log extension, IE: `verify.log`.

## Tools

### verify

The verify tool is used to check that the local machine is ready to run the other tools. It checks to make sure the Google GCP SDK is installed and that any other required programs are also installed.  It can additionally verify authentication to GCP projects.

### oauth-token

This tool is used to quickly retrieve a Google IAM oauth token for the given project and account or service account.  The program will wait until `ctrl-c` is pressed and then clean up any service account keys that were generated.

### migrate-bq

The BigQuery migration tool will look for any schema changes to BigQuery models that are listed in `rest-api/model/__init__.py`. This tool supports creating/updating/deleting tables and views.

### sync-consents

This tool will copy participant consent files to an organization's AOU google bucket.
