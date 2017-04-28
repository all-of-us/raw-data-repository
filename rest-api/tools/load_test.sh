#!/bin/bash -e
# Runs a load test against a deployed RDR.
#
# Instructions:
# *   Run this script, which starts a locust server.
# *   Once started, locust prints "Starting web monitor at *:8089". Open
#     http://localhost:8089 to view the control/status page.
# *   Set the number of users to 100 and hatch/sec to 5. (This is to match
#     weights/times in the locust file. For example, if users have the default
#     min_wait = max_wait = 1000 (ms), setting the number of users to 100
#     and hatch rate to 5 will ramp up to 100qps over 20s, and then sustain
#     100qps until you click "stop".)
# *   Click run, gather stats, click stop.
#
# Locust docs: http://docs.locust.io/en/latest/quickstart.html

if ! which locust
then
  echo "Install Locust with \"sudo pip install locustio\"."
  exit 1
fi
function usage {
  echo "Usage: load_test.sh --project all-of-us-rdr-test"
  exit 1
}
while true
do
  case "$1" in
    --project) PROJECT=$2; shift 2;;
    *) break;;
  esac
done
if [ "$@" ]
then
  echo "Unrecognized arguments: $@"
  usage
fi
if [ -z "$PROJECT" ]
then
  usage
fi
if [[ "$PROJECT" =~ "prod" ]]
then
  echo "Forbidden to load test against production instance $PROJECT."
  exit 1
fi

ACCOUNT=$USER@google.com
CREDS_ACCOUNT=$ACCOUNT
. tools/auth_setup.sh

# Some dependency data is loaded via DAOs before load testing via API.
run_cloud_sql_proxy
set_db_connection_string

# Prefer system-wide installs of some packages (specifically "requests"). The
# gcloud libraries added by set_path.sh include an older version.
OLDPATH=$PYTHONPATH
. tools/set_path.sh;
export PYTHONPATH=/usr/local/lib/python2.7/dist-packages:$OLDPATH:$PYTHONPATH

LOCUST_CREDS_FILE="$CREDS_FILE" \
LOCUST_TARGET_INSTANCE="https://$PROJECT.appspot.com" \
locust -f "$BASE_DIR"/tools/load_test_locustfile.py
