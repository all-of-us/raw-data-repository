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
# Install: sudo pip install locustio
ACCOUNT=$USER@google.com
CREDS_ACCOUNT=$ACCOUNT
PROJECT=all-of-us-rdr-dev

. tools/auth_setup.sh
# Some dependency data is loaded via DAOs before load testing via API.
run_cloud_sql_proxy
set_db_connection_string

OLDPATH=$PYTHONPATH
. tools/set_path.sh;
# Prefer system-wide installs of some packages (specifically "requests").
export PYTHONPATH=/usr/local/lib/python2.7/dist-packages:$OLDPATH:$PYTHONPATH
cd "$BASE_DIR";
LOCUST_CREDS_FILE="$CREDS_FILE" \
LOCUST_TARGET_INSTANCE="https://$PROJECT.appspot.com" \
locust -f "$BASE_DIR"/tools/load_test_locustfile.py
