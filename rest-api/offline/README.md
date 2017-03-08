# Batch jobs for processing RDR data

Offline jobs are scheduled in `rest-api/cron.yaml`, which hits endpoints
registered in `offline/main.py` (a separate service from `rest-api/main.py`).
There is also a "Run now" button in
[Cloud Console > AppEngine > Task queues > Cron Jobs](https://pantheon.corp.google.com/appengine/taskqueues?project=pmi-drc-api-test&serviceId=default&tab=CRON).

Tests are under the common `rest-api/test/unit_test/` directory.
