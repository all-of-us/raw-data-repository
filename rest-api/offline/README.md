# Batch jobs for processing RDR data

Offline jobs are scheduled in `rest-api/cron.yaml`, which hits endpoints
registered in `offline/main.py` (a separate service from `rest-api/main.py`).
There is also a "Run now" button in
[Cloud Console > AppEngine > Task queues > Cron Jobs](https://pantheon.corp.google.com/appengine/taskqueues?project=pmi-drc-api-test&serviceId=default&tab=CRON).

Tests are under the common `rest-api/test/unit_test/` directory.

# Metrics pipeline

The metrics pipeline is responsible for generating participant metrics buckets in the database
for each HPO and date combination, from the first date we have data to the current date.
This data is generated anew on a nightly basis (run by cron), performing the following two steps:

* Using a series of SQL statements (sharded by participant ID), generate CSV files in GCS
  where every row contains a participant ID and some relevant info (see metrics_export.py)
* Run an pipeline (see metrics_pipeline.py) which in a series of MRs:
	* Writes out a processing metrics version
	* Joins all the CSV data together by participant ID
	* Generates deltas for HPO + metrics when they change
	* Groups by HPO + metric + date, and calculates running totals for each HPO + metric
	* Group by HPO + date and write buckets containing all metrics to the database
	* Marks the processing metrics version as complete and active

When the pipeline finishes, new metrics will be served to clients based on the new metrics version.
