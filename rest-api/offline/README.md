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
  where every row contains a participant ID and some relevant info (see metrics_export.py).
  The three files contain participant data, a history of HPO IDs for participants, and
  a history of questionnaire response answers for participants. The participant data file will
  have one row per participant; the latter two can have many rows per participant (including
  changes in values over time.)
* Run an pipeline (see metrics_pipeline.py) which in a series of MRs:
	* Writes out a processing metrics version
	* Joins all the CSV data together by participant ID
	* Generates deltas for HPO + metrics when they change
	* Groups by HPO + metric + date, and calculates running totals for each HPO + metric
	* Group by HPO + date and write buckets containing all metrics to the database
	* Marks the processing metrics version as complete and active

When the pipeline finishes, new metrics will be served to clients based on the new metrics version.

# Biobank Reconciliation Pipeline

Match up orders received via API (BiobankOrder) and orders received at the
Biobank with records uploaded in CSVs (BiobankStoredSample); then generate a
report of how long it took any sample to get stored and whether any are missing.

This pipeline is necessary because samples have separate IDs on the
order/creation side and the storage side. Both are linked to test codes and
participants, which is how we correlate the two views of each sample.

## Input 1: Orders

Orders (generated when samples are taken) are sent to the
`/Participant/:participant_id:/BiobankOrder` API endpoint from HealthPro. One
order for a participant lists multiple samples included in that order, where
each sample is for a different test, identified by test code.

## Input 2: Sample CSVs

Received samples are listed in CSVs uploaded (by Biobank) to Google Cloud
Storage, in an environment-specific storage bucket like
`$ENV_biobank_samples_upload_bucket`.

## Output

The reconciliation pipeline writes three CSVs to a `reconciliation`
subdirectory of the input bucket. The CSVs all have the same columns, but are
different subsets of the rows:

1.  `report_$DATE_received.csv` All rows where sent orders and received samples
    match up. This is typically cases where one sample was ordered and one
    sample was received, for the same participant and test. However it could be
    a case where the same test was ordered twice and received twice for a
    participant.
1.  `report_$DATE_over_24h.csv` Rows where sent and received match up, but the
    elapsed time between ordered sample collection and sample receipt
    confirmation is more than 24 hours.
1.  `report_$DATE_missing.csv` Any case of order and receipt mismatch. This may
    be an order where no sample arrived, a received sample with no order, or
    more generally a case where a different number of orders and samples appear
    for the same participant/test pair: that is, rows where
    `sent_count` != `received_count`.

Colums in the CSVs are:

Column | Description | Example
--- | --- | ---
`biobank_id` | Participant ID (biobank format) | B103850270
`sent_test` | Ordered sample's test code. May be omitted if there was a received sample but no order, otherwise this is expected to match `received_test`. | 1ED04
`sent_count` | Number of orders for this participant/test, typically 1. See description of `*_missing.csv` above. | 1
`sent_order_id` | Order ID sent via API. Multi-valued: If multiple orders match this participant/test, this will be a quoted, comma-separated value, listing all the orders for this participant/test. | WEB1YLHVP765215278-16675602 or "WEB1YLHVP987349708-48169257,WEB1YLHVP987349708-54694248"
`sent_collection_time` | Time sent via API, ISO-8601 format with time zone (UTC). | 2016-12-28T21:12:42+00:00
`sent_finalized_time` | | 2016-12-30T10:29:42+00:00
`site_id` | ID of the site creating the order, sent via API. | 789012 or "789012,987210"
`received_test` | Received sample's test code. Typically the same as `sent_test`, see above. | 1ED04
`received_count` | | 1
`received_sample_id` | Received sample's ID, from "Sample Id" column. | 3663123 or "1685731,1809762"
`received_time` | Received sample's confirmed timestamp, ISO-8601 format. (Converted from Central time.) | 2016-09-22T08:38:42+00:00
`elapsed_hours` | Elapsed integer hours between `sent_collection_time` and `received_time`. | 20

