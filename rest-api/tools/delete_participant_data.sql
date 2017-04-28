# Deletes all participant data from the database. Use only as directed.

USE rdr;
START TRANSACTION;
DELETE FROM biobank_stored_sample;
DELETE FROM biobank_ordered_sample;
DELETE FROM biobank_order_identifier;
DELETE FROM biobank_order;
DELETE FROM metrics_bucket;
DELETE FROM metrics_version;
DELETE FROM questionnaire_response_answer;
DELETE FROM questionnaire_response;
DELETE FROM physical_measurements;
DELETE FROM participant_summary;
DELETE FROM participant_history;
DELETE FROM participant;
DELETE FROM log_position;
COMMIT;
