# Set to NULL any string fields in vocabulary tables that have empty strings
# and are compared to NULL in the ETL.
# (mysqlimport treats the absence of a field in CSV as empty string rather than
# NULL.)

UPDATE voc.concept_relationship SET invalid_reason = NULL WHERE invalid_reason = '';
UPDATE voc.concept SET invalid_reason = NULL WHERE invalid_reason = ''; 
