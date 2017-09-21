# This SELECT mirrors the UPDATE below, to serve as pre-validation.
# See https://precisionmedicineinitiative.atlassian.net/browse/DA-384 .
# TODO(markfickett) Delete this file once the backfill has been run (it only
# serves as a record / for review).

SELECT
  participant.hpo_id old_hpo_id,
  hpo.hpo_id new_hpo_id,
  CONCAT("[{\"primary\":true,\"organization\":{\"reference\":\"Organization/", hpo.name, "\"}}]") provider_link
FROM
  participant
JOIN
  participant_summary USING(participant_id)
JOIN
  biobank_order site_src USING(participant_id)
JOIN
  site ON site.site_id = site_src.finalized_site_id
JOIN
  hpo ON site.hpo_id = hpo.hpo_id
WHERE
  participant.hpo_id = 0
  AND hpo.hpo_id IS NOT NULL
ORDER BY new_hpo_id
;


UPDATE
  participant
JOIN
  participant_summary USING(participant_id)
JOIN
  # We will run it once with biobank_order as the `site_src` table, expecting to update
  # 110 rows; and then again using physical_measurements which will update 1 more row.
  biobank_order site_src USING(participant_id)
JOIN
  site ON site.site_id = site_src.finalized_site_id
JOIN
  hpo ON site.hpo_id = hpo.hpo_id
SET
  participant.hpo_id = hpo.hpo_id,
  participant.provider_link = CONCAT("[{\"primary\":true,\"organization\":{\"reference\":\"Organization/", hpo.name, "\"}}]"),
  participant_summary.hpo_id = hpo.hpo_id
WHERE
  participant.hpo_id = 0
  AND hpo.hpo_id IS NOT NULL
;


# 2nd copy of the update.
UPDATE
  participant
JOIN
  participant_summary USING(participant_id)
JOIN
  # biobank_order changed to physical_measurements
  physical_measurements site_src USING(participant_id)
JOIN
  site ON site.site_id = site_src.finalized_site_id
JOIN
  hpo ON site.hpo_id = hpo.hpo_id
SET
  participant.hpo_id = hpo.hpo_id,
  participant.provider_link = CONCAT("[{\"primary\":true,\"organization\":{\"reference\":\"Organization/", hpo.name, "\"}}]"),
  participant_summary.hpo_id = hpo.hpo_id
WHERE
  participant.hpo_id = 0
  AND hpo.hpo_id IS NOT NULL
;
