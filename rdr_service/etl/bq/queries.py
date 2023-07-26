queries = {
    "fact_relationship": {
        "query": """
            SELECT 21 AS domain_concept_id_1,
                -- Measurement
                tmp1.measurement_id AS fact_id_1,
                -- measurement_id of the first/second/third/mean systolic blood pressure
                21 AS domain_concept_id_2,
                -- Measurement
                tmp2.measurement_id AS fact_id_2,
                -- measurement_id of the first/second/third/mean diastolic blood pressure
                46233683 AS relationship_concept_id,
                -- Systolic to diastolic blood pressure measurement
                tmp1.src_id AS src_id
            FROM `{dataset_id}.tmp_fact_rel_sd` tmp1
                INNER JOIN `{dataset_id}.tmp_fact_rel_sd` tmp2 ON tmp1.person_id = tmp2.person_id
                AND tmp1.parent_id = tmp2.parent_id
                AND tmp1.systolic_blood_pressure_ind = tmp2.diastolic_blood_pressure_ind -- get the same index to refer between
                -- first, second, third and mean blood pressure measurements
            WHERE tmp1.systolic_blood_pressure_ind != 0 -- take only systolic blood pressure measurements
                AND tmp2.diastolic_blood_pressure_ind != 0
            UNION ALL
            SELECT 21 AS domain_concept_id_1,
                -- Measurement
                cdm_meas.measurement_id AS fact_id_1,
                21 AS domain_concept_id_2,
                -- Measurement
                cdm_meas.parent_id AS fact_id_2,
                581437 AS relationship_concept_id,
                -- 581437, Child to Parent Measurement
                cdm_meas.src_id AS src_id
            FROM `{dataset_id}.measurement` cdm_meas
            WHERE cdm_meas.parent_id IS NOT NULL
            UNION ALL
            SELECT 21 AS domain_concept_id_1,
                -- Measurement
                cdm_meas.parent_id AS fact_id_1,
                21 AS domain_concept_id_2,
                -- Measurement
                cdm_meas.measurement_id AS fact_id_2,
                581436 AS relationship_concept_id,
                -- 581436, Parent to Child Measurement
                cdm_meas.src_id AS src_id
            FROM `{dataset_id}.measurement` cdm_meas
            WHERE cdm_meas.parent_id IS NOT NULL
            UNION ALL
            SELECT 21 AS domain_concept_id_1,
                -- Measurement
                tmp2.measurement_id AS fact_id_1,
                -- measurement_id of the first/second/third/mean diastolic blood pressure
                21 AS domain_concept_id_2,
                -- Measurement
                tmp1.measurement_id AS fact_id_2,
                -- measurement_id of the first/second/third/mean systolic blood pressure
                46233682 AS relationship_concept_id,
                -- Diastolic to systolic blood pressure measurement
                tmp1.src_id AS src_id
            FROM `{dataset_id}.tmp_fact_rel_sd` tmp1
                INNER JOIN `{dataset_id}.tmp_fact_rel_sd` tmp2 ON tmp1.person_id = tmp2.person_id
                AND tmp1.parent_id = tmp2.parent_id
                AND tmp1.systolic_blood_pressure_ind = tmp2.diastolic_blood_pressure_ind -- get the same index to refer between
                -- first, second, third and mean blood pressurre measurements
            WHERE tmp1.systolic_blood_pressure_ind != 0 -- take only systolic blood pressure measurements
                AND tmp2.diastolic_blood_pressure_ind != 0 -- take only diastolic blood pressure measurements
            UNION ALL
            SELECT 27 AS domain_concept_id_1,
                -- Observation
                cdm_obs.observation_id AS fact_id_1,
                21 AS domain_concept_id_2,
                -- Measurement
                mtq.measurement_id AS fact_id_2,
                581410 AS relationship_concept_id,
                -- Observation to Measurement
                cdm_obs.src_id AS src_id
            FROM `{dataset_id}.observation` cdm_obs
                INNER JOIN `{dataset_id}.measurement_to_qualifier` mtq ON mtq.qualifier_id = cdm_obs.meas_id
            UNION ALL
            SELECT 21 AS domain_concept_id_1,
                -- Measurement
                mtq.measurement_id AS fact_id_1,
                27 AS domain_concept_id_2,
                -- Observation
                cdm_obs.observation_id AS fact_id_2,
                581411 AS relationship_concept_id,
                -- Measurement to Observation
                cdm_obs.src_id AS src_id
            FROM `{dataset_id}.observation` cdm_obs
                INNER JOIN `{dataset_id}.measurement_to_qualifier` mtq ON mtq.qualifier_id = cdm_obs.meas_id""",
        "destination": "fact_relationship",
        "append": False,
    },
    "src_race": {
        "query": """
            SELECT DISTINCT src_m.participant_id AS person_id,
                MIN(stcm1.source_code) AS ppi_code,
                MIN(stcm1.source_concept_id) AS race_source_concept_id,
                MIN(COALESCE(vc1.concept_id, 0)) AS race_target_concept_id
            FROM `{dataset_id}.src_mapped` src_m
                INNER JOIN `{dataset_id}.source_to_concept_map` stcm1 ON src_m.value_ppi_code = stcm1.source_code
                AND stcm1.priority = 1 -- priority 1
                AND stcm1.source_vocabulary_id = 'ppi-race'
                LEFT JOIN `{dataset_id}.concept` vc1 ON stcm1.target_concept_id = vc1.concept_id
                AND vc1.standard_concept = 'S'
                AND vc1.invalid_reason IS NULL
            GROUP BY src_m.participant_id
            HAVING COUNT(DISTINCT src_m.value_ppi_code) = 1""",
        "destination": "src_race",
        "append": False,
    },
    "src_race_2": {
        "query": """
            SELECT DISTINCT src_m.participant_id AS person_id,
                MIN(stcm1.source_code) AS ppi_code,
                MIN(stcm1.source_concept_id) AS race_source_concept_id,
                MIN(COALESCE(vc1.concept_id, 0)) AS race_target_concept_id
            FROM `{dataset_id}.src_mapped` src_m
                INNER JOIN `{dataset_id}.source_to_concept_map` stcm1 ON src_m.value_ppi_code = stcm1.source_code
                AND stcm1.priority = 2 -- priority 2
                AND stcm1.source_vocabulary_id = 'ppi-race'
                LEFT JOIN `{dataset_id}.concept` vc1 ON stcm1.target_concept_id = vc1.concept_id
                AND vc1.standard_concept = 'S'
                AND vc1.invalid_reason IS NULL
            WHERE NOT EXISTS (
                    SELECT *
                    FROM `{dataset_id}.src_race` g
                    WHERE src_m.participant_id = g.person_id
                )
            GROUP BY src_m.participant_id
            HAVING COUNT(DISTINCT src_m.value_ppi_code) = 1""",
        "destination": "src_race",
        "append": True,
    },
    "src_ethnicity": {
        "query": """
            SELECT
              DISTINCT src_m.participant_id AS person_id,
              MIN(stcm1.source_code) AS ppi_code,
              MIN(stcm1.source_concept_id) AS ethnicity_source_concept_id,
              MIN(COALESCE(vc1.concept_id, 0)) AS ethnicity_target_concept_id
            FROM
              `{dataset_id}.src_mapped` src_m
            INNER JOIN
              `{dataset_id}.source_to_concept_map` stcm1
            ON
              src_m.value_ppi_code = stcm1.source_code
              AND stcm1.priority = 1              -- priority 1
              AND stcm1.source_vocabulary_id = 'ppi-ethnicity'
            LEFT JOIN
              `{dataset_id}.concept` vc1
            ON
              stcm1.target_concept_id = vc1.concept_id
              AND vc1.standard_concept = 'S'
              AND vc1.invalid_reason IS NULL
            GROUP BY
              src_m.participant_id
            HAVING
              COUNT(DISTINCT src_m.value_ppi_code) = 1""",
        "destination": "src_ethnicity",
        "append": False,
    },
    "src_ethnicity_2": {
        "destination": "src_ethnicity",
        "append": True,
        "query": """
            SELECT
              DISTINCT src_m.participant_id AS person_id,
              MIN(stcm1.source_code) AS ppi_code,
              MIN(stcm1.source_concept_id) AS ethnicity_source_concept_id,
              MIN(COALESCE(vc1.concept_id, 0)) AS ethnicity_target_concept_id
            FROM
              `{dataset_id}.src_mapped` src_m
            INNER JOIN
              `{dataset_id}.source_to_concept_map` stcm1
            ON
              src_m.value_ppi_code = stcm1.source_code
              AND stcm1.priority = 2              -- priority 2
              AND stcm1.source_vocabulary_id = 'ppi-ethnicity'
            LEFT JOIN
              `{dataset_id}.concept` vc1
            ON
              stcm1.target_concept_id = vc1.concept_id
              AND vc1.standard_concept = 'S'
              AND vc1.invalid_reason IS NULL
            WHERE
              NOT EXISTS (
              SELECT
                *
              FROM
                `{dataset_id}.src_ethnicity` g
              WHERE
                src_m.participant_id = g.person_id)
            GROUP BY
              src_m.participant_id
            HAVING
              COUNT(DISTINCT src_m.value_ppi_code) = 1""",
    },
    "care_site": {
        "query": """
            SELECT
              DISTINCT site.site_id AS care_site_id,
              site.site_name AS care_site_name,
              0 AS place_of_service_concept_id,
              NULL AS location_id,
              site.site_id AS care_site_source_value,
              NULL AS place_of_service_source_value,
              '' AS src_id
            FROM
              `{dataset_id}.site` site""",
        "destination": "care_site",
        "append": False,
    },
    "location": {
        "query": """
            SELECT
             ROW_NUMBER() over () AS location_id,
             address_1,
             address_2,
             city,
             state,
             zip,
             county,
             location_source_value
            FROM (
               SELECT
               DISTINCT
               src.address_1 AS address_1,
               src.address_2 AS address_2,
               src.city AS city,
               src.state AS state,
               src.zip AS zip,
               NULL AS county,
               src.state_ppi_code AS location_source_value
               FROM
               `{dataset_id}.src_person_location` src
            )""",
        "destination": "location",
        "append": False,
    },
    "measurement": {
        "query": """
            SELECT
              meas.measurement_id AS measurement_id,
              meas.participant_id AS person_id,
              meas.cv_concept_id AS measurement_concept_id,
              DATE(meas.measurement_time) AS measurement_date,
              meas.measurement_time AS measurement_datetime,
              NULL AS measurement_time,
            IF
              (meas.collect_type <> 2
                OR meas.collect_type IS NULL, 44818701, 32865) AS measurement_type_concept_id,
              -- 44818701, From physical examination. 32865, Patient self-report
              0 AS operator_concept_id,
              meas.value_decimal AS value_as_number,
              meas.vcv_concept_id AS value_as_concept_id,
              meas.vu_concept_id AS unit_concept_id,
              NULL AS range_low,
              NULL AS range_high,
              NULL AS provider_id,
              meas.physical_measurements_id AS visit_occurrence_id,
              NULL AS visit_detail_id,
              meas.code_value AS measurement_source_value,
              meas.cv_source_concept_id AS measurement_source_concept_id,
              meas.value_unit AS unit_source_value,
              CASE
                WHEN meas.value_decimal IS NOT NULL OR meas.value_unit IS NOT NULL THEN CONCAT(COALESCE(CAST(meas.value_decimal AS STRING), ''), ' ', COALESCE(meas.value_unit, ''))     -- 'meas.dec'
                WHEN meas.value_code_value IS NOT NULL THEN meas.value_code_value             -- 'meas.value'
              ELSE
              NULL                                  -- 'meas.empty'
            END
              AS value_source_value,
              meas.parent_id AS parent_id,
              meas.src_id AS src_id
            FROM
              `{dataset_id}.src_meas_mapped` meas
            WHERE
              meas.cv_domain_id = 'Measurement'
              OR meas.cv_domain_id IS NULL""",
        "destination": "measurement",
        "append": False,
    },
    "note": {
        "query": """
            SELECT
              NULL AS note_id,
              meas.participant_id AS person_id,
              DATE(meas.measurement_time) AS note_date,
              meas.measurement_time AS note_datetime,
              44814645 AS note_type_concept_id,
              -- 44814645 - 'Note'
              0 AS note_class_concept_id,
              NULL AS note_title,
              COALESCE(meas.value_string, '') AS note_text,
              0 AS encoding_concept_id,
              4180186 AS language_concept_id,
              -- 4180186 - 'English language'
              NULL AS provider_id,
              NULL AS visit_detail_id,
              meas.code_value AS note_source_value,
              meas.physical_measurements_id AS visit_occurrence_id,
              'note' AS unit_id
            FROM
              `{dataset_id}.src_meas` meas
            WHERE
              meas.code_value = 'notes'""",
        "destination": "note",
        "append": False,
    },
    "person": {
        "query": """
            SELECT
              DISTINCT src_m.participant_id AS person_id,
              COALESCE(g.gender_target_concept_id, 0) AS gender_concept_id,
              EXTRACT(YEAR FROM b.date_of_birth) AS year_of_birth,
              EXTRACT(MONTH FROM b.date_of_birth) AS month_of_birth,
              EXTRACT(DAY FROM b.date_of_birth) AS day_of_birth,
              TIMESTAMP(b.date_of_birth) AS birth_datetime,
              COALESCE(r.race_target_concept_id, 0) AS race_concept_id,
              COALESCE(e.ethnicity_target_concept_id, 0) AS ethnicity_concept_id,
              person_loc.location_id AS location_id,
              NULL AS provider_id,
              NULL AS care_site_id,
              src_m.participant_id AS person_source_value,
              g.ppi_code AS gender_source_value,
              COALESCE(g.gender_source_concept_id, 0) AS gender_source_concept_id,
              r.ppi_code AS race_source_value,
              COALESCE(r.race_source_concept_id, 0) AS race_source_concept_id,
              e.ppi_code AS ethnicity_source_value,
              COALESCE(e.ethnicity_source_concept_id, 0) AS ethnicity_source_concept_id,
              b.src_id AS src_id
            FROM
              `{dataset_id}.src_mapped` src_m
            INNER JOIN
              `{dataset_id}.src_participant` b
            ON
              src_m.participant_id = b.participant_id
            LEFT JOIN
              `{dataset_id}.src_gender` g
            ON
              src_m.participant_id = g.person_id
            LEFT JOIN
              `{dataset_id}.src_race` r
            ON
              src_m.participant_id = r.person_id
            LEFT JOIN
              `{dataset_id}.src_ethnicity` e
            ON
              src_m.participant_id = e.person_id
            LEFT JOIN
              `{dataset_id}.src_person_location` person_loc
            ON
              src_m.participant_id = person_loc.participant_id""",
        "destination": "person",
        "append": False,
    },
    "src_gender": {
        "query": """
            SELECT
              DISTINCT src_m.participant_id AS person_id,
              MIN(stcm1.source_code) AS ppi_code,
              MIN(stcm1.source_concept_id) AS gender_source_concept_id,
              MIN(COALESCE(vc1.concept_id, 0)) AS gender_target_concept_id
            FROM
              `{dataset_id}.src_mapped` src_m
            INNER JOIN
              `{dataset_id}.source_to_concept_map` stcm1
            ON
              src_m.value_ppi_code = stcm1.source_code
              AND stcm1.priority = 1              -- priority 1
              AND stcm1.source_vocabulary_id = 'ppi-sex'
            LEFT JOIN
              `{dataset_id}.concept` vc1
            ON
              stcm1.target_concept_id = vc1.concept_id
              AND vc1.standard_concept = 'S'
              AND vc1.invalid_reason IS NULL
            GROUP BY
              src_m.participant_id
            HAVING
              COUNT(DISTINCT src_m.value_ppi_code) = 1""",
        "destination": "src_gender",
        "append": False,
    },
    "src_mapped": {
        "query": """
            SELECT
               src_c.participant_id                AS participant_id,
               src_c.date_of_survey                AS date_of_survey,
               src_c.question_ppi_code             AS question_ppi_code,
               src_c.question_code_id              AS question_code_id,
               COALESCE(vc1.concept_id, 0)         AS question_source_concept_id,
               COALESCE(vc2.concept_id, 0)         AS question_concept_id,
               src_c.value_ppi_code                AS value_ppi_code,
               src_c.topic_value                   AS topic_value,
               src_c.value_code_id                 AS value_code_id,
               COALESCE(vc3.concept_id, 0)         AS value_source_concept_id,
               CASE
                  WHEN src_c.is_invalid = 1 THEN 2000000010
                  ELSE COALESCE(vc4.concept_id, 0)
               END                                 AS value_concept_id,
               src_c.value_number                  AS value_number,
               src_c.value_boolean                 AS value_boolean,
               CASE
                  WHEN src_c.value_boolean = 1 THEN 45877994
                  WHEN src_c.value_boolean = 0 THEN 45878245
                  ELSE 0
               END                                 AS value_boolean_concept_id,
               src_c.value_date                    AS value_date,
               src_c.value_string                  AS value_string,
               src_c.questionnaire_response_id     AS questionnaire_response_id,
               src_c.unit_id                       AS unit_id,
               src_c.is_invalid                    as is_invalid,
               src_c.src_id                        AS src_id
            FROM `{dataset_id}.src_clean` src_c
            JOIN `{dataset_id}.src_participant` src_p
               ON  src_c.participant_id = src_p.participant_id
            LEFT JOIN `{dataset_id}.concept` vc1
               ON  src_c.question_ppi_code = vc1.concept_code
               AND vc1.vocabulary_id = 'PPI'
            LEFT JOIN `{dataset_id}.concept_relationship` vcr1
               ON  vc1.concept_id = vcr1.concept_id_1
               AND vcr1.relationship_id = 'Maps to'
               AND vcr1.invalid_reason IS NULL
            LEFT JOIN `{dataset_id}.concept` vc2
               ON  vcr1.concept_id_2 = vc2.concept_id
               AND vc2.standard_concept = 'S'
               AND vc2.invalid_reason IS NULL
            LEFT JOIN `{dataset_id}.concept` vc3
               ON  src_c.value_ppi_code = vc3.concept_code
               AND vc3.vocabulary_id = 'PPI'
            LEFT JOIN `{dataset_id}.concept_relationship` vcr2
               ON  vc3.concept_id = vcr2.concept_id_1
               AND vcr2.relationship_id = 'Maps to value'
               AND vcr2.invalid_reason IS NULL
            LEFT JOIN `{dataset_id}.concept` vc4
               ON  vcr2.concept_id_2 = vc4.concept_id
               AND vc4.standard_concept = 'S'
               AND vc4.invalid_reason IS NULL
            WHERE src_c.filter = 0""",
        "destination": "src_mapped",
        "append": False,
    },
    "src_meas_mapped": {
        "query": """
            SELECT
              meas.participant_id AS participant_id,
              meas.finalized_site_id AS finalized_site_id,
              meas.code_value AS code_value,
              COALESCE(tmp1.cv_source_concept_id, 0) AS cv_source_concept_id,
              COALESCE(tmp1.cv_concept_id, 0) AS cv_concept_id,
              tmp1.cv_domain_id AS cv_domain_id,
              meas.measurement_time AS measurement_time,
              meas.value_decimal AS value_decimal,
              meas.value_unit AS value_unit,
              COALESCE(vc1.concept_id, 0) AS vu_concept_id,
              meas.value_code_value AS value_code_value,
              COALESCE(tmp2.vcv_source_concept_id, 0) AS vcv_source_concept_id,
              COALESCE(tmp2.vcv_concept_id, 0) AS vcv_concept_id,
              meas.measurement_id AS measurement_id,
              meas.physical_measurements_id AS physical_measurements_id,
              meas.parent_id AS parent_id,
              meas.src_id AS src_id,
              meas.collect_type AS collect_type
            FROM
              `{dataset_id}.src_meas` meas
            LEFT JOIN
              `{dataset_id}.tmp_cv_concept_lk` tmp1
            ON
              meas.code_value = tmp1.code_value
            LEFT JOIN
              `{dataset_id}.concept` vc1           -- here we map units of measurements to standard concepts
            ON
              meas.value_unit = vc1.concept_code
              AND vc1.vocabulary_id = 'UCUM'
              AND vc1.standard_concept = 'S'
              AND vc1.invalid_reason IS NULL
            LEFT JOIN
              `{dataset_id}.tmp_vcv_concept_lk` tmp2
            ON
              meas.value_code_value = tmp2.value_code_value
            WHERE
              meas.code_value <> 'notes'""",
        "destination": "src_meas_mapped",
        "append": False,
    },
    "src_participant": {
        "query": """
            SELECT
              f1.participant_id,
              f1.latest_date_of_survey,
              f1.date_of_birth,
              f1.src_id
            FROM (
              SELECT
                t1.participant_id AS participant_id,
                t1.latest_date_of_survey AS latest_date_of_survey,
                MAX(DATE(t2.value_date)) AS date_of_birth,
                t1.src_id AS src_id
              FROM (
                SELECT
                  src_c.participant_id AS participant_id,
                  MAX(src_c.date_of_survey) AS latest_date_of_survey,
                  src_c.src_id AS src_id
                FROM
                  `{dataset_id}.src_clean` src_c
                WHERE
                  src_c.question_ppi_code = 'PIIBirthInformation_BirthDate'
                  AND src_c.value_date IS NOT NULL
                GROUP BY
                  src_c.participant_id,
                  src_c.src_id ) t1
              INNER JOIN
                `{dataset_id}.src_clean` t2
              ON
                t1.participant_id = t2.participant_id
                AND t1.latest_date_of_survey = t2.date_of_survey
                AND t2.question_ppi_code = 'PIIBirthInformation_BirthDate'
              GROUP BY
                t1.participant_id,
                t1.latest_date_of_survey,
                t1.src_id ) f1""",
        "destination": "src_participant",
        "append": False,
    },
    "src_person_location": {
        "query": """
            SELECT
              `{dataset_id}.src_participant`.participant_id AS participant_id,
              MAX(m_address_1.value_string) AS address_1,
              MAX(m_address_2.value_string) AS address_2,
              MAX(m_city.value_string) AS city,
              MAX(m_zip.value_string) AS zip,
              MAX(m_state.value_ppi_code) AS state_ppi_code,
              MAX(RIGHT(m_state.value_ppi_code, 2)) AS state,
              NULL AS location_id
            FROM
              `{dataset_id}.src_participant`
            INNER JOIN
              `{dataset_id}.src_mapped` m_address_1
            ON
              `{dataset_id}.src_participant`.participant_id = m_address_1.participant_id
              AND m_address_1.question_ppi_code = 'PIIAddress_StreetAddress'
            LEFT JOIN
              `{dataset_id}.src_mapped` m_address_2
            ON
              m_address_1.questionnaire_response_id = m_address_2.questionnaire_response_id
              AND m_address_2.question_ppi_code = 'PIIAddress_StreetAddress2'
            LEFT JOIN
              `{dataset_id}.src_mapped` m_city
            ON
              m_address_1.questionnaire_response_id = m_city.questionnaire_response_id
              AND m_city.question_ppi_code = 'StreetAddress_PIICity'
            LEFT JOIN
              `{dataset_id}.src_mapped` m_zip
            ON
              m_address_1.questionnaire_response_id = m_zip.questionnaire_response_id
              AND m_zip.question_ppi_code = 'StreetAddress_PIIZIP'
            LEFT JOIN
              `{dataset_id}.src_mapped` m_state
            ON
              m_address_1.questionnaire_response_id = m_state.questionnaire_response_id
              AND m_state.question_ppi_code = 'StreetAddress_PIIState'
            WHERE
              m_address_1.date_of_survey = (
              SELECT
                MAX(date_of_survey)
              FROM
                `{dataset_id}.src_mapped` m_address_1_2
              WHERE
                m_address_1_2.participant_id = m_address_1.participant_id
                AND m_address_1_2.question_ppi_code = 'PIIAddress_StreetAddress')
            GROUP BY
              `{dataset_id}.src_participant`.participant_id;""",
        "destination": "src_person_location",
        "append": False,
    },
    "tmp_cv_concept_lk": {
        "query": """
            SELECT
              DISTINCT meas.code_value AS code_value,
              vc1.concept_id AS cv_source_concept_id,
              vc2.concept_id AS cv_concept_id,
              COALESCE(vc2.domain_id, vc1.domain_id) AS cv_domain_id
            FROM
              `{dataset_id}.src_meas` meas
            LEFT JOIN
              `{dataset_id}.concept` vc1
            ON
              meas.code_value = vc1.concept_code
              AND vc1.vocabulary_id = 'PPI'
            LEFT JOIN
              `{dataset_id}.concept_relationship` vcr1
            ON
              vc1.concept_id = vcr1.concept_id_1
              AND vcr1.relationship_id = 'Maps to'
              AND vcr1.invalid_reason IS NULL
            LEFT JOIN
              `{dataset_id}.concept` vc2
            ON
              vc2.concept_id = vcr1.concept_id_2
              AND vc2.standard_concept = 'S'
              AND vc2.invalid_reason IS NULL
            WHERE
              meas.code_value IS NOT NULL""",
        "destination": "tmp_cv_concept_lk",
        "append": False,
    },
    "tmp_fact_rel_sd": {
        "query": """
            SELECT
              m.measurement_id AS measurement_id,
              CASE
                WHEN m.measurement_source_value = 'blood-pressure-systolic-1' THEN 1
                WHEN m.measurement_source_value = 'blood-pressure-systolic-2' THEN 2
                WHEN m.measurement_source_value = 'blood-pressure-systolic-3' THEN 3
                WHEN m.measurement_source_value = 'blood-pressure-systolic-mean' THEN 4
              ELSE
              0
            END
              AS systolic_blood_pressure_ind,
              CASE
                WHEN m.measurement_source_value = 'blood-pressure-diastolic-1' THEN 1
                WHEN m.measurement_source_value = 'blood-pressure-diastolic-2' THEN 2
                WHEN m.measurement_source_value = 'blood-pressure-diastolic-3' THEN 3
                WHEN m.measurement_source_value = 'blood-pressure-diastolic-mean' THEN 4
              ELSE
              0
            END
              AS diastolic_blood_pressure_ind,
              m.person_id AS person_id,
              m.parent_id AS parent_id,
              m.src_id AS src_id
            FROM
              `{dataset_id}.measurement` m
            WHERE
              m.measurement_source_value IN ( 'blood-pressure-systolic-1',
                'blood-pressure-systolic-2',
                'blood-pressure-systolic-3',
                'blood-pressure-systolic-mean',
                'blood-pressure-diastolic-1',
                'blood-pressure-diastolic-2',
                'blood-pressure-diastolic-3',
                'blood-pressure-diastolic-mean' )
              AND m.parent_id IS NOT NULL""",
        "destination": "tmp_fact_rel_sd",
        "append": False,
    },
    "tmp_vcv_concept_lk": {
        "query": """
            SELECT
              DISTINCT meas.value_code_value AS value_code_value,
              vcv1.concept_id AS vcv_source_concept_id,
              vcv2.concept_id AS vcv_concept_id,
              COALESCE(vcv2.domain_id, vcv2.domain_id) AS vcv_domain_id
            FROM
              `{dataset_id}.src_meas` meas
            LEFT JOIN
              `{dataset_id}.concept` vcv1
            ON
              meas.value_code_value = vcv1.concept_code
              AND vcv1.vocabulary_id = 'PPI'
            LEFT JOIN
              `{dataset_id}.concept_relationship` vcrv1
            ON
              vcv1.concept_id = vcrv1.concept_id_1
              AND vcrv1.relationship_id = 'Maps to'
              AND vcrv1.invalid_reason IS NULL
            LEFT JOIN
              `{dataset_id}.concept` vcv2
            ON
              vcv2.concept_id = vcrv1.concept_id_2
              AND vcv2.standard_concept = 'S'
              AND vcv2.invalid_reason IS NULL
            WHERE
              meas.value_code_value IS NOT NULL""",
        "destination": "tmp_vcv_concept_lk",
        "append": False,
    },
    "tmp_visits_src": {
        "query": """
            SELECT
              src_meas.physical_measurements_id AS visit_occurrence_id,
              src_meas.participant_id AS person_id,
              MIN(src_meas.measurement_time) AS visit_start_datetime,
              MAX(src_meas.measurement_time) AS visit_end_datetime,
              src_meas.finalized_site_id AS care_site_id,
              src_meas.src_id AS src_id
            FROM
              `{dataset_id}.src_meas` src_meas
            GROUP BY
              src_meas.physical_measurements_id,
              src_meas.participant_id,
              src_meas.finalized_site_id,
              src_meas.src_id""",
        "destination": "tmp_visits_src",
        "append": False,
    },
    "visit_occurrence": {
        "query": """
            SELECT
              src.visit_occurrence_id AS visit_occurrence_id,
              src.person_id AS person_id,
              9202 AS visit_concept_id,
              -- 9202 - 'Outpatient Visit'
              DATE(src.visit_start_datetime) AS visit_start_date,
              src.visit_start_datetime AS visit_start_datetime,
              DATE(src.visit_end_datetime) AS visit_end_date,
              src.visit_end_datetime AS visit_end_datetime,
              44818519 AS visit_type_concept_id,
              -- 44818519 - 'Clinical Study Visit'
              NULL AS provider_id,
              src.care_site_id AS care_site_id,
              src.visit_occurrence_id AS visit_source_value,
              0 AS visit_source_concept_id,
              0 AS admitting_source_concept_id,
              NULL AS admitting_source_value,
              0 AS discharge_to_concept_id,
              NULL AS discharge_to_source_value,
              NULL AS preceding_visit_occurrence_id,
              src.src_id AS src_id
            FROM
              `{dataset_id}.tmp_visits_src` src""",
        "destination": "visit_occurrence",
        "append": False,
    },
    "observation": {
        "query": """
            SELECT
              ROW_NUMBER() OVER() AS observation_id,
              src_m.participant_id AS person_id,
              src_m.question_concept_id AS observation_concept_id,
              DATE(src_m.date_of_survey) AS observation_date,
              src_m.date_of_survey AS observation_datetime,
              45905771 AS observation_type_concept_id,
              -- 45905771, Observation Recorded from a Survey
              src_m.value_number AS value_as_number,
              CASE
                WHEN src_m.value_ppi_code IS NOT NULL AND src_m.value_concept_id = 0 THEN src_m.value_string
                WHEN src_m.value_string IS NOT NULL
              AND src_m.value_ppi_code IS NULL THEN src_m.value_string
              ELSE
              NULL
            END
              AS value_as_string,
              CASE
                WHEN src_m.value_ppi_code IS NOT NULL THEN src_m.value_concept_id
                WHEN src_m.value_boolean IS NOT NULL THEN src_m.value_boolean_concept_id
              ELSE
              0
            END
              AS value_as_concept_id,
              0 AS qualifier_concept_id,
              0 AS unit_concept_id,
              NULL AS provider_id,
              NULL AS visit_occurrence_id,
              NULL AS visit_detail_id,
              src_m.question_ppi_code AS observation_source_value,
              src_m.question_source_concept_id AS observation_source_concept_id,
              CAST(NULL AS STRING) AS unit_source_value,
              NULL AS qualifier_source_value,
              src_m.value_source_concept_id AS value_source_concept_id,
              src_m.value_ppi_code AS value_source_value,
              src_m.questionnaire_response_id AS questionnaire_response_id,
              src_m.src_id AS src_id
            FROM
              `{dataset_id}.src_mapped` src_m
            WHERE
              src_m.question_ppi_code IS NOT null
              UNION ALL
              SELECT
              ROW_NUMBER() OVER() AS observation_id,
              meas.participant_id AS person_id,
              meas.cv_concept_id AS observation_concept_id,
              DATE(meas.measurement_time) AS observation_date,
              meas.measurement_time AS observation_datetime,
              581413 AS observation_type_concept_id,
              -- 581413, Observation from Measurement
              CAST(NULL AS NUMERIC) AS value_as_number,
              CAST(NULL AS STRING) AS value_as_string,
              meas.vcv_concept_id AS value_as_concept_id,
              0 AS qualifier_concept_id,
              meas.vu_concept_id AS unit_concept_id,
              NULL AS provider_id,
              meas.physical_measurements_id AS visit_occurrence_id,
              NULL AS visit_detail_id,
              meas.code_value AS observation_source_value,
              meas.cv_source_concept_id AS observation_source_concept_id,
              meas.value_unit AS unit_source_value,
              NULL AS qualifier_source_value,
              meas.vcv_source_concept_id AS value_source_concept_id,
              meas.value_code_value AS value_source_value,
              NULL AS questionnaire_response_id,
              meas.src_id AS src_id
            FROM
              `{dataset_id}.src_meas_mapped` meas
            WHERE
              meas.cv_domain_id = 'Observation'""",
        "destination": "observation",
        "append": False,
    },
    "src_clean_qrids": {
        "query": """
            SELECT
              DISTINCT questionnaire_response_id
            FROM
              `{dataset_id}.src_clean`
            WHERE
              FILTER = 0""",
        "destination": "src_clean_qrids",
        "append": False,
    },
    "filter_questions": {
        "query": """
            UPDATE
              `{dataset_id}.src_clean`
            SET
              `{dataset_id}.src_clean`.filter = 1
            WHERE
              `{dataset_id}.src_clean`.question_ppi_code IN (
              SELECT
                TRIM(question_ppi_code)
              FROM
                `{dataset_id}.combined_question_filter`)""",
        "destination": None,
        "append": False,
    },
    "filter_surveys": {
        "query": """
            UPDATE
              `{dataset_id}.src_clean`
            SET
              `{dataset_id}.src_clean`.filter = 1
            WHERE
              `{dataset_id}.src_clean`.survey_name IN (
              SELECT
                TRIM(survey_name)
              FROM
                `{dataset_id}.combined_survey_filter`)""",
        "destination": None,
        "append": False,
    },
    "update_location_id": {
        "append": False,
        "query": """
            UPDATE
              `{dataset_id}.src_person_location` person_loc
            SET
              person_loc.location_id = loc.location_id
              FROM `{dataset_id}.location` loc
            WHERE
              IFNULL(person_loc.address_1, '') = IFNULL(loc.address_1, '')
              AND IFNULL(person_loc.address_2, '') = IFNULL(loc.address_2, '')
              AND IFNULL(person_loc.city, '') = IFNULL(loc.city, '')
              AND IFNULL(person_loc.state, '') = IFNULL(loc.state, '')
              AND IFNULL(person_loc.zip, '') = IFNULL(loc.zip, '')""",
        "destination": None,
    },
    "survey_conduct": {
        "destination": "survey_conduct",
        "append": False,
        "query": """
            SELECT * FROM
              EXTERNAL_QUERY("all-of-us-rdr-prod.us-central1.bq-rdr-preprod-curation",
                "SELECT qr.questionnaire_response_id survey_conduct_id, p.participant_id person_id, """
              """COALESCE(voc_c.concept_id, 0) survey_concept_id, CAST(NULL AS DATE) survey_start_date, """
              """CAST(NULL AS DATETIME) survey_start_datetime, DATE(qr.authored) survey_end_date, """
              """qr.authored survey_end_datetime, 0 provider_id, CASE WHEN qr.non_participant_author = 'CATI' THEN """
              """42530794 ELSE 0 END assisted_concept_id, 0 respondent_type_concept_id, 0 timing_concept_id, CASE """
              """WHEN qr.non_participant_author = 'CATI' THEN 42530794 ELSE 42531021 END """
              """collection_method_concept_id, CASE WHEN qr.non_participant_author = 'CATI' THEN 'Telephone' ELSE """
              """'No matching concept' END assisted_source_value, CAST(NULL AS CHAR) respondent_type_source_value, """
              """'' timing_source_value, CASE WHEN qr.non_participant_author = 'CATI' THEN 'Telephone' ELSE """
              """'Electronic' END collection_method_source_value, mc.value survey_source_value, """
              """ mc.code_id survey_source_concept_id, qr.questionnaire_response_id survey_source_identifier, """
              """0 validated_survey_concept_id, '' validated_survey_source_value, CAST(NULL AS UNSIGNED) """
              """survey_version_number, '' visit_occurrence_id, """
              """'' response_visit_occurrence_id, p.participant_origin src_id FROM questionnaire_response qr """
              """INNER JOIN questionnaire_concept qc ON qc.questionnaire_id = qr.questionnaire_id AND """
              """qc.questionnaire_version = qr.questionnaire_version INNER JOIN code mc ON mc.code_id = qc.code_id """
              """INNER JOIN participant p ON p.participant_id = qr.participant_id LEFT JOIN voc.concept voc_c ON """
              """voc_c.concept_code = mc.value AND voc_c.vocabulary_id = 'PPI' AND voc_c.domain_id = 'observation' """
              """AND voc_c.concept_class_id = 'module' WHERE qr.questionnaire_response_id IN (SELECT DISTINCT """
              """sc.questionnaire_response_id FROM cdm.src_clean sc WHERE sc.filter = 0 )");""",
    },
    "death": {
        "destination": "death",
        "append": False,
        "query": """
            SELECT
              dr.participant_id AS person_id,
              dr.date_of_death AS death_date,
              CAST(dr.date_of_death AS DATETIME) AS death_datetime,
              '32809' AS death_type_concept_id,
              'NULL' AS cause_concept_id, -- CDR requires these columns to have a value of 'NULL'
              'NULL' AS cause_source_value,
              'NULL' AS cause_source_concept_id,
              'healthpro' AS src_id
            FROM
              `{dataset_id}.deceased_report` dr
            JOIN
              `{dataset_id}.person` per
            ON
              dr.participant_id = per.person_id
            WHERE
              dr.status = 2
            AND dr.authored < {cutoff}""",
    },
    "ehr_consent_temp_table": {
        "destination": "tmp_ehr_consent",
        "append": False,
        "query": """
            SELECT * FROM
              EXTERNAL_QUERY("all-of-us-rdr-prod.us-central1.bq-rdr-preprod-curation",
                "SELECT sc.participant_id, sc.research_id, sc.value_ppi_code, sc.date_of_survey, sc.src_id, """
                    """consent_file.created AS cf_created, consent_file.sync_status, consent_response.created AS """
                    """cr_created FROM cdm.src_clean sc LEFT JOIN rdr.consent_response ON """
                    """sc.questionnaire_response_id = consent_response.questionnaire_response_id LEFT JOIN """
                    """rdr.consent_file ON consent_response.id = consent_file.consent_response_id WHERE """
                    """sc.survey_name = 'EHRConsentPII' AND sc.question_ppi_code = """
                    """'EHRConsentPII_ConsentPermission'");""",
    },
    "ehr_consent": {
        "destination": "consent",
        "append": False,
        "query": """
            SELECT ec.participant_id AS person_id,
                ec.research_id,
                CASE
                    WHEN ec.value_ppi_code IN ('No', 'ConsentPermission_No') THEN 'SUBMITTED_NO'
                    WHEN ec.value_ppi_code IN ('Yes', 'ConsentPermission_Yes')
                    AND cr_created IS NULL THEN 'SUBMITTED'
                    WHEN ec.value_ppi_code IN ('Yes', 'ConsentPermission_Yes')
                    AND ec.cr_created IS NOT NULL
                    AND ec.cf_created IS NULL THEN 'SUBMITTED_NOT_VALIDATED'
                    WHEN ec.value_ppi_code IN ('Yes', 'ConsentPermission_Yes')
                    AND ec.cf_created > '{cutoff}' THEN 'SUBMITTED_NOT_VALIDATED'
                    WHEN ec.value_ppi_code IN ('Yes', 'ConsentPermission_Yes')
                    AND ec.sync_status IN (2, 4) THEN 'SUBMITTED'
                    WHEN ec.value_ppi_code IN ('Yes', 'ConsentPermission_Yes')
                    AND ec.sync_status NOT IN (2, 4) THEN 'SUBMITTED_INVALID'
                END consent_for_electronic_health_records,
                ec.date_of_survey AS consent_for_electronic_health_records_authored,
                ec.src_id
            FROM `{dataset_id}.tmp_ehr_consent` ec""",
    },
    "wear_consent": {
        "destination": "wear_consent",
        "append": False,
        "query": """SELECT * FROM EXTERNAL_QUERY("all-of-us-rdr-prod.us-central1.bq-rdr-preprod-curation",
                    "SELECT pa.participant_id AS person_id, pa.research_id, qr.authored, ac.value AS """
                    """consent_status, pa.participant_origin AS src_id"""
                    """ FROM rdr.participant pa JOIN rdr.questionnaire_response qr ON """
                    """pa.participant_id = qr.participant_id JOIN rdr.questionnaire_response_answer qra ON """
                    """qr.questionnaire_response_id = qra.questionnaire_response_id JOIN """
                    """rdr.questionnaire_question qq ON qra.question_id = qq.questionnaire_question_id """
                    """JOIN rdr.code qcd ON qq.code_id = qcd.code_id """
                    """LEFT JOIN rdr.code ac ON qra.value_code_id = ac.code_id JOIN """
                    """rdr.questionnaire q ON qr.questionnaire_id = q.questionnaire_id JOIN """
                    """rdr.questionnaire_concept qc ON q.questionnaire_id = qc.questionnaire_id AND """
                    """q.version = qc.questionnaire_version JOIN rdr.code cc ON qc.code_id = cc.code_id WHERE """
                    """ac.value IS NOT NULL AND cc.value = 'wear_consent' AND qcd.value = 'resultsconsent_wear' """
                    """AND pa.participant_id IN (SELECT DISTINCT participant_id FROM cdm.src_clean) ORDER BY """
                    """pa.participant_id, qr.authored;");""",
    },
    "participant_id_mapping": {
        "destination": "participant_id_mapping",
        "append": False,
        "query": """SELECT * FROM EXTERNAL_QUERY(
                                   "all-of-us-rdr-prod.us-central1.bq-rdr-preprod-curation",
                        "SELECT  pid_map.p_id,  pid_map.id_source,  pid_map.id_value,  pid_map.src_id  """
                        """FROM (SELECT participant.participant_id AS p_id, 'r_id' AS id_source, """
                        """participant.research_id AS id_value, participant.participant_origin AS src_id  """
                        """FROM rdr.participant UNION SELECT participant.participant_id AS p_id, 'vibrent_id' """
                        """AS id_source, participant.external_id AS id_value, participant.participant_origin """
                        """AS src_id  FROM rdr.participant) AS  pid_map  WHERE  pid_map.id_value """
                        """IS NOT NULL;");""",
    },
    "finalize": {
        "destination": None,
        "query": """
            ALTER TABLE `{dataset_id}.measurement` DROP COLUMN parent_id;
        """
    }
}
