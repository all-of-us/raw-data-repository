queries = {
    "audit_snapshot_create_table": {
        "destination": None,
        "append": False,
        "query": """
            CREATE TABLE IF NOT EXISTS `{audit_dataset_id}.{snapshot_table}` AS
            SELECT
                CAST(NULL AS STRING) AS audit_run_id,
                CAST(NULL AS TIMESTAMP) AS snapshot_ts,
                CAST(NULL AS STRING) AS source_project,
                CAST(NULL AS STRING) AS source_dataset,
                CAST(NULL AS STRING) AS snapshot_label,
                CAST(NULL AS STRING) AS etl_cutoff,
                src.*
            FROM `{dataset_id}.{source_table}` src
            WHERE 1 = 0
        """,
    },
    "audit_snapshot_insert_rows": {
        "destination": None,
        "append": False,
        "query": """
            INSERT INTO `{audit_dataset_id}.{snapshot_table}`
            SELECT
                {audit_run_id_sql} AS audit_run_id,
                {snapshot_ts_expr} AS snapshot_ts,
                {source_project_sql} AS source_project,
                {source_dataset_sql} AS source_dataset,
                {snapshot_label_sql} AS snapshot_label,
                {etl_cutoff_sql} AS etl_cutoff,
                src.*
            FROM `{dataset_id}.{source_table}` src
        """,
    },
    # ---------------------------------------------------------------------------
    # Phase 1 – Source data generation (replaces SQLAlchemy ORM in curation.py)
    # Reads from BigQuery tables replicated from MySQL via {rdr_dataset}.rdr_
    # ---------------------------------------------------------------------------
    "participant_filter": {
        # Replicates _select_participant_ids() from curation.py
        # Enum values: WithdrawalStatus.NO_USE=2, QuestionnaireStatus.SUBMITTED=1, UNSET=0
        "destination": "participant_filter",
        "append": False,
        "query": """
            SELECT DISTINCT p.participant_id
            FROM `{rdr_dataset}.rdr_participant` p
            JOIN `{rdr_dataset}.rdr_participant_summary` ps
                ON p.participant_id = ps.participant_id
            JOIN `{rdr_dataset}.rdr_hpo` h
                ON p.hpo_id = h.hpo_id
            WHERE (
                IFNULL(p.is_ghost_id, 0) != 1
                OR (
                    ps.participant_id IS NOT NULL
                    AND SAFE_CAST(p.date_added_ghost AS TIMESTAMP) > TIMESTAMP('2022-03-18')
                    AND (
                        ps.consent_for_electronic_health_records != 0
                        OR ps.questionnaire_on_the_basics = 1
                    )
                )
            )
            AND p.is_test_participant != 1
            AND h.name != 'TEST'
            AND ps.date_of_birth IS NOT NULL
            AND ps.consent_for_study_enrollment_first_yes_authored IS NOT NULL
            {age_filter}
            {withdrawal_filter}
            {origin_filter}
            {participant_selection_filter}
            {exclude_pid_filter}
            ORDER BY p.participant_id
        """,
    },
    "questionnaire_answers_by_module": {
        # Replicates _populate_questionnaire_answers_by_module() from curation.py.
        # Builds an intermediate lookup of (participant, survey, response, question)
        # used to determine which response is the "latest" per participant+module.
        # Enum values: status IN_PROGRESS=0, classification_type DUPLICATE=1, INVALID=6, PROFILE_UPDATE=2
        "destination": "questionnaire_answers_by_module",
        "append": False,
        "query": """
            SELECT DISTINCT
                qr.participant_id,
                qr.authored,
                qr.created,
                CASE WHEN mc.value = 'COPE' THEN qh.external_id ELSE mc.value END AS survey,
                qr.questionnaire_response_id AS response_id,
                qq.code_id AS question_code_id
            FROM `{rdr_dataset}.rdr_questionnaire_response` qr
            JOIN `{rdr_dataset}.rdr_questionnaire_concept` qc
                ON qc.questionnaire_id = qr.questionnaire_id
                AND qc.questionnaire_version = qr.questionnaire_version
            JOIN `{rdr_dataset}.rdr_code` mc
                ON mc.code_id = qc.code_id
            JOIN `{rdr_dataset}.rdr_questionnaire_history` qh
                ON qh.questionnaire_id = qr.questionnaire_id
                AND qh.version = qr.questionnaire_version
            JOIN `{rdr_dataset}.rdr_questionnaire_response_answer` qra
                ON qra.questionnaire_response_id = qr.questionnaire_response_id
            JOIN `{rdr_dataset}.rdr_questionnaire_question` qq
                ON qq.questionnaire_question_id = qra.question_id
            WHERE qr.status != 0
            AND qr.classification_type NOT IN (1, 6, 2)
            AND qr.participant_id IN (
                SELECT participant_id FROM `{dataset_id}.participant_filter`
            )
            {cutoff_authored_filter}
            {survey_filter}
        """,
    },
    "src_clean": {
        # Replicates _populate_src_clean() from curation.py.
        #
        # Two logical parts combined with UNION ALL:
        #   Part 1 – non-ConsentPII modules: only answers from the single most
        #            recent response per participant+module are included.
        #   Part 2 – ConsentPII (CONSENT_FOR_STUDY_ENROLLMENT_MODULE):
        #            "rolled-up" – latest answer per participant+question across
        #            all ConsentPII responses.  StreetAddress2 is also treated as
        #            stale when a newer StreetAddress1 response exists.
        #
        # Enum values (stored as integers in MySQL / BQ):
        #   QR status IN_PROGRESS=0 | classification DUPLICATE=1, INVALID=6, PROFILE_UPDATE=2
        #   CdrEtlCodeType MODULE=1, QUESTION=2, ANSWER=3
        "destination": "src_clean",
        "append": False,
        "query": """
            WITH
            -- ------------------------------------------------------------------
            -- Shared base: all eligible answer rows from the source RDR tables
            -- ------------------------------------------------------------------
            base_answers AS (
                SELECT
                    p.participant_id,
                    p.research_id,
                    p.external_id,
                    -- Use COPE questionnaire external_id as the survey name for COPE surveys
                    CASE WHEN mc.value = 'COPE' THEN qh.external_id ELSE mc.value END
                        AS survey_name,
                    COALESCE(qr.authored, qr.created)  AS date_of_survey,
                    qc_code.value                       AS question_ppi_code,
                    qq.code_id                          AS question_code_id,
                    -- When the answer is ignored, report PMI_Skip as the value PPI code
                    CASE WHEN qra.ignore = 1
                         THEN 'PMI_Skip'
                         ELSE ac.value
                    END                                 AS value_ppi_code,
                    ac.topic                            AS topic_value,
                    qra.ignore                AS is_invalid,
                    CASE WHEN qra.ignore = 1 THEN NULL ELSE qra.value_code_id END
                        AS value_code_id,
                    -- value_number: suppress for zip-code questions and for ignored answers
                    CASE
                        WHEN qra.ignore = 1 THEN NULL
                        WHEN qc_code.value IN (
                            'EmploymentWorkAddress_ZipCode', 'StreetAddress_PIIZIP'
                        ) THEN NULL
                        ELSE COALESCE(
                            qra.value_decimal,
                            CAST(qra.value_integer AS NUMERIC)
                        )
                    END                                 AS value_number,
                    CASE WHEN qra.ignore = 1 THEN NULL
                         ELSE CAST(qra.value_boolean AS INT64)
                    END                                 AS value_boolean,
                    CASE WHEN qra.ignore = 1 THEN NULL
                         ELSE COALESCE(
                             CAST(qra.value_date AS DATETIME),
                             qra.value_datetime
                         )
                    END                                 AS value_date,
                    -- value_string: cascade through multiple fallbacks, re-map zip-code
                    -- integers to string for zip-code question codes
                    CASE
                        WHEN qra.ignore = 1 THEN NULL
                        ELSE COALESCE(
                            LEFT(qra.value_string, 1024),
                            CAST(qra.value_date AS STRING),
                            CAST(qra.value_datetime AS STRING),
                            ac.display,
                            CASE WHEN qc_code.value IN (
                                     'EmploymentWorkAddress_ZipCode', 'StreetAddress_PIIZIP'
                                 )
                                 THEN CAST(qra.value_integer AS STRING)
                                 ELSE NULL
                            END
                        )
                    END                                 AS value_string,
                    qr.questionnaire_response_id,
                    CONCAT('cln.', CASE
                        WHEN qra.value_code_id   IS NOT NULL THEN 'code'
                        WHEN qra.value_integer   IS NOT NULL THEN 'int'
                        WHEN qra.value_decimal   IS NOT NULL THEN 'dec'
                        WHEN qra.value_boolean   IS NOT NULL THEN 'bool'
                        WHEN qra.value_date      IS NOT NULL THEN 'date'
                        WHEN qra.value_datetime  IS NOT NULL THEN 'dtime'
                        WHEN qra.value_string    IS NOT NULL THEN 'str'
                        ELSE ''
                    END)                                AS unit_id,
                    0                                   AS filter,
                    CASE
                        WHEN p.participant_origin = 'careevolution' THEN 'ce'
                        WHEN p.participant_origin = 'vibrent'       THEN 'vibrent'
                        ELSE p.participant_origin
                    END                                 AS src_id,
                    -- Internal columns used for CTE joins only (not selected in output)
                    mc.value    AS _module_value,
                    qr.authored AS _authored,
                    qr.created  AS _created
                FROM `{rdr_dataset}.rdr_participant` p
                JOIN `{rdr_dataset}.rdr_questionnaire_response` qr
                    ON qr.participant_id = p.participant_id
                JOIN `{rdr_dataset}.rdr_questionnaire_response_answer` qra
                    ON qra.questionnaire_response_id = qr.questionnaire_response_id
                JOIN `{rdr_dataset}.rdr_questionnaire_question` qq
                    ON qq.questionnaire_question_id = qra.question_id
                JOIN `{rdr_dataset}.rdr_questionnaire_concept` qc
                    ON qc.questionnaire_id  = qr.questionnaire_id
                    AND qc.questionnaire_version = qr.questionnaire_version
                JOIN `{rdr_dataset}.rdr_questionnaire_history` qh
                    ON qh.questionnaire_id = qr.questionnaire_id
                    AND qh.version         = qr.questionnaire_version
                JOIN `{rdr_dataset}.rdr_code` mc
                    ON mc.code_id = qc.code_id
                JOIN `{rdr_dataset}.rdr_code` qc_code
                    ON qc_code.code_id = qq.code_id
                LEFT JOIN `{rdr_dataset}.rdr_code` ac
                    ON ac.code_id = qra.value_code_id
                WHERE (
                    -- At least one answer value must be present
                    (qra.value_code_id IS NOT NULL AND ac.code_id IS NOT NULL)
                    OR qra.value_integer  IS NOT NULL
                    OR qra.value_decimal  IS NOT NULL
                    OR qra.value_boolean  IS NOT NULL
                    OR qra.value_date     IS NOT NULL
                    OR qra.value_datetime IS NOT NULL
                    OR qra.value_string   IS NOT NULL
                )
                AND qr.status != 0
                AND qr.classification_type NOT IN (1, 6, 2)
                -- Exclude module / question / answer codes flagged in cdr_excluded_code
                AND qc.code_id NOT IN (
                    SELECT code_id FROM `{rdr_dataset}.rdr_cdr_excluded_code` WHERE code_type = 1
                )
                AND qq.code_id NOT IN (
                    SELECT code_id FROM `{rdr_dataset}.rdr_cdr_excluded_code` WHERE code_type = 2
                )
                AND (
                    qra.value_code_id IS NULL
                    OR qra.value_code_id NOT IN (
                        SELECT code_id FROM `{rdr_dataset}.rdr_cdr_excluded_code` WHERE code_type = 3
                    )
                )
                AND mc.system = 'http://terminology.pmi-ops.org/CodeSystem/ppi'
                AND p.participant_id IN (
                    SELECT participant_id FROM `{dataset_id}.participant_filter`
                )
                {cutoff_authored_filter}
                {survey_filter}
            ),
            -- ------------------------------------------------------------------
            -- Part 1 helper: latest response per participant+module
            -- (used for all modules except ConsentPII)
            -- ------------------------------------------------------------------
            latest_response_per_module AS (
                SELECT participant_id, survey, response_id
                FROM (
                    SELECT
                        participant_id,
                        survey,
                        response_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY participant_id, survey
                            ORDER BY authored DESC, created DESC
                        ) AS rn
                    FROM `{dataset_id}.questionnaire_answers_by_module`
                )
                WHERE rn = 1
            ),
            -- ------------------------------------------------------------------
            -- Part 2 helpers for ConsentPII rolled-up logic
            -- ------------------------------------------------------------------
            -- For non-StreetAddress2 ConsentPII questions: latest answer per question
            rolled_up_latest_per_question AS (
                SELECT participant_id, survey, question_code_id, response_id
                FROM (
                    SELECT
                        participant_id,
                        survey,
                        question_code_id,
                        response_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY participant_id, survey, question_code_id
                            ORDER BY authored DESC, created DESC
                        ) AS rn
                    FROM `{dataset_id}.questionnaire_answers_by_module`
                    WHERE survey = 'ConsentPII'
                    AND question_code_id NOT IN (
                        SELECT code_id
                        FROM `{rdr_dataset}.rdr_code`
                        WHERE value = 'PIIAddress_StreetAddress2'
                    )
                )
                WHERE rn = 1
            ),
            -- StreetAddress2 is stale when a newer StreetAddress1 response exists.
            -- Find the most-recent ConsentPII response that answered SA1 or SA2.
            latest_sa_response AS (
                SELECT participant_id, response_id
                FROM (
                    SELECT DISTINCT participant_id, response_id, authored, created,
                        ROW_NUMBER() OVER (
                            PARTITION BY participant_id
                            ORDER BY authored DESC, created DESC
                        ) AS rn
                    FROM `{dataset_id}.questionnaire_answers_by_module`
                    WHERE survey = 'ConsentPII'
                    AND question_code_id IN (
                        SELECT code_id
                        FROM `{rdr_dataset}.rdr_code`
                        WHERE value IN (
                            'PIIAddress_StreetAddress',
                            'PIIAddress_StreetAddress2'
                        )
                    )
                )
                WHERE rn = 1
            ),
            sa2_latest AS (
                SELECT qabm.participant_id, qabm.survey,
                       qabm.question_code_id, qabm.response_id
                FROM `{dataset_id}.questionnaire_answers_by_module` qabm
                JOIN latest_sa_response lsr
                    ON qabm.participant_id = lsr.participant_id
                    AND qabm.response_id   = lsr.response_id
                WHERE qabm.survey = 'ConsentPII'
                AND qabm.question_code_id IN (
                    SELECT code_id
                    FROM `{rdr_dataset}.rdr_code`
                    WHERE value = 'PIIAddress_StreetAddress2'
                )
            ),
            -- Combined ConsentPII latest response per question (regular + SA2)
            latest_consent_per_question AS (
                SELECT participant_id, survey, question_code_id, response_id
                FROM rolled_up_latest_per_question
                UNION ALL
                SELECT participant_id, survey, question_code_id, response_id
                FROM sa2_latest
            )
            -- ==================================================================
            -- Part 1: answers from the latest response for each non-ConsentPII module
            -- ==================================================================
            SELECT
                ba.participant_id, research_id, external_id,
                survey_name, date_of_survey, question_ppi_code, question_code_id,
                value_ppi_code, topic_value, is_invalid, value_code_id,
                value_number, value_boolean, value_date, value_string,
                questionnaire_response_id, unit_id, filter, src_id
            FROM base_answers ba
            JOIN latest_response_per_module lr
                ON ba.participant_id              = lr.participant_id
                AND ba.survey_name                = lr.survey
                AND ba.questionnaire_response_id  = lr.response_id
            WHERE ba._module_value != 'ConsentPII'

            UNION ALL

            -- ==================================================================
            -- Part 2: rolled-up latest answers per question for ConsentPII
            -- ==================================================================
            SELECT
                ba.participant_id, research_id, external_id,
                survey_name, date_of_survey, question_ppi_code, ba.question_code_id,
                value_ppi_code, topic_value, is_invalid, value_code_id,
                value_number, value_boolean, value_date, value_string,
                questionnaire_response_id, unit_id, filter, src_id
            FROM base_answers ba
            JOIN latest_consent_per_question lcq
                ON ba.participant_id              = lcq.participant_id
                AND ba.survey_name                = lcq.survey
                AND ba.question_code_id           = lcq.question_code_id
                AND ba.questionnaire_response_id  = lcq.response_id
            WHERE ba._module_value = 'ConsentPII'
        """,
    },
    "src_meas": {
        # Replicates _populate_measurements() from curation.py.
        # Reads physical measurements from the replicated RDR tables.
        # Physical measurement status NO_USE = 2; collect_type REMOTE = 2.
        "destination": "src_meas",
        "append": False,
        "query": """
            SELECT
                CAST(
                    ROW_NUMBER() OVER (
                        ORDER BY pm.participant_id, pm.physical_measurements_id, meas.measurement_id
                    ) AS INT64
                )                               AS id,
                pm.participant_id               AS participant_id,
                pm.finalized_site_id            AS finalized_site_id,
                meas.code_value                 AS code_value,
                meas.measurement_time           AS measurement_time,
                meas.value_decimal              AS value_decimal,
                meas.value_unit                 AS value_unit,
                meas.value_code_value           AS value_code_value,
                LEFT(meas.value_string, 1024)   AS value_string,
                meas.measurement_id             AS measurement_id,
                pm.physical_measurements_id     AS physical_measurements_id,
                meas.parent_id                  AS parent_id,
                CASE
                    WHEN pm.origin = 'hpro'         THEN 'healthpro'
                    WHEN pm.origin = 'vibrent'       THEN 'vibrent'
                    WHEN pm.origin = 'careevolution' THEN 'ce'
                    ELSE pm.origin
                END                             AS src_id,
                pm.collect_type                 AS collect_type
            FROM `{rdr_dataset}.rdr_measurement` meas
            JOIN `{rdr_dataset}.rdr_physical_measurements` pm
                ON meas.physical_measurements_id = pm.physical_measurements_id
                AND pm.final = 1
                AND (pm.status != 2 OR pm.status IS NULL)
                {pm_collect_type_filter}
                {cutoff_finalized_filter}
            WHERE pm.participant_id IN (
                SELECT participant_id FROM `{dataset_id}.participant_filter`
            )
        """,
    },
    # ---------------------------------------------------------------------------
    # Phase 2 – Replace EXTERNAL_QUERY bridges with native BQ SQL
    # ---------------------------------------------------------------------------
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
                INNER JOIN `{rdr_dataset}.rdr_measurement_to_qualifier` mtq ON mtq.qualifier_id = cdm_obs.meas_id
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
                INNER JOIN `{rdr_dataset}.rdr_measurement_to_qualifier` mtq ON mtq.qualifier_id = cdm_obs.meas_id""",
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
                INNER JOIN `{etl_filters}.source_to_concept_map` stcm1 ON src_m.value_ppi_code = stcm1.source_code
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
                INNER JOIN `{etl_filters}.source_to_concept_map` stcm1 ON src_m.value_ppi_code = stcm1.source_code
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
              `{etl_filters}.source_to_concept_map` stcm1
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
              `{etl_filters}.source_to_concept_map` stcm1
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
              CAST(NULL AS STRING) AS place_of_service_source_value,
              'vibrent' AS src_id
            FROM
              `{rdr_dataset}.rdr_site` site""",
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
             location_source_value,
             src_id
            FROM (
               SELECT
               DISTINCT
               src.address_1 AS address_1,
               src.address_2 AS address_2,
               src.city AS city,
               src.state AS state,
               src.zip AS zip,
               NULL AS county,
               src.state_ppi_code AS location_source_value,
               src.src_id AS src_id
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
              meas.id as note_id,
              meas.participant_id AS person_id,
              DATE(meas.measurement_time) AS note_date,
              CAST(meas.measurement_time AS TIMESTAMP) AS note_datetime,
              44814645 AS note_type_concept_id,
              -- 44814645 - 'Note'
              0 AS note_class_concept_id,
              'Additional notes' AS note_title,
              COALESCE(meas.value_string, '') AS note_text,
              0 AS encoding_concept_id,
              4180186 AS language_concept_id,
              -- 4180186 - 'English language'
              CAST(NULL AS INTEGER) AS provider_id,
              meas.physical_measurements_id AS visit_occurrence_id,
              CAST(NULL AS INTEGER) AS visit_detail_id,
              meas.code_value AS note_source_value,
              'note' AS unit_id,
              meas.src_id AS src_id
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
              `{etl_filters}.source_to_concept_map` stcm1
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
               CAST(src_c.date_of_survey AS TIMESTAMP)
                                                   AS date_of_survey,
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
               AND vc1.vocabulary_id in ('PPI', 'AoU_Custom')
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
               AND vc3.vocabulary_id in ('PPI', 'AoU_Custom')
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
              CAST(meas.measurement_time AS TIMESTAMP) AS measurement_time,
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
            WITH latest_address AS (
              SELECT participant_id, date_of_survey,
                     ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY date_of_survey DESC) AS rn
              FROM `{dataset_id}.src_mapped`
              WHERE question_ppi_code = 'PIIAddress_StreetAddress'
            )
            SELECT
              p.participant_id AS participant_id,
              MAX(m_address_1.value_string) AS address_1,
              MAX(m_address_2.value_string) AS address_2,
              MAX(m_city.value_string) AS city,
              MAX(m_zip.value_string) AS zip,
              MAX(m_state.value_ppi_code) AS state_ppi_code,
              MAX(RIGHT(m_state.value_ppi_code, 2)) AS state,
              NULL AS location_id,
              p.src_id AS src_id
            FROM
              `{dataset_id}.src_participant` p
            INNER JOIN
              `{dataset_id}.src_mapped` m_address_1
            ON
              p.participant_id = m_address_1.participant_id
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
            JOIN latest_address la ON m_address_1.participant_id = la.participant_id AND la.rn = 1
            WHERE m_address_1.date_of_survey = la.date_of_survey
            GROUP BY
              p.participant_id, p.src_id;""",
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
              CAST(src.visit_start_datetime AS TIMESTAMP) AS visit_start_datetime,
              DATE(src.visit_end_datetime) AS visit_end_date,
              CAST(src.visit_end_datetime AS TIMESTAMP) AS visit_end_datetime,
              44818519 AS visit_type_concept_id,
              -- 44818519 - 'Clinical Study Visit'
              NULL AS provider_id,
              src.care_site_id AS care_site_id,
              src.visit_occurrence_id AS visit_source_value,
              0 AS visit_source_concept_id,
              0 AS admitting_source_concept_id,
              CAST(NULL AS STRING) AS admitting_source_value,
              0 AS discharge_to_concept_id,
              CAST(NULL AS STRING) AS discharge_to_source_value,
              NULL AS preceding_visit_occurrence_id,
              src.src_id AS src_id
            FROM
              `{dataset_id}.tmp_visits_src` src""",
        "destination": "visit_occurrence",
        "append": False,
    },
    "observation": {
        "query": """
            SELECT ROW_NUMBER() OVER() AS observation_id, obs.* FROM (
                SELECT
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
                  NULL AS meas_id,
                  src_m.src_id AS src_id
                FROM
                  `{dataset_id}.src_mapped` src_m
                WHERE
                  src_m.question_ppi_code IS NOT null
                  UNION ALL
                  SELECT
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
                  meas.measurement_id AS meas_id,
                  meas.src_id AS src_id
                FROM
                  `{dataset_id}.src_meas_mapped` meas
                WHERE
                  meas.cv_domain_id = 'Observation'
              ) obs""",
        "destination": "observation",
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
                `{etl_filters}.combined_question_filter`)""",
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
                `{etl_filters}.combined_survey_filter`)""",
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
              AND IFNULL(person_loc.zip, '') = IFNULL(loc.zip, '')
              AND IFNULL(person_loc.src_id, '') = IFNULL(loc.src_id, '')""",
        "destination": None,
    },
    "death": {
        "destination": "death",
        "append": False,
        "query": """
            SELECT
              dr.participant_id AS person_id,
              dr.date_of_death AS death_date,
              CAST(dr.date_of_death AS TIMESTAMP) AS death_datetime,
              32809 AS death_type_concept_id,
              CAST(NULL AS INT64) AS cause_concept_id,
              CAST(NULL AS STRING) AS cause_source_value,
              CAST(NULL AS INT64) AS cause_source_concept_id,
              'healthpro' AS src_id
            FROM
              `{rdr_dataset}.rdr_deceased_report` dr
            JOIN
              `{dataset_id}.person` per
            ON
              dr.participant_id = per.person_id
            WHERE
              dr.status = 2
            {cutoff_death_filter}""",
    },
    "ehr_consent_temp_table": {
        # Replaces EXTERNAL_QUERY against Cloud SQL with native BQ SQL on replicated tables.
        # ConsentSyncStatus: READY_FOR_SYNC=2, SYNC_COMPLETE=4
        "destination": "tmp_ehr_consent",
        "append": False,
        "query": """
            SELECT
                sc.participant_id,
                sc.research_id,
                sc.value_ppi_code,
                sc.date_of_survey,
                sc.src_id,
                cf.created  AS cf_created,
                cf.sync_status,
                cr.created  AS cr_created
            FROM `{dataset_id}.src_clean` sc
            LEFT JOIN `{rdr_dataset}.rdr_consent_response` cr
                ON cr.questionnaire_response_id = sc.questionnaire_response_id
            LEFT JOIN `{rdr_dataset}.rdr_consent_file` cf
                ON cf.consent_response_id = cr.id
            WHERE sc.survey_name      = 'EHRConsentPII'
            AND   sc.question_ppi_code = 'EHRConsentPII_ConsentPermission'
        """,
    },
    "ehr_consent": {
        "destination": "consent",
        "append": False,
        "query": """
            SELECT ec.participant_id AS person_id,
                ec.research_id,
                CASE
                    WHEN ec.value_ppi_code IN ('No', 'ConsentPermission_No')
                        THEN 'SUBMITTED_NO'
                    WHEN ec.value_ppi_code IN ('Yes', 'ConsentPermission_Yes')
                         AND ec.cr_created IS NULL
                        THEN 'SUBMITTED'
                    WHEN ec.value_ppi_code IN ('Yes', 'ConsentPermission_Yes')
                         AND ec.cr_created IS NOT NULL
                         AND ec.cf_created IS NULL
                        THEN 'SUBMITTED_NOT_VALIDATED'
                    WHEN ec.value_ppi_code IN ('Yes', 'ConsentPermission_Yes')
                         {ehr_consent_cutoff_not_validated_filter}
                        THEN 'SUBMITTED_NOT_VALIDATED'
                    WHEN ec.value_ppi_code IN ('Yes', 'ConsentPermission_Yes')
                         AND ec.sync_status IN (2, 4)
                        THEN 'SUBMITTED'
                    WHEN ec.value_ppi_code IN ('Yes', 'ConsentPermission_Yes')
                         AND ec.sync_status NOT IN (2, 4)
                        THEN 'SUBMITTED_INVALID'
                END AS consent_for_electronic_health_records,
                ec.date_of_survey AS consent_for_electronic_health_records_authored,
                ec.src_id
            FROM `{dataset_id}.tmp_ehr_consent` ec
        """,
    },
    "wear_consent": {
        # Replaces EXTERNAL_QUERY against Cloud SQL with native BQ SQL on replicated tables.
        "destination": "wear_consent",
        "append": False,
        "query": """
            SELECT
                pa.participant_id AS person_id,
                pa.research_id,
                qr.authored,
                ac.value          AS consent_status,
                CASE
                    WHEN pa.participant_origin = 'careevolution' THEN 'ce'
                    ELSE pa.participant_origin
                END               AS src_id
            FROM `{rdr_dataset}.rdr_participant` pa
            JOIN `{rdr_dataset}.rdr_questionnaire_response` qr
                ON pa.participant_id = qr.participant_id
            JOIN `{rdr_dataset}.rdr_questionnaire_response_answer` qra
                ON qr.questionnaire_response_id = qra.questionnaire_response_id
            JOIN `{rdr_dataset}.rdr_questionnaire_question` qq
                ON qra.question_id = qq.questionnaire_question_id
            JOIN `{rdr_dataset}.rdr_code` qcd
                ON qq.code_id = qcd.code_id
            LEFT JOIN `{rdr_dataset}.rdr_code` ac
                ON qra.value_code_id = ac.code_id
            JOIN `{rdr_dataset}.rdr_questionnaire` q
                ON qr.questionnaire_id = q.questionnaire_id
            JOIN `{rdr_dataset}.rdr_questionnaire_concept` qc
                ON q.questionnaire_id = qc.questionnaire_id
                AND q.version         = qc.questionnaire_version
            JOIN `{rdr_dataset}.rdr_code` cc
                ON qc.code_id = cc.code_id
            WHERE ac.value IS NOT NULL
            AND   cc.value   = 'wear_consent'
            AND   qcd.value  = 'resultsconsent_wear'
            AND pa.participant_id IN (
                SELECT DISTINCT participant_id FROM `{dataset_id}.src_clean`
            )
            ORDER BY pa.participant_id, qr.authored
        """,
    },
    "participant_id_mapping": {
        # Replaces EXTERNAL_QUERY against Cloud SQL with native BQ SQL on replicated tables.
        "destination": "participant_id_mapping",
        "append": False,
        "query": """
            SELECT pid_map.p_id, pid_map.id_source, pid_map.id_value, pid_map.src_id
            FROM (
                SELECT
                    participant_id  AS p_id,
                    'r_id'          AS id_source,
                    research_id     AS id_value,
                    participant_origin AS src_id
                FROM `{rdr_dataset}.rdr_participant`
                WHERE research_id IS NOT NULL
                UNION ALL
                SELECT
                    participant_id  AS p_id,
                    'vibrent_id'    AS id_source,
                    external_id     AS id_value,
                    participant_origin AS src_id
                FROM `{rdr_dataset}.rdr_participant`
                WHERE external_id IS NOT NULL
            ) AS pid_map
        """,
    },
    "finalize": {
        "destination": None,
        "append": False,
        "query": """
            ALTER TABLE `{dataset_id}.measurement` DROP COLUMN parent_id;
            ALTER TABLE `{dataset_id}.observation` DROP COLUMN meas_id;
            ALTER TABLE `{dataset_id}.questionnaire_response_additional_info` DROP COLUMN id;
        """
    },
    "qrai_author": {
        # Replaces EXTERNAL_QUERY against Cloud SQL with native BQ SQL on replicated tables.
        "destination": "questionnaire_response_additional_info",
        "append": False,
        "query": """
            SELECT DISTINCT
                0 AS id,
                qr.questionnaire_response_id,
                'NON_PARTICIPANT_AUTHOR_INDICATOR' AS type,
                qr.non_participant_author          AS value,
                CASE
                    WHEN p.participant_origin = 'careevolution' THEN 'ce'
                    ELSE p.participant_origin
                END AS src_id
            FROM `{rdr_dataset}.rdr_questionnaire_response` qr
            JOIN (
                SELECT DISTINCT questionnaire_response_id
                FROM `{dataset_id}.src_clean`
            ) qri ON qr.questionnaire_response_id = qri.questionnaire_response_id
            JOIN `{rdr_dataset}.rdr_participant` p
                ON qr.participant_id = p.participant_id
            WHERE qr.non_participant_author IS NOT NULL
        """,
    },
    "qrai_language": {
        # Replaces EXTERNAL_QUERY against Cloud SQL with native BQ SQL on replicated tables.
        "destination": "questionnaire_response_additional_info",
        "append": True,
        "query": """
            SELECT DISTINCT
                0 AS id,
                qr.questionnaire_response_id,
                'LANGUAGE'    AS type,
                qr.language   AS value,
                CASE
                    WHEN p.participant_origin = 'careevolution' THEN 'ce'
                    ELSE p.participant_origin
                END AS src_id
            FROM `{rdr_dataset}.rdr_questionnaire_response` qr
            JOIN (
                SELECT DISTINCT questionnaire_response_id
                FROM `{dataset_id}.src_clean`
            ) qri ON qr.questionnaire_response_id = qri.questionnaire_response_id
            JOIN `{rdr_dataset}.rdr_participant` p
                ON qr.participant_id = p.participant_id
            WHERE qr.language IS NOT NULL
        """,
    },
    "qrai_code": {
        # Replaces EXTERNAL_QUERY against Cloud SQL with native BQ SQL on replicated tables.
        "destination": "questionnaire_response_additional_info",
        "append": True,
        "query": """
            SELECT DISTINCT
                0 AS id,
                qr.questionnaire_response_id,
                'CODE'  AS type,
                c.value AS value,
                CASE
                    WHEN p.participant_origin = 'careevolution' THEN 'ce'
                    ELSE p.participant_origin
                END AS src_id
            FROM `{rdr_dataset}.rdr_questionnaire_response` qr
            JOIN `{rdr_dataset}.rdr_questionnaire_concept` qc
                ON qr.questionnaire_id      = qc.questionnaire_id
                AND qr.questionnaire_version = qc.questionnaire_version
            JOIN `{rdr_dataset}.rdr_code` c
                ON qc.code_id = c.code_id
            JOIN (
                SELECT DISTINCT questionnaire_response_id
                FROM `{dataset_id}.src_clean`
            ) qri ON qr.questionnaire_response_id = qri.questionnaire_response_id
            JOIN `{rdr_dataset}.rdr_participant` p
                ON qr.participant_id = p.participant_id
        """,
    },
    "tmp_survey_conduct": {
        # Replaces EXTERNAL_QUERY against Cloud SQL with native BQ SQL on replicated tables.
        "destination": "tmp_survey_conduct",
        "append": False,
        "query": """
            SELECT
                qr.questionnaire_response_id,
                qr.non_participant_author,
                qr.authored,
                qr.participant_id,
                mc.value,
                mc.code_id
            FROM `{rdr_dataset}.rdr_questionnaire_response` qr
            JOIN `{rdr_dataset}.rdr_questionnaire_concept` qc
                ON qc.questionnaire_id      = qr.questionnaire_id
                AND qc.questionnaire_version = qr.questionnaire_version
            JOIN `{rdr_dataset}.rdr_code` mc
                ON mc.code_id = qc.code_id
            WHERE qr.questionnaire_response_id IN (
                SELECT DISTINCT sc.questionnaire_response_id
                FROM `{dataset_id}.src_clean` sc
            )
        """,
    },
    "survey_conduct": {
        "destination": "survey_conduct",
        "append": False,
        "query": """
                SELECT tsc.questionnaire_response_id survey_conduct_id,
                        tsc.participant_id person_id,
                        CASE
                            WHEN tsc.value = 'wear_consent' AND p.src_id = 'ce' THEN 2100000011
                            -- Code for CE WEAR consent
                            WHEN tsc.value = 'wear_consent' AND p.src_id = 'vibrent' THEN 2100000012
                            -- Code for PTSC WEAR consent
                            ELSE COALESCE(voc_c.concept_id, 0)
                        END survey_concept_id,
                        CAST(NULL AS DATE) survey_start_date,
                        CAST(NULL AS TIMESTAMP) survey_start_datetime,
                        DATE(tsc.authored) survey_end_date,
                        CAST(tsc.authored AS TIMESTAMP) survey_end_datetime,
                        0 provider_id,
                        CASE WHEN
                            tsc.non_participant_author = 'CATI' THEN     42530794
                            ELSE                                        0
                        END assisted_concept_id,
                        CAST(0 AS INT64) respondent_type_concept_id,
                        0 timing_concept_id,
                        CASE WHEN
                            tsc.non_participant_author = 'CATI' THEN     42530794
                            ELSE                                        42531021
                        END collection_method_concept_id,
                        CASE WHEN
                            tsc.non_participant_author = 'CATI' THEN     'Telephone'
                            ELSE                                        'No matching concept'
                        END assisted_source_value,
                        CAST(NULL AS STRING) respondent_type_source_value,
                        '' timing_source_value,
                        CASE WHEN
                            tsc.non_participant_author = 'CATI' THEN     'Telephone'
                            ELSE                                        'Electronic'
                        END collection_method_source_value,
                        tsc.value survey_source_value,
                        tsc.code_id survey_source_concept_id,
                        CAST(tsc.questionnaire_response_id AS STRING) survey_source_identifier,
                        0 validated_survey_concept_id,
                        CAST(NULL AS STRING) validated_survey_source_value,
                        CAST(NULL AS STRING) survey_version_number,
                        CAST(NULL AS INT64) visit_occurrence_id,
                        CAST(NULL AS INT64) response_visit_occurrence_id,
                        CASE WHEN
                            p.src_id = 'careevolution' THEN 'ce'
                            ELSE p.src_id
                        END src_id
                FROM `{dataset_id}.tmp_survey_conduct` tsc
                LEFT JOIN `{dataset_id}.concept` voc_c
                    ON voc_c.concept_code = tsc.value AND voc_c.vocabulary_id = 'PPI'
                    AND voc_c.domain_id = 'Observation' AND voc_c.concept_class_id = 'Module'
                JOIN `{dataset_id}.person` p ON tsc.participant_id = p.person_id
                WHERE tsc.questionnaire_response_id in (
                    SELECT DISTINCT sc.questionnaire_response_id
                    FROM `{dataset_id}.src_clean` sc
                    WHERE sc.filter = 0
                )
            """
    },
    "create_empty_tables": {
        "destination": None,
        "append": False,
        "query": """CREATE TABLE IF NOT EXISTS `{dataset_id}.drug_era`
                    (
                      drug_era_id INT64,
                      person_id INT64,
                      drug_concept_id INT64,
                      drug_era_start_date TIMESTAMP,
                      drug_era_end_date TIMESTAMP,
                      drug_exposure_count INT64,
                      gap_days INT64,
                      src_id STRING
                    )
                    DEFAULT COLLATE 'und:ci';
                    CREATE TABLE IF NOT EXISTS `{dataset_id}.condition_era`
                    (
                      condition_era_id INT64,
                      person_id INT64,
                      condition_concept_id INT64,
                      condition_era_start_date TIMESTAMP,
                      condition_era_end_date TIMESTAMP,
                      condition_occurrence_count INT64,
                      src_id STRING
                    )
                    DEFAULT COLLATE 'und:ci';
                    CREATE TABLE IF NOT EXISTS `{dataset_id}.note_nlp`
                    (
                      note_nlp_id INT64,
                      note_id INT64,
                      section_concept_id INT64,
                      snippet STRING,
                      offset STRING,
                      lexical_variant STRING,
                      note_nlp_concept_id INT64,
                      note_nlp_source_concept_id INT64,
                      nlp_system STRING,
                      nlp_date DATE,
                      nlp_datetime TIMESTAMP,
                      term_exists STRING,
                      term_temporal STRING,
                      term_modifiers STRING,
                      src_id STRING
                    )
                    DEFAULT COLLATE 'und:ci';
                    CREATE TABLE IF NOT EXISTS `{dataset_id}.metadata`
                    (
                      metadata_concept_id INT64,
                      metadata_type_concept_id INT64,
                      name STRING,
                      value_as_string STRING,
                      value_as_concept_id INT64,
                      metadata_date DATE,
                      metadata_datetime TIMESTAMP,
                      src_id STRING
                    )
                    DEFAULT COLLATE 'und:ci';
                    CREATE TABLE IF NOT EXISTS `{dataset_id}.provider`
                    (
                      provider_id INT64,
                      provider_name STRING,
                      npi STRING,
                      dea STRING,
                      specialty_concept_id INT64,
                      care_site_id INT64,
                      year_of_birth INT64,
                      gender_concept_id INT64,
                      provider_source_value STRING,
                      specialty_source_value STRING,
                      specialty_source_concept_id INT64,
                      gender_source_value STRING,
                      gender_source_concept_id INT64,
                      src_id STRING
                    )
                    DEFAULT COLLATE 'und:ci';
                    CREATE TABLE IF NOT EXISTS `{dataset_id}.device_exposure`
                    (
                      device_exposure_id INT64,
                      person_id INT64,
                      device_concept_id INT64,
                      device_exposure_start_date DATE,
                      device_exposure_start_datetime TIMESTAMP,
                      device_exposure_end_date DATE,
                      device_exposure_end_datetime TIMESTAMP,
                      device_type_concept_id INT64,
                      unique_device_id STRING,
                      quantity INTEGER,
                      provider_id INT64,
                      visit_occurrence_id INT64,
                      visit_detail_id INT64,
                      device_source_value STRING,
                      device_source_concept_id INT64,
                      src_id STRING
                    )
                    DEFAULT COLLATE 'und:ci';
                    CREATE TABLE IF NOT EXISTS `{dataset_id}.payer_plan_period`
                    (
                      payer_plan_period_id INT64,
                      person_id INT64,
                      payer_plan_period_start_date DATE,
                      payer_plan_period_end_date DATE,
                      payer_concept_id INT64,
                      payer_source_value STRING,
                      payer_source_concept_id INT64,
                      plan_concept_id INT64,
                      plan_source_value STRING,
                      plan_source_concept_id INT64,
                      sponsor_source_value STRING,
                      sponsor_concept_id INT64,
                      sponsor_source_concept_id INT64,
                      family_source_value STRING,
                      stop_reason_concept_id INT64,
                      stop_reason_source_value STRING,
                      stop_reason_source_concept_id INT64,
                      src_id STRING
                    )
                    DEFAULT COLLATE 'und:ci';
                    CREATE TABLE IF NOT EXISTS `{dataset_id}.condition_occurrence`
                    (
                      condition_occurrence_id INT64,
                      person_id INT64,
                      condition_concept_id INT64,
                      condition_start_date DATE,
                      condition_start_datetime TIMESTAMP,
                      condition_end_date DATE,
                      condition_end_datetime TIMESTAMP,
                      condition_type_concept_id INT64,
                      condition_status_concept_id INT64,
                      stop_reason STRING,
                      provider_id INT64,
                      visit_occurrence_id INT64,
                      visit_detail_id INT64,
                      condition_source_value STRING,
                      condition_source_concept_id INT64,
                      condition_status_source_value STRING,
                      src_id STRING
                    )
                    DEFAULT COLLATE 'und:ci';
                    CREATE TABLE IF NOT EXISTS `{dataset_id}.drug_exposure`
                    (
                      drug_exposure_id INT64,
                      person_id INT64,
                      drug_concept_id INT64,
                      drug_exposure_start_date DATE,
                      drug_exposure_start_datetime TIMESTAMP,
                      drug_exposure_end_date DATE,
                      drug_exposure_end_datetime TIMESTAMP,
                      verbatim_end_date DATE,
                      drug_type_concept_id INT64,
                      stop_reason STRING,
                      refills INT64,
                      quantity NUMERIC,
                      days_supply INT64,
                      sig STRING,
                      route_concept_id INT64,
                      lot_number STRING,
                      provider_id INT64,
                      visit_occurrence_id INT64,
                      visit_detail_id INT64,
                      drug_source_value STRING,
                      drug_source_concept_id INT64,
                      route_source_value STRING,
                      dose_unit_source_value STRING
                    )
                    DEFAULT COLLATE 'und:ci';
                    CREATE TABLE IF NOT EXISTS `{dataset_id}.cost`
                    (
                      cost_id INT64,
                      cost_event_id INT64,
                      cost_domain_id STRING,
                      cost_type_concept_id INT64,
                      currency_concept_id INT64,
                      total_charge NUMERIC,
                      total_cost NUMERIC,
                      total_paid NUMERIC,
                      paid_by_payer NUMERIC,
                      paid_by_patient NUMERIC,
                      paid_patient_copay NUMERIC,
                      paid_patient_coinsurance NUMERIC,
                      paid_patient_deductible NUMERIC,
                      paid_by_primary NUMERIC,
                      paid_ingredient_cost NUMERIC,
                      paid_dispensing_fee NUMERIC,
                      payer_plan_period_id INT64,
                      amount_allowed NUMERIC,
                      revenue_code_concept_id INT64,
                      revenue_code_source_value STRING,
                      drg_concept_id INT64,
                      drg_source_value STRING,
                      src_id STRING
                    )
                    DEFAULT COLLATE 'und:ci';
                    CREATE TABLE IF NOT EXISTS `{dataset_id}.dose_era`
                    (
                      dose_era_id INT64,
                      person_id INT64,
                      drug_concept_id INT64,
                      unit_concept_id INT64,
                      dose_value NUMERIC,
                      dose_era_start_date DATE,
                      dose_era_end_date DATE,
                      src_id STRING
                    )
                    DEFAULT COLLATE 'und:ci';
                    CREATE TABLE IF NOT EXISTS `{dataset_id}.visit_detail`
                    (
                      visit_detail_id INT64,
                      person_id INT64,
                      visit_detail_concept_id INT64,
                      visit_detail_start_date DATE,
                      visit_detail_start_datetime TIMESTAMP,
                      visit_detail_end_date DATE,
                      visit_detail_end_datetime TIMESTAMP,
                      visit_detail_type_concept_id INT64,
                      provider_id INT64,
                      care_site_id INT64,
                      visit_detail_source_value STRING,
                      visit_detail_source_concept_id INT64,
                      admitting_source_value STRING,
                      admitting_source_concept_id INT64,
                      discharge_to_source_value STRING,
                      discharge_to_concept_id INT64,
                      preceding_visit_detail_id INT64,
                      visit_detail_parent_id INT64,
                      visit_occurrence_id INT64,
                      src_id STRING
                    )
                    DEFAULT COLLATE 'und:ci';
                    CREATE TABLE IF NOT EXISTS `{dataset_id}.specimen`
                    (
                      specimen_id INT64,
                      person_id INT64,
                      specimen_concept_id INT64,
                      specimen_type_concept_id INT64,
                      specimen_date DATE,
                      specimen_datetime TIMESTAMP,
                      quantity NUMERIC,
                      unit_concept_id INT64,
                      anatomic_site_concept_id INT64,
                      disease_status_concept_id INT64,
                      specimen_source_id STRING,
                      specimen_source_value STRING,
                      unit_source_value STRING,
                      anatomic_site_source_value STRING,
                      disease_status_source_value STRING,
                      src_id STRING,
                    )
                    DEFAULT COLLATE 'und:ci';
        """
    },
    "pid_rid_mapping": {
        "destination": "pid_rid_mapping",
        "append": False,
        "query": """SELECT DISTINCT sc.participant_id AS person_id, sc.research_id, sc.external_id AS vibrent_id,
                                    sc.src_id
                            FROM `{dataset_id}.src_clean` sc
                            join `{dataset_id}.person` p on sc.participant_id=p.person_id"""
    },
"cope_survey_semantic_version_map": {
        # Replaces EXTERNAL_QUERY against Cloud SQL with native BQ SQL on replicated tables.
        "destination": "cope_survey_semantic_version_map",
        "append": False,
        "query": """
            SELECT
                qr.participant_id,
                qr.questionnaire_response_id,
                qh.semantic_version,
                CASE
                    WHEN qh.external_id IN ('Vibrent_FORM_ID_1413', 'COPE Survey')
                        THEN 'may'
                    WHEN qh.external_id IN ('June COPE Survey', 'Vibrent_FORM_ID_1416')
                        THEN 'june'
                    WHEN qh.external_id IN ('July COPE Survey', 'Vibrent_FORM_ID_1424')
                        THEN 'july'
                    WHEN qh.external_id IN ('October COPE Survey', 'Vibrent_FORM_ID_1442')
                        THEN 'nov'
                    WHEN qh.external_id IN ('December COPE Survey', 'Vibrent_FORM_ID_1453')
                        THEN 'dec'
                    WHEN qh.external_id IN ('February COPE Survey', 'Vibrent_FORM_ID_1456')
                        THEN 'feb'
                    WHEN qh.external_id IN ('cope_vaccine1', 'Vibrent_FORM_ID_1502')
                        THEN 'vaccine1'
                    WHEN qh.external_id IN ('cope_vaccine2', 'Vibrent_FORM_ID_1513')
                        THEN 'vaccine2'
                    WHEN qh.external_id IN ('cope_vaccine3', 'Vibrent_FORM_ID_1535')
                        THEN 'vaccine3'
                    WHEN qh.external_id IN ('cope_vaccine4', 'Vibrent_FORM_ID_1545')
                        THEN 'vaccine4'
                END AS cope_month,
                CASE
                    WHEN p.participant_origin = 'careevolution' THEN 'ce'
                    ELSE p.participant_origin
                END AS src_id
            FROM `{rdr_dataset}.rdr_questionnaire_history` qh
            INNER JOIN `{rdr_dataset}.rdr_questionnaire_response` qr
                ON qr.questionnaire_id      = qh.questionnaire_id
                AND qr.questionnaire_version = qh.version
            JOIN `{rdr_dataset}.rdr_participant` p
                ON qr.participant_id = p.participant_id
            WHERE qh.external_id IN (
                'Vibrent_FORM_ID_1413', 'COPE Survey',
                'June COPE Survey',    'Vibrent_FORM_ID_1416',
                'July COPE Survey',    'Vibrent_FORM_ID_1424',
                'October COPE Survey', 'Vibrent_FORM_ID_1442',
                'December COPE Survey','Vibrent_FORM_ID_1453',
                'February COPE Survey','Vibrent_FORM_ID_1456',
                'cope_vaccine1',       'Vibrent_FORM_ID_1502',
                'cope_vaccine2',       'Vibrent_FORM_ID_1513',
                'cope_vaccine3',       'Vibrent_FORM_ID_1535',
                'cope_vaccine4',       'Vibrent_FORM_ID_1545'
            )
        """,
    },
    "procedure_occurrence": {
        "destination": "procedure_occurrence",
        "append": False,
        "query": """
                             SELECT ROW_NUMBER() OVER() AS procedure_occurrence_id, poid.* FROM (
                            SELECT
                                src_m1.participant_id                       AS person_id,
                                COALESCE(vc.concept_id, 0)                  AS procedure_concept_id,
                                src_m2.value_date                           AS procedure_date,
                                TIMESTAMP(src_m2.value_date)                AS procedure_datetime,
                                581412                                      AS procedure_type_concept_id,   -- 581412, Procedure Recorded from a Survey
                                0                                           AS modifier_concept_id,
                                NULL                                        AS quantity,
                                NULL                                        AS provider_id,
                                NULL                                        AS visit_occurrence_id,
                                NULL                                        AS visit_detail_id,
                                stcm.source_code                            AS procedure_source_value,
                                COALESCE(stcm.source_concept_id, 0)         AS procedure_source_concept_id,
                                NULL                                        AS modifier_source_value,
                                'procedure'                                 AS unit_id,
                                src_m1.src_id                               AS src_id
                            FROM `{dataset_id}.src_mapped` src_m1
                            INNER JOIN `{etl_filters}.source_to_concept_map` stcm
                                ON src_m1.value_ppi_code = stcm.source_code
                                AND stcm.source_vocabulary_id = 'ppi-proc'
                            INNER JOIN `{dataset_id}.src_mapped` src_m2
                                ON src_m1.participant_id = src_m2.participant_id
                                AND src_m2.question_ppi_code = 'OrganTransplant_Date'
                                AND src_m2.value_date IS NOT NULL
                            LEFT JOIN `{dataset_id}.concept` vc
                                ON stcm.target_concept_id = vc.concept_id
                                AND vc.standard_concept = 'S'
                                AND vc.invalid_reason IS NULL) poid"""
    },
    "temp_obs_target": {
        "destination": "temp_obs_target",
        "append": False,
        "query": """
        SELECT
          person_id,
          visit_start_date AS start_date,
          COALESCE(visit_end_date, visit_start_date) AS end_date
        FROM
          `{dataset_id}`.visit_occurrence
        UNION ALL
          -- CONDITION_OCCURRENCE
        SELECT
          person_id,
          condition_start_date AS start_date,
          COALESCE(condition_end_date, condition_start_date) AS end_date
        FROM
          `{dataset_id}`.condition_occurrence
        UNION ALL
          -- PROCEDURE_OCCURRENCE
        SELECT
          person_id,
          procedure_date AS start_date,
          procedure_date AS end_date
        FROM
          `{dataset_id}`.procedure_occurrence
        UNION ALL
          -- OBSERVATION
        SELECT
          person_id,
          observation_date AS start_date,
          observation_date AS end_date
        FROM
          `{dataset_id}`.observation
        UNION ALL
          -- MEASUREMENT
        SELECT
          person_id,
          measurement_date AS start_date,
          measurement_date AS end_date
        FROM
          `{dataset_id}`.measurement
        UNION ALL
          -- DEVICE_EXPOSURE
        SELECT
          person_id,
          device_exposure_start_date AS start_date,
          COALESCE( device_exposure_end_date, device_exposure_start_date) AS end_date
        FROM
          `{dataset_id}`.device_exposure
        UNION ALL
          -- DRUG_EXPOSURE
        SELECT
          person_id,
          drug_exposure_start_date AS start_date,
          COALESCE( drug_exposure_end_date, drug_exposure_start_date) AS end_date
        FROM
          `{dataset_id}`.drug_exposure
        """
    },
    "temp_obs": {
        "destination": "temp_obs",
        "append": False,
        "query": """
        SELECT DISTINCT
          person_id,
          start_date AS observation_start_date,
          (SELECT MIN(e) FROM UNNEST(ends) AS e WHERE e >= start_date) AS observation_end_date
        FROM (
          SELECT
            person_id,
            start_date,
            ARRAY_AGG(end_date + INTERVAL 1 DAY) OVER(PARTITION BY person_id ORDER BY start_date) AS ends
          FROM `{dataset_id}`.temp_obs_target
        )

        """
    },
    "observation_period": {
        "destination": "observation_period",
        "append": False,
        "query": """
            SELECT
              ROW_NUMBER() OVER(ORDER BY tobs.person_id) AS observation_period_id,
              tobs.person_id AS person_id,
              MIN(observation_start_date) AS observation_period_start_date,
              observation_end_date AS observation_period_end_date,
              44814725 AS period_type_concept_id,
              -- 44814725, Period inferred by algorithm
              'observ_period' AS unit_id,
              p.src_id AS src_id
            FROM
              `{dataset_id}`.temp_obs tobs
            JOIN
              `{dataset_id}`.person p
            ON
              tobs.person_id = p.person_id
            GROUP BY
              person_id,
              observation_end_date,
              src_id
        """
    },
    "empty_src_meas": {
        "destination": "src_meas",
        "append": False,
        "query": """
            SELECT
                CAST(NULL AS INT64) AS id,
                CAST(NULL AS INT64) AS participant_id,
                CAST(NULL AS INT64) AS finalized_site_id,
                CAST(NULL AS STRING) AS code_value,
                CAST(NULL AS DATETIME) AS measurement_time,
                CAST(NULL AS FLOAT64) AS value_decimal,
                CAST(NULL AS STRING) AS value_unit,
                CAST(NULL AS STRING) AS value_code_value,
                CAST(NULL AS STRING) AS value_string,
                CAST(NULL AS INT64) AS measurement_id,
                CAST(NULL AS INT64) AS physical_measurements_id,
                CAST(NULL AS INT64) AS parent_id,
                CAST(NULL AS STRING) AS src_id,
                CAST(NULL AS INT64) AS collect_type
            WHERE 1 = 0
        """
    }
}
