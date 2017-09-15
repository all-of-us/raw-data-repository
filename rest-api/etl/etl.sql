-- -------------------------------------------------------------------
-- @2015-2017, Odysseus Data Services, Inc. All rights reserved
-- PPI OMOP CDM Conversion
-- last updated September 13, 2017
--
-- script for ETL workflow, consisting of:
--
-- populating table cdm.src_clean (and aux tables)
-- populating table cdm.src_mapped (and aux tables)
--
-- populating table cdm.location
-- populating table cdm.person
-- populating table cdm.observation
-- populating table cdm.procedure_occurrence
--
-- post-conversion deduplication of event tables
--
-- populating table cdm_target.location
-- populating table cdm_target.person
-- populating table cdm_target.observation
-- populating table cdm_target.procedure_occurrence
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- add indexes to vocabulary tables
-- -------------------------------------------------------------------

ALTER TABLE voc.concept ADD PRIMARY KEY (concept_id);
ALTER TABLE voc.concept_relationship ADD KEY (concept_id_1, relationship_id);
ALTER TABLE voc.concept_relationship ADD KEY (concept_id_2);

-- -------------------------------------------------------------------
-- table: tmp_clean_all
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS cdm.tmp_clean_all;
CREATE TABLE cdm.tmp_clean_all
(
    participant_id              bigint,
    date_of_survey              datetime,
    question_ppi_code           varchar(200),
    question_code_id            bigint,
    value_ppi_code              varchar(200),
    topic_value                 varchar(200),
    value_code_id               bigint,
    value_number                decimal(20,6),
    value_boolean               tinyint,
    value_date                  datetime,
    value_string                varchar(1024),
    questionnaire_response_id   bigint,
    unit_id                     varchar(50)
);

-- -------------------------------------------------------------------
-- Rules all together
-- -------------------------------------------------------------------

INSERT INTO cdm.tmp_clean_all
SELECT
    pa.participant_id               AS participant_id,
    qr.created                      AS date_of_survey,
    co_q.value                      AS question_ppi_code,
    qq.code_id                      AS question_code_id,
    co_a.value                      AS value_ppi_code,
    co_a.topic                      AS topic_value,
    qra.value_code_id               AS value_code_id,
    COALESCE(
        qra.value_decimal,
        qra.value_integer)          AS value_number,
    qra.value_boolean               AS value_boolean,
    COALESCE(
        qra.value_date,
        qra.value_datetime)         AS value_date,
    COALESCE(
        qra.value_string,
        qra.value_date,
        qra.value_datetime,
        co_a.display)               AS value_string,
    qr.questionnaire_response_id    AS questionnaire_response_id,
    CONCAT('cln.', 
        CASE 
            WHEN qra.value_code_id IS NOT NULL THEN 'code'
            WHEN qra.value_integer IS NOT NULL THEN 'int'
            WHEN qra.value_decimal IS NOT NULL THEN 'dec'
            WHEN qra.value_boolean IS NOT NULL THEN 'bool'
            WHEN qra.value_date IS NOT NULL THEN 'date'
            WHEN qra.value_datetime IS NOT NULL THEN 'dtime'
            WHEN qra.value_string IS NOT NULL THEN 'str'
            ELSE ''
        END)                        AS unit_id

FROM rdr.participant pa
JOIN rdr.hpo hp
    ON  pa.hpo_id = hp.hpo_id
JOIN rdr.questionnaire_response qr
    ON  qr.participant_id = pa.participant_id
JOIN rdr.questionnaire_response_answer qra
    ON  qra.questionnaire_response_id = qr.questionnaire_response_id
JOIN rdr.questionnaire_question qq
    ON qra.question_id = qq.questionnaire_question_id
JOIN rdr.code co_q
    ON  qq.code_id = co_q.code_id
LEFT JOIN rdr.code co_a
    ON  qra.value_code_id = co_a.code_id
WHERE
    pa.withdrawal_status != 2
    AND hp.name != 'TEST'
    AND 
    (
        (qra.value_code_id IS NOT NULL AND co_a.code_id IS NOT NULL)
        OR qra.value_integer IS NOT NULL 
        OR qra.value_decimal IS NOT NULL
        OR qra.value_boolean IS NOT NULL
        OR qra.value_date IS NOT NULL 
        OR qra.value_datetime IS NOT NULL
        OR qra.value_string IS NOT NULL
    )
;


-- -------------------------------------------------------------------
-- table: src_clean
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS cdm.src_clean;
CREATE TABLE cdm.src_clean
(
    participant_id              bigint,
    date_of_survey              datetime,
    question_ppi_code           varchar(200),
    question_code_id            bigint,
    value_ppi_code              varchar(200),
    topic_value                 varchar(200),
    value_code_id               bigint,
    value_number                decimal(20,6),
    value_boolean               tinyint,
    value_date                  datetime,
    value_string                varchar(1024),
    questionnaire_response_id   bigint,
    unit_id                     varchar(50)
);

INSERT INTO cdm.src_clean
SELECT
    participant_id,
    date_of_survey,
    question_ppi_code,
    question_code_id,
    value_ppi_code,
    topic_value,
    value_code_id,
    value_number,
    value_boolean,
    value_date,
    value_string,
    MIN(questionnaire_response_id),
    unit_id
FROM cdm.tmp_clean_all a
GROUP BY
    participant_id,
    date_of_survey,
    question_ppi_code,
    question_code_id,
    value_ppi_code,
    topic_value,
    value_code_id,
    value_number,
    value_boolean,
    value_date,
    value_string,
    unit_id
;

ALTER TABLE cdm.src_clean ADD KEY (participant_id);
ALTER TABLE cdm.src_clean ADD KEY (question_ppi_code);


-- -------------------------------------------------------------------
-- table: src_participant
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS cdm.src_participant;
CREATE TABLE cdm.src_participant
(
    participant_id          bigint,
    latest_date_of_survey   datetime,
    PRIMARY KEY (participant_id)
);

INSERT INTO cdm.src_participant 
SELECT 
    src_c.participant_id        AS participant_id,
    MAX(date_of_survey)         AS latest_date_of_survey
FROM src_clean src_c
WHERE
    src_c.question_ppi_code = 'PIIBirthInformation_BirthDate' 
    AND src_c.value_date IS NOT NULL
GROUP BY
    src_c.participant_id
HAVING COUNT(DISTINCT src_c.value_date) = 1
;

ALTER TABLE cdm.src_participant ADD KEY (latest_date_of_survey);

-- -------------------------------------------------------------------
-- table: src_mapped
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS cdm.src_mapped;
CREATE TABLE cdm.src_mapped
(
    participant_id              bigint,
    date_of_survey              datetime,
    question_ppi_code           varchar(200),
    question_code_id            bigint,
    question_source_concept_id  bigint,
    question_concept_id         bigint,
    value_ppi_code              varchar(200),
    topic_value                 varchar(200),
    value_code_id               bigint,
    value_source_concept_id     bigint,
    value_concept_id            bigint,
    value_number                decimal(20,6),
    value_boolean               tinyint,
    value_boolean_concept_id    bigint,
    value_date                  datetime,
    value_string                varchar(1024),
    questionnaire_response_id   bigint,
    unit_id                     varchar(50)
);

INSERT INTO cdm.src_mapped
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
    COALESCE(vc4.concept_id, 0)         AS value_concept_id,
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
    src_c.unit_id                       AS unit_id 
FROM cdm.src_clean src_c
JOIN cdm.src_participant src_p
    ON  src_c.participant_id = src_p.participant_id
LEFT JOIN voc.concept vc1 
    ON  LEFT(src_c.question_ppi_code, 50) = vc1.concept_code
    AND vc1.vocabulary_id = 'AllOfUs_PPI'
LEFT JOIN voc.concept_relationship vcr1
    ON  vc1.concept_id = vcr1.concept_id_1
    AND vcr1.relationship_id = 'Maps to'
    AND vcr1.invalid_reason IS NULL
LEFT JOIN voc.concept vc2
    ON  vcr1.concept_id_2 = vc2.concept_id
    AND vc2.standard_concept = 'S'
LEFT JOIN voc.concept vc3 
    ON  LEFT(src_c.value_ppi_code, 50) = vc3.concept_code
    AND vc3.vocabulary_id = 'AllOfUs_PPI'
LEFT JOIN voc.concept_relationship vcr2
    ON  vc3.concept_id = vcr2.concept_id_1
    AND vcr2.relationship_id = 'Maps to'
    AND vcr2.invalid_reason IS NULL
LEFT JOIN voc.concept vc4 
    ON  vcr2.concept_id_2 = vc4.concept_id
    AND vc4.standard_concept = 'S'
;

ALTER TABLE cdm.src_mapped ADD KEY (participant_id);


-- -------------------------------------------------------------------
-- table: cdm.src_person_location
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS cdm.src_person_location;
CREATE TABLE cdm.src_person_location
(
    participant_id bigint,
    address_1 varchar(255),
    address_2 varchar(255),
    city varchar(255),
    zip varchar(255),
    state_ppi_code varchar(255),
    state varchar(255),
    PRIMARY KEY (participant_id)
);


INSERT cdm.src_person_location
SELECT 
    src_m.participant_id,
    MAX(IF(src_m.question_ppi_code = 'PIIAddress_StreetAddress', 
                src_m.value_string, NULL))                          AS address_1,
    MAX(IF(src_m.question_ppi_code = 'PIIAddress_StreetAddress2', 
                src_m.value_string, NULL))                          AS address_2,
    MAX(IF(src_m.question_ppi_code = 'StreetAddress_PIICity', 
                src_m.value_string, NULL))                          AS city,
    MAX(IF(src_m.question_ppi_code = 'StreetAddress_PIIZIP', 
                src_m.value_string, NULL))                          AS zip,
    MAX(IF(src_m.question_ppi_code = 'StreetAddress_PIIState' AND src_m.topic_value = 'States', 
                src_m.value_ppi_code, NULL))                        AS state_ppi_code,
    MAX(IF(src_m.question_ppi_code = 'StreetAddress_PIIState' AND src_m.topic_value = 'States', 
                RIGHT(src_m.value_ppi_code, 2), NULL))               AS state
FROM cdm.src_mapped src_m
JOIN cdm.src_participant src_p
    ON  src_m.participant_id = src_p.participant_id
    AND src_m.date_of_survey = src_p.latest_date_of_survey
GROUP BY
    src_m.participant_id
;

-- -------------------------------------------------------------------
-- table: location
-- -------------------------------------------------------------------

TRUNCATE TABLE location;

INSERT cdm.location
SELECT DISTINCT
    NULL                            AS location_id,
    src.address_1                   AS address_1,
    src.address_2                   AS address_2,
    src.city                        AS city,
    src.state                       AS state,
    src.zip                         AS zip,
    NULL                            AS county,
    src.state_ppi_code              AS location_source_value,
    'loc'                           AS unit_id
FROM cdm.src_person_location src
;


-- ---------------------------------------------------
-- found the gender_concept_id
-- ---------------------------------------------------
DROP TABLE IF EXISTS cdm.src_gender;

CREATE TABLE cdm.src_gender
(
    person_id                   bigint,
    ppi_code                    varchar(255),
    gender_concept_id           bigint,
    PRIMARY KEY (person_id)
);

INSERT INTO cdm.src_gender
SELECT DISTINCT
    src_m.participant_id            AS person_id,
    NULL                            AS ppi_code,
    0                               AS gender_concept_id
FROM cdm.src_mapped src_m
WHERE
    src_m.value_ppi_code IN ('SexAtBirth_Male', 'SexAtBirth_Female')
GROUP BY src_m.participant_id
HAVING 
        COUNT(distinct value_ppi_code) > 1
;

INSERT INTO cdm.src_gender
SELECT DISTINCT
    src_m.participant_id            AS person_id,
    src_m.value_ppi_code            AS ppi_code,
    CASE
        WHEN src_m.value_ppi_code = 'SexAtBirth_Male' THEN 8507
        WHEN src_m.value_ppi_code = 'SexAtBirth_Female' THEN 8532
    END                             AS gender_concept_id
FROM cdm.src_mapped src_m
WHERE
    src_m.value_ppi_code IN ('SexAtBirth_Male', 'SexAtBirth_Female')
    AND NOT EXISTS (SELECT * FROM cdm.src_gender g
                    WHERE src_m.participant_id = g.person_id)
;

-- ---------------------------------------------------
-- found the date_of_birth
-- ---------------------------------------------------
DROP TABLE IF EXISTS cdm.src_birth;

CREATE TABLE cdm.src_birth
(
    person_id                   bigint,
    value_date                  date,
    year                        int,
    month                       int,
    day                         int,
    PRIMARY KEY (person_id)
);

INSERT INTO cdm.src_birth
SELECT DISTINCT
    src_m.participant_id           AS person_id,
    DATE(src_m.value_date)         AS ppi_code,
    YEAR(src_m.value_date)         AS year,
    MONTH(src_m.value_date)        AS month,
    DAY(src_m.value_date)          AS day
FROM cdm.src_mapped src_m
WHERE
    src_m.question_ppi_code = 'PIIBirthInformation_BirthDate'
;

-- ---------------------------------------------------
-- found the race_concept_id
-- ---------------------------------------------------
DROP TABLE IF EXISTS cdm.src_race;

CREATE TABLE cdm.src_race
(
    person_id                   bigint,
    ppi_code                    varchar(255),
    race_source_concept_id      bigint,
    race_target_concept_id      bigint,
    PRIMARY KEY (person_id)
);

INSERT INTO cdm.src_race
SELECT DISTINCT
    src_m.participant_id                    AS person_id,
    MIN(stcm1.source_code)                  AS ppi_code,
    MIN(stcm1.source_concept_id)            AS race_source_concept_id,
    MIN(COALESCE(vc1.concept_id, 0))        AS race_target_concept_id
FROM cdm.src_mapped src_m
INNER JOIN cdm.source_to_concept_map stcm1
    ON src_m.value_ppi_code = stcm1.source_code
    AND stcm1.priority = 1
    AND stcm1.source_vocabulary_id = 'ppi-race'
LEFT JOIN voc.concept vc1
    ON stcm1.target_concept_id = vc1.concept_id
    AND vc1.standard_concept = 'S'
GROUP BY src_m.participant_id
HAVING
    COUNT(distinct src_m.value_ppi_code) = 1
;

INSERT INTO cdm.src_race
SELECT DISTINCT
    src_m.participant_id                    AS person_id,
    MIN(stcm1.source_code)                  AS ppi_code,
    MIN(stcm1.source_concept_id)            AS race_source_concept_id,
    MIN(COALESCE(vc1.concept_id, 0))        AS race_target_concept_id
FROM cdm.src_mapped src_m
INNER JOIN cdm.source_to_concept_map stcm1
    ON src_m.value_ppi_code = stcm1.source_code
    AND stcm1.priority = 2
    AND stcm1.source_vocabulary_id = 'ppi-race'
LEFT JOIN voc.concept vc1
    ON stcm1.target_concept_id = vc1.concept_id
    AND vc1.standard_concept = 'S'
WHERE
    NOT EXISTS (SELECT * FROM cdm.src_race g
                WHERE src_m.participant_id = g.person_id)
GROUP BY src_m.participant_id
HAVING
    COUNT(distinct src_m.value_ppi_code) = 1
;

-- ---------------------------------------------------
-- found the location_id
-- ---------------------------------------------------
DROP TABLE IF EXISTS cdm.src_location_for_pers;

CREATE TABLE cdm.src_location_for_pers
(
    person_id               bigint,
    address_1               varchar(50),
    address_2               varchar(50),
    city                    varchar(50),
    state                   varchar(2),
    zip                     varchar(9),
    location_id             bigint,
    PRIMARY KEY (person_id)
);

INSERT INTO cdm.src_location_for_pers
SELECT DISTINCT
    src_l.participant_id            AS person_id,
    src_l.address_1                 AS address_1,
    src_l.address_2                 AS address_2,
    src_l.city                      AS city,
    src_l.state                     AS state,
    src_l.zip                       AS zip,
    L.location_id                   AS location_id
FROM cdm.src_person_location src_l
LEFT JOIN cdm.location L
    ON COALESCE(L.address_1, 0) = COALESCE(src_l.address_1, 0)
    AND COALESCE(L.address_2, 0) = COALESCE(src_l.address_2, 0)
    AND COALESCE(L.city, 0) = COALESCE(src_l.city, 0)
    AND COALESCE(L.state, 0) = COALESCE(src_l.state, 0)
    AND COALESCE(L.zip, 0) = COALESCE(src_l.zip, 0)
;

-- -------------------------------------------------------------------
-- table: person
-- -------------------------------------------------------------------
TRUNCATE TABLE cdm.person;

INSERT INTO cdm.person
SELECT DISTINCT
    src_m.participant_id                        AS person_id,
    COALESCE(g.gender_concept_id, 0)            AS gender_concept_id,
    b.year                                      AS year_of_birth,
    b.month                                     AS month_of_birth,
    b.day                                       AS day_of_birth,
    NULL                                        AS time_of_birth,
    COALESCE(r.race_target_concept_id, 0)       AS race_concept_id,
    0                                           AS ethnicity_concept_id,
    loc.location_id                             AS location_id,
    NULL                                        AS provider_id,
    NULL                                        AS care_site_id,
    src_m.participant_id                        AS person_source_value,
    g.ppi_code                                  AS gender_source_value,
    0                                           AS gender_source_concept_id,
    r.ppi_code                                  AS race_source_value,
    COALESCE(r.race_source_concept_id, 0)       AS race_source_concept_id,
    NULL                                        AS ethnisity_source_value,
    0                                           AS ethnicity_source_concept_id,
    'person'                                    AS unit_id
FROM cdm.src_mapped src_m
INNER JOIN cdm.src_birth b
    ON src_m.participant_id = b.person_id
LEFT JOIN cdm.src_gender g
    ON src_m.participant_id = g.person_id
LEFT JOIN cdm.src_race r
    ON src_m.participant_id = r.person_id
LEFT JOIN cdm.src_location_for_pers loc
    ON src_m.participant_id = loc.person_id
;

-- -------------------------------------------------------------------
-- Drop Temporary Tables
-- -------------------------------------------------------------------
  DROP TABLE IF EXISTS cdm.src_birth;
  DROP TABLE IF EXISTS cdm.src_gender;
  DROP TABLE IF EXISTS cdm.src_race;
  DROP TABLE IF EXISTS cdm.src_location_for_pers;


-- -------------------------------------------------------------------
-- table: cdm.observation
-- -------------------------------------------------------------------
TRUNCATE TABLE cdm.observation;

INSERT INTO cdm.observation
SELECT
    NULL                                        AS observation_id,
    src_m.participant_id                        AS person_id,
    src_m.question_concept_id                   AS observation_concept_id,
    DATE(src_m.date_of_survey)                  AS observation_date,
    TIME(src_m.date_of_survey)                  AS observation_time,
    45905771                                    AS observation_type_concept_id,    -- Standard CDM concept: 'Observation Recorded from a Survey'
    src_m.value_number                          AS value_as_number,
    CASE
        WHEN src_m.value_ppi_code IS NOT NULL
             AND  src_m.value_concept_id = 0    THEN src_m.value_string
        WHEN src_m.value_string IS NOT NULL
             AND src_m.value_ppi_code IS NULL   THEN src_m.value_string
        ELSE NULL
    END                                         AS value_as_string,
    CASE
        WHEN src_m.value_ppi_code IS NOT NULL THEN src_m.value_concept_id
        WHEN src_m.value_boolean IS NOT NULL THEN src_m.value_boolean_concept_id
        ELSE 0
    END                                         AS value_as_concept_id,
    0                                           AS qualifier_concept_id,
    0                                           AS unit_concept_id,
    NULL                                        AS provider_id,
    NULL                                        AS visit_occurrence_id,
    src_m.question_ppi_code                     AS observation_source_value,
    src_m.question_source_concept_id            AS observation_source_concept_id,
    NULL                                        AS unit_source_value,
    NULL                                        AS qualifier_source_value,
    src_m.value_source_concept_id               AS value_source_concept_id,
    src_m.value_ppi_code                        AS value_source_value,
    src_m.questionnaire_response_id             AS questionnaire_response_id,
    NULL                                        AS meas_id,
    CASE
        WHEN src_m.value_ppi_code IS NOT NULL       THEN 'observ.code'
        WHEN src_m.value_ppi_code IS NULL
             AND src_m.value_string IS NOT NULL     THEN 'observ.str'
        WHEN src_m.value_number IS NOT NULL         THEN 'observ.num'
        WHEN src_m.value_boolean IS NOT NULL        THEN 'observ.bool'
    END                                         AS unit_id
FROM cdm.src_mapped src_m
;


-- -------------------------------------------------------------------
-- table: procedure_occurrence
-- -------------------------------------------------------------------
TRUNCATE TABLE cdm.procedure_occurrence;

INSERT INTO cdm.procedure_occurrence
SELECT
    NULL                                        AS procedure_occurrence_id,
    src_m1.participant_id                       AS person_id,
    COALESCE(vc.concept_id, 0)                  AS procedure_concept_id,
    src_m2.value_date                           AS procedure_date,
    581412                                      AS procedure_type_concept_id,   -- Procedure Recorded from a Survey
    0                                           AS modifier_concept_id,
    NULL                                        AS quantity,
    NULL                                        AS provider_id,
    NULL                                        AS visit_occurrence_id,
    stcm.source_code                            AS procedure_source_value,
    COALESCE(stcm.source_concept_id, 0)         AS procedure_source_concept_id,
    NULL                                        AS qualifier_source_value,
    'procedure'                                 AS unit_id
FROM cdm.src_mapped src_m1
INNER JOIN cdm.source_to_concept_map stcm
    ON src_m1.value_ppi_code = stcm.source_code
    AND stcm.source_vocabulary_id = 'ppi-proc'
INNER JOIN cdm.src_mapped src_m2
    ON src_m1.participant_id = src_m2.participant_id
    AND src_m2.question_ppi_code = 'OrganTransplant_Date'
    AND src_m2.value_date IS NOT NULL
LEFT JOIN voc.concept vc
    ON stcm.target_concept_id = vc.concept_id
    AND vc.standard_concept = 'S'
;


-- -------------------------------------------------------------------
-- procedure_occurrence
-- -------------------------------------------------------------------
SELECT NULL INTO @partition_expr;
SELECT NULL INTO @last_part_expr;
SELECT NULL INTO @row_number;
SELECT NULL INTO @reset_num;

DROP TABLE IF EXISTS cdm.tmp_procedure_occurrence;

CREATE TABLE cdm.tmp_procedure_occurrence AS
SELECT 
    @partition_expr := CONCAT_WS('-',
                        t.person_id,
                        t.procedure_concept_id,
                        t.procedure_date,
                        t.procedure_source_value,
                        t.procedure_source_concept_id
        )                                                       AS partition_expr,
    @reset_num :=
        CASE
            WHEN @partition_expr = @last_part_expr THEN 0
            ELSE 1
        END                                                      AS reset_num,
    @last_part_expr := @partition_expr                           AS last_part_expr,
    @row_number :=
        CASE
            WHEN @reset_num = 0 THEN @row_number + 1
            ELSE 1
        END                                                      AS row_number,
    t.procedure_occurrence_id                           AS procedure_occurrence_id,
    t.person_id                                         AS person_id,
    t.procedure_concept_id                              AS procedure_concept_id,
    t.procedure_date                                    AS procedure_date,
    t.procedure_type_concept_id                         AS procedure_type_concept_id,
    t.modifier_concept_id                               AS modifier_concept_id,
    t.quantity                                          AS quantity,
    t.provider_id                                       AS provider_id,
    t.visit_occurrence_id                               AS visit_occurrence_id,
    t.procedure_source_value                            AS procedure_source_value,
    t.procedure_source_concept_id                       AS procedure_source_concept_id,
    t.qualifier_source_value                            AS qualifier_source_value,
    t.unit_id                                           AS unit_id
FROM 
    cdm.procedure_occurrence t
ORDER BY
    t.person_id,
    t.procedure_concept_id,
    t.procedure_date,
    t.procedure_source_value,
    t.procedure_source_concept_id
;

TRUNCATE TABLE cdm.procedure_occurrence;

INSERT INTO cdm.procedure_occurrence
SELECT 
    t.procedure_occurrence_id,
    t.person_id,
    t.procedure_concept_id,
    t.procedure_date,
    t.procedure_type_concept_id,
    t.modifier_concept_id,
    t.quantity,
    t.provider_id,
    t.visit_occurrence_id,
    t.procedure_source_value,
    t.procedure_source_concept_id,
    t.qualifier_source_value,
    t.unit_id
FROM
    cdm.tmp_procedure_occurrence t
WHERE
    t.row_number = 1
;

DROP TABLE cdm.tmp_procedure_occurrence;

-- -------------------------------------------------------------------
-- observation
-- -------------------------------------------------------------------
SELECT NULL INTO @partition_expr;
SELECT NULL INTO @last_part_expr;
SELECT NULL INTO @row_number;
SELECT NULL INTO @reset_num;

DROP TABLE IF EXISTS cdm.tmp_observation;

CREATE TABLE cdm.tmp_observation AS
SELECT 
    @partition_expr := CONCAT_WS('-',
                        t.person_id,
                        t.observation_concept_id,
                        t.observation_date,
                        t.observation_time, 
                        t.observation_type_concept_id,
                        t.value_as_number,
                        t.value_as_string,
                        t.value_as_concept_id,
                        t.visit_occurrence_id,
                        t.observation_source_value,
                        t.observation_source_concept_id,
                        t.value_source_concept_id,
                        t.value_source_value
        )                                                       AS partition_expr,
    @reset_num :=
        CASE
            WHEN @partition_expr = @last_part_expr THEN 0
            ELSE 1
        END                                                      AS reset_num,
    @last_part_expr := @partition_expr                           AS last_part_expr,
    @row_number :=
        CASE
            WHEN @reset_num = 0 THEN @row_number + 1
            ELSE 1
        END                                                      AS row_number,
    t.observation_id                                    AS observation_id,
    t.person_id                                         AS person_id,
    t.observation_concept_id                            AS observation_concept_id,
    t.observation_date                                  AS observation_date,
    t.observation_time                                  AS observation_time,
    t.observation_type_concept_id                       AS observation_type_concept_id,
    t.value_as_number                                   AS value_as_number,
    t.value_as_string                                   AS value_as_string,
    t.value_as_concept_id                               AS value_as_concept_id,
    t.qualifier_concept_id                              AS qualifier_concept_id,
    t.unit_concept_id                                   AS unit_concept_id,
    t.provider_id                                       AS provider_id,
    t.visit_occurrence_id                               AS visit_occurrence_id,
    t.observation_source_value                          AS observation_source_value,
    t.observation_source_concept_id                     AS observation_source_concept_id,
    t.unit_source_value                                 AS unit_source_value,
    t.qualifier_source_value                            AS qualifier_source_value,
    t.value_source_concept_id                           AS value_source_concept_id,
    t.value_source_value                                AS value_source_value,
    t.questionnaire_response_id                         AS questionnaire_response_id,
    t.meas_id                                           AS meas_id,
    t.unit_id                                           AS unit_id
FROM 
    cdm.observation t
ORDER BY
    t.person_id,
    t.observation_concept_id,
    t.observation_date,
    t.observation_time, 
    t.observation_type_concept_id,
    t.value_as_number,
    t.value_as_string,
    t.value_as_concept_id,
    t.visit_occurrence_id,
    t.observation_source_value,
    t.observation_source_concept_id,
    t.value_source_concept_id,
    t.value_source_value
;

TRUNCATE TABLE cdm.observation;

INSERT INTO cdm.observation
SELECT 
    t.observation_id,
    t.person_id,
    t.observation_concept_id,
    t.observation_date,
    t.observation_time,
    t.observation_type_concept_id,
    t.value_as_number,
    t.value_as_string,
    t.value_as_concept_id,
    t.qualifier_concept_id,
    t.unit_concept_id,
    t.provider_id,
    t.visit_occurrence_id,
    t.observation_source_value,
    t.observation_source_concept_id,
    t.unit_source_value,
    t.qualifier_source_value,
    t.value_source_concept_id,
    t.value_source_value,
    t.questionnaire_response_id,
    t.meas_id,
    t.unit_id
FROM
    cdm.tmp_observation t
WHERE
    t.row_number = 1
;

DROP TABLE cdm.tmp_observation;


-- -----------------------------------------------
-- populate cdm_target tables from cdm
-- -----------------------------------------------

-- -----------------------------------------------
-- location
-- -----------------------------------------------
TRUNCATE TABLE cdm_target.location;

INSERT INTO cdm_target.location
SELECT
    location_id,
    address_1,
    address_2,
    city,
    state,
    zip,
    county,
    location_source_value
FROM cdm.location;

-- -----------------------------------------------
-- person
-- -----------------------------------------------
TRUNCATE TABLE cdm_target.person;

INSERT INTO cdm_target.person
SELECT
    person_id,
    gender_concept_id,
    year_of_birth,
    month_of_birth,
    day_of_birth,
    time_of_birth,
    race_concept_id,
    ethnicity_concept_id,
    location_id,
    provider_id,
    care_site_id,
    person_source_value,
    gender_source_value,
    gender_source_concept_id,
    race_source_value,
    race_source_concept_id,
    ethnicity_source_value,
    ethnicity_source_concept_id
FROM cdm.person;

-- -----------------------------------------------
-- procedure_occurrence
-- -----------------------------------------------
TRUNCATE TABLE cdm_target.procedure_occurrence;

INSERT INTO cdm_target.procedure_occurrence
SELECT
    procedure_occurrence_id,
    person_id,
    procedure_concept_id,
    procedure_date,
    procedure_type_concept_id,
    modifier_concept_id,
    quantity,
    provider_id,
    visit_occurrence_id,
    procedure_source_value,
    procedure_source_concept_id,
    qualifier_source_value
FROM cdm.procedure_occurrence;

-- -----------------------------------------------
-- observation
-- -----------------------------------------------
TRUNCATE TABLE cdm_target.observation;

INSERT INTO cdm_target.observation
SELECT
    observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_time,
    observation_type_concept_id,
    value_as_number,
    value_as_string,
    value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    observation_source_value,
    observation_source_concept_id,
    unit_source_value,
    qualifier_source_value,
    value_source_concept_id,
    value_source_value,
    questionnaire_response_id
FROM cdm.observation;

