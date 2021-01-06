USE cdm;

-- -------------------------------------------------------------------
-- Update cdm.src_clean to filter specific surveys.
-- -------------------------------------------------------------------
UPDATE combined_survey_filter SET survey_name = REPLACE(survey_name, '\r', '');

UPDATE cdm.src_clean
    INNER JOIN cdm.combined_survey_filter ON
        cdm.src_clean.survey_name = cdm.combined_survey_filter.survey_name
SET cdm.src_clean.filter = 1
WHERE TRUE;

-- -------------------------------------------------------------------
-- Update cdm.src_clean to filter specific survey questions.
-- -------------------------------------------------------------------
UPDATE combined_question_filter SET question_ppi_code = REPLACE(question_ppi_code, '\r', '');

UPDATE cdm.src_clean
    INNER JOIN cdm.combined_question_filter ON
        cdm.src_clean.question_ppi_code = cdm.combined_question_filter.question_ppi_code
SET cdm.src_clean.filter = 1
WHERE TRUE;

-- -------------------------------------------------------------------
-- source_file: src/src_mapped.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: src_participant
-- Provides participant with birthday date and last observation date.
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS cdm.src_participant;
CREATE TABLE cdm.src_participant
(
    participant_id              bigint,
    latest_date_of_survey       datetime,
    date_of_birth               date,
    PRIMARY KEY (participant_id)
);

-- ----------------------------------------------------------------
-- Birthday date is defined as 'value_date' answer to the
-- 'PPIBirthInformation_BirthDate' question in the last survey.
-- ----------------------------------------------------------------
INSERT INTO cdm.src_participant
SELECT
    f1.participant_id,
    f1.latest_date_of_survey,
    f1.date_of_birth
FROM
    (SELECT
        t1.participant_id           AS participant_id,
        t1.latest_date_of_survey    AS latest_date_of_survey,
        MAX(DATE(t2.value_date))    AS date_of_birth
    FROM
        (
        SELECT
            src_c.participant_id        AS participant_id,
            MAX(src_c.date_of_survey)   AS latest_date_of_survey
        FROM cdm.src_clean src_c
        WHERE
            src_c.question_ppi_code = 'PIIBirthInformation_BirthDate'
            AND src_c.value_date IS NOT NULL
        GROUP BY
            src_c.participant_id
        ) t1
    INNER JOIN cdm.src_clean t2
        ON t1.participant_id = t2.participant_id
        AND t1.latest_date_of_survey = t2.date_of_survey
        AND t2.question_ppi_code = 'PIIBirthInformation_BirthDate'
    GROUP BY
        t1.participant_id,
        t1.latest_date_of_survey
    ) f1
;

ALTER TABLE cdm.src_participant ADD KEY (participant_id);

-- -------------------------------------------------------------------
-- table: src_mapped
-- This is src_clean, mapped to standard concepts.
-- 'question_source_concept_id' must be related to
-- 'question_concept_id' as 'Maps To' relation.
-- 'value_source_concept_id' and 'value_concept_id' - the same
-- relation.
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
    ON  src_c.question_ppi_code = vc1.concept_code
    AND vc1.vocabulary_id = 'PPI'
LEFT JOIN voc.concept_relationship vcr1
    ON  vc1.concept_id = vcr1.concept_id_1
    AND vcr1.relationship_id = 'Maps to'
    AND vcr1.invalid_reason IS NULL
LEFT JOIN voc.concept vc2
    ON  vcr1.concept_id_2 = vc2.concept_id
    AND vc2.standard_concept = 'S'
    AND vc2.invalid_reason IS NULL
LEFT JOIN voc.concept vc3
    ON  src_c.value_ppi_code = vc3.concept_code
    AND vc3.vocabulary_id = 'PPI'
LEFT JOIN voc.concept_relationship vcr2
    ON  vc3.concept_id = vcr2.concept_id_1
    AND vcr2.relationship_id = 'Maps to value'
    AND vcr2.invalid_reason IS NULL
LEFT JOIN voc.concept vc4
    ON  vcr2.concept_id_2 = vc4.concept_id
    AND vc4.standard_concept = 'S'
    AND vc4.invalid_reason IS NULL
WHERE src_c.filter = 0
;

ALTER TABLE cdm.src_mapped ADD KEY (question_ppi_code);
CREATE INDEX mapped_p_id_and_ppi ON cdm.src_mapped (participant_id, question_ppi_code);
CREATE INDEX mapped_qr_id_and_ppi ON cdm.src_mapped (questionnaire_response_id, question_ppi_code);

-- -------------------------------------------------------------------
-- source_file: src/location.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: cdm.src_person_location
-- Address is taken as answer to address-related questions during
-- last survey.
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
    location_id bigint,
    PRIMARY KEY (participant_id)
);

INSERT cdm.src_person_location
SELECT
    src_participant.participant_id        AS participant_id,
    MAX(m_address_1.value_string)         AS address_1,
    MAX(m_address_2.value_string)         AS address_2,
    MAX(m_city.value_string)              AS city,
    MAX(m_zip.value_string)               AS zip,
    MAX(m_state.value_ppi_code)           AS state_ppi_code,
    MAX(RIGHT(m_state.value_ppi_code, 2)) AS state,
    NULL                                  AS location_id
FROM src_participant
  INNER JOIN
    cdm.src_mapped m_address_1
      ON src_participant.participant_id = m_address_1.participant_id
     AND m_address_1.question_ppi_code = 'PIIAddress_StreetAddress'
  LEFT JOIN
    cdm.src_mapped m_address_2
      ON m_address_1.questionnaire_response_id = m_address_2.questionnaire_response_id
     AND m_address_2.question_ppi_code = 'PIIAddress_StreetAddress2'
  LEFT JOIN
    cdm.src_mapped m_city
      ON m_address_1.questionnaire_response_id = m_city.questionnaire_response_id
     AND m_city.question_ppi_code = 'StreetAddress_PIICity'
  LEFT JOIN
    cdm.src_mapped m_zip
      ON m_address_1.questionnaire_response_id = m_zip.questionnaire_response_id
     AND m_zip.question_ppi_code = 'StreetAddress_PIIZIP'
  LEFT JOIN
    cdm.src_mapped m_state
      ON m_address_1.questionnaire_response_id = m_state.questionnaire_response_id
     AND m_state.question_ppi_code = 'StreetAddress_PIIState'
WHERE m_address_1.date_of_survey =
  (SELECT MAX(date_of_survey)
     FROM cdm.src_mapped m_address_1_2
    WHERE m_address_1_2.participant_id = m_address_1.participant_id
      AND m_address_1_2.question_ppi_code = 'PIIAddress_StreetAddress')
GROUP BY src_participant.participant_id;

-- -------------------------------------------------------------------
-- table: location
-- -------------------------------------------------------------------

TRUNCATE TABLE cdm.location;

SET @row_number = 0;
INSERT cdm.location
SELECT DISTINCT
    (@row_number:=@row_number + 1)  AS id,
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

CREATE INDEX location_address ON cdm.location (address_1, zip);

UPDATE cdm.src_person_location person_loc, cdm.location loc
   SET person_loc.location_id = loc.location_id
 WHERE person_loc.address_1 <=> loc.address_1
   AND person_loc.address_2 <=> loc.address_2
   AND person_loc.city <=> loc.city
   AND person_loc.state <=> loc.state
   AND person_loc.zip <=> loc.zip;

-- -------------------------------------------------------------------
-- source_file: src/person.sql
-- -------------------------------------------------------------------

-- ---------------------------------------------------
-- table src_gender.
-- Contains gender information from patient surveys.
-- ---------------------------------------------------
DROP TABLE IF EXISTS cdm.src_gender;

CREATE TABLE cdm.src_gender
(
    person_id                   bigint,
    ppi_code                    varchar(255),
    gender_source_concept_id    bigint,
    gender_target_concept_id    bigint,
    PRIMARY KEY (person_id)
);

-- ------------------------------------------------------
-- Map many non-standard genders from src_mapped to allowed
-- by cdm standards by 'source_to_concept_map' relation.
-- -------------------------------------------------------
INSERT INTO cdm.src_gender
SELECT DISTINCT
    src_m.participant_id                    AS person_id,
    MIN(stcm1.source_code)                  AS ppi_code,
    MIN(stcm1.source_concept_id)            AS gender_source_concept_id,
    MIN(COALESCE(vc1.concept_id, 0))        AS gender_target_concept_id
FROM cdm.src_mapped src_m
INNER JOIN cdm.source_to_concept_map stcm1
    ON src_m.value_ppi_code = stcm1.source_code
    AND stcm1.priority = 1              -- priority 1
    AND stcm1.source_vocabulary_id = 'ppi-sex'
LEFT JOIN voc.concept vc1
    ON stcm1.target_concept_id = vc1.concept_id
    AND vc1.standard_concept = 'S'
    AND vc1.invalid_reason IS NULL
GROUP BY src_m.participant_id
HAVING
    COUNT(distinct src_m.value_ppi_code) = 1
;

-- ---------------------------------------------------
-- table src_race.
-- Contains racial information from patient surveys.
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

-- ------------------------------------------------------
-- Map many non-standard races from src_mapped to allowed
-- by cdm standards by 'source_to_concept_map' relation.
-- priority = 1 means more detailed racial
-- information over priority = 2. So if patient provides
-- detailed answer about his/her race, we firstly
-- use it.
-- -------------------------------------------------------
INSERT INTO cdm.src_race
SELECT DISTINCT
    src_m.participant_id                    AS person_id,
    MIN(stcm1.source_code)                  AS ppi_code,
    MIN(stcm1.source_concept_id)            AS race_source_concept_id,
    MIN(COALESCE(vc1.concept_id, 0))        AS race_target_concept_id
FROM cdm.src_mapped src_m
INNER JOIN cdm.source_to_concept_map stcm1
    ON src_m.value_ppi_code = stcm1.source_code
    AND stcm1.priority = 1              -- priority 1
    AND stcm1.source_vocabulary_id = 'ppi-race'
LEFT JOIN voc.concept vc1
    ON stcm1.target_concept_id = vc1.concept_id
    AND vc1.standard_concept = 'S'
    AND vc1.invalid_reason IS NULL
GROUP BY src_m.participant_id
HAVING
    COUNT(distinct src_m.value_ppi_code) = 1
;

-- ----------------------------------------------------
-- Then we find and insert priority-2 (more common)
-- race info, if priority-1 info was not already
-- provided.
-- ----------------------------------------------------
INSERT INTO cdm.src_race
SELECT DISTINCT
    src_m.participant_id                    AS person_id,
    MIN(stcm1.source_code)                  AS ppi_code,
    MIN(stcm1.source_concept_id)            AS race_source_concept_id,
    MIN(COALESCE(vc1.concept_id, 0))        AS race_target_concept_id
FROM cdm.src_mapped src_m
INNER JOIN cdm.source_to_concept_map stcm1
    ON src_m.value_ppi_code = stcm1.source_code
    AND stcm1.priority = 2              -- priority 2
    AND stcm1.source_vocabulary_id = 'ppi-race'
LEFT JOIN voc.concept vc1
    ON stcm1.target_concept_id = vc1.concept_id
    AND vc1.standard_concept = 'S'
    AND vc1.invalid_reason IS NULL
WHERE
    NOT EXISTS (SELECT * FROM cdm.src_race g
                WHERE src_m.participant_id = g.person_id)
GROUP BY src_m.participant_id
HAVING
    COUNT(distinct src_m.value_ppi_code) = 1
;

-- ---------------------------------------------------
-- table src_ethnicity.
-- Contains ethnicity information from patient surveys.
-- ---------------------------------------------------
DROP TABLE IF EXISTS cdm.src_ethnicity;

CREATE TABLE cdm.src_ethnicity
(
    person_id                   bigint,
    ppi_code                    varchar(255),
    ethnicity_source_concept_id bigint,
    ethnicity_target_concept_id bigint,
    PRIMARY KEY (person_id)
);

-- ------------------------------------------------------
-- Map many non-standard ethnicities from src_mapped to allowed
-- by cdm standards by 'source_to_concept_map' relation.
-- priority = 1 means more detailed ethnic
-- information over priority = 2. So if patient provides
-- detailed answer about his/her ethnicity, we firstly
-- use it.
-- -------------------------------------------------------
INSERT INTO cdm.src_ethnicity
SELECT DISTINCT
    src_m.participant_id                    AS person_id,
    MIN(stcm1.source_code)                  AS ppi_code,
    MIN(stcm1.source_concept_id)            AS ethnicity_source_concept_id,
    MIN(COALESCE(vc1.concept_id, 0))        AS ethnicity_target_concept_id
FROM cdm.src_mapped src_m
INNER JOIN cdm.source_to_concept_map stcm1
    ON src_m.value_ppi_code = stcm1.source_code
    AND stcm1.priority = 1              -- priority 1
    AND stcm1.source_vocabulary_id = 'ppi-ethnicity'
LEFT JOIN voc.concept vc1
    ON stcm1.target_concept_id = vc1.concept_id
    AND vc1.standard_concept = 'S'
    AND vc1.invalid_reason IS NULL
GROUP BY src_m.participant_id
HAVING
    COUNT(distinct src_m.value_ppi_code) = 1
;

-- ----------------------------------------------------
-- Then we find and insert priority-2 (more common)
-- ethnicity info, if priority-1 info was not already
-- provided.
-- ----------------------------------------------------
INSERT INTO cdm.src_ethnicity
SELECT DISTINCT
    src_m.participant_id                    AS person_id,
    MIN(stcm1.source_code)                  AS ppi_code,
    MIN(stcm1.source_concept_id)            AS ethnicity_source_concept_id,
    MIN(COALESCE(vc1.concept_id, 0))        AS ethnicity_target_concept_id
FROM cdm.src_mapped src_m
INNER JOIN cdm.source_to_concept_map stcm1
    ON src_m.value_ppi_code = stcm1.source_code
    AND stcm1.priority = 2              -- priority 2
    AND stcm1.source_vocabulary_id = 'ppi-ethnicity'
LEFT JOIN voc.concept vc1
    ON stcm1.target_concept_id = vc1.concept_id
    AND vc1.standard_concept = 'S'
    AND vc1.invalid_reason IS NULL
WHERE
    NOT EXISTS (SELECT * FROM cdm.src_ethnicity g
                WHERE src_m.participant_id = g.person_id)
GROUP BY src_m.participant_id
HAVING
    COUNT(distinct src_m.value_ppi_code) = 1
;

-- -------------------------------------------------------------------
-- table: cdm_person
-- Assembles person's birthday, gender, racial, ethnicity and
-- location information altogether from 'src_mapped', 'src_gender',
-- 'src_race', 'src_ethnicity', 'src_person_location' relations.
-- -------------------------------------------------------------------
TRUNCATE TABLE cdm.person;

DROP TABLE IF EXISTS cdm.tmp_person;
CREATE TABLE cdm.tmp_person LIKE cdm.person;
ALTER TABLE cdm.tmp_person DROP COLUMN id;

INSERT INTO cdm.tmp_person
SELECT DISTINCT
    src_m.participant_id                        AS person_id,
    COALESCE(g.gender_target_concept_id, 0)     AS gender_concept_id,
    YEAR(b.date_of_birth)                       AS year_of_birth,
    MONTH(b.date_of_birth)                      AS month_of_birth,
    DAY(b.date_of_birth)                        AS day_of_birth,
    TIMESTAMP(b.date_of_birth)                  AS birth_datetime,
    COALESCE(r.race_target_concept_id, 0)       AS race_concept_id,
    COALESCE(e.ethnicity_target_concept_id, 0)  AS ethnicity_concept_id,
    person_loc.location_id                      AS location_id,
    NULL                                        AS provider_id,
    NULL                                        AS care_site_id,
    src_m.participant_id                        AS person_source_value,
    g.ppi_code                                  AS gender_source_value,
    COALESCE(g.gender_source_concept_id, 0)     AS gender_source_concept_id,
    r.ppi_code                                  AS race_source_value,
    COALESCE(r.race_source_concept_id, 0)       AS race_source_concept_id,
    e.ppi_code                                  AS ethnicity_source_value,
    COALESCE(e.ethnicity_source_concept_id, 0) AS ethnicity_source_concept_id,
    'person'                                    AS unit_id
FROM cdm.src_mapped src_m
INNER JOIN cdm.src_participant b
    ON src_m.participant_id = b.participant_id
LEFT JOIN cdm.src_gender g
    ON src_m.participant_id = g.person_id
LEFT JOIN cdm.src_race r
    ON src_m.participant_id = r.person_id
LEFT JOIN cdm.src_ethnicity e
    ON src_m.participant_id = e.person_id
LEFT JOIN cdm.src_person_location person_loc
    ON src_m.participant_id = person_loc.participant_id;
;

SET @row_number = 0;
INSERT INTO cdm.person
SELECT
  (@row_number:=@row_number + 1)              AS id,
  cdm.tmp_person.*
FROM cdm.tmp_person;

DROP TABLE cdm.tmp_person;

-- -------------------------------------------------------------------
-- Drop Temporary Tables
-- -------------------------------------------------------------------
  DROP TABLE IF EXISTS cdm.src_gender;
  DROP TABLE IF EXISTS cdm.src_race;
  DROP TABLE IF EXISTS cdm.src_person_location;

-- -------------------------------------------------------------------
-- source_file: src/procedure_occurrence.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: procedure_occurrence
-- In patient surveys data only organs transplantation information
-- fits the procedure_occurence table.
-- -------------------------------------------------------------------
TRUNCATE TABLE cdm.procedure_occurrence;

SET @row_number = 0;
INSERT INTO cdm.procedure_occurrence
SELECT
    (@row_number:=@row_number + 1)              AS id,
    NULL                                        AS procedure_occurrence_id,
    src_m1.participant_id                       AS person_id,
    COALESCE(vc.concept_id, 0)                  AS procedure_concept_id,
    src_m2.value_date                           AS procedure_date,
    TIMESTAMP(src_m2.value_date)                AS procedure_datetime,
    581412                                      AS procedure_type_concept_id,   -- 581412, Procedure Recorded from a Survey
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
    AND vc.invalid_reason IS NULL
;

-- -------------------------------------------------------------------
-- source_file: src/src_meas_mapped.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: src_meas
-- Contains information about physical measurements of patient, which
-- is necessary for filling OMOP CDM tables.
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS cdm.src_meas;

CREATE TABLE cdm.src_meas
(
    participant_id              bigint NOT NULL,
    finalized_site_id           int,
    code_value                  varchar(255) NOT NULL,
    measurement_time            datetime NOT NULL,
    value_decimal               float,
    value_unit                  varchar(255),
    value_code_value            varchar(255),
    value_string                varchar(1024),
    measurement_id              bigint,
    physical_measurements_id     int NOT NULL,
    parent_id                    bigint
);

INSERT INTO cdm.src_meas
SELECT
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
    meas.parent_id                  AS parent_id
FROM rdr.measurement meas
INNER JOIN rdr.physical_measurements pm
    ON meas.physical_measurements_id = pm.physical_measurements_id
    AND pm.final = 1
    AND (pm.status <> 2 OR pm.status IS NULL)
INNER JOIN cdm.person pe
    ON pe.person_id = pm.participant_id
;

ALTER TABLE cdm.src_meas ADD KEY (code_value);
ALTER TABLE cdm.src_meas ADD KEY (physical_measurements_id);

-- -------------------------------------------------------------------
-- additional table: tmp_cv_concept_lk
-- Maps measurements code values to standard concept_ids.
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS cdm.tmp_cv_concept_lk;

CREATE TABLE cdm.tmp_cv_concept_lk
(
    code_value                  varchar(500),
    cv_source_concept_id        bigint,
    cv_concept_id               bigint,
    cv_domain_id                varchar(50),
    PRIMARY KEY (code_value)
);

INSERT INTO cdm.tmp_cv_concept_lk
SELECT DISTINCT
    meas.code_value                                 AS code_value,
    vc1.concept_id                                  AS cv_source_concept_id,
    vc2.concept_id                                  AS cv_concept_id,
    COALESCE(vc2.domain_id, vc1.domain_id)          AS cv_domain_id
FROM cdm.src_meas meas
LEFT JOIN voc.concept vc1
    ON meas.code_value = vc1.concept_code
    AND vc1.vocabulary_id = 'PPI'
LEFT JOIN voc.concept_relationship vcr1
    ON vc1.concept_id = vcr1.concept_id_1
    AND vcr1.relationship_id = 'Maps to'
    AND vcr1.invalid_reason IS NULL
LEFT JOIN voc.concept vc2
    ON vc2.concept_id = vcr1.concept_id_2
    AND vc2.standard_concept = 'S'
    AND vc2.invalid_reason IS NULL
WHERE
    meas.code_value IS NOT NULL
;

-- -------------------------------------------------------------------
-- additional table: tmp_vcv_concept_lk
-- Maps measurement results value_code_value to standard concept_ids.
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS cdm.tmp_vcv_concept_lk;

CREATE TABLE cdm.tmp_vcv_concept_lk
(
    value_code_value                varchar(500),
    vcv_source_concept_id           bigint,
    vcv_concept_id                  bigint,
    vcv_domain_id                   varchar(50),
    PRIMARY KEY (value_code_value)
);

INSERT INTO cdm.tmp_vcv_concept_lk
SELECT DISTINCT
    meas.value_code_value                           AS value_code_value,
    vcv1.concept_id                                 AS vcv_source_concept_id,
    vcv2.concept_id                                 AS vcv_concept_id,
    COALESCE(vcv2.domain_id, vcv2.domain_id)        AS vcv_domain_id
FROM cdm.src_meas meas
LEFT JOIN voc.concept vcv1
    ON meas.value_code_value = vcv1.concept_code
    AND vcv1.vocabulary_id = 'PPI'
LEFT JOIN voc.concept_relationship vcrv1
    ON vcv1.concept_id = vcrv1.concept_id_1
    AND vcrv1.relationship_id = 'Maps to'
    AND vcrv1.invalid_reason IS NULL
LEFT JOIN voc.concept vcv2
    ON vcv2.concept_id = vcrv1.concept_id_2
    AND vcv2.standard_concept = 'S'
    AND vcv2.invalid_reason IS NULL
WHERE
    meas.value_code_value IS NOT NULL
;

-- -------------------------------------------------------------------
-- table: src_meas_mapped
-- Joins altogether patient measurements information in source and
-- cdm codes from 'tmp_cv_concept_lk' and 'tmp_vcv_concept_lk',
-- excluding notes, because notes will migrate to cdm.note.
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS cdm.src_meas_mapped;

CREATE TABLE cdm.src_meas_mapped
(
    participant_id              bigint NOT NULL,
    finalized_site_id           int,
    code_value                  varchar(255) NOT NULL,
    cv_source_concept_id        bigint,
    cv_concept_id               bigint,
    cv_domain_id                varchar(50),
    measurement_time            datetime NOT NULL,
    value_decimal               float,
    value_unit                  varchar(255),
    vu_concept_id               bigint,
    value_code_value            varchar(255),
    vcv_source_concept_id       bigint,
    vcv_concept_id              bigint,
    measurement_id              bigint,
    physical_measurements_id    int NOT NULL,
    parent_id                   bigint
);

INSERT INTO cdm.src_meas_mapped
SELECT
    meas.participant_id                         AS participant_id,
    meas.finalized_site_id                      AS finalized_site_id,
    meas.code_value                             AS code_value,
    COALESCE(tmp1.cv_source_concept_id, 0)      AS cv_source_concept_id,
    COALESCE(tmp1.cv_concept_id, 0)             AS cv_concept_id,
    tmp1.cv_domain_id                           AS cv_domain_id,
    meas.measurement_time                       AS measurement_time,
    meas.value_decimal                          AS value_decimal,
    meas.value_unit                             AS value_unit,
    COALESCE(vc1.concept_id, 0)                 AS vu_concept_id,
    meas.value_code_value                       AS value_code_value,
    COALESCE(tmp2.vcv_source_concept_id, 0)     AS vcv_source_concept_id,
    COALESCE(tmp2.vcv_concept_id, 0)            AS vcv_concept_id,
    meas.measurement_id                         AS measurement_id,
    meas.physical_measurements_id               AS physical_measurements_id,
    meas.parent_id                              AS parent_id
FROM cdm.src_meas meas
LEFT JOIN cdm.tmp_cv_concept_lk tmp1
    ON meas.code_value = tmp1.code_value
LEFT JOIN voc.concept vc1           -- here we map units of measurements to standard concepts
    ON meas.value_unit = vc1.concept_code
    AND vc1.vocabulary_id = 'UCUM'
    AND vc1.standard_concept = 'S'
    AND vc1.invalid_reason IS NULL
LEFT JOIN cdm.tmp_vcv_concept_lk tmp2
    ON meas.value_code_value = tmp2.value_code_value
WHERE
    meas.code_value <> 'notes'
;

alter table cdm.src_meas_mapped add key (physical_measurements_id);
alter table cdm.src_meas_mapped add key (measurement_id);
CREATE INDEX src_meas_pm_ids ON cdm.src_meas_mapped (physical_measurements_id, measurement_id);

-- -------------------------------------------------------------------
-- Drop Temporary Tables
-- -------------------------------------------------------------------
  DROP TABLE IF EXISTS cdm.tmp_cv_concept_lk;
  DROP TABLE IF EXISTS cdm.tmp_vcv_concept_lk;

-- -------------------------------------------------------------------
-- source_file: src/care_site.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: cdm.care_site
-- -------------------------------------------------------------------
TRUNCATE TABLE cdm.care_site;

DROP TABLE IF EXISTS cdm.tmp_care_site;
CREATE TABLE cdm.tmp_care_site LIKE cdm.care_site;
ALTER TABLE cdm.tmp_care_site DROP COLUMN id;

INSERT INTO cdm.tmp_care_site
SELECT DISTINCT
    site.site_id                            AS care_site_id,
    site.site_name                          AS care_site_name,
    0                                       AS place_of_service_concept_id,
    NULL                                    AS location_id,
    site.site_id                            AS care_site_source_value,
    NULL                                    AS place_of_service_source_value,
    'care_site'                             AS unit_id
FROM rdr.site site
;

SET @row_number = 0;
INSERT INTO cdm.care_site
SELECT
  (@row_number:=@row_number + 1)              AS id,
  cdm.tmp_care_site.*
FROM cdm.tmp_care_site;

DROP TABLE IF EXISTS cdm.tmp_care_site;

-- -------------------------------------------------------------------
-- source_file: src/visit_occurrence.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: cdm.tmp_visits_src.
-- 'physical_measurements_id' is an id of a set of measurements during
-- one patient's observation, so we use it as 'visit_occurence_id'.
-- Min and Max measurements time over one physical_measurements_id
-- is used as start and end visit dates.
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS cdm.tmp_visits_src;

CREATE TABLE cdm.tmp_visits_src
(
    visit_occurrence_id bigint NOT NULL,
    person_id bigint NOT NULL,
    visit_start_datetime datetime NOT NULL,
    visit_end_datetime datetime NOT NULL,
    care_site_id bigint,
    PRIMARY KEY (visit_occurrence_id)
);

INSERT INTO cdm.tmp_visits_src
SELECT
    src_meas.physical_measurements_id       AS visit_occurrence_id,
    src_meas.participant_id                 AS person_id,
    MIN(src_meas.measurement_time)          AS visit_start_datetime,
    MAX(src_meas.measurement_time)          AS visit_end_datetime,
    src_meas.finalized_site_id              AS care_site_id
FROM cdm.src_meas src_meas
GROUP BY
    src_meas.physical_measurements_id,
    src_meas.participant_id,
    src_meas.finalized_site_id
;

-- -------------------------------------------------------------------
-- table: cdm.visit_occurrence
-- -------------------------------------------------------------------
TRUNCATE TABLE cdm.visit_occurrence;

-- -------------------------------------------------------------------
-- Here we form visit_occurence table from 'tmp_visits_src'
-- -------------------------------------------------------------------
SET @row_number = 0;
INSERT INTO cdm.visit_occurrence
SELECT
    (@row_number:=@row_number + 1)          AS id,
    src.visit_occurrence_id                 AS visit_occurrence_id,
    src.person_id                           AS person_id,
    9202                                    AS visit_concept_id, -- 9202 - 'Outpatient Visit'
    DATE(src.visit_start_datetime)          AS visit_start_date,
    src.visit_start_datetime                AS visit_start_datetime,
    DATE(src.visit_end_datetime)            AS visit_end_date,
    src.visit_end_datetime                  AS visit_end_datetime,
    44818519                                AS visit_type_concept_id, -- 44818519 - 'Clinical Study Visit'
    NULL                                    AS provider_id,
    src.care_site_id                        AS care_site_id,
    src.visit_occurrence_id                 AS visit_source_value,
    0                                       AS visit_source_concept_id,
    0                                       AS admitting_source_concept_id,
    NULL                                    AS admitting_source_value,
    0                                       AS discharge_to_concept_id,
    NULL                                    AS discharge_to_source_value,
    NULL                                    AS preceding_visit_occurrence_id,
    'vis.meas'                              AS unit_id
FROM cdm.tmp_visits_src src
;

-- -------------------------------------------------------------------
-- Drop Temporary Tables
-- -------------------------------------------------------------------
  DROP TABLE IF EXISTS cdm.tmp_visits_src;

-- -------------------------------------------------------------------
-- source_file: src/observation.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: cdm.observation
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- units: observ.code, observ.str, observ.num, observ.bool
-- 'observation' table consists of 2 parts:
-- 1) patient's questionnaries
-- 2) patient's observations from measurements
-- First part we fill from 'src_mapped', second -
-- from 'src_meas_mapped'
-- -------------------------------------------------------------------
TRUNCATE TABLE cdm.observation;

SET @row_number = 0;
INSERT INTO cdm.observation
SELECT
    (@row_number:=@row_number + 1)              AS id,
    NULL                                        AS observation_id,
    src_m.participant_id                        AS person_id,
    src_m.question_concept_id                   AS observation_concept_id,
    DATE(src_m.date_of_survey)                  AS observation_date,
    src_m.date_of_survey                        AS observation_datetime,
    45905771                                    AS observation_type_concept_id, -- 45905771, Observation Recorded from a Survey
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
WHERE src_m.question_ppi_code is not null
;

-- -------------------------------------------------------------------
-- unit: observ.meas - observations from measurement table
-- -------------------------------------------------------------------
INSERT INTO cdm.observation
SELECT
    (@row_number:=@row_number + 1)          AS id,
    NULL                                    AS observation_id,
    meas.participant_id                     AS person_id,
    meas.cv_concept_id                      AS observation_concept_id,
    DATE(meas.measurement_time)             AS observation_date,
    meas.measurement_time                   AS observation_datetime,
    581413                                  AS observation_type_concept_id,   -- 581413, Observation from Measurement
    NULL                                    AS value_as_number,
    NULL                                    AS value_as_string,
    meas.vcv_concept_id                     AS value_as_concept_id,
    0                                       AS qualifier_concept_id,
    meas.vu_concept_id                      AS unit_concept_id,
    NULL                                    AS provider_id,
    meas.physical_measurements_id           AS visit_occurrence_id,
    meas.code_value                         AS observation_source_value,
    meas.cv_source_concept_id               AS observation_source_concept_id,
    meas.value_unit                         AS unit_source_value,
    NULL                                    AS qualifier_source_value,
    meas.vcv_source_concept_id              AS value_source_concept_id,
    meas.value_code_value                   AS value_source_value,
    NULL                                    AS questionnaire_response_id,
    meas.measurement_id                     AS meas_id,
    'observ.meas'                           AS unit_id
FROM cdm.src_meas_mapped meas
WHERE
    meas.cv_domain_id = 'Observation'
;

ALTER TABLE cdm.observation ADD KEY (meas_id);
-- remove special character
update cdm.observation set value_as_string = replace(value_as_string, '\0', '') where value_as_string like '%\0%';

-- -------------------------------------------------------------------
-- source_file: src/measurement.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: cdm.measurement
-- -------------------------------------------------------------------
TRUNCATE TABLE cdm.measurement;

-- -------------------------------------------------------------------
-- unit: meas.dec   - measurements represented as decimal values
-- unit: meas.value - measurements represented as value_code_value
-- unit: meas.empty - measurements with empty value_decimal and value_code_value fields
-- 'measurement' table is filled ftom src_meas_mapped table only.
-- -------------------------------------------------------------------
SET @row_number = 0;
INSERT INTO cdm.measurement
SELECT
    (@row_number:=@row_number + 1)          AS id,
    meas.measurement_id                     AS measurement_id,
    meas.participant_id                     AS person_id,
    meas.cv_concept_id                      AS measurement_concept_id,
    DATE(meas.measurement_time)             AS measurement_date,
    meas.measurement_time                   AS measurement_datetime,
    44818701                                AS measurement_type_concept_id, -- 44818701, From physical examination
    0                                       AS operator_concept_id,
    meas.value_decimal                      AS value_as_number,
    meas.vcv_concept_id                     AS value_as_concept_id,
    meas.vu_concept_id                      AS unit_concept_id,
    NULL                                    AS range_low,
    NULL                                    AS range_high,
    NULL                                    AS provider_id,
    meas.physical_measurements_id           AS visit_occurrence_id,
    meas.code_value                         AS measurement_source_value,
    meas.cv_source_concept_id               AS measurement_source_concept_id,
    meas.value_unit                         AS unit_source_value,
    CASE
        WHEN meas.value_decimal IS NOT NULL OR meas.value_unit IS NOT NULL
            THEN CONCAT(COALESCE(meas.value_decimal, ''), ' ',
                COALESCE(meas.value_unit, ''))     -- 'meas.dec'
        WHEN meas.value_code_value IS NOT NULL
            THEN meas.value_code_value             -- 'meas.value'
        ELSE NULL                                  -- 'meas.empty'
    END                                     AS value_source_value,
    meas.parent_id                          AS parent_id,
    CASE
        WHEN meas.value_decimal IS NOT NULL OR meas.value_unit IS NOT NULL
            THEN 'meas.dec'
        WHEN meas.value_code_value IS NOT NULL
            THEN 'meas.value'
        ELSE 'meas.empty'
    END                                     AS unit_id
FROM cdm.src_meas_mapped meas
WHERE
    meas.cv_domain_id = 'Measurement' OR meas.cv_domain_id IS NULL
;

CREATE INDEX measurement_idx ON cdm.measurement (person_id, measurement_date, measurement_datetime, parent_id);

-- -------------------------------------------------------------------
-- source_file: src/note.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: note
-- -------------------------------------------------------------------
TRUNCATE TABLE cdm.note;

SET @row_number = 0;
INSERT INTO cdm.note
SELECT
    (@row_number:=@row_number + 1)          AS id,
    NULL                                    AS note_id,
    meas.participant_id                     AS person_id,
    DATE(meas.measurement_time)             AS note_date,
    meas.measurement_time                   AS note_datetime,
    44814645                                AS note_type_concept_id,    -- 44814645 - 'Note'
    0                                       AS note_class_concept_id,
    NULL                                    AS note_title,
    COALESCE(meas.value_string, '')         AS note_text,
    0                                       AS encoding_concept_id,
    4180186                                 AS language_concept_id,     -- 4180186 - 'English language'
    NULL                                    AS provider_id,
    meas.code_value                         AS note_source_value,
    meas.physical_measurements_id           AS visit_occurrence_id,
    'note'                                  AS unit_id
FROM cdm.src_meas meas
WHERE
    meas.code_value = 'notes'
;

-- -------------------------------------------------------------------
-- source_file: src/observation_period.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: observation_period
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- tmp_obs_target contains dates of all person's clinical events
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS cdm.temp_obs_target;
CREATE TABLE cdm.temp_obs_target
(
    person_id                   bigint,
    start_date                  date,
    end_date                    date
);

INSERT INTO cdm.temp_obs_target
-- VISIT_OCCURENCE
SELECT
    person_id,
    visit_start_date                               AS start_date,
    COALESCE(visit_end_date, visit_start_date) AS end_date
FROM cdm.visit_occurrence

UNION
-- CONDITION_OCCURRENCE
SELECT
    person_id,
    condition_start_date                                   AS start_date,
    COALESCE(condition_end_date, condition_start_date) AS end_date
FROM cdm.condition_occurrence

UNION
-- PROCEDURE_OCCURRENCE
SELECT
    person_id,
    procedure_date                    AS start_date,
    procedure_date                    AS end_date
FROM cdm.procedure_occurrence

UNION
-- OBSERVATION
SELECT
    person_id,
    observation_date                    AS start_date,
    observation_date                    AS end_date
FROM cdm.observation

UNION
-- MEASUREMENT
SELECT
    person_id,
    measurement_date                    AS start_date,
    measurement_date                    AS end_date
FROM cdm.measurement

UNION
-- DEVICE_EXPOSURE
SELECT
    person_id,
    device_exposure_start_date                                          AS start_date,
    COALESCE( device_exposure_end_date, device_exposure_start_date) AS end_date
FROM cdm.device_exposure

UNION
-- DRUG_EXPOSURE
SELECT
    person_id,
    drug_exposure_start_date                                        AS start_date,
    COALESCE( drug_exposure_end_date, drug_exposure_start_date) AS end_date
FROM cdm.drug_exposure
;

CREATE INDEX temp_obs_target_idx_start ON cdm.temp_obs_target (person_id, start_date);
CREATE INDEX temp_obs_target_idx_end ON cdm.temp_obs_target (person_id, end_date);

-- -----------------------------------------------------------------------------------
-- In 'temp_obs_end_union' we number observations from 'tmp_obs_target' by start_date.
-- start_ordinal column contains number of start patient's observation events: first
-- is 1, subsequent is 2, 3 and so on.
-- End person observation events contains null in start_ordinal column.
-- It is necessary for finding possibly intersecting patient observations intervals.
-- -----------------------------------------------------------------------------------
DROP TABLE IF EXISTS cdm.temp_obs_end_union;
CREATE TABLE cdm.temp_obs_end_union
(
    person_id               bigint,
    event_date              date,
    event_type              int,
    start_ordinal           int
);

SELECT NULL INTO @partition_expr;
SELECT NULL INTO @last_part_expr;
SELECT NULL INTO @row_number;
SELECT NULL INTO @reset_num;

INSERT INTO cdm.temp_obs_end_union
SELECT
  person_id                   AS person_id,
  start_date                  AS event_date,
  -1                          AS event_type,
  row_number                  AS start_ordinal
FROM
    ( SELECT
        @partition_expr := person_id                                AS partition_expr,
        @reset_num :=
            CASE
                WHEN @partition_expr = @last_part_expr THEN 0
                ELSE 1
            END                                                     AS reset_num,
        @last_part_expr := @partition_expr                          AS last_part_expr,
        @row_number :=
            CASE
                WHEN @reset_num = 0 THEN @row_number + 1
                ELSE 1
            END                                                     AS row_number,
        person_id,
        start_date
      FROM cdm.temp_obs_target
      ORDER BY
        person_id,
        start_date
    ) F
UNION ALL
SELECT
  person_id                         AS person_id,
  (end_date + INTERVAL 1 DAY)       AS event_date,
  1                                 AS event_type,
  NULL                              AS start_ordinal
FROM cdm.temp_obs_target
  ;

DROP TABLE IF EXISTS cdm.temp_obs_end_union_part;
CREATE TABLE cdm.temp_obs_end_union_part
(
    person_id                       bigint,
    event_date                      date,
    event_type                      int,
    start_ordinal                   int,
    overall_ord                     int
);

SELECT NULL INTO @partition_expr;
SELECT NULL INTO @last_part_expr;
SELECT NULL INTO @row_number;
SELECT NULL INTO @reset_num;
SELECT NULL INTO @row_max;

-- ----------------------------------------------------------------------------------------
-- We need to re-count event ordinal number in 'overall_ord' and define null start_ordinal
-- by start_ordinal of start_event. So events, onwed by the same observation, will have
-- the same ordinal number - the start_ordinal of start observation event.
-- overall_ord is overall counter of start and end observations events.
-- ----------------------------------------------------------------------------------------
INSERT INTO cdm.temp_obs_end_union_part
SELECT
    person_id                            AS person_id,
    event_date                           AS event_date,
    event_type                           AS event_type,
    row_max                              AS start_ordinal,
    row_number                           AS overall_ord
FROM  (
        SELECT
            @partition_expr := person_id                                 AS partition_expr,
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
            @row_max :=
                CASE
                    WHEN @reset_num = 1 THEN start_ordinal
                    ELSE COALESCE(start_ordinal, @row_max)
                END                                                      AS row_max,
            person_id,
            event_date,
            event_type,
            start_ordinal
        FROM cdm.temp_obs_end_union
        ORDER BY
            person_id,
            event_date,
            event_type
            ) F
;

DROP TABLE IF EXISTS cdm.temp_obs_end;
CREATE TABLE cdm.temp_obs_end
(
    person_id                   bigint,
    end_date                    date,
    start_ordinal               int,
    overall_ord                  int
);

-- --------------------------------------------------------------------
-- Here we just filter observations ends. As start_ordinal of start and
-- end events is the same, expression
-- (2 * start_ordinal) == e.overall_ord gives us observation end event.
-- --------------------------------------------------------------------
INSERT INTO  cdm.temp_obs_end
SELECT
    person_id                                     AS person_id,
    (event_date - INTERVAL 1 DAY)                 AS end_date,
    start_ordinal                                 AS start_ordinal,
    overall_ord                                   AS overall_ord
FROM cdm.temp_obs_end_union_part e
WHERE
    (2 * e.start_ordinal) - e.overall_ord = 0
;

CREATE INDEX temp_obs_end_idx ON cdm.temp_obs_end (person_id, end_date);

DROP TABLE IF EXISTS cdm.temp_obs;
CREATE TABLE cdm.temp_obs
(
    person_id                       bigint,
    observation_start_date          date,
    observation_end_date            date
);

-- -------------------------------------------------------------------
-- Here we form observations start and end dates. For each start_date
-- we look for minimal end_date for the particular person observation.
-- -------------------------------------------------------------------
INSERT INTO cdm.temp_obs
SELECT
    dt.person_id,
    dt.start_date                 AS observation_start_date,
    MIN(e.end_date)               AS observation_end_date
FROM cdm.temp_obs_target dt
JOIN cdm.temp_obs_end e
    ON dt.person_id = e.person_id AND
    e.end_date >= dt.start_date
GROUP BY
    dt.person_id,
    dt.start_date
;

CREATE INDEX temp_obs_idx ON cdm.temp_obs (person_id, observation_end_date);

TRUNCATE TABLE cdm.observation_period;

-- -------------------------------------------------------------------
-- observation_period is formed as merged possibly intersecting
-- tmp_obs intervals
-- -------------------------------------------------------------------
SET @row_number = 0;
INSERT INTO cdm.observation_period
SELECT
    (@row_number:=@row_number + 1)          AS id,
    NULL                                    AS observation_period_id,
    person_id                               AS person_id,
    MIN(observation_start_date)             AS observation_period_start_date,
    observation_end_date                    AS observation_period_end_date,
    44814725                                AS period_type_concept_id,         -- 44814725, Period inferred by algorithm
    'observ_period'                       AS unit_id
FROM cdm.temp_obs
GROUP BY
    person_id,
    observation_end_date
  ;

-- -------------------------------------------------------------------
-- Drop Temporary Tables
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS cdm.temp_cdm_observation_period;
DROP TABLE IF EXISTS cdm.temp_obs_target;
DROP TABLE IF EXISTS cdm.temp_obs_end_union;
DROP TABLE IF EXISTS cdm.temp_obs_end;
DROP TABLE IF EXISTS cdm.temp_obs_end_union_part;
DROP TABLE IF EXISTS cdm.temp_obs;

-- -------------------------------------------------------------------
-- source_file: src/fact_relationship.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: cdm.fact_relationship
-- -------------------------------------------------------------------
TRUNCATE TABLE cdm.fact_relationship;

-- -------------------------------------------------------------------
-- Insert to fact_relationships measurement-to-observation relations
-- -------------------------------------------------------------------
SET @row_number = 0;
INSERT INTO cdm.fact_relationship
SELECT
    (@row_number:=@row_number + 1)  AS id,
    21                              AS domain_concept_id_1,     -- Measurement
    mtq.measurement_id              AS fact_id_1,
    27                              AS domain_concept_id_2,     -- Observation
    cdm_obs.observation_id          AS fact_id_2,
    581411                          AS relationship_concept_id, -- Measurement to Observation
    'observ.meas1'                  AS unit_id
FROM cdm.observation cdm_obs
INNER JOIN rdr.measurement_to_qualifier mtq
    ON mtq.qualifier_id = cdm_obs.meas_id
;

INSERT INTO cdm.fact_relationship
SELECT
    (@row_number:=@row_number + 1)  AS id,
    27                              AS domain_concept_id_1,     -- Observation
    cdm_obs.observation_id          AS fact_id_1,
    21                              AS domain_concept_id_2,     -- Measurement
    mtq.measurement_id              AS fact_id_2,
    581410                          AS relationship_concept_id, -- Observation to Measurement
    'observ.meas2'                  AS unit_id
FROM cdm.observation cdm_obs
INNER JOIN rdr.measurement_to_qualifier mtq
    ON mtq.qualifier_id = cdm_obs.meas_id
;

-- -------------------------------------------------------------------
-- unit: syst.diast[1,2] - to link systolic and diastolic blood pressure
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS cdm.tmp_fact_rel_sd;

-- -------------------------------------------------------------------
-- tmp_fact_rel_sd contains blood pressure measurements
-- -------------------------------------------------------------------
CREATE TABLE cdm.tmp_fact_rel_sd
(
    measurement_id bigint NOT NULL,
    systolic_blood_pressure_ind int NOT NULL,
    diastolic_blood_pressure_ind int NOT NULL,
    person_id bigint NOT NULL,
    parent_id bigint
);

-- -----------------------------------------------------------------------------------------------------------------
-- temporary table for populating cdm_fact_relationship table from systolic and diastolic blood pressure measurements
-- -----------------------------------------------------------------------------------------------------------------

INSERT INTO cdm.tmp_fact_rel_sd
SELECT
    m.measurement_id                                            AS measurement_id,
    CASE
        WHEN m.measurement_source_value = 'blood-pressure-systolic-1'     THEN 1
        WHEN m.measurement_source_value = 'blood-pressure-systolic-2'     THEN 2
        WHEN m.measurement_source_value = 'blood-pressure-systolic-3'     THEN 3
        WHEN m.measurement_source_value = 'blood-pressure-systolic-mean'  THEN 4
        ELSE 0
    END                                                         AS systolic_blood_pressure_ind,
    CASE
        WHEN m.measurement_source_value = 'blood-pressure-diastolic-1'    THEN 1
        WHEN m.measurement_source_value = 'blood-pressure-diastolic-2'    THEN 2
        WHEN m.measurement_source_value = 'blood-pressure-diastolic-3'    THEN 3
        WHEN m.measurement_source_value = 'blood-pressure-diastolic-mean' THEN 4
        ELSE 0
    END                                                         AS diastolic_blood_pressure_ind,
    m.person_id                                                 AS person_id,
    m.parent_id                                                 AS parent_id

FROM cdm.measurement m
WHERE
    m.measurement_source_value IN (
        'blood-pressure-systolic-1', 'blood-pressure-systolic-2',
        'blood-pressure-systolic-3', 'blood-pressure-systolic-mean',
        'blood-pressure-diastolic-1', 'blood-pressure-diastolic-2',
        'blood-pressure-diastolic-3', 'blood-pressure-diastolic-mean'
    )
    AND m.parent_id IS NOT NULL
;

ALTER TABLE cdm.tmp_fact_rel_sd ADD KEY (person_id, parent_id);

-- ---------------------------------------------------------------------------
-- unit: syst.diast.*[1,2] - to link systolic and diastolic blood pressure
-- Insert into fact_relationship table systolic to disatolic blood pressure
-- measurements relations
-- ---------------------------------------------------------------------------
INSERT INTO cdm.fact_relationship
SELECT
    (@row_number:=@row_number + 1)  AS id,
    21                          AS domain_concept_id_1,     -- Measurement
    tmp1.measurement_id         AS fact_id_1,               -- measurement_id of the first/second/third/mean systolic blood pressure
    21                          AS domain_concept_id_2,     -- Measurement
    tmp2.measurement_id         AS fact_id_2,               -- measurement_id of the first/second/third/mean diastolic blood pressure
    46233683                    AS relationship_concept_id, -- Systolic to diastolic blood pressure measurement
    CASE
      WHEN tmp1.systolic_blood_pressure_ind = 1 THEN 'syst.diast.first1'
      WHEN tmp1.systolic_blood_pressure_ind = 2 THEN 'syst.diast.second1'
      WHEN tmp1.systolic_blood_pressure_ind = 3 THEN 'syst.diast.third1'
      WHEN tmp1.systolic_blood_pressure_ind = 4 THEN 'syst.diast.mean1'
    END                         AS unit_id
FROM cdm.tmp_fact_rel_sd tmp1
INNER JOIN cdm.tmp_fact_rel_sd tmp2
    ON tmp1.person_id = tmp2.person_id
    AND tmp1.parent_id = tmp2.parent_id
    AND tmp1.systolic_blood_pressure_ind = tmp2.diastolic_blood_pressure_ind   -- get the same index to refer between
                                                                               -- first, second, third and mean blood pressurre measurements
WHERE tmp1.systolic_blood_pressure_ind != 0              -- take only systolic blood pressure measurements
    AND tmp2.diastolic_blood_pressure_ind != 0             -- take only diastolic blood pressure measurements
;

-- ----------------------------------------------------------------------------
-- Insert into fact_relationship diastolic to systolic blood pressure
-- measurements relation
-- ----------------------------------------------------------------------------
INSERT INTO cdm.fact_relationship
SELECT
    (@row_number:=@row_number + 1)  AS id,
    21                          AS domain_concept_id_1,     -- Measurement
    tmp2.measurement_id         AS fact_id_1,               -- measurement_id of the first/second/third/mean diastolic blood pressure
    21                          AS domain_concept_id_2,     -- Measurement
    tmp1.measurement_id         AS fact_id_2,               -- measurement_id of the first/second/third/mean systolic blood pressure
    46233682                    AS relationship_concept_id, -- Diastolic to systolic blood pressure measurement
    CASE
      WHEN tmp1.systolic_blood_pressure_ind = 1 THEN 'syst.diast.first2'
      WHEN tmp1.systolic_blood_pressure_ind = 2 THEN 'syst.diast.second2'
      WHEN tmp1.systolic_blood_pressure_ind = 3 THEN 'syst.diast.third2'
      WHEN tmp1.systolic_blood_pressure_ind = 4 THEN 'syst.diast.mean2'
    END                         AS unit_id
FROM cdm.tmp_fact_rel_sd tmp1
INNER JOIN cdm.tmp_fact_rel_sd tmp2
    ON tmp1.person_id = tmp2.person_id
    AND tmp1.parent_id = tmp2.parent_id
    AND tmp1.systolic_blood_pressure_ind = tmp2.diastolic_blood_pressure_ind   -- get the same index to refer between
                                                                               -- first, second, third and mean blood pressurre measurements
WHERE tmp1.systolic_blood_pressure_ind != 0              -- take only systolic blood pressure measurements
    AND tmp2.diastolic_blood_pressure_ind != 0             -- take only diastolic blood pressure measurements
;

-- ---------------------------------------------------------------------
-- Insert into fact_relationship child-to-parent measurements relations
-- ---------------------------------------------------------------------
INSERT INTO cdm.fact_relationship
SELECT
    (@row_number:=@row_number + 1)  AS id,
    21                              AS domain_concept_id_1,     -- Measurement
    cdm_meas.measurement_id         AS fact_id_1,
    21                              AS domain_concept_id_2,     -- Measurement
    cdm_meas.parent_id              AS fact_id_2,
    581437                          AS relationship_concept_id, -- 581437, Child to Parent Measurement
    'meas.meas1'                    AS unit_id
FROM cdm.measurement cdm_meas
WHERE cdm_meas.parent_id IS NOT NULL;

INSERT INTO cdm.fact_relationship
SELECT
    (@row_number:=@row_number + 1)  AS id,
    21                              AS domain_concept_id_1,     -- Measurement
    cdm_meas.parent_id              AS fact_id_1,
    21                              AS domain_concept_id_2,     -- Measurement
    cdm_meas.measurement_id         AS fact_id_2,
    581436                          AS relationship_concept_id, -- 581436, Parent to Child Measurement
    'meas.meas2'                    AS unit_id
FROM cdm.measurement cdm_meas
WHERE cdm_meas.parent_id IS NOT NULL;


DROP TABLE IF EXISTS cdm.pid_rid_mapping;
CREATE TABLE cdm.pid_rid_mapping (
    participant_id              bigint,
    research_id                 bigint
);
INSERT INTO cdm.pid_rid_mapping SELECT DISTINCT participant_id, research_id FROM cdm.src_clean;

-- -------------------------------------------------------------------
-- Drop Temporary Tables
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS cdm.tmp_fact_rel_sd;

-- -------------------------------------------------------------------
-- source_file: cdm_cleanup.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Drop columns only used for ETL purposes
-- -------------------------------------------------------------------

ALTER TABLE cdm.care_site DROP COLUMN unit_id, DROP COLUMN id;
ALTER TABLE cdm.condition_era DROP COLUMN unit_id, DROP COLUMN id;
ALTER TABLE cdm.condition_occurrence DROP COLUMN unit_id, DROP COLUMN id;
ALTER TABLE cdm.cost DROP COLUMN unit_id, DROP COLUMN id;
ALTER TABLE cdm.death DROP COLUMN unit_id, DROP COLUMN id;
ALTER TABLE cdm.device_exposure DROP COLUMN unit_id, DROP COLUMN id;
ALTER TABLE cdm.dose_era DROP COLUMN unit_id, DROP COLUMN id;
ALTER TABLE cdm.drug_era DROP COLUMN unit_id, DROP COLUMN id;
ALTER TABLE cdm.drug_exposure DROP COLUMN unit_id, DROP COLUMN id;
ALTER TABLE cdm.fact_relationship DROP COLUMN unit_id, DROP COLUMN id;
ALTER TABLE cdm.location DROP COLUMN unit_id, DROP COLUMN id;
ALTER TABLE cdm.measurement DROP COLUMN unit_id, DROP COLUMN parent_id, DROP COLUMN id;
ALTER TABLE cdm.observation DROP COLUMN unit_id, DROP COLUMN meas_id, DROP COLUMN id;
ALTER TABLE cdm.observation_period DROP COLUMN unit_id, DROP COLUMN id;
ALTER TABLE cdm.payer_plan_period DROP COLUMN unit_id, DROP COLUMN id;
ALTER TABLE cdm.person DROP COLUMN unit_id, DROP COLUMN id;
ALTER TABLE cdm.procedure_occurrence DROP COLUMN unit_id, DROP COLUMN id;
ALTER TABLE cdm.provider DROP COLUMN unit_id, DROP COLUMN id;
ALTER TABLE cdm.visit_occurrence DROP COLUMN unit_id, DROP COLUMN id;
