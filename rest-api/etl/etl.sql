-- -------------------------------------------------------------------
-- @2015-2017, Odysseus Data Services, Inc. All rights reserved
-- PPI OMOP CDM Conversion
-- last updated September 22, 2017
-- -------------------------------------------------------------------

-- -----------------------------------------------
-- re-create tables for cdm schema
-- -----------------------------------------------
USE cdm;


-- ---------------------------------------------------------------------------------------------------------------------------
-- Preliminary step to remove duplicated infofrmation while joining to standard vocabularies.
-- Problem: the same concept_code's represented with the spaces after and without. Mysql removes spaces in the join statement.
-- We need this step to avoid duplicated infromation.
-- ---------------------------------------------------------------------------------------------------------------------------
Delete from voc.concept
WHERE concept_id IN (1585549, 1585565, 1585548);

commit;

-- -------------------------------------------------------------------
-- source_file: ddl_cdm.sql
-- -------------------------------------------------------------------

-- -----------------------------------------------
-- location
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.location;

CREATE TABLE cdm.location
(
    location_id bigint AUTO_INCREMENT NOT NULL,
    address_1 varchar(50),
    address_2 varchar(50),
    city varchar(50),
    state varchar(2),
    zip varchar(9),
    county varchar(20),
    location_source_value varchar(50),
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (location_id)
);

-- -----------------------------------------------
-- care_site
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.care_site;

CREATE TABLE cdm.care_site
(
    care_site_id bigint NOT NULL,
    care_site_name varchar(255),
    place_of_service_concept_id bigint NOT NULL,
    location_id bigint,
    care_site_source_value varchar(50) NOT NULL,
    place_of_service_source_value varchar(50),
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (care_site_id)
);

-- -----------------------------------------------
-- provider
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.provider;

CREATE TABLE cdm.provider
(
    provider_id bigint AUTO_INCREMENT NOT NULL,
    provider_name varchar(50),
    npi varchar(20),
    dea varchar(20),
    specialty_concept_id bigint NOT NULL,
    care_site_id bigint,
    year_of_birth int,
    gender_concept_id bigint NOT NULL,
    provider_source_value varchar(50) NOT NULL,
    specialty_source_value varchar(50),
    specialty_source_concept_id bigint NOT NULL,
    gender_source_value varchar(50),
    gender_source_concept_id bigint NOT NULL,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (provider_id)
);

-- -----------------------------------------------
-- person
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.person;

CREATE TABLE cdm.person
(
    person_id bigint NOT NULL,
    gender_concept_id bigint NOT NULL,
    year_of_birth int NOT NULL,
    month_of_birth int,
    day_of_birth int,
    birth_datetime datetime,
    race_concept_id bigint NOT NULL,
    ethnicity_concept_id bigint NOT NULL,
    location_id bigint,
    provider_id bigint,
    care_site_id bigint,
    person_source_value varchar(50) NOT NULL,
    gender_source_value varchar(50),
    gender_source_concept_id bigint NOT NULL,
    race_source_value varchar(50),
    race_source_concept_id bigint NOT NULL,
    ethnicity_source_value varchar(50),
    ethnicity_source_concept_id bigint NOT NULL,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (person_id)
);

-- -----------------------------------------------
-- death
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.death;

CREATE TABLE cdm.death
(
    person_id bigint NOT NULL,
    death_date date NOT NULL,
    death_datetime datetime,
    death_type_concept_id bigint NOT NULL,
    cause_concept_id bigint NOT NULL,
    cause_source_value varchar(50),
    cause_source_concept_id bigint NOT NULL,
    unit_id varchar(50) NOT NULL
);

-- -----------------------------------------------
-- observation_period
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.observation_period;

CREATE TABLE cdm.observation_period
(
    observation_period_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    observation_period_start_date date NOT NULL,
    observation_period_end_date date NOT NULL,
    period_type_concept_id bigint NOT NULL,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (observation_period_id)
);

-- -----------------------------------------------
-- payer_plan_period
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.payer_plan_period;

CREATE TABLE cdm.payer_plan_period
(
    payer_plan_period_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    payer_plan_period_start_date date NOT NULL,
    payer_plan_period_end_date date NOT NULL,
    payer_source_value varchar(50),
    plan_source_value varchar(50),
    family_source_value varchar(50),
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (payer_plan_period_id)
);

-- -----------------------------------------------
-- visit_occurrence
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.visit_occurrence;

CREATE TABLE cdm.visit_occurrence
(
    visit_occurrence_id bigint NOT NULL,
    person_id bigint NOT NULL,
    visit_concept_id bigint NOT NULL,
    visit_start_date date NOT NULL,
    visit_start_datetime datetime NOT NULL,
    visit_end_date date NOT NULL,
    visit_end_datetime datetime NOT NULL,
    visit_type_concept_id bigint NOT NULL,
    provider_id bigint,
    care_site_id bigint,
    visit_source_value varchar(150),
    visit_source_concept_id bigint NOT NULL,
    admitting_source_concept_id bigint NOT NULL,
    admitting_source_value varchar(50),
    discharge_to_concept_id bigint NOT NULL,
    discharge_to_source_value varchar(50),
    preceding_visit_occurrence_id bigint,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (visit_occurrence_id)
);

-- -----------------------------------------------
-- condition_occurrence
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.condition_occurrence;

CREATE TABLE cdm.condition_occurrence
(
    condition_occurrence_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    condition_concept_id bigint NOT NULL,
    condition_start_date date NOT NULL,
    condition_start_datetime datetime,
    condition_end_date date,
    condition_end_datetime datetime,
    condition_type_concept_id bigint NOT NULL,
    stop_reason varchar(20),
    provider_id bigint,
    visit_occurrence_id bigint,
    condition_source_value varchar(50) NOT NULL,
    condition_source_concept_id bigint NOT NULL,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (condition_occurrence_id)
);

-- -----------------------------------------------
-- procedure_occurrence
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.procedure_occurrence;

CREATE TABLE cdm.procedure_occurrence
(
    procedure_occurrence_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    procedure_concept_id bigint NOT NULL,
    procedure_date date NOT NULL,
    procedure_datetime datetime,
    procedure_type_concept_id bigint NOT NULL,
    modifier_concept_id bigint NOT NULL,
    quantity int,
    provider_id bigint,
    visit_occurrence_id bigint,
    procedure_source_value varchar(1024) NOT NULL,
    procedure_source_concept_id bigint NOT NULL,
    qualifier_source_value varchar(50),
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (procedure_occurrence_id)
);

-- -----------------------------------------------
-- observation
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.observation;

CREATE TABLE cdm.observation
(
    observation_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    observation_concept_id bigint NOT NULL,
    observation_date date NOT NULL,
    observation_datetime datetime,
    observation_type_concept_id bigint NOT NULL,
    value_as_number double,
    value_as_string varchar(1024),
    value_as_concept_id bigint NOT NULL,
    qualifier_concept_id bigint NOT NULL,
    unit_concept_id bigint NOT NULL,
    provider_id bigint,
    visit_occurrence_id bigint,
    observation_source_value varchar(255) NOT NULL,
    observation_source_concept_id bigint NOT NULL,
    unit_source_value varchar(50),
    qualifier_source_value varchar(50),
    -- specific to this ETL
    value_source_concept_id bigint,
    value_source_value varchar(255),
    questionnaire_response_id bigint,
    meas_id bigint,
    --
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (observation_id)
);

-- -----------------------------------------------
-- measurement
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.measurement;

CREATE TABLE cdm.measurement
(
    measurement_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    measurement_concept_id bigint NOT NULL,
    measurement_date date NOT NULL,
    measurement_datetime datetime,
    measurement_type_concept_id bigint NOT NULL,
    operator_concept_id bigint NOT NULL,
    value_as_number double,
    value_as_concept_id bigint NOT NULL,
    unit_concept_id bigint NOT NULL,
    range_low double,
    range_high double,
    provider_id bigint,
    visit_occurrence_id bigint,
    measurement_source_value varchar(50),
    measurement_source_concept_id bigint NOT NULL,
    unit_source_value varchar(50),
    -- specific for this ETL
    value_source_value varchar(50),
    meas_id bigint,
    parent_id bigint,
    -- 
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (measurement_id)
);


-- -----------------------------------------------
-- note
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.note;

CREATE TABLE cdm.note 
( 
    note_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    note_date date NOT NULL,
    note_datetime datetime,
    note_type_concept_id bigint NOT NULL,
    note_class_concept_id bigint NOT NULL,
    note_title varchar(250),
    note_text text NOT NULL,
    encoding_concept_id bigint NOT NULL,
    language_concept_id bigint NOT NULL,
    provider_id bigint,
    note_source_value varchar(50),
    visit_occurrence_id bigint,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (note_id)
);

-- -----------------------------------------------
-- drug_exposure
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.drug_exposure;

CREATE TABLE cdm.drug_exposure
(
    drug_exposure_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    drug_concept_id bigint NOT NULL,
    drug_exposure_start_date date NOT NULL,
    drug_exposure_start_datetime datetime,
    drug_exposure_end_date date,
    drug_exposure_end_datetime datetime,
    drug_type_concept_id bigint NOT NULL,
    stop_reason varchar(20),
    refills int,
    quantity double,
    days_supply int,
    sig varchar(1024),
    route_concept_id bigint,
    lot_number varchar(50),
    provider_id bigint,
    visit_occurrence_id bigint,
    drug_source_value varchar(50) NOT NULL,
    drug_source_concept_id bigint,
    route_source_value varchar(50),
    dose_unit_source_value varchar(50),
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (drug_exposure_id)
);

-- -----------------------------------------------
-- device_exposure
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.device_exposure;

CREATE TABLE cdm.device_exposure
(
    device_exposure_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    device_concept_id bigint NOT NULL,
    device_exposure_start_date date NOT NULL,
    device_exposure_start_datetime datetime,
    device_exposure_end_date date,
    device_exposure_end_datetime datetime,
    device_type_concept_id bigint NOT NULL,
    unique_device_id varchar(50),
    quantity double,
    provider_id bigint,
    visit_occurrence_id bigint,
    device_source_value varchar(50) NOT NULL,
    device_source_concept_id bigint,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (device_exposure_id)
);

-- -----------------------------------------------
-- cost
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.cost;

CREATE TABLE cdm.cost
(
    cost_id bigint AUTO_INCREMENT NOT NULL,
    cost_event_id bigint NOT NULL,
    cost_domain_id varchar(20) NOT NULL,
    cost_type_concept_id bigint NOT NULL,
    currency_concept_id bigint NOT NULL,
    total_charge double,
    total_cost double,
    total_paid double,
    paid_by_payer double,
    paid_by_patient double,
    paid_patient_copay double,
    paid_patient_coinsurance double,
    paid_patient_deductible double,
    paid_by_primary double,
    paid_ingredient_cost double,
    paid_dispensing_fee double,
    payer_plan_period_id bigint,
    amount_allowed double,
    revenue_code_concept_id bigint NOT NULL,
    revenue_code_source_value varchar(50),
    drg_concept_id bigint NOT NULL,
    drg_source_value varchar(50),
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (cost_id)
);

-- -----------------------------------------------
-- fact_relationship
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.fact_relationship;

CREATE TABLE cdm.fact_relationship
(
    domain_concept_id_1 int NOT NULL,
    fact_id_1 bigint NOT NULL,
    domain_concept_id_2 int NOT NULL,
    fact_id_2 bigint NOT NULL,
    relationship_concept_id bigint NOT NULL,
    unit_id varchar(50) NOT NULL
);

-- -----------------------------------------------
-- condition_era
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.condition_era;

CREATE TABLE cdm.condition_era
(
    condition_era_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    condition_concept_id bigint NOT NULL,
    condition_era_start_date date NOT NULL,
    condition_era_end_date date NOT NULL,
    condition_occurrence_count int,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (condition_era_id)
);

-- -----------------------------------------------
-- drug_era
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.drug_era;

CREATE TABLE cdm.drug_era
(
    drug_era_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    drug_concept_id bigint NOT NULL,
    drug_era_start_date date NOT NULL,
    drug_era_end_date date NOT NULL,
    drug_exposure_count int,
    gap_days int,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (drug_era_id)
);

-- -----------------------------------------------
-- dose_era
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.dose_era;

CREATE TABLE cdm.dose_era
(
    dose_era_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    drug_concept_id bigint NOT NULL,
    unit_concept_id bigint NOT NULL,
    dose_value double NOT NULL,
    dose_era_start_date date NOT NULL,
    dose_era_end_date date NOT NULL,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (dose_era_id)
);

-- -------------------------------------------------------------------
-- source_file: src/src_clean.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: src_clean
-- Contains persons observations
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
    value_number                double,
    value_boolean               tinyint,
    value_date                  datetime,
    value_string                varchar(1024),
    questionnaire_response_id   bigint,
    unit_id                     varchar(50)
);

-- -------------------------------------------------------------------
-- Rules all together
-- Takes data from source tables and puts together participant_id
-- and Q&A from quiestionnaire's.
-- -------------------------------------------------------------------

INSERT INTO cdm.src_clean
SELECT
    pa.participant_id               AS participant_id,
    qr.created                      AS date_of_survey,
    co_q.short_value                AS question_ppi_code,
    qq.code_id                      AS question_code_id,
    co_a.short_value                AS value_ppi_code,
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


ALTER TABLE cdm.src_clean ADD KEY (participant_id);

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
    value_number                double,
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
    AND vcr2.relationship_id = 'Maps to'
    AND vcr2.invalid_reason IS NULL
LEFT JOIN voc.concept vc4
    ON  vcr2.concept_id_2 = vc4.concept_id
    AND vc4.standard_concept = 'S'
    AND vc4.invalid_reason IS NULL
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
-- table scr_gender
-- Contains persons's gender in concept_id form.
-- Forms as answer to the gender question if all such
-- answers remains the same during all observations.
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
    MIN(src_m.value_ppi_code)       AS ppi_code,
    CASE
        WHEN MIN(src_m.value_ppi_code) = 'SexAtBirth_Male' THEN 8507
        WHEN MIN(src_m.value_ppi_code) = 'SexAtBirth_Female' THEN 8532
        ELSE 0
    END                             AS gender_concept_id
FROM cdm.src_mapped src_m
WHERE
    src_m.value_ppi_code IN ('SexAtBirth_Male', 'SexAtBirth_Female')
    AND NOT EXISTS (SELECT * FROM cdm.src_gender g
                    WHERE src_m.participant_id = g.person_id)
GROUP BY
    src_m.participant_id
HAVING count(distinct src_m.value_ppi_code) = 1
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
-- priority = 1 means more detailed racial or ethnicial
-- information over priority = 2. So if patient provides
-- detailed answer about his/her ethnicity, we firstly
-- use it.
---------------------------------------------------------
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
-- ethnicity info, if priority-1 info was not already
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

-- -------------------------------------------------------------------
-- table: cdm_person
-- Assembles person's birthday, gender, racial, ethnicity and
-- location information altogether from 'src_mapped', 'src_gender',
-- 'src_race', 'src_person_location' relations.
-- -------------------------------------------------------------------
TRUNCATE TABLE cdm.person;

INSERT INTO cdm.person
SELECT DISTINCT
    src_m.participant_id                        AS person_id,
    COALESCE(g.gender_concept_id, 0)            AS gender_concept_id,
    YEAR(b.date_of_birth)                       AS year_of_birth,
    MONTH(b.date_of_birth)                      AS month_of_birth,
    DAY(b.date_of_birth)                        AS day_of_birth,
    NULL                                        AS birth_datetime,
    COALESCE(r.race_target_concept_id, 0)       AS race_concept_id,
    0                                           AS ethnicity_concept_id,
    person_loc.location_id                      AS location_id,
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
INNER JOIN cdm.src_participant b
    ON src_m.participant_id = b.participant_id
LEFT JOIN cdm.src_gender g
    ON src_m.participant_id = g.person_id
LEFT JOIN cdm.src_race r
    ON src_m.participant_id = r.person_id
LEFT JOIN cdm.src_person_location person_loc
    ON src_m.participant_id = person_loc.participant_id;
;

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

INSERT INTO cdm.procedure_occurrence
SELECT
    NULL                                        AS procedure_occurrence_id,
    src_m1.participant_id                       AS person_id,
    COALESCE(vc.concept_id, 0)                  AS procedure_concept_id,
    src_m2.value_date                           AS procedure_date,
    NULL                                        AS procedure_datetime,
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
    meas.value_string               AS value_string,
    meas.measurement_id             AS measurement_id,
    pm.physical_measurements_id     AS physical_measurements_id,
    meas.parent_id                  AS parent_id
FROM rdr.measurement meas
INNER JOIN rdr.physical_measurements pm
    ON meas.physical_measurements_id = pm.physical_measurements_id
    AND pm.final = 1
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

INSERT INTO cdm.care_site
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
-- table: cdm.tmp_visits_num
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS cdm.tmp_visits_num;

-- -------------------------------------------------------------------
-- Here we order visits by person and visit_end_time, numbering each
-- person's visit in 'row_number'. If it's first person's visit, then
-- row_number is 1, subsequent numbers is 2, 3 and so on.
-- This is necessary for obtaining preceding occurence id.
-- -------------------------------------------------------------------
CREATE TABLE cdm.tmp_visits_num AS
SELECT
    src.visit_occurrence_id                                     AS visit_occurrence_id,
    src.person_id                                               AS person_id, 
    src.visit_start_datetime                                    AS visit_start_datetime,
    src.visit_end_datetime                                      AS visit_end_datetime,
    src.care_site_id                                            AS care_site_id,
    -- person visits enumeration
    @partition_expr := src.person_id                            AS partition_expr,
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
        END                                                     AS row_number
FROM 
    cdm.tmp_visits_src src
ORDER BY 
    src.person_id, src.visit_end_datetime DESC, src.visit_start_datetime DESC
;

CREATE INDEX tmp_visits_num_ids ON cdm.tmp_visits_num (person_id, row_number);
ALTER TABLE cdm.tmp_visits_num ADD KEY (visit_start_datetime);
ALTER TABLE cdm.tmp_visits_num ADD KEY (visit_end_datetime);

-- -------------------------------------------------------------------
-- table: cdm.visit_occurrence
-- -------------------------------------------------------------------
TRUNCATE TABLE cdm.visit_occurrence;

-- -------------------------------------------------------------------
-- Here we form visit_occurence table from 'tmp_visits_num', filling
-- preceding_visit_occurence_id as visit_occurence_id with smaller
-- by 1 row_number
-- -------------------------------------------------------------------
INSERT INTO cdm.visit_occurrence
SELECT
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
    vprev.visit_occurrence_id               AS preceding_visit_occurrence_id,
    'vis.meas'                              AS unit_id
FROM cdm.tmp_visits_num src
LEFT JOIN cdm.tmp_visits_num vprev
    ON  src.person_id = vprev.person_id
    AND src.visit_start_datetime >= vprev.visit_end_datetime
    AND src.row_number + 1 = vprev.row_number
;

-- -------------------------------------------------------------------
-- Drop Temporary Tables
-- -------------------------------------------------------------------
  DROP TABLE IF EXISTS cdm.tmp_visits_src;
  DROP TABLE IF EXISTS cdm.tmp_visits_num;

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

INSERT INTO cdm.observation
SELECT
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
;

-- -------------------------------------------------------------------
-- unit: observ.meas - observations from measurement table
-- -------------------------------------------------------------------

INSERT INTO cdm.observation
SELECT
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
    0                                       AS unit_concept_id,
    NULL                                    AS provider_id,
    meas.physical_measurements_id           AS visit_occurrence_id,
    meas.code_value                         AS observation_source_value,
    meas.cv_source_concept_id               AS observation_source_concept_id,
    NULL                                    AS unit_source_value,
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
INSERT INTO cdm.measurement
SELECT
    NULL                                    AS measurement_id,
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
    meas.measurement_id                     AS meas_id,
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

INSERT INTO cdm.note 
SELECT
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
INSERT INTO cdm.observation_period
SELECT
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
-- unit: observ.meas[1,2] - to link measurements and their qualifiers
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS cdm.tmp_fact_rel_om;

-- -------------------------------------------------------------------
-- This is temporary table for saving original relationships between
-- measurements and their qualifiers in source tables.
-- -------------------------------------------------------------------
CREATE TABLE cdm.tmp_fact_rel_om AS 
SELECT
    cdm_meas.measurement_id         AS measurement_id,
    cdm_obs.observation_id          AS observation_id
FROM cdm.measurement cdm_meas
INNER JOIN rdr.measurement_to_qualifier  mtq
    ON cdm_meas.meas_id = mtq.measurement_id
INNER JOIN cdm.observation cdm_obs
    ON mtq.qualifier_id = cdm_obs.meas_id
;

-- -------------------------------------------------------------------
-- Insert to fact_relationships measurement-to-observation relations
-- -------------------------------------------------------------------
INSERT INTO cdm.fact_relationship
SELECT
    21                              AS domain_concept_id_1,     -- Measurement
    fr.measurement_id               AS fact_id_1,
    27                              AS domain_concept_id_2,     -- Observation
    fr.observation_id               AS fact_id_2,
    581411                          AS relationship_concept_id,  -- Measurement to Observation
    'observ.meas1'                  AS unit_id
FROM cdm.tmp_fact_rel_om fr
;

-- -------------------------------------------------------------------
-- Insert to fact_relationships backwards observation-to-measurement
-- relations
-- -------------------------------------------------------------------
INSERT INTO cdm.fact_relationship
SELECT
    27                              AS domain_concept_id_1,     -- Observation
    fr.observation_id               AS fact_id_1,
    21                              AS domain_concept_id_2,     -- Measurement
    fr.measurement_id               AS fact_id_2,
    581410                          AS relationship_concept_id,  -- Observation to Measurement
    'observ.meas2'                  AS unit_id
FROM cdm.tmp_fact_rel_om fr
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
-- unit: meas.meas1 - to link parent measurements and child measurements
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS cdm.tmp_fact_rel_mm;

-- ---------------------------------------------------------------------
-- tmp_fact_rel_mm contains child-to-parent measurements relations
-- ---------------------------------------------------------------------
CREATE TABLE cdm.tmp_fact_rel_mm AS 
SELECT
    cdm_meas1.measurement_id         AS measurement_id_child,
    cdm_meas2.measurement_id         AS measurement_id_parent
FROM cdm.measurement cdm_meas1
INNER JOIN cdm.measurement cdm_meas2
    ON cdm_meas1.parent_id = cdm_meas2.meas_id
;

-- ---------------------------------------------------------------------
-- Insert into fact_relationship child-to-parent measurements relations
-- ---------------------------------------------------------------------
INSERT INTO cdm.fact_relationship
SELECT
    21                              AS domain_concept_id_1,     -- Measurement
    fr.measurement_id_child         AS fact_id_1,
    21                              AS domain_concept_id_2,     -- Measurement
    fr.measurement_id_parent        AS fact_id_2,
    581437                          AS relationship_concept_id,  -- 581437, Child to Parent Measurement
    'meas.meas1'                    AS unit_id
FROM cdm.tmp_fact_rel_mm fr
;

-- ---------------------------------------------------------------------
-- Insert into fact_relationship parent-to-child measurements relations
-- ---------------------------------------------------------------------
INSERT INTO cdm.fact_relationship
SELECT
    21                              AS domain_concept_id_1,     -- Measurement
    fr.measurement_id_parent        AS fact_id_1,
    21                              AS domain_concept_id_2,     -- Measurement
    fr.measurement_id_child         AS fact_id_2,
    581436                          AS relationship_concept_id,  -- 581436, Parent to Child Measurement
    'meas.meas2'                    AS unit_id
FROM cdm.tmp_fact_rel_mm fr
;

-- -------------------------------------------------------------------
-- Drop Temporary Tables
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS cdm.tmp_fact_rel_om;
DROP TABLE IF EXISTS cdm.tmp_fact_rel_sd;
DROP TABLE IF EXISTS cdm.tmp_fact_rel_mm;


-- -------------------------------------------------------------------
-- source_file: cdm_cleanup.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Drop columns only used for ETL purposes
-- -------------------------------------------------------------------

-- ALTER TABLE cdm.care_site DROP COLUMN unit_id;
-- ALTER TABLE cdm.condition_era DROP COLUMN unit_id;
-- ALTER TABLE cdm.condition_occurrence DROP COLUMN unit_id;
-- ALTER TABLE cdm.cost DROP COLUMN unit_id;
-- ALTER TABLE cdm.death DROP COLUMN unit_id;
-- ALTER TABLE cdm.device_exposure DROP COLUMN unit_id;
-- ALTER TABLE cdm.dose_era DROP COLUMN unit_id;
-- ALTER TABLE cdm.drug_era DROP COLUMN unit_id;
-- ALTER TABLE cdm.drug_exposure DROP COLUMN unit_id;
-- ALTER TABLE cdm.fact_relationship DROP COLUMN unit_id;
-- ALTER TABLE cdm.location DROP COLUMN unit_id;
-- ALTER TABLE cdm.measurement DROP COLUMN unit_id, DROP COLUMN meas_id, DROP COLUMN parent_id;
-- ALTER TABLE cdm.observation DROP COLUMN unit_id, DROP COLUMN meas_id;
-- ALTER TABLE cdm.observation_period DROP COLUMN unit_id;
-- ALTER TABLE cdm.payer_plan_period DROP COLUMN unit_id;
-- ALTER TABLE cdm.person DROP COLUMN unit_id;
-- ALTER TABLE cdm.procedure_occurrence DROP COLUMN unit_id;
-- ALTER TABLE cdm.provider DROP COLUMN unit_id;
-- ALTER TABLE cdm.visit_occurrence DROP COLUMN unit_id;
