-- -------------------------------------------------------------------
-- @2015-2017, Odysseus Data Services, Inc. All rights reserved
-- PPI OMOP CDM Conversion
-- last updated September 22, 2017
-- -------------------------------------------------------------------

-- -----------------------------------------------
-- re-create tables for cdm schema
-- -----------------------------------------------
USE cdm;

-- -----------------------------------------------
-- location
-- -----------------------------------------------
DROP TABLE IF EXISTS location;

CREATE TABLE location
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
DROP TABLE IF EXISTS care_site;

CREATE TABLE care_site
(
    care_site_id bigint AUTO_INCREMENT NOT NULL,
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
DROP TABLE IF EXISTS provider;

CREATE TABLE provider
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
DROP TABLE IF EXISTS person;

CREATE TABLE person
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
DROP TABLE IF EXISTS death;

CREATE TABLE death
(
    person_id bigint NOT NULL,
    death_date datetime NOT NULL,
    death_type_concept_id bigint NOT NULL,
    cause_concept_id bigint NOT NULL,
    cause_source_value varchar(50),
    cause_source_concept_id bigint NOT NULL,
    unit_id varchar(50) NOT NULL
);

-- -----------------------------------------------
-- observation_period
-- -----------------------------------------------
DROP TABLE IF EXISTS observation_period;

CREATE TABLE observation_period
(
    observation_period_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    observation_period_start_date date NOT NULL,
    observation_period_start_datetime datetime NOT NULL,
    observation_period_end_date date NOT NULL,
    observation_period_end_datetime datetime NOT NULL,
    period_type_concept_id bigint NOT NULL,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (observation_period_id)
);

-- -----------------------------------------------
-- payer_plan_period
-- -----------------------------------------------
DROP TABLE IF EXISTS payer_plan_period;

CREATE TABLE payer_plan_period
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
DROP TABLE IF EXISTS visit_occurrence;

CREATE TABLE visit_occurrence
(
    visit_occurrence_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    visit_concept_id bigint NOT NULL,
    visit_start_date date NOT NULL,
    visit_start_datetime datetime NULL,
    visit_end_date date NOT NULL,
    visit_end_datetime datetime NULL,
    visit_type_concept_id bigint NOT NULL,
    provider_id bigint,
    care_site_id bigint,
    visit_source_value varchar(150),
    visit_source_concept_id bigint,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (visit_occurrence_id)
);

-- -----------------------------------------------
-- condition_occurrence
-- -----------------------------------------------
DROP TABLE IF EXISTS condition_occurrence;

CREATE TABLE condition_occurrence
(
    condition_occurrence_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    condition_concept_id bigint NOT NULL,
    condition_start_date date NOT NULL,
    condition_start_datetime datetime NOT NULL,
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
DROP TABLE IF EXISTS procedure_occurrence;

CREATE TABLE procedure_occurrence
(
    procedure_occurrence_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    procedure_concept_id bigint NOT NULL,
    procedure_date date NOT NULL,
    procedure_datetime datetime NOT NULL,
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
DROP TABLE IF EXISTS observation;

CREATE TABLE observation
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
    value_source_concept_id bigint,
    value_source_value varchar(255),
    questionnaire_response_id bigint,
    meas_id bigint,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (observation_id)
);

-- -----------------------------------------------
-- measurement
-- -----------------------------------------------
DROP TABLE IF EXISTS measurement;

CREATE TABLE measurement
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
    measurement_source_value varchar(50) NOT NULL,
    measurement_source_concept_id bigint NOT NULL,
    unit_source_value varchar(50),
    value_source_value varchar(50),
    meas_id bigint,
    parent_id bigint,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (measurement_id)
);

-- -----------------------------------------------
-- drug_exposure
-- -----------------------------------------------
DROP TABLE IF EXISTS drug_exposure;

CREATE TABLE drug_exposure
(
    drug_exposure_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    drug_concept_id bigint NOT NULL,
    drug_exposure_start_date date NOT NULL,
    drug_exposure_end_date date,
    drug_type_concept_id bigint NOT NULL,
    stop_reason varchar(20),
    refills int,
    quantity double,
    days_supply int,
    sig varchar(20),
    route_concept_id bigint,
    effective_drug_dose double,
    dose_unit_concept_id bigint NOT NULL,
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
DROP TABLE IF EXISTS device_exposure;

CREATE TABLE device_exposure
(
    device_exposure_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    device_concept_id bigint NOT NULL,
    device_exposure_start_date date NOT NULL,
    device_exposure_start_datetime datetime NOT NULL,
    device_exposure_end_date date,
    device_exposure_end_datetime datetime NOT NULL,
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
DROP TABLE IF EXISTS cost;

CREATE TABLE cost
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
    paid_patient_coinsurence double,
    paid_patient_deductible double,
    paid_by_primary double,
    paid_ingredient_cost double,
    paid_dispensing_fee double,
    payer_plan_period_id bigint,
    amount_allowed double,
    revenue_code_concept_id bigint NOT NULL,
    revenue_code_source_value varchar(50),
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (cost_id)
);

-- -----------------------------------------------
-- fact_relationship
-- -----------------------------------------------
DROP TABLE IF EXISTS fact_relationship;

CREATE TABLE fact_relationship
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
DROP TABLE IF EXISTS condition_era;

CREATE TABLE condition_era
(
    condition_era_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    condition_concept_id bigint NOT NULL,
    condition_era_start_date date NOT NULL,
    condition_era_end_date date NOT NULL,
    condition_occurrence_count int NOT NULL,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (condition_era_id)
);

-- -----------------------------------------------
-- drug_era
-- -----------------------------------------------
DROP TABLE IF EXISTS drug_era;

CREATE TABLE drug_era
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
DROP TABLE IF EXISTS dose_era;

CREATE TABLE dose_era
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

-- -------------------------------------------------------------------
-- Rules all together
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
ALTER TABLE cdm.src_clean ADD KEY (question_ppi_code);


-- -------------------------------------------------------------------
-- source_file: src/src_mapped.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: src_participant
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS cdm.src_participant;
CREATE TABLE cdm.src_participant
(
    participant_id              bigint,
    latest_date_of_survey       datetime,
    date_of_birth               date,
    latest_date_of_location     datetime,
    PRIMARY KEY (participant_id)
);

-- ----------------------------------------------------------------
-- define valid date of birth
-- ----------------------------------------------------------------
INSERT INTO cdm.src_participant
SELECT
    f1.participant_id,
    f1.latest_date_of_survey,
    f1.date_of_birth,
    f2.latest_date_of_location
FROM
    (SELECT
        t1.participant_id           AS participant_id,
        t1.latest_date_of_survey    AS latest_date_of_survey,
        MAX(DATE(t2.value_date))    AS date_of_birth,
        NULL                        AS latest_date_of_location
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
LEFT JOIN
    (SELECT DISTINCT
        s1.participant_id,
        max(s1.date_of_survey)   AS latest_date_of_location
    FROM cdm.src_clean s1
    WHERE
        (s1.question_ppi_code IN
            (   'PIIAddress_StreetAddress',
                'PIIAddress_StreetAddress2',
                'StreetAddress_PIICity',
                'StreetAddress_PIIZIP')
        AND s1.value_string IS NOT NULL
        )

        OR

        (s1.question_ppi_code = 'StreetAddress_PIIState'
        AND s1.topic_value = 'States'
        AND s1.value_code_id IS NOT NULL
        )
    GROUP BY
        s1.participant_id
    ) f2
    ON f1.participant_id = f2.participant_id
;

ALTER TABLE cdm.src_participant ADD KEY (latest_date_of_location);
ALTER TABLE cdm.src_participant ADD KEY (participant_id);

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
    ON  src_c.question_ppi_code = vc1.concept_code
    AND vc1.vocabulary_id = 'PPI'
LEFT JOIN voc.concept_relationship vcr1
    ON  vc1.concept_id = vcr1.concept_id_1
    AND vcr1.relationship_id = 'Maps to'
    AND vcr1.invalid_reason IS NULL
LEFT JOIN voc.concept vc2
    ON  vcr1.concept_id_2 = vc2.concept_id
    AND vc2.standard_concept = 'S'
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
;

ALTER TABLE cdm.src_mapped ADD KEY (question_ppi_code);
CREATE INDEX mapped_p_id_and_ppi ON cdm.src_mapped (participant_id, question_ppi_code);
CREATE INDEX mapped_qr_id_and_ppi ON cdm.src_mapped (questionnaire_response_id, question_ppi_code);

-- -------------------------------------------------------------------
-- source_file: src/location.sql
-- -------------------------------------------------------------------

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

-- -------------------------------------------------------------------
-- table: person
-- -------------------------------------------------------------------
TRUNCATE TABLE cdm.person;

INSERT INTO cdm.person
SELECT DISTINCT
    src_m.participant_id                        AS person_id,
    COALESCE(g.gender_concept_id, 0)            AS gender_concept_id,
    YEAR(b.date_of_birth)                       AS year_of_birth,
    MONTH(b.date_of_birth)                      AS month_of_birth,
    DAY(b.date_of_birth)                        AS day_of_birth,
    TIMESTAMP(b.date_of_birth)                  AS birth_datetime,
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

-- -------------------------------------------------------------------
-- Drop Temporary Tables
-- -------------------------------------------------------------------
  DROP TABLE IF EXISTS cdm.src_gender;
  DROP TABLE IF EXISTS cdm.src_race;
  DROP TABLE IF EXISTS cdm.src_location_for_pers;
  DROP TABLE IF EXISTS cdm.src_person_location;

-- -------------------------------------------------------------------
-- source_file: src/procedure_occurrence.sql
-- -------------------------------------------------------------------

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
;

-- -------------------------------------------------------------------
-- source_file: src/src_meas_mapped.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: src_meas
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
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS cdm.tmp_cv_concept_lk;

CREATE TABLE cdm.tmp_cv_concept_lk
(
    code_value                  varchar(500),
    cv_source_concept_id        bigint,
    cv_concept_class_id         varchar(50),
    cv_concept_id               bigint,
    cv_domain_id                varchar(50),
    PRIMARY KEY (code_value)
);

INSERT INTO cdm.tmp_cv_concept_lk
SELECT DISTINCT
    meas.code_value             AS code_value,
    vc1.concept_id              AS cv_source_concept_id,
    vc1.concept_class_id        AS cv_concept_class_id,
    vc1.concept_id              AS cv_concept_id,
    vc1.domain_id               AS cv_domain_id
FROM cdm.src_meas meas
INNER JOIN voc.concept vc1
    ON meas.code_value = vc1.concept_code
    AND vc1.standard_concept = 'S'
    AND vc1.vocabulary_id IN ('PPI', 'LOINC')
WHERE meas.code_value IS NOT NULL
;

INSERT INTO cdm.tmp_cv_concept_lk
SELECT DISTINCT
    meas.code_value                                 AS code_value,
    vc1.concept_id                                  AS cv_source_concept_id,
    vc1.concept_class_id                            AS cv_concept_class_id,
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
WHERE
    meas.code_value NOT IN (SELECT code_value FROM cdm.tmp_cv_concept_lk)
    AND meas.code_value IS NOT NULL
;

-- -------------------------------------------------------------------
-- additional table: tmp_vcv_concept_lk
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
    meas.value_code_value           AS value_code_value,
    vc1.concept_id                  AS vcv_source_concept_id,
    vc1.concept_id                  AS vcv_concept_id,
    vc1.domain_id                   AS vcv_domain_id
FROM cdm.src_meas meas
INNER JOIN voc.concept vc1
    ON meas.value_code_value = vc1.concept_code
    AND vc1.standard_concept = 'S'
    AND vc1.vocabulary_id IN ('PPI', 'LOINC')
WHERE meas.value_code_value IS NOT NULL
;

INSERT INTO cdm.tmp_vcv_concept_lk
SELECT DISTINCT
    meas.value_code_value                           AS value_code_value,
    vc1.concept_id                                  AS vcv_source_concept_id,
    vc2.concept_id                                  AS vcv_concept_id,
    COALESCE(vc2.domain_id, vc1.domain_id)          AS vcv_domain_id
FROM cdm.src_meas meas
LEFT JOIN voc.concept vc1
    ON meas.value_code_value = vc1.concept_code
    AND vc1.vocabulary_id = 'PPI'
LEFT JOIN voc.concept_relationship vcr1
    ON vc1.concept_id = vcr1.concept_id_1
    AND vcr1.relationship_id = 'Maps to'
    AND vcr1.invalid_reason IS NULL
LEFT JOIN voc.concept vc2
    ON vc2.concept_id = vcr1.concept_id_2
    AND vc2.standard_concept = 'S'
WHERE
    meas.value_code_value NOT IN (SELECT value_code_value FROM cdm.tmp_vcv_concept_lk)
    AND meas.value_code_value IS NOT NULL
;

-- -------------------------------------------------------------------
-- table: src_meas_mapped
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS cdm.src_meas_mapped;

CREATE TABLE cdm.src_meas_mapped
(
    participant_id              bigint NOT NULL,
    finalized_site_id           int,
    code_value                  varchar(255) NOT NULL,
    cv_source_concept_id        bigint,
    cv_concept_class_id         varchar(50),
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
    tmp1.cv_concept_class_id                    AS cv_concept_class_id,
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
LEFT JOIN voc.concept vc1
    ON meas.value_unit = vc1.concept_code
    AND vc1.vocabulary_id = 'UCUM'
    AND vc1.standard_concept = 'S'
LEFT JOIN cdm.tmp_vcv_concept_lk tmp2
    ON meas.value_code_value = tmp2.value_code_value
;

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
    NULL                                    AS care_site_id,
    site.site_name                          AS care_site_name,
    0                                       AS place_of_service_concept_id,
    NULL                                    AS location_id,
    src_meas.finalized_site_id              AS care_site_source_value,
    NULL                                    AS place_of_service_source_value,
    'care_site'                             AS unit_id
FROM cdm.src_meas src_meas
JOIN rdr.site site
    ON  site.site_id = src_meas.finalized_site_id
;

-- -------------------------------------------------------------------
-- source_file: src/visit_occurrence.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: cdm.visit_occurrence
-- -------------------------------------------------------------------
TRUNCATE TABLE cdm.visit_occurrence;

INSERT INTO cdm.visit_occurrence
SELECT
    NULL                                    AS visit_occurrence_id,
    src_meas.participant_id                 AS person_id,
    9202                                    AS visit_concept_id, -- 9202 - 'Outpatient Visit'
    DATE(MIN(src_meas.measurement_time))    AS visit_start_date,
    MIN(src_meas.measurement_time)          AS visit_start_datetime,
    DATE(MAX(src_meas.measurement_time))    AS visit_end_date,
    MAX(src_meas.measurement_time)          AS visit_end_datetime,
    44818519                                AS visit_type_concept_id, -- 44818519 - 'Clinical Study Visit'
    NULL                                    AS provider_id,
    cs.care_site_id                         AS care_site_id,
    src_meas.physical_measurements_id       AS visit_source_value,
    0                                       AS visit_source_concept_id,
    'vis.survey'                            AS unit_id
FROM cdm.src_meas src_meas
LEFT JOIN cdm.care_site cs
    ON src_meas.finalized_site_id = cs.care_site_source_value
GROUP BY
    src_meas.participant_id,
    cs.care_site_id,
    src_meas.physical_measurements_id
;

ALTER TABLE cdm.visit_occurrence ADD KEY (visit_source_value);

-- -------------------------------------------------------------------
-- source_file: src/observation.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: cdm.observation
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- units: observ.code, observ.str, observ.num, observ.bool
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
SELECT DISTINCT
    NULL                                    AS observation_id,
    meas.participant_id                     AS person_id,
    meas.cv_concept_id                      AS observation_concept_id,
    DATE(meas.measurement_time)             AS observation_date,
    meas.measurement_time                   AS observation_time,
    581413                                  AS observation_type_concept_id,   -- 581413, Observation from Measurement
    NULL                                    AS value_as_number,
    NULL                                    AS value_as_string,
    meas.vcv_concept_id                     AS value_as_concept_id,
    0                                       AS qualifier_concept_id,
    0                                       AS unit_concept_id,
    NULL                                    AS provider_id,
    vis.visit_occurrence_id                 AS visit_occurrence_id,
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
LEFT JOIN cdm.visit_occurrence vis
    ON vis.visit_source_value = meas.physical_measurements_id
WHERE
    meas.cv_domain_id = 'Observation'
    OR meas.cv_concept_class_id = 'PPI Modifier'
    OR meas.measurement_id IN (SELECT qualifier_id FROM rdr.measurement_to_qualifier)
;

-- -------------------------------------------------------------------
-- source_file: src/measurement.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: cdm.measurement
-- -------------------------------------------------------------------
TRUNCATE TABLE cdm.measurement;

alter table cdm.src_meas_mapped add key (physical_measurements_id);
alter table cdm.visit_occurrence add key (visit_source_value);

-- -------------------------------------------------------------------
-- unit: meas.dec - measurements represented as decimal values
-- -------------------------------------------------------------------

INSERT INTO cdm.measurement
SELECT DISTINCT
    NULL                                    AS measurement_id,
    meas.participant_id                     AS person_id,
    meas.cv_concept_id                      AS measurement_concept_id,
    DATE(meas.measurement_time)             AS measurement_date,
    meas.measurement_time                   AS measurement_datetime,
    44818701                                AS measurement_type_concept_id,  -- 44818701, From physical examination
    0                                       AS operator_concept_id,
    meas.value_decimal                      AS value_as_number,
    0                                       AS value_as_concept_id,
    meas.vu_concept_id                      AS unit_concept_id,
    NULL                                    AS range_low,
    NULL                                    AS range_high,
    NULL                                    AS provider_id,
    vis.visit_occurrence_id                 AS visit_occurrence_id,
    meas.code_value                         AS measurement_source_value,
    meas.cv_source_concept_id               AS measurement_source_concept_id,
    meas.value_unit                         AS unit_source_value,
    CONCAT(meas.value_decimal, ' - ', meas.value_unit)                    AS value_source_value,
    meas.measurement_id                     AS meas_id,
    meas.parent_id                          AS parent_id,
    'meas.dec'                              AS unit_id
FROM cdm.src_meas_mapped meas
LEFT JOIN rdr.measurement_to_qualifier mq
    ON  meas.measurement_id = mq.qualifier_id
LEFT JOIN cdm.visit_occurrence vis
    ON meas.physical_measurements_id = vis.visit_source_value
WHERE
    mq.qualifier_id IS NULL
    AND (meas.cv_domain_id = 'Measurement' OR meas.cv_domain_id IS NULL)
    AND meas.value_decimal IS NOT NULL
    AND COALESCE(meas.cv_concept_class_id, '0' ) != 'PPI Modifier'
;

-- -------------------------------------------------------------------
-- unit: meas.value - measurements represented as value_code_value
-- -------------------------------------------------------------------

INSERT INTO cdm.measurement
SELECT DISTINCT
    NULL                                    AS measurement_id,
    meas.participant_id                     AS person_id,
    meas.cv_concept_id                      AS measurement_concept_id,
    DATE(meas.measurement_time)             AS measurement_date,
    meas.measurement_time                   AS measurement_time,
    44818701                                AS measurement_type_concept_id, -- 44818701, From physical examination
    0                                       AS operator_concept_id,
    NULL                                    AS value_as_number,
    meas.vcv_concept_id                     AS value_as_concept_id,
    0                                       AS unit_concept_id,
    NULL                                    AS range_low,
    NULL                                    AS range_high,
    NULL                                    AS provider_id,
    vis.visit_occurrence_id                 AS visit_occurrence_id,
    meas.code_value                         AS measurement_source_value,
    meas.cv_source_concept_id               AS measurement_source_concept_id,
    NULL                                    AS unit_source_value,
    meas.value_code_value                   AS value_source_value,
    meas.measurement_id                     AS meas_id,
    meas.parent_id                          AS parent_id,
    'meas.value'                            AS unit_id
FROM cdm.src_meas_mapped meas
LEFT JOIN rdr.measurement_to_qualifier mq
    ON  meas.measurement_id = mq.qualifier_id
LEFT JOIN cdm.visit_occurrence vis
    ON vis.visit_source_value = meas.physical_measurements_id
WHERE
    mq.qualifier_id IS NULL
    AND (meas.cv_domain_id = 'Measurement' OR meas.cv_domain_id IS NULL)
    AND meas.value_code_value IS NOT NULL
    AND COALESCE(meas.cv_concept_class_id, '0' ) != 'PPI Modifier'
;

-- -------------------------------------------------------------------
-- unit: meas.empty - measurements with empty value_decimal and value_code_value fields
-- -------------------------------------------------------------------

INSERT INTO cdm.measurement
SELECT DISTINCT
    NULL                                    AS measurement_id,
    meas.participant_id                     AS person_id,
    meas.cv_concept_id                      AS measurement_concept_id,
    DATE(meas.measurement_time)             AS measurement_date,
    meas.measurement_time                   AS measurement_time,
    44818701                                AS measurement_type_concept_id, -- 44818701, From physical examination
    0                                       AS operator_concept_id,
    NULL                                    AS value_as_number,
    0                                       AS value_as_concept_id,
    0                                       AS unit_concept_id,
    NULL                                    AS range_low,
    NULL                                    AS range_high,
    NULL                                    AS provider_id,
    vis.visit_occurrence_id                 AS visit_occurrence_id,
    meas.code_value                         AS measurement_source_value,
    meas.cv_source_concept_id               AS measurement_source_concept_id,
    NULL                                    AS unit_source_value,
    NULL                                    AS value_source_value,
    meas.measurement_id                     AS meas_id,
    meas.parent_id                          AS parent_id,
    'meas.empty'                            AS unit_id
FROM cdm.src_meas_mapped meas
LEFT JOIN rdr.measurement_to_qualifier mq
    ON  meas.measurement_id = mq.qualifier_id
LEFT JOIN cdm.visit_occurrence vis
    ON vis.visit_source_value = meas.physical_measurements_id
WHERE
    mq.qualifier_id IS NULL
    AND (meas.cv_domain_id = 'Measurement' OR meas.cv_domain_id IS NULL)
    AND (meas.value_code_value IS NULL AND meas.value_decimal IS NULL)
    AND COALESCE(meas.cv_concept_class_id, '0' ) != 'PPI Modifier'
;

-- -------------------------------------------------------------------
-- source_file: src/condition_occurrence.sql
-- -------------------------------------------------------------------

-- ---------------------------------------------------
-- table: condition_occurrence
-- ---------------------------------------------------
TRUNCATE TABLE cdm.condition_occurrence;

INSERT INTO cdm.condition_occurrence
SELECT
    NULL                            AS condition_occurrence_id,
    meas.participant_id             AS person_id,
    meas.cv_concept_id              AS condition_concept_id,
    DATE(meas.measurement_time)     AS condition_start_date,
    meas.measurement_time           AS condition_start_datetime,
    NULL                            AS condition_end_date,
    NULL                            AS condition_end_datetime,
    45905770                        AS condition_type_concept_id,   -- 45905770, Patient Self-Reported Condition
    NULL                            AS stop_reason,
    NULL                            AS provider_id,
    vis.visit_occurrence_id         AS visit_occurrence_id,
    meas.code_value                 AS condition_source_value,
    meas.cv_source_concept_id       AS condition_source_concept_id,
    'condition'                     AS unit_id
FROM cdm.src_meas_mapped meas
LEFT JOIN cdm.visit_occurrence vis
    ON meas.physical_measurements_id  = vis.visit_source_value
WHERE
    (   meas.cv_domain_id = 'Condition'
        AND meas.code_value != 'wheelchair-bound-status'
    )
    OR
    (   meas.cv_domain_id = 'Condition'
        AND meas.code_value = 'wheelchair-bound-status'
        AND meas.value_code_value = 'wheelchair-bound'
    )
;

-- -------------------------------------------------------------------
-- source_file: src/condition_era.sql
-- -------------------------------------------------------------------

-- ---------------------------------------------------
-- table: condition_era
-- ---------------------------------------------------

-- ---------------------------------------------------
-- intermediate tables for cdm.condition_era
-- ---------------------------------------------------
DROP TABLE IF EXISTS cdm.temp_cteConditionTarget;
CREATE TABLE cdm.temp_cteConditionTarget
(
    condition_occurrence_id         bigint,
    person_id                       bigint,
    condition_concept_id            bigint,
    condition_start_date            date,
    condition_end_date              date
);

INSERT INTO cdm.temp_cteConditionTarget
SELECT
    CO.condition_occurrence_id                                       AS condition_occurrence_id,
    CO.person_id                                                     AS person_id,
    CO.condition_concept_id                                          AS condition_concept_id,
    CO.condition_start_date                                          AS condition_start_date,
    COALESCE(CO.condition_end_date,
      CO.condition_start_date + INTERVAL 1 DAY)                      AS condition_end_date
    -- Depending on the needs of data, include more filters in cteConditionTarget
    -- For example
    -- - to exclude unmapped condition_concept_id's (i.e. condition_concept_id = 0)
          -- from being included in same era
    -- - to set condition_era_end_date to same condition_era_start_date
          -- or condition_era_start_date + INTERVAL '1 day', when condition_end_date IS NULL
FROM cdm.condition_occurrence CO
WHERE
    CO.condition_concept_id != 0
;

DROP TABLE IF EXISTS cdm.temp_cteEndDates_UnionPart;
CREATE TABLE cdm.temp_cteEndDates_UnionPart
(
    person_id                       bigint,
    condition_concept_id            bigint,
    event_date                      date,
    event_type                      int,
    start_ordinal                   int
);

SELECT NULL INTO @partition_expr;
SELECT NULL INTO @last_part_expr;
SELECT NULL INTO @row_number;
SELECT NULL INTO @reset_num;

INSERT INTO cdm.temp_cteEndDates_UnionPart
SELECT
    person_id                   AS person_id,
    condition_concept_id        AS condition_concept_id,
    condition_start_date        AS event_date,
    -1                          AS event_type,
    row_number                  AS start_ordinal
FROM
    ( SELECT
        @partition_expr := CONCAT(  person_id,
                                    '-',
                                    condition_concept_id)           AS partition_expr,
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
        person_id,
        condition_concept_id,
        condition_start_date
      FROM cdm.temp_cteConditionTarget
      ORDER BY
        person_id,
        condition_concept_id,
        condition_start_date
    ) F
UNION
SELECT
    person_id                                             AS person_id,
    condition_concept_id                                  AS condition_concept_id,
    (condition_end_date + INTERVAL 30 DAY)                AS event_date,
    1                                                     AS event_type,
    NULL                                                  AS start_ordinal
FROM cdm.temp_cteConditionTarget
;

DROP TABLE IF EXISTS cdm.temp_cteEndDates_SelectFromUn;
CREATE TABLE cdm.temp_cteEndDates_SelectFromUn
(
    person_id                       bigint,
    condition_concept_id            bigint,
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

INSERT INTO cdm.temp_cteEndDates_SelectFromUn
SELECT
    person_id                            AS person_id,
    condition_concept_id                 AS condition_concept_id,
    event_date                           AS event_date,
    event_type                           AS event_type,
    row_max                              AS start_ordinal,
    row_number                           AS overall_ord
FROM  (
        SELECT
            @partition_expr := CONCAT(  person_id,
                                        '-',
                                        condition_concept_id)           AS partition_expr,
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
            condition_concept_id,
            event_date,
            event_type,
            start_ordinal
        FROM cdm.temp_cteEndDates_UnionPart
        ORDER BY
            person_id,
            condition_concept_id,
            event_date,
            event_type
            ) F
;


DROP TABLE IF EXISTS cdm.temp_cteEndDates;
CREATE TABLE cdm.temp_cteEndDates
(
    person_id                       bigint,
    condition_concept_id            bigint,
    end_date                        date
);

INSERT INTO cdm.temp_cteEndDates
SELECT
    person_id                                   AS person_id,
    condition_concept_id                        AS condition_concept_id,
    (event_date - INTERVAL 30 DAY)              AS end_date          -- unpad the end date
FROM cdm.temp_cteEndDates_SelectFromUn e
WHERE
    (2 * e.start_ordinal) - e.overall_ord = 0
;

DROP TABLE IF EXISTS cdm.temp_cteConditionEnds;
CREATE TABLE cdm.temp_cteConditionEnds
(
    person_id                       bigint,
    condition_concept_id            bigint,
    condition_start_date            date,
    era_end_date                    date
);

INSERT INTO cdm.temp_cteConditionEnds
SELECT
    c.person_id                         AS person_id,
    c.condition_concept_id              AS condition_concept_id,
    c.condition_start_date              AS condition_start_date,
    MIN(e.end_date)                     AS era_end_date
FROM cdm.temp_cteConditionTarget c
JOIN cdm.temp_cteEndDates e
    ON c.person_id = e.person_id
    AND c.condition_concept_id = e.condition_concept_id
    AND e.end_date >= c.condition_start_date
GROUP BY
    c.condition_occurrence_id,
    c.person_id,
    c.condition_concept_id,
    c.condition_start_date
  ;

-- ----------------------------------------------------------------------------
-- cdm.condition_era
-- ----------------------------------------------------------------------------
TRUNCATE TABLE cdm.condition_era;

INSERT INTO cdm.condition_era
SELECT
    NULL                                           AS condition_era_id,
    person_id                                      AS person_id,
    condition_concept_id                           AS condition_concept_id,
    MIN(condition_start_date)                      AS condition_era_start_date,
    era_end_date                                   AS condition_era_end_date,
    COUNT(*)                                       AS condition_occurrence_count,
    'condition_era'                                AS unit_id
FROM cdm.temp_cteConditionEnds
GROUP BY
    person_id,
    condition_concept_id,
    era_end_date
ORDER BY
    person_id,
    condition_concept_id
;

-- -------------------------------------------------------------------
-- Drop Temporary Tables
-- -------------------------------------------------------------------
  DROP TABLE IF EXISTS cdm.temp_cdm_condition_era;
  DROP TABLE IF EXISTS cdm.temp_cteConditionTarget;
  DROP TABLE IF EXISTS cdm.temp_cteEndDates_UnionPart;
  DROP TABLE IF EXISTS cdm.temp_cteEndDates_SelectFromUn;
  DROP TABLE IF EXISTS cdm.temp_cteEndDates;
  DROP TABLE IF EXISTS cdm.temp_cteConditionEnds;
  DROP TABLE IF EXISTS cdm.temp_cteEndDates_1;

-- -------------------------------------------------------------------
-- source_file: src/observation_period.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: observation_period
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

DROP TABLE IF EXISTS cdm.temp_obs;
CREATE TABLE cdm.temp_obs
(
    person_id                       bigint,
    observation_start_date          date,
    observation_end_date            date
);

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

TRUNCATE TABLE cdm.observation_period;

INSERT INTO cdm.observation_period
SELECT
    NULL                                    AS observation_period_id,
    person_id                               AS person_id,
    MIN(observation_start_date)             AS observation_period_start_date,
    TIMESTAMP(MIN(observation_start_date))  AS observation_period_start_datetime,
    observation_end_date                    AS observation_period_end_date,
    TIMESTAMP(observation_end_date)         AS observation_period_end_datetime,
    44814725                                AS period_type_concept_id,         -- Period inferred by algorithm
    'T.observ_period'                       AS unit_id
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
INSERT INTO cdm.fact_relationship
SELECT
    21                              AS domain_concept_id_1,     -- Measurement
    cdm_meas.measurement_id         AS fact_id_1,
    27                              AS domain_concept_id_2,     -- Observation
    cdm_obs.observation_id          AS fact_id_2,
    581411                          AS relationship_concept_id,  -- Measurement to Observation
    'observ.meas1'                  AS unit_id
FROM cdm.measurement cdm_meas
INNER JOIN rdr.measurement_to_qualifier  mtq
    ON cdm_meas.meas_id = mtq.measurement_id
INNER JOIN cdm.observation cdm_obs
    ON mtq.qualifier_id = cdm_obs.meas_id
;

INSERT INTO cdm.fact_relationship
SELECT
    27                              AS domain_concept_id_1,     -- Observation
    cdm_obs.observation_id          AS fact_id_1,
    21                              AS domain_concept_id_2,     -- Measurement
    cdm_meas.measurement_id         AS fact_id_2,
    581410                          AS relationship_concept_id,  -- Observation to Measurement
    'observ.meas2'                  AS unit_id
FROM cdm.measurement cdm_meas
INNER JOIN rdr.measurement_to_qualifier  mtq
    ON cdm_meas.meas_id = mtq.measurement_id
INNER JOIN cdm.observation cdm_obs
    ON mtq.qualifier_id = cdm_obs.meas_id
;
-- -------------------------------------------------------------------
-- unit: syst.diast[1,2] - to link systolic and diastolic blood pressure
-- -------------------------------------------------------------------
INSERT INTO cdm.fact_relationship
SELECT
    21                          AS domain_concept_id_1,     -- Measurement
    m1.measurement_id           AS fact_id_1,
    21                          AS domain_concept_id_2,     -- Measurement
    m2.measurement_id           AS fact_id_2,
    46233683                    AS relationship_concept_id,  -- Systolic to diastolic blood pressure measurement
    'syst.diast1'               AS unit_id
FROM cdm.measurement m1
INNER JOIN cdm.measurement m2
    ON m1.person_id = m2.person_id
    AND m1.measurement_date = m2.measurement_date
    AND m1.measurement_datetime = m2.measurement_datetime
    AND m1.parent_id = m2.parent_id
    AND m1.measurement_source_value = '8480-6'   -- code for Systolic blood pressure measurement
    AND m2.measurement_source_value = '8462-4'   -- code for Diastolic blood pressure measurement
    AND m1.parent_id IS NOT NULL
    AND m2.parent_id IS NOT NULL
;

INSERT INTO cdm.fact_relationship
SELECT
    21                          AS domain_concept_id_1,     -- Measurement
    m2.measurement_id           AS fact_id_1,
    21                          AS domain_concept_id_2,     -- Measurement
    m1.measurement_id           AS fact_id_2,
    46233682                    AS relationship_concept_id,  -- Diastolic to systolic blood pressure measurement
    'syst.diast2'               AS unit_id
FROM cdm.measurement m1
INNER JOIN cdm.measurement m2
    ON m1.person_id = m2.person_id
    AND m1.measurement_date = m2.measurement_date
    AND m1.measurement_datetime = m2.measurement_datetime
    AND m1.parent_id = m2.parent_id
    AND m1.measurement_source_value = '8462-4'   -- code for Diastolic blood pressure measurement
    AND m2.measurement_source_value = '8480-6'   -- code for Systolic blood pressure measurement
    AND m1.parent_id IS NOT NULL
    AND m2.parent_id IS NOT NULL
;

-- -------------------------------------------------------------------
-- Drop columns only used for ETL purposes
-- -------------------------------------------------------------------

ALTER TABLE cdm.care_site DROP COLUMN unit_id;
ALTER TABLE cdm.condition_era DROP COLUMN unit_id;
ALTER TABLE cdm.condition_occurrence DROP COLUMN unit_id;
ALTER TABLE cdm.cost DROP COLUMN unit_id;
ALTER TABLE cdm.death DROP COLUMN unit_id;
ALTER TABLE cdm.device_exposure DROP COLUMN unit_id;
ALTER TABLE cdm.dose_era DROP COLUMN unit_id;
ALTER TABLE cdm.drug_era DROP COLUMN unit_id;
ALTER TABLE cdm.drug_exposure DROP COLUMN unit_id;
ALTER TABLE cdm.fact_relationship DROP COLUMN unit_id;
ALTER TABLE cdm.location DROP COLUMN unit_id;
ALTER TABLE cdm.measurement DROP COLUMN unit_id, DROP COLUMN meas_id, DROP COLUMN parent_id;
ALTER TABLE cdm.observation DROP COLUMN unit_id, DROP COLUMN meas_id;
ALTER TABLE cdm.observation_period DROP COLUMN unit_id;
ALTER TABLE cdm.payer_plan_period DROP COLUMN unit_id;
ALTER TABLE cdm.person DROP COLUMN unit_id;
ALTER TABLE cdm.procedure_occurrence DROP COLUMN unit_id;
ALTER TABLE cdm.provider DROP COLUMN unit_id;
ALTER TABLE cdm.visit_occurrence DROP COLUMN unit_id;
