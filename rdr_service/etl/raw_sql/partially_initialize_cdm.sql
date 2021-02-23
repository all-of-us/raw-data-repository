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
    id bigint NOT NULL,
    location_id bigint AUTO_INCREMENT NOT NULL,
    address_1 varchar(255),
    address_2 varchar(255),
    city varchar(255),
    state varchar(255),
    zip varchar(255),
    county varchar(255),
    location_source_value varchar(255),
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (location_id),
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- care_site
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.care_site;

CREATE TABLE cdm.care_site
(
    id bigint NOT NULL,
    care_site_id bigint NOT NULL,
    care_site_name varchar(255),
    place_of_service_concept_id bigint NOT NULL,
    location_id bigint,
    care_site_source_value varchar(50) NOT NULL,
    place_of_service_source_value varchar(50),
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (care_site_id),
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- provider
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.provider;

CREATE TABLE cdm.provider
(
    id bigint NOT NULL,
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
    PRIMARY KEY (provider_id),
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- person
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.person;

CREATE TABLE cdm.person
(
    id bigint NOT NULL,
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
    PRIMARY KEY (person_id),
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- death
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.death;

CREATE TABLE cdm.death
(
    id bigint NOT NULL,
    person_id bigint NOT NULL,
    death_date date NOT NULL,
    death_datetime datetime,
    death_type_concept_id bigint NOT NULL,
    cause_concept_id bigint NOT NULL,
    cause_source_value varchar(50),
    cause_source_concept_id bigint NOT NULL,
    unit_id varchar(50) NOT NULL,
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- observation_period
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.observation_period;

CREATE TABLE cdm.observation_period
(
    id bigint NOT NULL,
    observation_period_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    observation_period_start_date date NOT NULL,
    observation_period_end_date date NOT NULL,
    period_type_concept_id bigint NOT NULL,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (observation_period_id),
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- payer_plan_period
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.payer_plan_period;

CREATE TABLE cdm.payer_plan_period
(
    id bigint NOT NULL,
    payer_plan_period_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    payer_plan_period_start_date date NOT NULL,
    payer_plan_period_end_date date NOT NULL,
    payer_source_value varchar(50),
    plan_source_value varchar(50),
    family_source_value varchar(50),
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (payer_plan_period_id),
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- visit_occurrence
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.visit_occurrence;

CREATE TABLE cdm.visit_occurrence
(
    id bigint NOT NULL,
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
    PRIMARY KEY (visit_occurrence_id),
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- condition_occurrence
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.condition_occurrence;

CREATE TABLE cdm.condition_occurrence
(
    id bigint NOT NULL,
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
    PRIMARY KEY (condition_occurrence_id),
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- procedure_occurrence
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.procedure_occurrence;

CREATE TABLE cdm.procedure_occurrence
(
    id bigint NOT NULL,
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
    PRIMARY KEY (procedure_occurrence_id),
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- observation
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.observation;

CREATE TABLE cdm.observation
(
    id bigint NOT NULL,
    observation_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    observation_concept_id bigint NOT NULL,
    observation_date date NOT NULL,
    observation_datetime datetime,
    observation_type_concept_id bigint NOT NULL,
    value_as_number decimal(20,6),
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
    PRIMARY KEY (observation_id),
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- measurement
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.measurement;

CREATE TABLE cdm.measurement
(
    id bigint NOT NULL,
    measurement_id bigint NOT NULL,
    person_id bigint NOT NULL,
    measurement_concept_id bigint NOT NULL,
    measurement_date date NOT NULL,
    measurement_datetime datetime,
    measurement_type_concept_id bigint NOT NULL,
    operator_concept_id bigint NOT NULL,
    value_as_number decimal(20,6),
    value_as_concept_id bigint NOT NULL,
    unit_concept_id bigint NOT NULL,
    range_low decimal(20,6),
    range_high decimal(20,6),
    provider_id bigint,
    visit_occurrence_id bigint,
    measurement_source_value varchar(50),
    measurement_source_concept_id bigint NOT NULL,
    unit_source_value varchar(50),
    -- specific for this ETL
    value_source_value varchar(50),
    parent_id bigint,
    --
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (measurement_id),
    UNIQUE KEY (id)
);


-- -----------------------------------------------
-- note
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.note;

CREATE TABLE cdm.note
(
    id bigint NOT NULL,
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
    PRIMARY KEY (note_id),
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- drug_exposure
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.drug_exposure;

CREATE TABLE cdm.drug_exposure
(
    id bigint NOT NULL,
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
    quantity decimal(20,6),
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
    PRIMARY KEY (drug_exposure_id),
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- device_exposure
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.device_exposure;

CREATE TABLE cdm.device_exposure
(
    id bigint NOT NULL,
    device_exposure_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    device_concept_id bigint NOT NULL,
    device_exposure_start_date date NOT NULL,
    device_exposure_start_datetime datetime,
    device_exposure_end_date date,
    device_exposure_end_datetime datetime,
    device_type_concept_id bigint NOT NULL,
    unique_device_id varchar(50),
    quantity decimal(20,6),
    provider_id bigint,
    visit_occurrence_id bigint,
    device_source_value varchar(50) NOT NULL,
    device_source_concept_id bigint,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (device_exposure_id),
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- cost
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.cost;

CREATE TABLE cdm.cost
(
    id bigint NOT NULL,
    cost_id bigint AUTO_INCREMENT NOT NULL,
    cost_event_id bigint NOT NULL,
    cost_domain_id varchar(20) NOT NULL,
    cost_type_concept_id bigint NOT NULL,
    currency_concept_id bigint NOT NULL,
    total_charge decimal(20,6),
    total_cost decimal(20,6),
    total_paid decimal(20,6),
    paid_by_payer decimal(20,6),
    paid_by_patient decimal(20,6),
    paid_patient_copay decimal(20,6),
    paid_patient_coinsurance decimal(20,6),
    paid_patient_deductible decimal(20,6),
    paid_by_primary decimal(20,6),
    paid_ingredient_cost decimal(20,6),
    paid_dispensing_fee decimal(20,6),
    payer_plan_period_id bigint,
    amount_allowed decimal(20,6),
    revenue_code_concept_id bigint NOT NULL,
    revenue_code_source_value varchar(50),
    drg_concept_id bigint NOT NULL,
    drg_source_value varchar(50),
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (cost_id),
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- fact_relationship
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.fact_relationship;

CREATE TABLE cdm.fact_relationship
(
    id bigint NOT NULL,
    domain_concept_id_1 int NOT NULL,
    fact_id_1 bigint NOT NULL,
    domain_concept_id_2 int NOT NULL,
    fact_id_2 bigint NOT NULL,
    relationship_concept_id bigint NOT NULL,
    unit_id varchar(50) NOT NULL,
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- condition_era
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.condition_era;

CREATE TABLE cdm.condition_era
(
    id bigint NOT NULL,
    condition_era_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    condition_concept_id bigint NOT NULL,
    condition_era_start_date date NOT NULL,
    condition_era_end_date date NOT NULL,
    condition_occurrence_count int,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (condition_era_id),
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- drug_era
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.drug_era;

CREATE TABLE cdm.drug_era
(
    id bigint NOT NULL,
    drug_era_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    drug_concept_id bigint NOT NULL,
    drug_era_start_date date NOT NULL,
    drug_era_end_date date NOT NULL,
    drug_exposure_count int,
    gap_days int,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (drug_era_id),
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- dose_era
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.dose_era;

CREATE TABLE cdm.dose_era
(
    id bigint NOT NULL,
    dose_era_id bigint AUTO_INCREMENT NOT NULL,
    person_id bigint NOT NULL,
    drug_concept_id bigint NOT NULL,
    unit_concept_id bigint NOT NULL,
    dose_value decimal(20,6) NOT NULL,
    dose_era_start_date date NOT NULL,
    dose_era_end_date date NOT NULL,
    unit_id varchar(50) NOT NULL,
    PRIMARY KEY (dose_era_id),
    UNIQUE KEY (id)
);

-- -----------------------------------------------
-- create and populate tmp_questionnaire_response
-- -----------------------------------------------
DROP TABLE IF EXISTS cdm.tmp_questionnaire_response;
CREATE TABLE cdm.tmp_questionnaire_response (
      questionnaire_response_id int(11),
      participant_id int(11),
      questionnaire_id int(11),
      created DATETIME,
      authored DATETIME,
      identifier varchar(80),
      answers_hash VARCHAR(80),
      duplicate int(11),
      removed int(11)
);

SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
INSERT INTO cdm.tmp_questionnaire_response (
   questionnaire_response_id, participant_id, questionnaire_id, created, authored, identifier, duplicate, removed)
SELECT questionnaire_response_id, participant_id, questionnaire_id, created, authored,
       REPLACE((JSON_EXTRACT(CONVERT(resource USING utf8 ), '$.identifier.value')), '"', '') AS identifier,
       0 AS duplicate, 0 AS removed
FROM rdr.questionnaire_response qr;

CREATE UNIQUE INDEX uidx_qr_lookup ON cdm.tmp_questionnaire_response (questionnaire_response_id);

drop temporary table if exists cdm.answer_hash_values;
create temporary table cdm.answer_hash_values (
      questionnaire_response_id int(11),
      answers_hash VARCHAR(80)
);

set session group_concat_max_len=10000;
insert into cdm.answer_hash_values (questionnaire_response_id, answers_hash)
select qr.questionnaire_response_id , SHA2(CONCAT(
	CAST(qr.participant_id AS CHAR),
	tqr.identifier,
	GROUP_CONCAT(
		CAST(COALESCE(
			qra.value_code_id,
			qra.value_integer,
			qra.value_decimal,
			qra.value_boolean,
			qra.value_string,
			qra.value_system,
			qra.value_uri,
			qra.value_date,
			qra.value_datetime
		) AS CHAR)
	ORDER BY qr.created)
), 256) as answer_hash
from rdr.questionnaire_response qr
inner join cdm.tmp_questionnaire_response tqr on tqr.questionnaire_response_id = qr.questionnaire_response_id
inner join rdr.questionnaire_response_answer qra on qra.questionnaire_response_id = qr.questionnaire_response_id
group by qr.questionnaire_response_id, qr.participant_id, tqr.identifier;

create index idx_answer_hash_qr_id on cdm.answer_hash_values (questionnaire_response_id);

UPDATE cdm.tmp_questionnaire_response tqr
INNER JOIN cdm.answer_hash_values ahv on tqr.questionnaire_response_id = ahv.questionnaire_response_id
SET tqr.answers_hash = ahv.answers_hash
WHERE tqr.questionnaire_response_id = ahv.questionnaire_response_id;

update cdm.tmp_questionnaire_response tqr
inner join (
	select participant_id, identifier, authored, answers_hash, max(created) as max_created, count(1) as total
	from cdm.tmp_questionnaire_response
	where identifier is not null and created between '2021-01-01' and '2021-02-20'
	group by participant_id, identifier, authored, answers_hash
	having total > 10
) duplicate_response on
	tqr.participant_id = duplicate_response.participant_id
	and tqr.authored = duplicate_response.authored
	and tqr.identifier = duplicate_response.identifier
	and tqr.answers_hash = duplicate_response.answers_hash
	and tqr.created != duplicate_response.max_created
set duplicate = 1;
