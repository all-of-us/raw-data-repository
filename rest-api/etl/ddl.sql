-- -------------------------------------------------------------------
-- @2015-2017, Odysseus Data Services, Inc. All rights reserved
-- PPI OMOP CDM Conversion
-- last updated September 13, 2017
--
-- ddl script for schemas:
-- voc          - vocabulary tables
-- cdm          - cdm tables with intermediate fields and source_to_concept_map table
-- cdm_target   - cdm tables without intermediate fields
-- -----------------------------------------------

-- -----------------------------------------------
-- re-create vocabulary tables
-- -----------------------------------------------

USE voc;

-- -----------------------------------------------
-- concept
-- -----------------------------------------------
DROP TABLE IF EXISTS concept;

CREATE TABLE concept
(
    concept_id int,
    concept_name varchar(1000),
    domain_id varchar(1000),
    vocabulary_id varchar(1000),
    concept_class_id varchar(1000),
    standard_concept varchar(1000),
    concept_code varchar(1000),
    valid_start_date datetime,
    valid_end_date datetime,
    invalid_reason varchar(1000)
);

-- -----------------------------------------------
-- concept_ancestor
-- -----------------------------------------------
DROP TABLE IF EXISTS concept_ancestor;

CREATE TABLE concept_ancestor
(
    ancestor_concept_id int,
    descendant_concept_id int,
    min_levels_of_separation int,
    max_levels_of_separation int
);

-- -----------------------------------------------
-- concept_class
-- -----------------------------------------------
DROP TABLE IF EXISTS concept_class;

CREATE TABLE concept_class
(
    concept_class_id varchar(1000),
    concept_class_name varchar(1000),
    concept_class_concept_id int
);

-- -----------------------------------------------
-- concept_relationship
-- -----------------------------------------------
DROP TABLE IF EXISTS concept_relationship;

CREATE TABLE concept_relationship
(
    concept_id_1 int,
    concept_id_2 int,
    relationship_id varchar(1000),
    valid_start_date datetime,
    valid_end_date datetime,
    invalid_reason varchar(1000)
);

-- -----------------------------------------------
-- concept_synonym
-- -----------------------------------------------
DROP TABLE IF EXISTS concept_synonym;

CREATE TABLE concept_synonym
(
    concept_id int,
    concept_synonym_name varchar(1000),
    language_concept_id int
);

-- -----------------------------------------------
-- domain
-- -----------------------------------------------
DROP TABLE IF EXISTS domain;

CREATE TABLE domain
(
    domain_id varchar(1000),
    domain_name varchar(1000),
    domain_concept_id int
);

-- -----------------------------------------------
-- drug_strength
-- -----------------------------------------------
DROP TABLE IF EXISTS drug_strength;

CREATE TABLE drug_strength
(
    drug_concept_id int,
    ingredient_concept_id int,
    amount_value decimal(20,6),
    amount_unit_concept_id int,
    numerator_value decimal(20,6),
    numerator_unit_concept_id int,
    denominator_value decimal(20,6),
    denominator_unit_concept_id int,
    box_size int,
    valid_start_date datetime,
    valid_end_date datetime,
    invalid_reason varchar(1000)
);

-- -----------------------------------------------
-- relationship
-- -----------------------------------------------
DROP TABLE IF EXISTS relationship;

CREATE TABLE relationship
(
    relationship_id varchar(1000),
    relationship_name varchar(1000),
    is_hierarchical int,
    defines_ancestry int,
    reverse_relationship_id varchar(1000),
    relationship_concept_id int
);

-- -----------------------------------------------
-- vocabulary
-- -----------------------------------------------
DROP TABLE IF EXISTS vocabulary;

CREATE TABLE vocabulary
(
    vocabulary_id varchar(1000),
    vocabulary_name varchar(1000),
    vocabulary_reference varchar(1000),
    vocabulary_version varchar(1000),
    vocabulary_concept_id int
);

-- -------------------------------------------------------------------
-- create indexes for table: concept
-- -------------------------------------------------------------------

ALTER TABLE concept ADD PRIMARY KEY (concept_id);
ALTER TABLE concept ADD KEY (vocabulary_id);
ALTER TABLE concept ADD KEY (concept_code);

-- -------------------------------------------------------------------
-- create indexes for table: concept_relationship
-- -------------------------------------------------------------------

ALTER TABLE concept_relationship ADD KEY (concept_id_1, relationship_id);
ALTER TABLE concept_relationship ADD KEY (concept_id_2);

-- -----------------------------------------------
-- cdm schema: source_to_concept_map
-- -----------------------------------------------
USE cdm;

DROP TABLE IF EXISTS cdm.source_to_concept_map;

CREATE TABLE cdm.source_to_concept_map
(
    source_code                 varchar(1000),
    source_concept_id           int,
    source_vocabulary_id        varchar(1000),
    source_code_description     varchar(1000),
    target_concept_id           int,
    target_vocabulary_id        varchar(20),
    valid_start_date            datetime default current_timestamp,
    valid_end_date              datetime default current_timestamp,
    invalid_reason              varchar(1),
    priority                    int
);

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
    time_of_birth datetime default current_timestamp,
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
    observation_period_end_date date NOT NULL,
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
    visit_start_time time NULL,
    visit_end_date date NOT NULL,
    visit_end_time time NULL,
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
    condition_end_date date,
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
    observation_time time,
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
    measurement_time time,
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
    device_exposure_end_date date,
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