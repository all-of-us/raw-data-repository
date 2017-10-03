-- -------------------------------------------------------------------
-- @2015-2017, Odysseus Data Services, Inc. All rights reserved
-- PPI OMOP CDM Conversion
-- last updated September 13, 2017
--
-- ddl script for schemas:
-- voc          - vocabulary tables
-- cdm          - cdm tables with intermediate fields and source_to_concept_map table
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
    valid_start_date date,
    valid_end_date date,
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
    valid_start_date date,
    valid_end_date date,
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
    valid_start_date date,
    valid_end_date date,
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

