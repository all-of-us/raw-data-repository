-- -----------------------------------------------
-- @2015-2017, Odysseus Data Services, Inc. All rights reserved 
-- CDMKit MySQL 
-- Generate script: 
-- srcload, master_data.conf 
-- -----------------------------------------------
-- -----------------------------------------------
-- new load batch
-- -----------------------------------------------

USE cdm;

-- -----------------------------------------------
-- source_to_concept_map
-- -----------------------------------------------
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

CREATE UNIQUE INDEX ux_source_to_concept_map ON cdm.source_to_concept_map (source_code, source_concept_id, target_concept_id, target_vocabulary_id);


-- -----------------------------------------------
-- source_to_concept_map
-- -----------------------------------------------
TRUNCATE TABLE source_to_concept_map;
LOAD DATA LOCAL INFILE 'D:\\home\\ppi_dev\\projects\\ppi\\load\\source_to_concept_map.csv'
INTO TABLE source_to_concept_map
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
IGNORE 0 LINES
(
    source_code,
    source_concept_id,
    source_vocabulary_id,
    source_code_description,
    @target_concept_id,
    @target_vocabulary_id,
    @valid_start_date,
    @valid_end_date,
    @invalid_reason,
    @priority
)
SET 
    target_concept_id = if((length(@target_concept_id) = 0 or @target_concept_id = '\r'), 
        NULL, @target_concept_id),
    target_vocabulary_id = if((length(@target_vocabulary_id) = 0 or @target_vocabulary_id = '\r'), 
        NULL, @target_vocabulary_id),
    valid_start_date = if((length(@valid_start_date) = 0 or @valid_start_date = '\r'), 
        NULL, @valid_start_date),
    valid_end_date = if((length(@valid_end_date) = 0 or @valid_end_date = '\r'), 
        NULL, @valid_end_date),
    invalid_reason = if((length(@invalid_reason) = 0 or @invalid_reason = '\r'), 
        NULL, @invalid_reason),
    priority = if((length(@priority) = 0 or @priority = '\r'), 
        NULL, @priority)
;

