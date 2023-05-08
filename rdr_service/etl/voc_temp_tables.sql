-- Create and populate
DROP TABLE IF EXISTS voc.tmp_con_rel_mapsto;
create table voc.tmp_con_rel_mapsto
(
    concept_id_1 int null,
    concept_id_2 int null
);

create index tmp_con_rel_mapsto_concept_id_1_index
    on voc.tmp_con_rel_mapsto (concept_id_1);

create index tmp_con_rel_mapsto_concept_id_2_index
    on voc.tmp_con_rel_mapsto (concept_id_2);

DROP TABLE IF EXISTS voc.tmp_con_rel_mapstoval;
create table voc.tmp_con_rel_mapstoval
(
    concept_id_1 int null,
    concept_id_2 int null
);

create index tmp_con_rel_mapstoval_concept_id_1_index
    on voc.tmp_con_rel_mapstoval (concept_id_1);

create index tmp_con_rel_mapstoval_concept_id_2_index
    on voc.tmp_con_rel_mapstoval (concept_id_2);

DROP TABLE IF EXISTS voc.tmp_voc_concept;
create table voc.tmp_voc_concept
(
    concept_id   int           not null
        primary key,
    concept_code varchar(1000) null
);

create index tmp_voc_concept_concept_code_index
    on voc.tmp_voc_concept (concept_code);

DROP TABLE IF EXISTS voc.tmp_voc_concept_s;
create table voc.tmp_voc_concept_s
(
    concept_id int not null
        primary key
);

INSERT INTO voc.tmp_voc_concept
SELECT concept.concept_id AS concept_id,
       concept.concept_code AS concept_code
FROM voc.concept
WHERE concept.vocabulary_id = 'PPI'
;

INSERT INTO voc.tmp_voc_concept_s
SELECT concept_id
FROM voc.concept
WHERE standard_concept = 'S'
AND invalid_reason IS NULL
;

INSERT INTO voc.tmp_con_rel_mapsto
SELECT concept_id_1, concept_id_2
FROM voc.concept_relationship
WHERE relationship_id = 'Maps to'
AND invalid_reason IS NULL
;

INSERT INTO voc.tmp_con_rel_mapsto
SELECT concept_id_1, concept_id_2
FROM voc.concept_relationship
WHERE relationship_id = 'Maps to value'
AND invalid_reason IS NULL
;
