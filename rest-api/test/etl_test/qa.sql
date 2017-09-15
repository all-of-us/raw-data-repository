-- -------------------------------------------------------------------
-- @2015-2017, Odysseus Data Services, Inc. All rights reserved
-- PPI OMOP CDM Conversion
-- last updated September 13, 2017
--
-- QA (integration tests) for:
-- table src_clean
-- table person
-- table observation
--
-- Results are saved in table cdm.qa_result
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- result table: cdm.qa_result
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS cdm.qa_result;

CREATE TABLE cdm.qa_result
(
    qa_result_id int AUTO_INCREMENT NOT NULL,
    test_dat datetime DEFAULT current_timestamp,
    test_table varchar(255),
    test_unit varchar(255),
    test_descr varchar(255),
    test_result int,
    test_passed int AS (CASE test_result WHEN 0 THEN 1 ELSE 0 END),
    PRIMARY KEY (qa_result_id)
);

-- -------------------------------------------------------------------
-- testing table: cdm.src_clean
-- -------------------------------------------------------------------

-- check that at least one answer field is always populated
SELECT NULL INTO @test_result;

SELECT count(*) INTO @test_result
FROM cdm.src_clean
WHERE value_code_id is NULL
    AND value_boolean is NULL
    AND value_string is NULL
    AND value_number is NULL
;

INSERT INTO cdm.qa_result
(
    test_table, test_unit, test_descr, test_result
)
VALUES
(
    'src_clean', 
    '',
    'check that at least one answer field is always populated',
    @test_result
);

-- check counts for participants
SELECT NULL INTO @test_result;

SELECT c1.c - c2.c INTO @test_result
FROM
(
    select count(distinct pa.participant_id) as c
    from rdr.participant pa
    inner join rdr.hpo hp
        on pa.hpo_id = hp.hpo_id
        and hp.name != 'TEST'
        and pa.withdrawal_status != 2
    inner join rdr.questionnaire_response qr
        on pa.participant_id = qr.participant_id
    inner join rdr.questionnaire_response_answer qra
        on qr.questionnaire_response_id = qra.questionnaire_response_id
) c1,
(
    select count(distinct participant_id) as c
    from cdm.src_clean
) c2
;

INSERT INTO cdm.qa_result
(
    test_table, test_unit, test_descr, test_result
)
VALUES
(
    'src_clean', 
    '',
    'check counts for participants',
    @test_result
);


-- -------------------------------------------------------------------
-- testing table: cdm.person
-- -------------------------------------------------------------------

-- check duplicates in cdm person
SELECT NULL INTO @test_result;

SELECT COUNT(*) INTO @test_result
FROM
(
    select person_source_value
    from cdm.person
    group by person_source_value
    having count(*) > 1
) c1
;

INSERT INTO cdm.qa_result
(
    test_table, test_unit, test_descr, test_result
)
VALUES
(
    'person', 
    '',
    'check duplicates in cdm person',
    @test_result
);

-- check counts
SELECT NULL INTO @test_result;

SELECT c1.c - c2.c INTO @test_result
from
    (
        select count(distinct person_source_value) AS c
        from cdm.person
    ) c1,
    (
        
        select count(*) as c
        from
            (
                select pa.participant_id, count( distinct qra.value_date)
                from rdr.participant pa
                inner join rdr.hpo hpo
                    on pa.hpo_id = hpo.hpo_id
                    and hpo.name != 'TEST'
                inner join rdr.questionnaire_response qr
                    on pa.participant_id = qr.participant_id
                inner join rdr.questionnaire_response_answer qra
                    on qra.questionnaire_response_id = qr.questionnaire_response_id
                inner join rdr.questionnaire_question qq
                    on qra.question_id = qq.questionnaire_question_id
                inner join rdr.code co_q
                    on qq.code_id = co_q.code_id
                    and co_q.value = 'PIIBirthInformation_BirthDate'
                where pa.withdrawal_status !=2
                and qra.value_date IS NOT NULL
                group by pa.participant_id
                having count( distinct qra.value_date) = 1
            ) a
    ) c2       
;

INSERT INTO cdm.qa_result
(
    test_table, test_unit, test_descr, test_result
)
VALUES
(
    'person', 
    '',
    'check counts',
    @test_result
);


-- -------------------------------------------------------------------
-- testing table: cdm.observation
-- -------------------------------------------------------------------

-- questionnaire_response_answer.value_boolean
SELECT NULL INTO @test_result;

select  sum(
        (qr.participant_id - obs.person_id) -- person_id
        +
        (coalesce(cr.concept_id_2, 0) -  obs.observation_concept_id) -- observation_concept_id
        +
        (date(qr.created) - obs.observation_date) -- observation_date
        +
        (time(qr.created) - obs.observation_time) -- observation_time
        +
        (obs.observation_type_concept_id - 45905771) -- observation_type_concept_id
        +
        (coalesce(obs.value_as_number, 0)) -- value_as_number
        +
        (coalesce(obs.value_as_string, 0)) -- value_as_string
        +
        CASE 
            WHEN qra.value_boolean = 1 THEN (obs.value_as_concept_id - 45877994)
            WHEN qra.value_boolean = 0 THEN (obs.value_as_concept_id - 45878245)
            -- ELSE 0
        END
        +
        (obs.qualifier_concept_id - 0) -- qualifier_concept_id
        +
        (obs.unit_concept_id - 0) -- unit_concept_id
        +
        -- TODO: provider
        -- TODO: visit
        (cd.value - obs.observation_source_value) -- observation_source_value
        +
        (obs.observation_source_concept_id - coalesce(c.concept_id, 0)) -- observation_source_concept_id
        +
        (coalesce(obs.unit_source_value, 0))-- unit_source_value
        +
        (coalesce(obs.qualifier_source_value, 0)) -- qualifier_source_value
        +
        (coalesce(obs.value_source_concept_id, 0)) -- value_source_concept_id
        +
        (coalesce(obs.value_source_value, 0)) -- value_source_value
     ) INTO @test_result
from rdr.questionnaire_response_answer qra
inner join rdr.questionnaire_response qr on qra.questionnaire_response_id = qr.questionnaire_response_id
inner join rdr.questionnaire_question qq on qq.questionnaire_question_id = qra.question_id
inner join rdr.code cd on cd.code_id = qq.code_id

left join voc.concept c on c.concept_code = cd.value AND c.vocabulary_id = 'AllOfUs_PPI'
left join voc.concept_relationship cr 
    on c.concept_id = cr.concept_id_1 
        and cr.invalid_reason is null
        and cr.relationship_id = 'Maps to'

inner join cdm.observation obs 
    on qr.questionnaire_response_id = obs.questionnaire_response_id
    and obs.observation_source_value = cd.value
    and 
    CASE 
            WHEN obs.value_as_concept_id = 45877994 THEN qra.value_boolean = 1
            WHEN obs.value_as_concept_id = 45878245 THEN qra.value_boolean = 0
    END
    
where qra.value_boolean is not null and obs.unit_id = 'observ.bool'
;

INSERT INTO cdm.qa_result
(
    test_table, test_unit, test_descr, test_result
)
VALUES
(
    'observation', 
    '',
    'questionnaire_response_answer.value_boolean',
    @test_result
);
