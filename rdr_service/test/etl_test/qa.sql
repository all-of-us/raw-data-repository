-- -------------------------------------------------------------------
-- @2015-2017, Odysseus Data Services, Inc. All rights reserved
-- PPI OMOP CDM Conversion
-- last updated September 22, 2017
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- source_file: qa/qa_result_create.sql
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
    test_result decimal(20),
    test_passed int AS (CASE test_result WHEN 0 THEN 1 ELSE 0 END),
    PRIMARY KEY (qa_result_id)
);



-- -------------------------------------------------------------------
-- source_file: qa/qa_src_clean_value.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------
-- table: src_clean
-- -------------------------------------------------------

-- check src_clean for duplicates
SELECT NULL INTO @test_result;
select count(*) INTO @test_result
from
(
    select participant_id, date_of_survey, question_ppi_code,
    COALESCE(value_code_id, value_number, value_boolean, value_date, value_string),
    count(*)
    from cdm.src_clean
    group by participant_id, date_of_survey, question_ppi_code,
    COALESCE(value_code_id, value_number, value_boolean, value_date, value_string)
    having count(*) > 1
) as t
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'src_clean', '',
    'check src_clean for duplicates',
    @test_result
);


-- Test 1: compare source and src_clean
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from

  (
    select count(*) as c
    from cdm.src_clean c1
    where value_boolean is not null and questionnaire_response_id is not null
  ) c1,

 (
    SELECT count(*) as c
    FROM
    (
        SELECT  pa.participant_id,
                qr.created,
                co_q.short_value as v1,
                qq.code_id,
                co_a.short_value as v2,
                co_a.topic,
                qra.value_code_id,
                coalesce(qra.value_integer, qra.value_decimal),
                qra.value_boolean,
                coalesce(qra.value_date, qra.value_datetime),
                coalesce(co_a.display, qra.value_date, qra.value_datetime, qra.value_string)
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
        AND qra.value_boolean IS NOT NULL
        GROUP BY  pa.participant_id,
                  qr.created,
                  co_q.short_value,
                  qq.code_id,
                  co_a.short_value,
                  co_a.topic,
                  qra.value_code_id,
                  coalesce(qra.value_integer, qra.value_decimal),
                  qra.value_boolean,
                  coalesce(qra.value_date, qra.value_datetime),
                  coalesce(co_a.display, qra.value_date, qra.value_datetime, qra.value_string)

    ) as t
  ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'src_clean', 'value_boolean',
    'compare source and src_clean',
    @test_result
);

-- compare src_clean amd src_mapped
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
  (
    select count(*) as c
    from cdm.src_clean c1
    JOIN cdm.person src_p
    ON  c1.participant_id = src_p.person_id
    where value_boolean is not null  and questionnaire_response_id is not null
  ) c1,

 (
    select count(*) as c
    from cdm.src_mapped
    where value_boolean is not null  and questionnaire_response_id is not null

  ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'src_clean', 'value_boolean',
    'compare src_clean amd src_mapped',
    @test_result
);

-- compare src_mapped and observation
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
  (
    select count(*) as c
    from cdm.observation obs
    where value_as_concept_id in (45877994, 45878245) and questionnaire_response_id and unit_id = 'observ.bool'
  ) c1,

 (
    select count(*) as c
    from cdm.src_mapped
    where value_boolean is not null and questionnaire_response_id is not null

  ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'src_clean', 'observ.bool',
    'compare src_mapped and observation',
    @test_result
);

-- compare source and src_clean
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
  (
    select count(*) as c
    from cdm.src_clean c1
    where value_string is not null and questionnaire_response_id is not null
  ) c1,

 (
    select count(*) as c
    from
    (
        SELECT  pa.participant_id,
                qr.created,
                co_q.short_value as v1,
                qq.code_id,
                co_a.short_value as v2,
                co_a.topic,
                qra.value_code_id,
                coalesce(qra.value_integer, qra.value_decimal),
                qra.value_boolean,
                coalesce(qra.value_date, qra.value_datetime),
                coalesce(co_a.display, qra.value_date, qra.value_datetime, qra.value_string)
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
            AND (qra.value_string IS NOT NULL
                    or
                 qra.value_date is not null
                    or
                 qra.value_datetime is not null
                    or
                 co_a.short_value is not null
                )
        group by  pa.participant_id,
                  qr.created,
                  co_q.short_value,
                  qq.code_id,
                  co_a.short_value,
                  co_a.topic,
                  qra.value_code_id,
                  coalesce(qra.value_integer, qra.value_decimal),
                  qra.value_boolean,
                  coalesce(qra.value_date, qra.value_datetime),
                  coalesce(co_a.display, qra.value_date, qra.value_datetime, qra.value_string)
    ) as t
  ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'src_clean', 'value_string',
    'compare source and src_clean',
    @test_result
);

-- compare src_clean amd src_mapped
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
  (
    select count(*) as c
    from cdm.src_clean c1
    JOIN cdm.person src_p
    ON  c1.participant_id = src_p.person_id
    where value_string is not null and questionnaire_response_id is not null
  ) c1,

 (
    select count(*) as c
    from cdm.src_mapped
    where value_string is not null and questionnaire_response_id is not null

  ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'src_clean', 'value_string',
    'compare src_clean amd src_mapped',
    @test_result
);

-- compare src_mapped and observation
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
  (
    select count(*) as c
    from cdm.observation obs
    where value_as_string is not null and questionnaire_response_id is not null and unit_id = 'observ.str'
  ) c1,

 (
    select count(*) as c
    from cdm.src_mapped
    where value_string is not null
        and value_ppi_code IS NULL
        and questionnaire_response_id is not null

  ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'src_clean', 'observ.str',
    'compare src_mapped and observation',
    @test_result
);

-- compare source and src_clean
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
  (
    select count(*) as c
    from cdm.src_clean c1
    where value_number is not null and questionnaire_response_id is not null
  ) c1,

 (
    SELECT count(*) as c
    FROM
    (   SELECT  pa.participant_id,
                qr.created,
                co_q.short_value as v1,
                qq.code_id,
                co_a.short_value as v2,
                co_a.topic,
                qra.value_code_id,
                coalesce(qra.value_integer, qra.value_decimal),
                qra.value_boolean,
                coalesce(qra.value_date, qra.value_datetime),
                coalesce(co_a.display, qra.value_date, qra.value_datetime, qra.value_string)
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
        (qra.value_integer IS NOT NULL
            or
         qra.value_decimal is not null
        )

        group by  pa.participant_id,
                  qr.created,
                  co_q.short_value,
                  qq.code_id,
                  co_a.short_value,
                  co_a.topic,
                  qra.value_code_id,
                  coalesce(qra.value_integer, qra.value_decimal),
                  qra.value_boolean,
                  coalesce(qra.value_date, qra.value_datetime),
                  coalesce(co_a.display, qra.value_date, qra.value_datetime, qra.value_string)
    ) as t
  ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'src_clean', 'value_number',
    'compare source and src_clean',
    @test_result
);

-- compare src_clean amd src_mapped
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
  (
    select count(*) as c
    from cdm.src_clean c1
    JOIN cdm.person src_p
    ON  c1.participant_id = src_p.person_id
    where value_number is not null and questionnaire_response_id is not null
  ) c1,

 (
    select count(*) as c
    from cdm.src_mapped
    where value_number is not null and questionnaire_response_id is not null

  ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'src_clean', 'value_number',
    'compare src_clean amd src_mapped',
    @test_result
);

-- compare src_mapped and observation
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
  (
    select count(*) as c
    from cdm.observation obs
    where value_as_number is not null and questionnaire_response_id is not null and unit_id = 'observ.num'
  ) c1,

 (
    select count(*) as c
    from cdm.src_mapped
    where value_number is not null and questionnaire_response_id is not null

  ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'src_clean', 'value_number',
    'compare src_mapped and observation',
    @test_result
);


-- compare source and src_clean
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
  (
    select count(*) as c
    from cdm.src_clean c1
    where value_ppi_code is not null and questionnaire_response_id is not null
  ) c1,

 (
    select count(*) as c
    from
    (
        SELECT  pa.participant_id,
                qr.created,
                co_q.short_value as v1,
                qq.code_id,
                co_a.short_value as v2,
                co_a.topic,
                qra.value_code_id,
                coalesce(qra.value_integer, qra.value_decimal),
                qra.value_boolean,
                coalesce(qra.value_date, qra.value_datetime),
                coalesce(co_a.display, qra.value_date, qra.value_datetime, qra.value_string)
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
            AND co_a.short_value is not null
        group by  pa.participant_id,
                  qr.created,
                  co_q.short_value,
                  qq.code_id,
                  co_a.short_value,
                  co_a.topic,
                  qra.value_code_id,
                  coalesce(qra.value_integer, qra.value_decimal),
                  qra.value_boolean,
                  coalesce(qra.value_date, qra.value_datetime),
                  coalesce(co_a.display, qra.value_date, qra.value_datetime, qra.value_string)
    ) as t
  ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'src_clean', 'value_code',
    'compare source and src_clean',
    @test_result
);

-- compare src_clean amd src_mapped
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
  (
    select count(*) as c
    from cdm.src_clean c1
    JOIN cdm.person src_p
    ON  c1.participant_id = src_p.person_id
    where value_ppi_code is not null and questionnaire_response_id is not null
  ) c1,

 (
    select count(*) as c
    from cdm.src_mapped
    where value_ppi_code is not null and questionnaire_response_id is not null

  ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'src_clean', 'value_code',
    'compare src_clean amd src_mapped',
    @test_result
);

-- compare mapped and observation
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
  (
    select count(*) as c
    from cdm.observation obs
    where questionnaire_response_id is not null and unit_id = 'observ.code'
  ) c1,

 (
    select count(*) as c
    from cdm.src_mapped
    where value_ppi_code IS NOT NULL
        and questionnaire_response_id is not null

  ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'src_clean', 'observ.code',
    'compare src_mapped and observation',
    @test_result
);




-- -------------------------------------------------------------------
-- source_file: qa/qa_location.sql
-- -------------------------------------------------------------------

-- -----------------------------------------------------
-- table: location
-- -----------------------------------------------------

-- comment: check that states coming from topic states
SELECT NULL INTO @test_result;
select count(*) INTO @test_result
from cdm.location loc
left join rdr.code co
    on loc.location_source_value = co.short_value
    and co.topic = 'States'
where loc.location_source_value is not null
    and co.short_value is null;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'location', '',
    'check that states coming from topic states',
    @test_result
);

-- Comment: check duplicates for cdm location table
SELECT NULL INTO @test_result;
SELECT c1.c - c2.c INTO @test_result
from
    (
        select count(*) AS c
        from cdm.location
    ) c1,
    (
        select count(*) as c
        from
            (
                select distinct address_1, address_2, city, state, zip
                from cdm.location
            ) a
    ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'location', '',
    'check duplicates for cdm location table',
    @test_result
);

-- Comment : Check values of fields
SELECT NULL INTO @test_result;
select COALESCE(c2.result, c1.count) as result INTO @test_result
from
    (select count(*) as count
     from cdm.location
    ) c1,

    (select  sum(
            (coalesce(t_src.person_id, 0) - coalesce(t_cdm.person_id, 0))                       -- address_1
             +
            (coalesce(t_src.address_1, 0) - coalesce(t_cdm.address_1, 0))                       -- address_1
             +
            (coalesce(t_src.address_2, 0) - coalesce(t_cdm.address_2, 0))                       -- address_2
             +
            (coalesce(t_src.city, 0) - coalesce(t_cdm.city, 0))                                 -- city
             +
            (coalesce(t_src.state, 0) - coalesce(t_cdm.state, 0))                               -- state
             +
            (coalesce(t_src.zip, 0) - coalesce(t_cdm.zip, 0))                                   -- zip
             +
            coalesce(t_cdm.county, 0)                                                           -- county
             +
            (coalesce(t_src.ppi_code, 0) - coalesce(t_cdm.location_source_value, 0))            -- location_source_value

         ) as result
    from (
        select distinct
            a1.participant_id       as person_id,
            a1.created              as last_date,
            a2.value_string         as address_1,
            a3.value_string         as address_2,
            a4.value_string         as city,
            right(a5.short_value, 2)      as state,
            a6.value_string         as zip,
            a5.short_value                as ppi_code
        from
            (select distinct
                qr.participant_id,
                max(qr.created)   as created
            from rdr.questionnaire_response_answer qra
            inner join rdr.questionnaire_response qr
                on qra.questionnaire_response_id = qr.questionnaire_response_id
            inner join rdr.questionnaire_question qq
                on qq.questionnaire_question_id = qra.question_id
            inner join rdr.code cd_q
                on cd_q.code_id = qq.code_id
            inner join cdm.person p
                on p.person_id = qr.participant_id
            left join rdr.code cd_a
                    on qra.value_code_id = cd_a.code_id
            where
                (cd_q.short_value in ( 'PIIAddress_StreetAddress',
                                'PIIAddress_StreetAddress2',
                                'StreetAddress_PIICity',
                                'StreetAddress_PIIZIP')
                and qra.value_string is not null
                )

                or

                (cd_q.short_value = 'StreetAddress_PIIState'
                and cd_a.topic = 'States'
                and cd_a.short_value is not null
                    )
            group by
                qr.participant_id
            ) a1
        left join
                (select distinct
                    qr.participant_id,
                    qr.created,
                    qra.value_string
                from rdr.questionnaire_response_answer qra
                inner join rdr.questionnaire_response qr
                    on qra.questionnaire_response_id = qr.questionnaire_response_id
                inner join rdr.questionnaire_question qq
                    on qq.questionnaire_question_id = qra.question_id
                inner join rdr.code cd
                    on cd.code_id = qq.code_id
                where cd.short_value = 'PIIAddress_StreetAddress'
                ) a2
            on a1.participant_id = a2.participant_id
            and a1.created = a2.created

        left join
                (select distinct
                    qr.participant_id,
                    qr.created,
                    qra.value_string
                from rdr.questionnaire_response_answer qra
                inner join rdr.questionnaire_response qr
                    on qra.questionnaire_response_id = qr.questionnaire_response_id
                inner join rdr.questionnaire_question qq
                    on qq.questionnaire_question_id = qra.question_id
                inner join rdr.code cd
                    on cd.code_id = qq.code_id
                where cd.short_value = 'PIIAddress_StreetAddress2'
                ) a3
            on a1.participant_id = a3.participant_id
            and a1.created = a3.created

        left join
                (select distinct
                    qr.participant_id,
                    qr.created,
                    qra.value_string
                from rdr.questionnaire_response_answer qra
                inner join rdr.questionnaire_response qr
                    on qra.questionnaire_response_id = qr.questionnaire_response_id
                inner join rdr.questionnaire_question qq
                    on qq.questionnaire_question_id = qra.question_id
                inner join rdr.code cd
                    on cd.code_id = qq.code_id
                where cd.short_value = 'StreetAddress_PIICity'
                ) a4
            on a1.participant_id = a4.participant_id
            and a1.created = a4.created

        left join
                (select distinct
                    qr.participant_id,
                    qr.created,
                    cd_a.short_value
                from rdr.questionnaire_response_answer qra
                inner join rdr.questionnaire_response qr
                    on qra.questionnaire_response_id = qr.questionnaire_response_id
                inner join rdr.questionnaire_question qq
                    on qq.questionnaire_question_id = qra.question_id
                inner join rdr.code cd_q
                    on cd_q.code_id = qq.code_id
                inner join rdr.code cd_a
                    on qra.value_code_id = cd_a.code_id
                where cd_q.short_value = 'StreetAddress_PIIState'
                    and cd_a.topic = 'States'
                ) a5
            on a1.participant_id = a5.participant_id
            and a1.created = a5.created

        left join
                (select distinct
                    qr.participant_id,
                    qr.created,
                    qra.value_string
                from rdr.questionnaire_response_answer qra
                inner join rdr.questionnaire_response qr
                    on qra.questionnaire_response_id = qr.questionnaire_response_id
                inner join rdr.questionnaire_question qq
                    on qq.questionnaire_question_id = qra.question_id
                inner join rdr.code cd
                    on cd.code_id = qq.code_id
                where cd.short_value = 'StreetAddress_PIIZIP'
                ) a6
            on a1.participant_id = a6.participant_id
            and a1.created = a6.created
        ) t_src
    left join
        (   select
                per.person_id,
                loc.address_1,
                loc.address_2,
                loc.city,
                loc.state,
                loc.zip,
                loc.location_source_value,
                loc.county
            from cdm.person per
            inner join cdm.location loc
                on per.location_id = loc.location_id
            ) t_cdm
        ON t_src.person_id = t_cdm.person_id
    ) c2
;

    INSERT INTO cdm.qa_result
    (test_table, test_unit, test_descr, test_result)
    VALUES
    (   'location', '',
        'check values of fields',
        @test_result
    );



-- -------------------------------------------------------------------
-- source_file: qa/qa_care_site.sql
-- -------------------------------------------------------------------

-- ---------------------------------------------------
-- table: care_site
-- ---------------------------------------------------

-- Check duplicates in cdm Care_site
SELECT NULL INTO @test_result;
SELECT c1.c - c2.c INTO @test_result
from
    (
        select count(*) AS c
        from cdm.care_site
    ) c1,
    (
        select count(*) as c
        from
            (
                select distinct care_site_name, care_site_source_value
                from cdm.care_site
            ) a
    ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'care_site', '',
    'Check duplicates in cdm Care_site',
    @test_result
);

-- Test 2: check counts in cdm Care_site table and in the source data
SELECT NULL INTO @test_result;
SELECT c1.c - c2.c INTO @test_result
from
    (
        select count(*) AS c
        from cdm.care_site
    ) c1,
    (
        select count(distinct pm.finalized_site_id) AS c
        from rdr.physical_measurements pm
        inner join cdm.person pe
                on pm.participant_id = pe.person_id
        where pm.finalized_site_id is not null
    ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'care_site', '',
    'check counts in cdm Care_site table and in the source data',
    @test_result
);

-- Test 3: check if care_site_name field is properly populated
SELECT NULL INTO @test_result;
select count(*) INTO @test_result
from cdm.care_site cs
inner join rdr.site site
    on cs.care_site_source_value = site.site_id
    and cs.care_site_name != site.site_name;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'care_site', '',
    'check if care_site_name field is properly populated',
    @test_result
);

-- Test 4: Check values of fields
SELECT NULL INTO @test_result;
select COALESCE(c2.result, c1.count) as result INTO @test_result
from
    (select count(*) as count
     from cdm.care_site
    ) c1,

    (select  sum(
            (coalesce(s.site_name, 0) - cs.care_site_name)              -- care_site_name
             +
            (coalesce(cs.place_of_service_concept_id, 0))               -- place_of_service_concept_id
             +
            (coalesce(cs.location_id, 0))                               -- location_id
             +
            (pm.finalized_site_id - cs.care_site_source_value)          -- care_site_source_value
             +
            (coalesce(cs.place_of_service_source_value, 0))             -- place_of_service_source_value
         ) as result
    from rdr.physical_measurements pm
    inner join rdr.measurement me
        on pm.physical_measurements_id = me.physical_measurements_id
        and pm.final = 1
        and pm.finalized_site_id is not null
    inner join cdm.person per
        on pm.participant_id = per.person_id
    inner join rdr.site s
        on pm.finalized_site_id = s.site_id
    left join cdm.care_site cs
        on cs.care_site_source_value = pm.finalized_site_id
    ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'care_site', '',
    'Check values of fields',
    @test_result
);



-- -------------------------------------------------------------------
-- source_file: qa/qa_person.sql
-- -------------------------------------------------------------------

-- --------------------------------------------------------
-- table:person
-- --------------------------------------------------------

-- Comment: check counts
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
                inner join
                (select t2.participant_id, max(t2.created) as created
                    from
                    (
                        select  qr.participant_id, qr.created
                        from rdr.questionnaire_response qr
                            inner join rdr.questionnaire_response_answer qra
                                on qra.questionnaire_response_id = qr.questionnaire_response_id
                            inner join rdr.questionnaire_question qq on qq.questionnaire_question_id = qra.question_id
                            inner join rdr.code cd on cd.code_id = qq.code_id
                            -- left join rdr.code cd_ans on cd_ans.code_id = qra.value_code_id
                            where cd.short_value in ('PIIBirthInformation_BirthDate') and qra.value_date is not null
                    ) as t2
                 group by t2.participant_id
                ) t3
                on pa.participant_id = t3.participant_id
                where pa.withdrawal_status !=2
                and qra.value_date IS NOT NULL
                group by pa.participant_id
            ) a
    ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'person', '',
    'check counts in cdm person',
    @test_result
);

-- Comment: check duplicates for cdm person table
SELECT NULL INTO @test_result;
SELECT c1.c - c2.c INTO @test_result
from
    (
        select count(*) AS c
        from cdm.person
    ) c1,
    (
        select count(*) as c
        from
            (
                select distinct person_source_value
                from cdm.person
            ) a
    ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'person', '',
    'check duplicates for cdm person',
    @test_result
);

-- Check gender count------------------------------
SELECT NULL INTO @test_result;
select c1.c - c2.c as c INTO @test_result
from
(
    (
        select count(*) as c
        from
        (
            select  person_id, cd_ans.short_value
            from cdm.person p

            inner join rdr.questionnaire_response qr on p.person_id = qr.participant_id
            inner join rdr.questionnaire_response_answer qra
                on qra.questionnaire_response_id = qr.questionnaire_response_id
            inner join rdr.code cd_ans
                on cd_ans.code_id = qra.value_code_id
            where cd_ans.short_value in ('SexAtBirth_Male', 'SexAtBirth_Female')
            group by person_id, cd_ans.short_value -- , gender_concept_id, gender_source_value
            having count(distinct cd_ans.short_value) = 1
        ) t
    ) c1,

    (
        select count(*) as c
        from cdm.person
        where gender_concept_id > 0
    ) c2

) ;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'person', '',
    'Check gender count in cdm person (!= 0)',
    @test_result
);


-- Check gender related field values
SELECT NULL INTO @test_result;
    select  sum( p.person_id - coalesce(t.person_id, 0)
                +
                CASE
                    WHEN t.gender = 'SexAtBirth_Male' THEN (p.gender_concept_id - 8507)
                    WHEN t.gender = 'SexAtBirth_Female' THEN (p.gender_concept_id - 8532)
                    -- ELSE 0
                END
               +
               p.gender_source_value - t.gender
               ) as result INTO @test_result
    from cdm.person p
    left join
    (
        select  person_id, cd_ans.short_value as gender
        from cdm.person p

        inner join rdr.questionnaire_response qr on p.person_id = qr.participant_id
        inner join rdr.questionnaire_response_answer qra
            on qra.questionnaire_response_id = qr.questionnaire_response_id
        inner join rdr.code cd_ans
            on cd_ans.code_id = qra.value_code_id
        where cd_ans.short_value in ('SexAtBirth_Male', 'SexAtBirth_Female')
        group by person_id, cd_ans.short_value -- , gender_concept_id, gender_source_value
        having count(distinct cd_ans.short_value) = 1
    ) t
    on t.person_id = p.person_id
    where gender_concept_id > 0;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'person', '',
    'Check gender related field values in cdm person',
    @test_result
);

-- Check birth date count--------------------------
SELECT NULL INTO @test_result;
select c1.c - c2.c as c INTO @test_result
from
(
        select count(*) as c
        from
        (       select person_id, max(value_date)
                from
                (
                    select  person_id, qra.value_date, qr.created
                    from cdm.person p

                    inner join rdr.questionnaire_response qr on p.person_id = qr.participant_id
                    inner join rdr.questionnaire_response_answer qra
                        on qra.questionnaire_response_id = qr.questionnaire_response_id
                    inner join rdr.questionnaire_question qq on qra.question_id = qq.questionnaire_question_id
                    inner join rdr.code cd
                        on cd.code_id = qq.code_id
                    where cd.short_value in ('PIIBirthInformation_BirthDate') and qra.value_date is not null
                ) t1
                inner join
                (select t2.participant_id, max(t2.created) as created
                    from
                    (
                        select  qr.participant_id, qr.created
                        from rdr.questionnaire_response qr
                            inner join rdr.questionnaire_response_answer qra
                                on qra.questionnaire_response_id = qr.questionnaire_response_id
                            inner join rdr.questionnaire_question qq on qq.questionnaire_question_id = qra.question_id
                            inner join rdr.code cd on cd.code_id = qq.code_id
                            -- left join rdr.code cd_ans on cd_ans.code_id = qra.value_code_id
                            where cd.short_value in ('PIIBirthInformation_BirthDate') and qra.value_date is not null
                    ) as t2
                 group by t2.participant_id
                ) t3
                    on t3.created = t1.created and t3.participant_id = t1.person_id
                group by person_id
        ) t4

) c1,
(   select count(*) as c
    from cdm.person

) c2;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'person', '',
    'Check birth date count in cdm person',
    @test_result
);

-- Check birth date related values
SELECT NULL INTO @test_result;
select sum(p.person_id - t.person_id
            +
            p.year_of_birth - year(t.birthdate)
            +
            p.month_of_birth - month(t.birthdate)
            +
            p.day_of_birth - day(t.birthdate)
            +
            coalesce(p.time_of_birth, 0)
            ) as result INTO @test_result
from cdm.person p
left join
(select person_id, max(value_date) as birthdate
                from
                (
                    select  person_id, qra.value_date, qr.created
                    from cdm.person p

                    inner join rdr.questionnaire_response qr on p.person_id = qr.participant_id
                    inner join rdr.questionnaire_response_answer qra
                        on qra.questionnaire_response_id = qr.questionnaire_response_id
                    inner join rdr.questionnaire_question qq on qra.question_id = qq.questionnaire_question_id
                    inner join rdr.code cd
                        on cd.code_id = qq.code_id
                    where cd.short_value in ('PIIBirthInformation_BirthDate') and qra.value_date is not null
                ) t1
                inner join
                (select t2.participant_id, max(t2.created) as created
                    from
                    (
                        select  qr.participant_id, qr.created
                        from rdr.questionnaire_response qr
                            inner join rdr.questionnaire_response_answer qra
                                on qra.questionnaire_response_id = qr.questionnaire_response_id
                            inner join rdr.questionnaire_question qq on qq.questionnaire_question_id = qra.question_id
                            inner join rdr.code cd on cd.code_id = qq.code_id
                            -- left join rdr.code cd_ans on cd_ans.code_id = qra.value_code_id
                            where cd.short_value in ('PIIBirthInformation_BirthDate') and qra.value_date is not null
                    ) as t2
                 group by t2.participant_id
                ) t3
                    on t3.created = t1.created and t3.participant_id = t1.person_id
                group by person_id

) as t on p.person_id = t.person_id
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'person', '',
    'Check birth date related values in cdm person',
    @test_result
);



-- -------------------------------------------------------------------
-- source_file: qa/qa_procedure_occurrence.sql
-- -------------------------------------------------------------------

-- --------------------------------------------------------
-- table:procedure_occurrence
-- --------------------------------------------------------

-- Check counts in cdm procedure occurrence
SELECT NULL INTO @test_result;
SELECT c1.c - c2.c  INTO @test_result
from
    (
        select count(*) AS c
        from cdm.procedure_occurrence
    ) c1,
    (
        select count(*) as c
        from
            (
                select distinct
                    qr1.participant_id,
                    COALESCE(vc.concept_id, 0) as proc_concept_id,
                    qra2.value_date,
                    cd_q1.code_id
                from rdr.questionnaire_response_answer qra1
                inner join rdr.questionnaire_response qr1
                    on qra1.questionnaire_response_id = qr1.questionnaire_response_id
                inner join cdm.person pp
                    on pp.person_id = qr1.participant_id
                inner join rdr.questionnaire_question qq1
                    on qq1.questionnaire_question_id = qra1.question_id
                inner join rdr.code cd_q1
                    on cd_q1.code_id = qq1.code_id
                inner join rdr.code cd_a1
                    on qra1.value_code_id = cd_a1.code_id
                inner join cdm.source_to_concept_map stcm
                    on cd_a1.short_value = stcm.source_code
                    and stcm.source_vocabulary_id = 'ppi-proc'

                inner join rdr.questionnaire_response_answer qra2
                    on qra1.questionnaire_response_id = qra2.questionnaire_response_id
                inner join rdr.questionnaire_response qr2
                    on qra2.questionnaire_response_id = qr2.questionnaire_response_id
                inner join rdr.questionnaire_question qq2
                    on qq2.questionnaire_question_id = qra2.question_id
                inner join rdr.code cd_q2
                    on cd_q2.code_id = qq2.code_id
                    and cd_q2.short_value = 'OrganTransplant_Date'
                    and qra2.value_date is not null

                left join voc.concept vc
                    on stcm.target_concept_id = vc.concept_id
                    and vc.standard_concept = 'S'
            ) a
    ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'procedure_occurrence', '',
    'Check counts in cdm procedure occurrence',
    @test_result
);

-- Check values of fields in cdm procedure occurrence
SELECT NULL INTO @test_result;
select sum(
        (qr1.participant_id - cdm_p.person_id )                                         -- person_id
         +
        (coalesce(vc.concept_id, 0) - cdm_p.procedure_concept_id)                       -- procedure_concept_id
         +
        (qra2.value_date - cdm_p.procedure_date)                                        -- procedure_date
         +
        (581412 - cdm_p.procedure_type_concept_id)                                      -- procedure_type_concept_id
         +
        (cdm_p.modifier_concept_id)                                                     -- modifier_concept_id
         +
        (coalesce(cdm_p.quantity, 0))                                                   -- quantity
         +
        (coalesce(cdm_p.provider_id, 0))                                                -- provider
         +
        (coalesce(cdm_p.visit_occurrence_id, 0))                                        -- visit_occurrence_id
         +
        (cd_a1.short_value - cdm_p.procedure_source_value)                                    -- procedure_source_value
         +
        (coalesce(stcm.source_concept_id, 0) - cdm_p.procedure_source_concept_id)       -- procedure_source_concept_id
         +
        (coalesce(cdm_p.qualifier_source_value, 0))                                     -- qualifier_source_value
     ) as result                                                                    INTO @test_result
from rdr.questionnaire_response_answer qra1
inner join rdr.questionnaire_response qr1
    on qra1.questionnaire_response_id = qr1.questionnaire_response_id
inner join cdm.person pp
    on pp.person_id = qr1.participant_id
inner join rdr.questionnaire_question qq1
    on qq1.questionnaire_question_id = qra1.question_id
inner join rdr.code cd_q1
    on cd_q1.code_id = qq1.code_id
inner join rdr.code cd_a1
    on qra1.value_code_id = cd_a1.code_id
inner join cdm.source_to_concept_map stcm
    on cd_a1.short_value = stcm.source_code
    and stcm.source_vocabulary_id = 'ppi-proc'

inner join rdr.questionnaire_response_answer qra2
    on qra1.questionnaire_response_id = qra2.questionnaire_response_id
inner join rdr.questionnaire_response qr2
    on qra2.questionnaire_response_id = qr2.questionnaire_response_id
inner join rdr.questionnaire_question qq2
    on qq2.questionnaire_question_id = qra2.question_id
inner join rdr.code cd_q2
    on cd_q2.code_id = qq2.code_id
    and cd_q2.short_value = 'OrganTransplant_Date'
    and qra2.value_date is not null

left join voc.concept vc
    on stcm.target_concept_id = vc.concept_id
    and vc.standard_concept = 'S'

left join cdm.procedure_occurrence cdm_p
    on cdm_p.person_id = qr1.participant_id
    and cdm_p.procedure_source_value = cd_a1.short_value
    and cdm_p.procedure_date = qra2.value_date
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'procedure_occurrence', '',
    'Check values of fields in cdm procedure occurrence',
    @test_result
);

-- -------------------------------------------------------------------
-- source_file: qa/qa_observation_qra_value.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- table: observation (survey)
-- -------------------------------------------------------------------

-- questionnaire_response_answer.value_boolean
SELECT NULL INTO @test_result;
select  sum(
        (qr.participant_id - obs.person_id) -- person_id
        +
        (coalesce(c1.concept_id, 0) -  obs.observation_concept_id) -- observation_concept_id
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
        (cd.short_value - obs.observation_source_value) -- observation_source_value
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

left join voc.concept c on c.concept_code = cd.short_value AND c.vocabulary_id = 'PPI'
left join voc.concept_relationship cr
    on c.concept_id = cr.concept_id_1
        and cr.invalid_reason is null
        and cr.relationship_id = 'Maps to'
left join voc.concept c1
    on c1.concept_id = cr.concept_id_2
        and c1.invalid_reason is null
        and c1.standard_concept = 'S'

inner join cdm.observation obs
    on qr.questionnaire_response_id = obs.questionnaire_response_id
    and obs.observation_source_value = cd.short_value
    and
    CASE
            WHEN obs.value_as_concept_id = 45877994 THEN qra.value_boolean = 1
            WHEN obs.value_as_concept_id = 45878245 THEN qra.value_boolean = 0
    END

where qra.value_boolean is not null and obs.unit_id = 'observ.bool'
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'observation', 'observ.bool',
    'questionnaire_response_answer.value_boolean',
    @test_result
);

-- questionnaire_response_answer.value_code
SELECT NULL INTO @test_result;
select  sum(
        (qr.participant_id - obs.person_id)                                                 -- person_id
        +
        (coalesce(c2.concept_id, 0) -  obs.observation_concept_id)                          -- observation_concept_id
        +
        (date(qr.created) - obs.observation_date)                                           -- observation_date
        +
        (time(qr.created) - obs.observation_time)                                           -- observation_time
        +
        (obs.observation_type_concept_id - 45905771)                                        -- observation_type_concept_id
        +
        (coalesce(obs.value_as_number, 0))                                                  -- value_as_number
        +
        CASE
            WHEN cr.concept_id_2 is null THEN (obs.value_as_string - cd_ans.display)        -- value_as_string
            ELSE 0
        END
        +
        (obs.value_as_concept_id - coalesce(c3.concept_id, 0))                              -- value_as_concept_id
        +
        (obs.qualifier_concept_id - 0)                                                      -- qualifier_concept_id
        +
        (obs.unit_concept_id - 0)                                                           -- unit_concept_id
        +
        (cd.short_value - obs.observation_source_value)                                           -- observation_source_value
        +
        (obs.observation_source_concept_id - coalesce(c.concept_id, 0))                     -- observation_source_concept_id
        +
        (coalesce(obs.unit_source_value, 0))                                                -- unit_source_value
        +
        (coalesce(obs.qualifier_source_value, 0))                                           -- qualifier_source_value
        +
        (obs.value_source_concept_id - coalesce(c1.concept_id, 0))                          -- value_source_concept_id
        +
        (obs.value_source_value - cd_ans.short_value)                                             -- value_source_value
     ) INTO @test_result
from rdr.questionnaire_response_answer qra
inner join rdr.questionnaire_response qr on qra.questionnaire_response_id = qr.questionnaire_response_id
inner join rdr.questionnaire_question qq on qq.questionnaire_question_id = qra.question_id
inner join rdr.code cd on cd.code_id = qq.code_id
inner join rdr.code cd_ans on qra.value_code_id = cd_ans.code_id

left join voc.concept c on c.concept_code = cd.short_value AND c.vocabulary_id = 'PPI'
left join voc.concept_relationship cr
    on c.concept_id = cr.concept_id_1
        and cr.invalid_reason is null
        and cr.relationship_id = 'Maps to'
left join voc.concept c2
    on c2.concept_id = cr.concept_id_2
        and c2.invalid_reason is null
        and c2.standard_concept = 'S'

left join voc.concept c1
    on c1.concept_code = cd_ans.short_value
        AND c1.vocabulary_id = 'PPI'
left join voc.concept_relationship cr1
    on c1.concept_id = cr1.concept_id_1
        and cr.invalid_reason is null
left join voc.concept c3
    on c3.concept_id = cr1.concept_id_2
        and c3.invalid_reason is null
        and c3.standard_concept = 'S'

inner join cdm.observation obs
    on qr.questionnaire_response_id = obs.questionnaire_response_id -- questionnaire instance
    and obs.observation_source_value = cd.short_value -- question
    and obs.value_source_value = cd_ans.short_value -- answer

where (qra.value_code_id is not null and cd_ans.code_id IS NOT NULL) and unit_id like 'observ.code'
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'observation', 'observ.code',
    'questionnaire_response_answer.value_code',
    @test_result
);

-- questionnaire_response_answer.value_date
SELECT NULL INTO @test_result;
select  sum(
        (qr.participant_id - obs.person_id) -- person_id
        +
        (coalesce(c1.concept_id, 0) -  obs.observation_concept_id) -- observation_concept_id
        +
        (date(qr.created) - obs.observation_date) -- observation_date
        +
        (time(qr.created) - obs.observation_time) -- observation_time
        +
        (obs.observation_type_concept_id - 45905771) -- observation_type_concept_id
        +
        (coalesce(obs.value_as_number, 0)) -- value_as_number
        +
        (obs.value_as_string - cast(qra.value_date as char)) -- value_as_string
        +
        (coalesce(obs.value_as_concept_id, 0)) -- value_as_concept_id
        +
        (obs.qualifier_concept_id - 0) -- qualifier_concept_id
        +
        (obs.unit_concept_id - 0) -- unit_concept_id
        +
        (cd.short_value - obs.observation_source_value) -- observation_source_value
        +
        (obs.observation_source_concept_id - coalesce(c.concept_id, 0)) -- observation_source_concept_id
        +
        (coalesce(obs.unit_source_value, 0)) -- unit_source_value
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

left join voc.concept c on c.concept_code = cd.short_value AND c.vocabulary_id = 'PPI'
left join voc.concept_relationship cr
    on c.concept_id = cr.concept_id_1
        and cr.invalid_reason is null
        and cr.relationship_id = 'Maps to'
left join voc.concept c1
    on c1.concept_id = cr.concept_id_2
    and c1.invalid_reason is null
    and c1.standard_concept = 'S'

inner join cdm.observation obs
    on qr.questionnaire_response_id = obs.questionnaire_response_id -- questionnaire instance id
    and obs.observation_source_value = cd.short_value -- question
    and obs.value_as_string = qra.value_date -- answer

where qra.value_date is not null and obs.unit_id = 'observ.str'
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'observation', 'observ.str',
    'questionnaire_response_answer.value_date',
    @test_result
);

-- Verify the difference between source values and observation table in case of values_integer
SELECT NULL INTO @test_result;
select sum(
        (qr.participant_id - obs.person_id) -- person_id
        +
        (coalesce(c1.concept_id, 0) -  obs.observation_concept_id) -- observation_concept_id
        +
        (date(qr.created) - obs.observation_date) -- observation_date
        +
        (time(qr.created) - obs.observation_time) -- observation_time
        +
        (obs.observation_type_concept_id - 45905771) -- observation_type_concept_id
        +
        (obs.value_as_number - qra.value_integer) -- value_as_number
        +
        (coalesce(obs.value_as_string, 0)) -- value_as_string
        +
        (obs.value_as_concept_id - 0) -- value_as_concept_id
        +
        (obs.qualifier_concept_id - 0) -- qualifier_concept_id
        +
        (obs.unit_concept_id - 0) -- unit_concept_id
        +
        (cd.short_value - obs.observation_source_value) -- observation_source_value
        +
        (obs.observation_source_concept_id - coalesce(c.concept_id, 0)) -- observation_source_concept_id
        +
        (coalesce(obs.unit_source_value, 0)) -- unit_source_value
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

left join voc.concept c on c.concept_code = cd.short_value AND c.vocabulary_id = 'PPI'
left join voc.concept_relationship cr
    on c.concept_id = cr.concept_id_1
        and cr.invalid_reason is null
        and cr.relationship_id = 'Maps to'
left join voc.concept c1 on c1.concept_id = cr.concept_id_2
    and c1.invalid_reason is null
    and c1.standard_concept = 'S'

inner join cdm.observation obs
    on qr.questionnaire_response_id = obs.questionnaire_response_id
    and obs.observation_source_value = cd.short_value
    and obs.value_as_number = qra.value_integer

where
    obs.unit_id = 'observ.num'
    and (qra.value_integer is not null
        or qra.value_decimal is not null)
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'observation', 'observ.num',
    'Verify the difference between source values and observation table in case of values_integer/value_decimal',
    @test_result
);

-- questionnaire_response_answer.value_string
SELECT NULL INTO @test_result;
select  sum(
        (qr.participant_id - obs.person_id) -- person_id
        +
        (coalesce(c1.concept_id, 0) -  obs.observation_concept_id) -- observation_concept_id
        +
        (date(qr.created) - obs.observation_date) -- observation_date
        +
        (time(qr.created) - obs.observation_time) -- observation_time
        +
        (obs.observation_type_concept_id - 45905771) -- observation_type_concept_id
        +
        (coalesce(obs.value_as_number, 0) - 0) -- value_as_number
        +
        (qra.value_string - obs.value_as_string) -- value_as_string
        +
        (obs.value_as_concept_id - 0) -- value_as_concept_id
        +
        (obs.qualifier_concept_id - 0) -- qualifier_concept_id
        +
        (obs.unit_concept_id - 0) -- unit_concept_id
        +
        (cd.short_value - obs.observation_source_value) -- observation_source_value
        +
        (obs.observation_source_concept_id - coalesce(c.concept_id, 0)) -- observation_source_concept_id
        +
        (coalesce(obs.unit_source_value, 0)) -- unit_source_value
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

left join voc.concept c on c.concept_code = cd.short_value AND c.vocabulary_id = 'PPI'
left join voc.concept_relationship cr
    on c.concept_id = cr.concept_id_1
        and cr.invalid_reason is null
        and cr.relationship_id = 'Maps to'
left join voc.concept c1
    on c1.concept_id = cr.concept_id_2
        and c1.invalid_reason is null
        and c1.standard_concept = 'S'

inner join cdm.observation obs
    on qr.questionnaire_response_id = obs.questionnaire_response_id
    and obs.observation_source_value = cd.short_value
    and obs.value_as_string = qra.value_string

where qra.value_string is not null and obs.unit_id = 'observ.str'
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'observation', 'observ.str',
    'questionnaire_response_answer.value_string',
    @test_result
);


-- -------------------------------------------------------------------
-- source_file: qa/qa_observation_observ_meas_count.sql
-- -------------------------------------------------------------------

-- --------------------------------------------------------
-- table: observation (measurement)
-- --------------------------------------------------------

-- observ.meas count testing
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
(
    select count(*) as c
    from
    (
        select
            pe.person_id,
            meas.measurement_time,
            coalesce(c1.concept_id, c3.concept_id, c2.concept_id),
            meas.code_value,
            meas.value_code_value
        from rdr.measurement meas
        inner join  rdr.physical_measurements pm
            on pm.physical_measurements_id = meas.physical_measurements_id
                and
                pm.final = 1
        -- inner join rdr.measurement_to_qualifier mtq
        --  on meas.measurement_id = mtq.qualifier_id

        inner join cdm.person pe
            on pe.person_id = pm.participant_id

        left join voc.concept c1 -- source_concept_id standard
            on meas.code_value = c1.concept_code
                AND c1.standard_concept = 'S'
                AND c1.vocabulary_id IN ('PPI', 'LOINC')
        left join voc.concept c2 -- source_cocenpt_id non-standard
            on meas.code_value = c2.concept_code
                AND c2.vocabulary_id IN ('PPI')
        left join voc.concept_relationship cr1
            on c2.concept_id = cr1.concept_id_1
                and cr1.relationship_id = 'Maps to'
                and cr1.invalid_reason is null
        left join voc.concept c3                -- concept_id
            on c3.concept_id = cr1.concept_id_2
                and c3.standard_concept = 'S'
                and c3.invalid_reason is null
        where coalesce(c1.domain_id, c3.domain_id, c2.domain_id) = 'Observation'
                OR COALESCE(c1.concept_class_id, c2.concept_class_id) = 'PPI Modifier'

        group by
            coalesce(c1.concept_id, c3.concept_id, c2.concept_id), meas.code_value, pe.person_id, meas.measurement_time, meas.value_code_value
    )  as t
) c1,
(
        select  count(*) as c
        from cdm.observation
        where unit_id = 'observ.meas'
) c2;


INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'observation', 'ovserv.meas',
    'observ.meas count testing',
    @test_result
);



-- -------------------------------------------------------------------
-- source_file: qa/qa_measurement.sql
-- -------------------------------------------------------------------

-- --------------------------------------------------------
-- table: measurement
-- --------------------------------------------------------

-- Test1 : Check duplicates in cdm Measurement
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
    (
        select count(*) as c
        from cdm.measurement
    ) c1,
    (
        select count(*) as c
        from
            (
                select distinct person_id, measurement_concept_id, measurement_date, measurement_time,
                    value_as_number, value_as_concept_id, unit_concept_id, visit_occurrence_id,
                    measurement_source_value, measurement_source_concept_id, unit_source_value,
                    value_source_value
                from cdm.measurement
            ) a
    ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'measurement', '',
    'Check duplicates in cdm measurement',
    @test_result
);

-- Test2 : Check counts [unit: meas.dec]
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
    (
        select count(*) AS c
        from cdm.measurement
        where unit_id = 'meas.dec'
    ) c1,
    (
        select count(*) as c
        from
        (
            select distinct per.person_id, me.measurement_time, me.code_value,
                me.value_decimal, me.value_unit
            from rdr.physical_measurements pm
            inner join rdr.measurement me
                on pm.physical_measurements_id = me.physical_measurements_id
                and pm.final = 1
                and me.value_decimal is not null
            inner join cdm.person per
                on pm.participant_id = per.person_id
            inner join rdr.measurement_to_qualifier mtq
                on me.measurement_id != mtq.qualifier_id
            left join voc.concept vc1
                on me.code_value = vc1.concept_code
                and vc1.vocabulary_id IN ('LOINC', 'PPI')
                and vc1.standard_concept = 'S'
            left join voc.concept vc2
                on me.code_value = vc2.concept_code
                and vc2.vocabulary_id = 'PPI'
            left join voc.concept_relationship vcr
                on vc2.concept_id = vcr.concept_id_1
                and vcr.relationship_id = 'Maps to'
                and vcr.invalid_reason IS null
            left join voc.concept vc3
                on vcr.concept_id_2 = vc3.concept_id
                and vc3.standard_concept = 'S'
            where
                COALESCE(vc1.domain_id, vc3.domain_id, vc2.domain_id, 'Measurement') = 'Measurement'
                and COALESCE(vc1.concept_class_id, vc2.concept_class_id, '0') != 'PPI Modifier'
        ) a
    ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'measurement', 'meas.dec',
    'Check counts in cdm measurement',
    @test_result
);

-- Test3 : Check counts [unit: meas.value]
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
    (
        select count(*) as c
        from cdm.measurement
        where unit_id = 'meas.value'
    ) c1,
    (
        select count(*) as c
        from
        (
            select distinct per.person_id, me.measurement_time, me.code_value,
                me.value_decimal, me.value_unit
            from rdr.physical_measurements pm
            inner join rdr.measurement me
                on pm.physical_measurements_id = me.physical_measurements_id
                and pm.final = 1
                and me.value_code_value is not null
            inner join cdm.person per
                on pm.participant_id = per.person_id
            inner join rdr.measurement_to_qualifier mtq
                on me.measurement_id != mtq.qualifier_id
            left join voc.concept vc1
                on me.code_value = vc1.concept_code
                and vc1.vocabulary_id IN ('LOINC', 'PPI')
                and vc1.standard_concept = 'S'
            left join voc.concept vc2
                on me.code_value = vc2.concept_code
                and vc2.vocabulary_id = 'PPI'
            left join voc.concept_relationship vcr
                on vc2.concept_id = vcr.concept_id_1
                and vcr.relationship_id = 'Maps to'
                and vcr.invalid_reason IS null
            left join voc.concept vc3
                on vcr.concept_id_2 = vc3.concept_id
                and vc3.standard_concept = 'S'
            where
                COALESCE(vc1.domain_id, vc3.domain_id, vc2.domain_id, 'Measurement') = 'Measurement'
                and COALESCE(vc1.concept_class_id, vc2.concept_class_id, '0') != 'PPI Modifier'
        ) a
    ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'measurement', 'meas.value',
    'Check counts in cdm measurement',
    @test_result
);

-- Test4 : Check counts [unit: meas.empty]
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
    (
        select count(*) as c
        from cdm.measurement
        where unit_id = 'meas.empty'
    ) c1,
    (
        select count(*) as c
        from
        (
            select distinct per.person_id, me.measurement_time, me.code_value,
                me.value_decimal, me.value_unit
            from rdr.physical_measurements pm
            inner join rdr.measurement me
                on pm.physical_measurements_id = me.physical_measurements_id
                and pm.final = 1
                and me.value_code_value is null
                and me.value_decimal is null
            inner join cdm.person per
                on pm.participant_id = per.person_id
            inner join rdr.measurement_to_qualifier mtq
                on me.measurement_id != mtq.qualifier_id
            left join voc.concept vc1
                on me.code_value = vc1.concept_code
                and vc1.vocabulary_id IN ('LOINC', 'PPI')
                and vc1.standard_concept = 'S'
            left join voc.concept vc2
                on me.code_value = vc2.concept_code
                and vc2.vocabulary_id = 'PPI'
            left join voc.concept_relationship vcr
                on vc2.concept_id = vcr.concept_id_1
                and vcr.relationship_id = 'Maps to'
                and vcr.invalid_reason IS NULL
            left join voc.concept vc3
                on vcr.concept_id_2 = vc3.concept_id
                and vc3.standard_concept = 'S'
            where
                COALESCE(vc1.domain_id, vc3.domain_id, vc2.domain_id, 'Measurement') = 'Measurement'
                and COALESCE(vc1.concept_class_id, vc2.concept_class_id, '0') != 'PPI Modifier'
        ) a
    ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'measurement', 'meas.empty',
    'Check counts in cdm measurement',
    @test_result
);

-- Test5: Check values of fields [unit: meas.dec]
SELECT NULL INTO @test_result;
select  sum(
        (pm.participant_id - cdm_m.person_id)                                               -- person_id
         +
        (coalesce(vc1.concept_id, vc3.concept_id, 0) -  cdm_m.measurement_concept_id)       -- measurement_concept_id
         +
        (date(me.measurement_time) - cdm_m.measurement_date)                                -- measurement_date
         +
        (time(me.measurement_time) - cdm_m.measurement_time)                                -- measurement_time
         +
        (44818701 - cdm_m.measurement_type_concept_id)                                      -- measurement_type_concept_id
         +
        (coalesce(cdm_m.operator_concept_id, 0))                                            -- operator_concept_id
         +
        (me.value_decimal - cdm_m.value_as_number)                                          -- value_as_number
         +
        (coalesce(cdm_m.value_as_concept_id, 0))                                            -- value_as_concept_id
         +
        (coalesce(vc4.concept_id, 0) - cdm_m.unit_concept_id)                               -- unit_concept_id
         +
        (coalesce(cdm_m.range_low, 0))                                                      -- range_low
         +
        (coalesce(cdm_m.range_high, 0))                                                     -- range_high
         +
        (coalesce(cdm_m.provider_id, 0))                                                    -- provider_id
         +
        (coalesce(vis.visit_occurrence_id, 0) - coalesce(cdm_m.visit_occurrence_id, 0))     -- visit_occurrence_id
         +
        (me.code_value - cdm_m.measurement_source_value)                                    -- measurement_source_value
         +
        (coalesce(vc1.concept_id, vc2.concept_id, 0) - cdm_m.measurement_source_concept_id) -- measurement_source_concept_id
         +
        (coalesce(me.value_unit, 0) - coalesce(cdm_m.unit_source_value, 0))                 -- unit_source_value
         +
        (concat(me.value_decimal, ' - ', me.value_unit) - cdm_m.value_source_value)         -- value_source_value
     ) as result                                                                                    INTO @test_result
from rdr.physical_measurements pm
inner join rdr.measurement me
    on pm.physical_measurements_id = me.physical_measurements_id
    and pm.final = 1
    and me.value_decimal is not null
inner join cdm.person per
    on pm.participant_id = per.person_id
inner join rdr.measurement_to_qualifier mtq
    on me.measurement_id != mtq.qualifier_id
inner join cdm.measurement cdm_m
    on cdm_m.unit_id = 'meas.dec'
    and cdm_m.measurement_source_value = me.code_value
    and cdm_m.person_id = pm.participant_id
    and cdm_m.value_as_number = me.value_decimal
    and CONCAT(' ', cdm_m.measurement_date, ' ', cdm_m.measurement_time) = CONCAT(' ', me.measurement_time)
left join cdm.visit_occurrence vis
    on me.physical_measurements_id = vis.visit_source_value
left join voc.concept vc1
    on me.code_value = vc1.concept_code
    and vc1.vocabulary_id IN ('LOINC', 'PPI')
    and vc1.standard_concept = 'S'
left join voc.concept vc2
    on me.code_value = vc2.concept_code
    and vc2.vocabulary_id = 'PPI'
left join voc.concept_relationship vcr
    on vc2.concept_id = vcr.concept_id_1
    and vcr.relationship_id = 'Maps to'
    and vcr.invalid_reason is null
left join voc.concept vc3
    on vcr.concept_id_2 = vc3.concept_id
    and vc3.standard_concept = 'S'
left join voc.concept vc4
    on me.value_unit = vc4.concept_code
    and vc4.vocabulary_id = 'UCUM'
where
    COALESCE(vc1.domain_id, vc3.domain_id, vc2.domain_id, 'Measurement') = 'Measurement'
    and COALESCE(vc1.concept_class_id, vc2.concept_class_id, '0') != 'PPI Modifier'
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'measurement', 'meas.dec',
    'Check values of fields in cdm measurement',
    @test_result
);

-- Test6: Check values of fields [unit: meas.value]
SELECT NULL INTO @test_result;
select  sum(
        (pm.participant_id - cdm_m.person_id)                                               -- person_id
        +
        (coalesce(vc1.concept_id, vc3.concept_id, 0) -  cdm_m.measurement_concept_id)       -- measurement_concept_id
         +
        (date(me.measurement_time) - cdm_m.measurement_date)                                -- measurement_date
         +
        (time(me.measurement_time) - cdm_m.measurement_time)                                -- measurement_time
         +
        (44818701 - cdm_m.measurement_type_concept_id)                                      -- measurement_type_concept_id
         +
        (coalesce(cdm_m.operator_concept_id, 0))                                            -- operator_concept_id
         +
        (coalesce(cdm_m.value_as_number, 0))                                                -- value_as_number
         +
        (coalesce(vcv1.concept_id, vcv3.concept_id, 0) - cdm_m.value_as_concept_id)         -- value_as_concept_id
         +
        (coalesce(cdm_m.unit_concept_id, 0))                                                -- unit_concept_id
         +
        (coalesce(cdm_m.range_low, 0))                                                      -- range_low
         +
        (coalesce(cdm_m.range_high, 0))                                                     -- range_high
         +
        (coalesce(cdm_m.provider_id, 0))                                                    -- provider_id
         +
        (coalesce(vis.visit_occurrence_id, 0) - coalesce(cdm_m.visit_occurrence_id, 0))     -- visit_occurrence_id
         +
        (me.code_value - cdm_m.measurement_source_value)                                    -- measurement_source_value
         +
        (coalesce(vc1.concept_id, vc2.concept_id, 0) - cdm_m.measurement_source_concept_id) -- measurement_source_concept_id
         +
        (coalesce(cdm_m.unit_source_value, 0))                                              -- unit_source_value
         +
        (coalesce(me.value_code_value, 0) - coalesce(cdm_m.value_source_value, 0))          -- value_source_value
     ) as result                                                                                    INTO @test_result
from rdr.physical_measurements pm
inner join rdr.measurement me
    on pm.physical_measurements_id = me.physical_measurements_id
    and pm.final = 1
    and me.value_code_value is not null
inner join cdm.person per
    on pm.participant_id = per.person_id
inner join rdr.measurement_to_qualifier mtq
    on me.measurement_id != mtq.qualifier_id
inner join cdm.measurement cdm_m
    on cdm_m.unit_id = 'meas.value'
    and cdm_m.measurement_source_value = me.code_value
    and cdm_m.person_id = pm.participant_id
    and cdm_m.value_source_value = me.value_code_value
    and CONCAT(' ', cdm_m.measurement_date, ' ', cdm_m.measurement_time) = CONCAT(' ', me.measurement_time)
left join cdm.visit_occurrence vis
    on me.physical_measurements_id = vis.visit_source_value
left join voc.concept vc1
    on me.code_value = vc1.concept_code
    and vc1.vocabulary_id IN ('LOINC', 'PPI')
    and vc1.standard_concept = 'S'
left join voc.concept vc2
    on me.code_value = vc2.concept_code
    and vc2.vocabulary_id = 'PPI'
left join voc.concept_relationship vcr
    on vc2.concept_id = vcr.concept_id_1
    and vcr.relationship_id = 'Maps to'
    and vcr.invalid_reason is null
left join voc.concept vc3
    on vcr.concept_id_2 = vc3.concept_id
    and vc3.standard_concept = 'S'
    left join voc.concept vcv1
    on me.value_code_value = vcv1.concept_code
    and vcv1.vocabulary_id IN ('LOINC', 'PPI')
    and vcv1.standard_concept = 'S'
left join voc.concept vcv2
    on me.code_value = vcv2.concept_code
    and vcv2.vocabulary_id = 'PPI'
left join voc.concept_relationship vcvr
    on vcv2.concept_id = vcvr.concept_id_1
    and vcvr.relationship_id = 'Maps to'
    and vcvr.invalid_reason is null
left join voc.concept vcv3
    on vcvr.concept_id_2 = vcv3.concept_id
    and vcv3.standard_concept = 'S'
left join voc.concept vc4
    on me.value_unit = vc4.concept_code
    and vc4.vocabulary_id = 'UCUM'
where
    COALESCE(vc1.domain_id, vc3.domain_id, vc2.domain_id, 'Measurement') = 'Measurement'
    and COALESCE(vc1.concept_class_id, vc2.concept_class_id, '0') != 'PPI Modifier'
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'measurement', 'meas.value',
    'Check values of fields in cdm measurement',
    @test_result
);

-- Test7: Check values of fields [unit: meas.empty]
SELECT NULL INTO @test_result;
select  sum(
        (pm.participant_id - cdm_m.person_id)                                               -- person_id
        +
        (coalesce(vc1.concept_id, vc3.concept_id, 0) -  cdm_m.measurement_concept_id)       -- measurement_concept_id
         +
        (date(me.measurement_time) - cdm_m.measurement_date)                                -- measurement_date
         +
        (time(me.measurement_time) - cdm_m.measurement_time)                                -- measurement_time
         +
        (44818701 - cdm_m.measurement_type_concept_id)                                      -- measurement_type_concept_id
         +
        (coalesce(cdm_m.operator_concept_id, 0))                                            -- operator_concept_id
         +
        (coalesce(cdm_m.value_as_number, 0))                                                -- value_as_number
         +
        (coalesce( cdm_m.value_as_concept_id, 0))                                           -- value_as_concept_id
         +
        (coalesce(cdm_m.unit_concept_id, 0))                                                -- unit_concept_id
         +
        (coalesce(cdm_m.range_low, 0))                                                      -- range_low
         +
        (coalesce(cdm_m.range_high, 0))                                                     -- range_high
         +
        (coalesce(cdm_m.provider_id, 0))                                                    -- provider_id
         +
        (coalesce(vis.visit_occurrence_id, 0) - coalesce(cdm_m.visit_occurrence_id, 0))     -- visit_occurrence_id
         +
        (me.code_value - cdm_m.measurement_source_value)                                    -- measurement_source_value
         +
        (coalesce(vc1.concept_id, vc2.concept_id, 0) - cdm_m.measurement_source_concept_id) -- measurement_source_concept_id
         +
        (coalesce(cdm_m.unit_source_value, 0))                                              -- unit_source_value
         +
        (coalesce(cdm_m.value_source_value, 0))                                             -- value_source_value
     ) as result                                                                                    INTO @test_result
from rdr.physical_measurements pm
inner join rdr.measurement me
    on pm.physical_measurements_id = me.physical_measurements_id
    and pm.final = 1
    and me.value_decimal is null
    and me.value_code_value is null
inner join cdm.person per
    on pm.participant_id = per.person_id
inner join rdr.measurement_to_qualifier mtq
    on me.measurement_id != mtq.qualifier_id
inner join cdm.measurement cdm_m
    on cdm_m.unit_id = 'meas.empty'
    and cdm_m.measurement_source_value = me.code_value
    and cdm_m.person_id = pm.participant_id
    and CONCAT(' ', cdm_m.measurement_date, ' ', cdm_m.measurement_time) = CONCAT(' ', me.measurement_time)
left join cdm.visit_occurrence vis
    on me.physical_measurements_id = vis.visit_source_value
left join voc.concept vc1
    on me.code_value = vc1.concept_code
    and vc1.vocabulary_id IN ('LOINC', 'PPI')
    and vc1.standard_concept = 'S'
left join voc.concept vc2
    on me.code_value = vc2.concept_code
    and vc2.vocabulary_id = 'PPI'
left join voc.concept_relationship vcr
    on vc2.concept_id = vcr.concept_id_1
    and vcr.relationship_id = 'Maps to'
    and vcr.invalid_reason is null
left join voc.concept vc3
    on vcr.concept_id_2 = vc3.concept_id
    and vc3.standard_concept = 'S'
left join voc.concept vc4
    on me.value_unit = vc4.concept_code
    and vc4.vocabulary_id = 'UCUM'
where
    COALESCE(vc1.domain_id, vc3.domain_id, vc2.domain_id, 'Measurement') = 'Measurement'
    and COALESCE(vc1.concept_class_id, vc2.concept_class_id, '0') != 'PPI Modifier'
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'measurement', 'meas.empty',
    'Check values of fields in cdm measurement',
    @test_result
);


-- -------------------------------------------------------------------
-- source_file: qa/qa_condition_occurrence.sql
-- -------------------------------------------------------------------

-- --------------------------------------------------------
-- table: condition_occurrence
-- --------------------------------------------------------

-- Test1 : Check duplicates in cdm condition occurrence
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
    (
        select count(*) as c
        from cdm.condition_occurrence
    ) c1,
    (
        select count(*) as c
        from
            (
                select distinct person_id,
                                condition_concept_id,
                                condition_start_date,
                                condition_type_concept_id,
                                visit_occurrence_id,
                                condition_source_value,
                                condition_source_concept_id
                from cdm.condition_occurrence
            ) a
    ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'condition_occurrence', '',
    'Check duplicates in cdm condition_occurrence',
    @test_result
);

-- Test2 : Check counts in cdm condition occurrence
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
    (
        select count(*) AS c
        from cdm.condition_occurrence
    ) c1,
    (
        select count(*) as c
        from
        (
            select distinct per.person_id,
                            me.measurement_time,
                            me.code_value,
                            COALESCE(vc1.concept_id, vc3.concept_id, 0) as cond_concept_id,
                            COALESCE(vc1.concept_id, vc2.concept_id, 0) as cond_source_concept_id
            from rdr.physical_measurements pm
            inner join rdr.measurement me
                on pm.physical_measurements_id = me.physical_measurements_id
                and pm.final = 1
            inner join cdm.person per
                on pm.participant_id = per.person_id
            inner join rdr.measurement_to_qualifier mtq
                on me.measurement_id != mtq.qualifier_id
            left join voc.concept vc1
                on me.code_value = vc1.concept_code
                and vc1.vocabulary_id IN ('LOINC', 'PPI')
                and vc1.standard_concept = 'S'
            left join voc.concept vc2
                on me.code_value = vc2.concept_code
                and vc2.vocabulary_id = 'PPI'
            left join voc.concept_relationship vcr
                on vc2.concept_id = vcr.concept_id_1
                and vcr.relationship_id = 'Maps to'
                and vcr.invalid_reason IS null
            left join voc.concept vc3
                on vcr.concept_id_2 = vc3.concept_id
                and vc3.standard_concept = 'S'
            where
                COALESCE(vc1.domain_id, vc3.domain_id, vc2.domain_id, 0) = 'Condition'
                and
                    (   me.code_value != 'wheelchair-bound-status'
                        or
                        (   me.code_value = 'wheelchair-bound-status'
                            and
                            me.value_code_value = 'wheelchair-bound'
                        )
                    )
        ) a
    ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'condition_occurrence', '',
    'Check counts in cdm condition occurrence',
    @test_result
);

-- Test3: Check values of fields in cdm condition occurrence
SELECT NULL INTO @test_result;
select COALESCE(c2.result, c1.count) as result INTO @test_result
from
    (select count(*) as count
     from cdm.condition_occurrence
    ) c1,

    (select  sum(
            (pm.participant_id - cdm_c.person_id)                                               -- person_id
             +
            (coalesce(vc1.concept_id, vc3.concept_id, 0) -  cdm_c.condition_concept_id)         -- measurement_concept_id
             +
            (date(me.measurement_time) - cdm_c.condition_start_date)                            -- condition_start_date
             +
            (coalesce(cdm_c.condition_end_date, 0))                                             -- condition_end_date
             +
            (45905770 - cdm_c.condition_type_concept_id)                                        -- condition_type_concept_id
             +
            (coalesce(cdm_c.stop_reason, 0))                                                    -- stop_reason
             +
            (coalesce(cdm_c.provider_id, 0))                                                    -- provider_id
             +
            (coalesce(vis.visit_occurrence_id, 0) - coalesce(cdm_c.visit_occurrence_id, 0))     -- visit_occurrence_id
             +
            (me.code_value - cdm_c.condition_source_value)                                      -- condition_source_value
             +
            (coalesce(vc1.concept_id, vc2.concept_id, 0) - cdm_c.condition_source_concept_id)   -- condition_source_concept_id
         ) as result
    from rdr.physical_measurements pm
    inner join rdr.measurement me
        on pm.physical_measurements_id = me.physical_measurements_id
        and pm.final = 1
    inner join cdm.person per
        on pm.participant_id = per.person_id
    inner join rdr.measurement_to_qualifier mtq
        on me.measurement_id != mtq.qualifier_id
    left join cdm.visit_occurrence vis
        on me.physical_measurements_id = vis.visit_source_value

    left join voc.concept vc1
        on me.code_value = vc1.concept_code
        and vc1.vocabulary_id IN ('LOINC', 'PPI')
        and vc1.standard_concept = 'S'
    left join voc.concept vc2
        on me.code_value = vc2.concept_code
        and vc2.vocabulary_id = 'PPI'
    left join voc.concept_relationship vcr
        on vc2.concept_id = vcr.concept_id_1
        and vcr.relationship_id = 'Maps to'
        and vcr.invalid_reason IS null
    left join voc.concept vc3
        on vcr.concept_id_2 = vc3.concept_id
        and vc3.standard_concept = 'S'

    left join cdm.condition_occurrence cdm_c
        on cdm_c.person_id = pm.participant_id
        and cdm_c.condition_start_date = date(me.measurement_time)
        and cdm_c.condition_source_value = me.code_value

    where
        COALESCE(vc1.domain_id, vc3.domain_id, vc2.domain_id, 0) = 'Condition'
        and
            (   me.code_value != 'wheelchair-bound-status'
                or
                (   me.code_value = 'wheelchair-bound-status'
                    and
                    me.value_code_value = 'wheelchair-bound'
                )
            )
    ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'condition_occurrence', '',
    'Check values of fields in cdm condition occurrence',
    @test_result
);

-- -------------------------------------------------------------------
-- source_file: qa/qa_visit_occurrence.sql
-- -------------------------------------------------------------------

-- --------------------------------------------------------
-- table: visit_occurrence
-- --------------------------------------------------------

-- Test1: Check duplicates in cdm Visit_occurrence
SELECT NULL INTO @test_result;
SELECT c1.c - c2.c INTO @test_result
from
    (
        select count(*) AS c
        from cdm.visit_occurrence
    ) c1,
    (
        select count(*) as c
        from
            (
                select distinct person_id, visit_start_date, visit_start_time, visit_end_date, visit_end_time,
                    care_site_id, visit_source_value
                from cdm.visit_occurrence
            ) a
    ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'visit_occurrence', '',
    'Check duplicates in cdm Visit_occurrence',
    @test_result
);

-- Test2: Check counts in cdm Visit_occurrence
SELECT NULL INTO @test_result;
select c1.c - c2.c INTO @test_result
from
    (   select count(*) as c from cdm.visit_occurrence) c1,
    (   select count(*) as c
        from
        (  select distinct
                pm.participant_id,
                min(meas.measurement_time),
                max(meas.measurement_time),
                pm.physical_measurements_id
            from rdr.measurement meas
            inner join rdr.physical_measurements pm
                on meas.physical_measurements_id = pm.physical_measurements_id
                and pm.final = 1
            inner join cdm.person pe
                on pe.person_id = pm.participant_id
            group by
                pm.participant_id,
                pm.physical_measurements_id
        ) t
    ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'visit_occurrence', '',
    'Check counts in cdm Visit_occurrence',
    @test_result
);

-- Test3: Check dates in cdm Visit_occurrence
SELECT NULL INTO @test_result;
select count(*) as c INTO @test_result
from cdm.visit_occurrence
where visit_start_date < visit_end_date
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'visit_occurrence', '',
    'Check dates in cdm Visit_occurrence',
    @test_result
);

-- Test4: Check values of fields in cdm Visit_occurrence
SELECT NULL INTO @test_result;
select  sum(
        (me.participant_id - vis.person_id)                                 -- person_id
         +
        (9202 - vis.visit_concept_id)                                       -- visit_concept_id
         +
        (DATE(me.start_date) - vis.visit_start_date)                        -- visit_start_date
         +
        (TIME(me.start_date) - vis.visit_start_time)                        -- visit_start_time
         +
        (DATE(me.end_date) - vis.visit_end_date)                            -- visit_end_date
         +
        (TIME(me.end_date) - vis.visit_end_time)                            -- visit_end_time
         +
        (44818519 - vis.visit_type_concept_id)                              -- visit_type_concept_id
         +
        (coalesce(vis.provider_id, 0))                                      -- provider_id
         +
        (coalesce(cs.care_site_id, 0) - coalesce(vis.care_site_id, 0))      -- care_site_id
         +
        (me.physical_measurements_id - vis.visit_source_value)              -- visit_source_value
         +
        (coalesce(vis.visit_source_concept_id, 0))                          -- visit_source_concept_id
     ) as result                                                        INTO @test_result
from (  select distinct
                pm.participant_id,
                min(meas.measurement_time) as start_date,
                max(meas.measurement_time) as end_date,
                pm.physical_measurements_id,
                pm.finalized_site_id
            from rdr.measurement meas
            inner join rdr.physical_measurements pm
                on meas.physical_measurements_id = pm.physical_measurements_id
                and pm.final = 1
            inner join cdm.person pe
                on pe.person_id = pm.participant_id
            group by
                pm.participant_id,
                pm.physical_measurements_id,
                pm.finalized_site_id
        ) me
left join cdm.visit_occurrence vis
    on me.physical_measurements_id = vis.visit_source_value
    and me.participant_id = vis.person_id
    and coalesce(me.finalized_site_id, 0) = coalesce(vis.care_site_id, 0)
left join cdm.care_site cs
    on me.finalized_site_id = cs.care_site_source_value
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'visit_occurrence', '',
    'Check values of fields in cdm Visit_occurrence',
    @test_result
);



-- -------------------------------------------------------------------
-- source_file: qa/qa_observation_period.sql
-- -------------------------------------------------------------------


-- --------------------------------------------------------
-- table: observation_period
-- --------------------------------------------------------

-- Comment: check duplicates for cdm observation_period table
SELECT NULL INTO @test_result;
SELECT c1.c - c2.c INTO @test_result
from
    (
        select count(*) AS c
        from cdm.observation_period
    ) c1,
    (
        select count(*) as c
        from
            (
                select distinct person_id, observation_period_start_date, observation_period_end_date
                from cdm.observation_period
            ) a
    ) c2
;

INSERT INTO cdm.qa_result
(test_table, test_unit, test_descr, test_result)
VALUES
(   'observation_period', '',
    'Check duplicates in cdm observation_period',
    @test_result
);

-- -------------------------------------------------------------------
-- source_file: qa/qa_fact_relationship.sql
-- -------------------------------------------------------------------


-- -------------------------------------------------------------------
-- table: fact_relationship
-- -------------------------------------------------------------------

-- Test 1 :Check duplicates in cdm Fact_relationship
SELECT NULL INTO @test_result;

SELECT c1.c - c2.c INTO @test_result
from
    (
        select count(*) AS c
        from cdm.fact_relationship
    ) c1,
    (
        select count(*) as c
        from
            (
                select distinct domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id
                from cdm.fact_relationship
            ) a
    ) c2
;

INSERT INTO cdm.qa_result (test_table, test_unit, test_descr, test_result)
VALUES
(
    'fact_relationship',
    '',
    'Check duplicates in cdm Fact_relationship',
    @test_result
);

-- Test 2: Check counts for unit_id = 'observ.meas1'
SELECT NULL INTO @test_result;

SELECT c1.c - c2.c INTO @test_result
from
    (
        select count(*) AS c
        from cdm.fact_relationship
        where unit_id = 'observ.meas1'
    ) c1,
    (
        select count(*) as c
        from cdm.measurement meas
        inner join rdr.measurement_to_qualifier mtq
            on meas.meas_id = mtq.measurement_id
        inner join cdm.observation obs
            on mtq.qualifier_id = obs.meas_id
    ) c2
;

INSERT INTO cdm.qa_result (test_table, test_unit, test_descr, test_result)
VALUES
(
    'fact_relationship',
    'observ.meas1',
    'Check counts for unit_id = \'observ.meas1\'',
    @test_result
);


-- Test 3: Check counts for unit_id = 'observ.meas2'
SELECT NULL INTO @test_result;

SELECT c1.c - c2.c INTO @test_result
from
    (
        select count(*) AS c
        from cdm.fact_relationship
        where unit_id = 'observ.meas2'
    ) c1,
    (
        select count(*) as c
        from cdm.measurement meas
        inner join rdr.measurement_to_qualifier mtq
            on meas.meas_id = mtq.measurement_id
        inner join cdm.observation obs
            on mtq.qualifier_id = obs.meas_id
    ) c2
;

INSERT INTO cdm.qa_result (test_table, test_unit, test_descr, test_result)
VALUES
(
    'fact_relationship',
    'observ.meas2',
    'Check counts for unit_id = \'observ.meas2\'',
    @test_result
);

-- Test 4: Check counts for unit_id = 'syst.diast1'
SELECT NULL INTO @test_result;

SELECT c1.c - c2.c INTO @test_result
from
    (
        select count(*) AS c
        from cdm.fact_relationship
        where unit_id = 'syst.diast1'
    ) c1,
    (
        select count(*) as c
        from cdm.measurement meas1
        inner join cdm.measurement meas2
            on meas1.parent_id = meas2.parent_id
            and meas1.person_id = meas2.person_id
            and meas1.measurement_time = meas2.measurement_time
            and meas1.measurement_date = meas2.measurement_date
            and meas1.measurement_source_value = '8480-6'
            and meas2.measurement_source_value = '8462-4'
    ) c2
;

INSERT INTO cdm.qa_result (test_table, test_unit, test_descr, test_result)
VALUES
(
    'fact_relationship',
    'syst.diast1',
    'Check counts for unit_id = \'syst.diast1\'',
    @test_result
);


-- Test 5: Check counts for unit_id = 'syst.diast2'
SELECT NULL INTO @test_result;

SELECT c1.c - c2.c INTO @test_result
from
    (
        select count(*) AS c
        from cdm.fact_relationship
        where unit_id = 'syst.diast2'
    ) c1,
    (
        select count(*) as c
        from cdm.measurement meas1
        inner join cdm.measurement meas2
            on meas1.parent_id = meas2.parent_id
            and meas1.person_id = meas2.person_id
            and meas1.measurement_time = meas2.measurement_time
            and meas1.measurement_date = meas2.measurement_date
            and meas1.measurement_source_value = '8480-6'
            and meas2.measurement_source_value = '8462-4'
    ) c2
;

INSERT INTO cdm.qa_result (test_table, test_unit, test_descr, test_result)
VALUES
(
    'fact_relationship',
    'syst.diast2',
    'Check counts for unit_id = \'syst.diast2\'',
    @test_result
);


