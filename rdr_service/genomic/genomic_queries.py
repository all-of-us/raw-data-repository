from rdr_service.participant_enums import GenomicSubProcessResult


class GenomicQueryClass:

    def __init__(self):
        pass

    # BEGIN AW0 Queries
    @staticmethod
    def remaining_c2_participants():
        return  """
                SELECT DISTINCT
                    ps.biobank_id,
                    ps.participant_id,
                    0 AS biobank_order_id,
                    0 AS collected_site_id,
                    NULL as state_id,
                    0 AS biobank_stored_sample_id,
                    CASE
                    WHEN ps.withdrawal_status = :withdrawal_param THEN 1 ELSE 0
                    END as valid_withdrawal_status,
                    CASE
                    WHEN ps.suspension_status = :suspension_param THEN 1 ELSE 0
                    END as valid_suspension_status,
                    CASE
                    WHEN ps.consent_for_study_enrollment = :general_consent_param THEN 1 ELSE 0
                    END as general_consent_given,
                    CASE
                    WHEN ps.date_of_birth < DATE_SUB(now(), INTERVAL :dob_param YEAR) THEN 1 ELSE 0
                    END AS valid_age,
                    CASE
                    WHEN c.value = "SexAtBirth_Male" THEN "M"
                    WHEN c.value = "SexAtBirth_Female" THEN "F"
                    ELSE "NA"
                    END as sab,
                    CASE
                    WHEN ps.consent_for_genomics_ror = :general_consent_param THEN 1 ELSE 0
                    END AS gror_consent,
                    CASE
                        WHEN native.participant_id IS NULL THEN 1 ELSE 0
                    END AS valid_ai_an
                FROM
                    participant_summary ps
                    JOIN code c ON c.code_id = ps.sex_id
                    LEFT JOIN (
                        SELECT ra.participant_id
                        FROM participant_race_answers ra
                            JOIN code cr ON cr.code_id = ra.code_id
                                AND SUBSTRING_INDEX(cr.value, "_", -1) = "AIAN"
                    ) native ON native.participant_id = ps.participant_id
                    LEFT JOIN genomic_set_member m ON m.participant_id = ps.participant_id
                        AND m.genomic_workflow_state <> :ignore_param
                WHERE TRUE
                    AND (
                            ps.sample_status_1ed04 = :sample_status_param
                            OR
                            ps.sample_status_1ed10 = :sample_status_param
                            OR
                            ps.sample_status_1sal2 = :sample_status_param
                        )
                    AND ps.consent_cohort = :cohort_2_param
                    AND m.id IS NULL
                    AND ps.participant_origin = "vibrent"
                    AND ps.participant_id NOT IN (100970764,107203867,116826403,123453327,127143262,127773987,128545396,128622709,130736717,131222193,132582884,132635204,137401172,143651600,143898266,144370102,146943931,148469844,149756945,151548615,152817117,153484694,155119608,156747568,156753742,157931334,160560261,163769090,164039282,164355738,167391067,169077902,170702109,174708556,185531104,185972507,186903103,187240354,188315209,191470435,196300924,200889121,203699131,209571362,211403090,213030956,213928819,215583519,218481380,219273391,223723276,226749442,228059843,243116857,245556279,250405733,250989699,258878935,263454070,266152278,266958045,267391279,268236279,269603729,270191653,277277613,279559788,281626461,285104639,287500246,290792168,291400567,291545406,294341256,294734803,298117719,298669202,298721343,300476476,303970614,304225283,304840344,307624541,311098094,311337192,316097003,317679728,321351007,326889159,328230727,328601272,329020340,330162708,331451633,336078084,336838765,337578880,337818631,338277574,338831211,340458347,341698344,341988377,343012588,343435863,345258636,345280414,348012855,349620163,350912055,353773042,353848033,355660493,357058315,366857236,367705012,367939265,368240133,371315218,373114023,373162526,376781979,382605411,387230641,394651840,395735978,397977638,401194112,401424274,402104802,404304180,406912635,408027090,412250175,413496554,431396034,436809693,445162321,447932598,448037661,449361516,451048031,458280644,458515650,458681939,465592827,467657277,468130853,468961337,469269759,470528358,481461052,484016469,489283953,491462847,491888224,496637872,497888796,505750540,514445174,515661072,529646079,531969575,533267737,533379876,537803205,539806802,545941327,548196679,551747640,553252185,555891024,557033065,557377304,558439473,561776629,563367445,563802897,566767416,568390849,568735259,570490060,571815053,577553524,579658179,587057490,592555841,593812228,594546037,600807440,601640155,602227169,604041895,607156896,613594760,617641484,619814680,625366140,626172751,627028164,630926453,634835987,639809016,641008375,642879319,643252934,643373675,645025662,648826157,650105369,651798663,654420029,659112961,659376061,665004584,670063147,672914529,673004218,677456779,678068632,685327852,687124540,689461120,700970968,701990050,705135637,705468325,709734671,710588213,714707946,721905336,725179865,725207446,727268499,727591271,729208904,737668017,738372198,740851280,745773279,746777052,747474400,759931867,760205595,761761517,763410162,767017612,768381785,779730744,779949118,785952569,787926464,791219357,794811079,795578393,797279247,810288873,812524724,814899961,819265696,819950283,820073586,823758344,830302099,831226243,836837874,837721794,838889749,841403776,844613792,848159433,851224539,851621512,852294303,853072726,853712780,855034665,857694544,858161509,859351011,859568629,865187888,865784343,866140322,868852263,869844960,877677445,879078463,880167360,880568948,884161802,885461547,893773868,894252703,895672943,905449614,907123717,907405741,910111063,913827794,914200698,914732151,918627535,918911212,920156311,921352328,922432405,924531063,924542857,926555538,926818829,926876459,927094113,933731079,945880676,950456601,951122259,953465025,955165843,955764343,962466291,966404836,973375903,974122861,979468523,979907745,984560898,985298078,985584918,985624484,987888386,989747214,990592038,990715447,993699312,994927821,995295154,995686976,999527800,999988289,116826403,148469844,186903103,228059843,298721343,311337192,329020340,336078084,338831211,367705012,382605411,387230641,413496554,447932598,468961337,481461052,489283953,533267737,568390849,645025662,672914529,837721794,838889749,865187888,866140322,924531063,926818829,955764343,270776467,295092346,303973848,322963871,593191281,696155559,705391345,711822745)
                HAVING TRUE
                    # Validations for Cohort 2
                    AND valid_ai_an = 1
                    AND valid_age = 1
                    AND general_consent_given = 1
                    AND valid_suspension_status = 1
                    AND valid_withdrawal_status = 1
                ORDER BY ps.biobank_id
            """

    @staticmethod
    def remaining_saliva_participants(config):

        is_ror = ""
        originated = {
            # at home
            1: {
                'sql': 'JOIN biobank_mail_kit_order mk ON mk.participant_id = ps.participant_id'
            },
            # in clinic
            2: {
                'sql': 'LEFT JOIN biobank_mail_kit_order mk ON mk.participant_id = ps.participant_id'
            }
        }

        if config['ror'] >= 0:
            # unset = 0
            # submitted = 1
            # submitted_not_consent = 2
            if config['ror'] == 0:
                is_ror = """AND (ps.consent_for_genomics_ror = {}  \
                    OR ps.consent_for_genomics_ror IS NULL) """.format(config['ror'])
            else:
                is_ror = 'AND ps.consent_for_genomics_ror = {}'.format(config['ror'])

        # in clinic
        is_clinic_id_null = "AND mk.id IS NULL" \
            if config['origin'] and config['origin'] == 2 else ""

        is_home_or_clinic = originated[config['origin']]['sql'] \
            if config['origin'] else ""

        # Base query for only saliva samples in RDR w/options passed in
        return """
        SELECT DISTINCT
            ps.biobank_id,
            ps.participant_id,
            0 AS biobank_order_id,
            0 AS collected_site_id,
            NULL as state_id,
            0 AS biobank_stored_sample_id,
            CASE
            WHEN ps.withdrawal_status = :withdrawal_param THEN 1 ELSE 0
            END as valid_withdrawal_status,
            CASE
            WHEN ps.suspension_status = :suspension_param THEN 1 ELSE 0
            END as valid_suspension_status,
            CASE
            WHEN ps.consent_for_study_enrollment = :general_consent_param THEN 1 ELSE 0
            END as general_consent_given,
            CASE
            WHEN ps.date_of_birth < DATE_SUB(now(), INTERVAL :dob_param YEAR) THEN 1 ELSE 0
            END AS valid_age,
            CASE
            WHEN c.value = "SexAtBirth_Male" THEN "M"
            WHEN c.value = "SexAtBirth_Female" THEN "F"
            ELSE "NA"
            END as sab,
            CASE
            WHEN ps.consent_for_genomics_ror = :general_consent_param THEN 1 ELSE 0
            END AS gror_consent,
            CASE
                WHEN native.participant_id IS NULL THEN 1 ELSE 0
            END AS valid_ai_an

        FROM
            participant_summary ps
            JOIN code c ON c.code_id = ps.sex_id
            LEFT JOIN (
                SELECT ra.participant_id
                FROM participant_race_answers ra
                    JOIN code cr ON cr.code_id = ra.code_id
                        AND SUBSTRING_INDEX(cr.value, "_", -1) = "AIAN"
            ) native ON native.participant_id = ps.participant_id
            {is_home_or_clinic}
        WHERE TRUE
            AND ps.sample_status_1sal2 = 1
            {is_ror}
            {is_clinic_id_null}
        HAVING TRUE
            AND valid_ai_an = 1
            AND valid_age = 1
            AND general_consent_given = 1
            AND valid_suspension_status = 1
            AND valid_withdrawal_status = 1
        ORDER BY ps.biobank_id
        """.format(
            is_ror=is_ror,
            is_clinic_id_null=is_clinic_id_null,
            is_home_or_clinic=is_home_or_clinic,
        )

    @staticmethod
    def new_c1_participants():
        return """
            SELECT DISTINCT
              ps.biobank_id,
              ps.participant_id,
              0 AS biobank_order_id,
              0 AS collected_site_id,
              NULL as state_id,
              0 AS biobank_stored_sample_id,
              CASE
                WHEN ps.withdrawal_status = :withdrawal_param THEN 1 ELSE 0
              END as valid_withdrawal_status,
              CASE
                WHEN ps.suspension_status = :suspension_param THEN 1 ELSE 0
              END as valid_suspension_status,
              CASE
                WHEN ps.consent_for_study_enrollment = :general_consent_param THEN 1 ELSE 0
              END as general_consent_given,
              CASE
                WHEN ps.date_of_birth < DATE_SUB(now(), INTERVAL :dob_param YEAR) THEN 1 ELSE 0
              END AS valid_age,
              CASE
                WHEN c.value = "SexAtBirth_Male" THEN "M"
                WHEN c.value = "SexAtBirth_Female" THEN "F"
                ELSE "NA"
              END as sab,
              CASE
                WHEN ps.consent_for_genomics_ror = :general_consent_param THEN 1 ELSE 0
              END AS gror_consent,
              CASE
                  WHEN native.participant_id IS NULL THEN 1 ELSE 0
              END AS valid_ai_an
            FROM
                participant_summary ps
                JOIN code c ON c.code_id = ps.sex_id
                LEFT JOIN (
                  SELECT ra.participant_id
                  FROM participant_race_answers ra
                      JOIN code cr ON cr.code_id = ra.code_id
                          AND SUBSTRING_INDEX(cr.value, "_", -1) = "AIAN"
                ) native ON native.participant_id = ps.participant_id
                LEFT JOIN genomic_set_member m ON m.participant_id = ps.participant_id
                    AND m.genomic_workflow_state <> :ignore_param
                JOIN questionnaire_response qr
                    ON qr.participant_id = ps.participant_id
                JOIN questionnaire_response_answer qra
                    ON qra.questionnaire_response_id = qr.questionnaire_response_id
                JOIN code recon ON recon.code_id = qra.value_code_id
                    AND recon.value = :c1_reconsent_param
            WHERE TRUE
                AND (
                        ps.sample_status_1ed04 = :sample_status_param
                        OR
                        ps.sample_status_1sal2 = :sample_status_param
                    )
                AND ps.consent_cohort = :cohort_1_param
                AND qr.authored > :from_date_param
                AND m.id IS NULL
            HAVING TRUE
                # Validations for Cohort 1
                AND valid_ai_an = 1
                AND valid_age = 1
                AND general_consent_given = 1
                AND valid_suspension_status = 1
                AND valid_withdrawal_status = 1
            ORDER BY ps.biobank_id
        """

    @staticmethod
    def new_c2_participants():
        return """
             SELECT DISTINCT
              ps.biobank_id,
              ps.participant_id,
              0 AS biobank_order_id,
              0 AS collected_site_id,
              NULL as state_id,
              0 AS biobank_stored_sample_id,
              CASE
                WHEN ps.withdrawal_status = :withdrawal_param THEN 1 ELSE 0
              END as valid_withdrawal_status,
              CASE
                WHEN ps.suspension_status = :suspension_param THEN 1 ELSE 0
              END as valid_suspension_status,
              CASE
                WHEN ps.consent_for_study_enrollment = :general_consent_param THEN 1 ELSE 0
              END as general_consent_given,
              CASE
                WHEN ps.date_of_birth < DATE_SUB(now(), INTERVAL :dob_param YEAR) THEN 1 ELSE 0
              END AS valid_age,
              CASE
                WHEN c.value = "SexAtBirth_Male" THEN "M"
                WHEN c.value = "SexAtBirth_Female" THEN "F"
                ELSE "NA"
              END as sab,
              CASE
                WHEN ps.consent_for_genomics_ror = :general_consent_param THEN 1 ELSE 0
              END AS gror_consent,
              CASE
                  WHEN native.participant_id IS NULL THEN 1 ELSE 0
              END AS valid_ai_an
            FROM
                participant_summary ps
                JOIN code c ON c.code_id = ps.sex_id
                LEFT JOIN (
                  SELECT ra.participant_id
                  FROM participant_race_answers ra
                      JOIN code cr ON cr.code_id = ra.code_id
                          AND SUBSTRING_INDEX(cr.value, "_", -1) = "AIAN"
                ) native ON native.participant_id = ps.participant_id
                LEFT JOIN genomic_set_member m ON m.participant_id = ps.participant_id
                    AND m.genomic_workflow_state <> :ignore_param
            WHERE TRUE
                AND (
                        ps.sample_status_1ed04 = :sample_status_param
                        OR
                        ps.sample_status_1sal2 = :sample_status_param
                    )
                AND ps.consent_cohort = :cohort_2_param
                AND ps.questionnaire_on_dna_program_authored > :from_date_param
                AND ps.questionnaire_on_dna_program = :general_consent_param
                AND m.id IS NULL
            HAVING TRUE
                # Validations for Cohort 2
                AND valid_ai_an = 1
                AND valid_age = 1
                AND general_consent_given = 1
                AND valid_suspension_status = 1
                AND valid_withdrawal_status = 1
            ORDER BY ps.biobank_id
        """

    @staticmethod
    def usable_blood_sample():
        return """
        # Latest 1ED04 or 1ED10 Sample
        SELECT ssed.biobank_stored_sample_id AS blood_sample
            , oed.collected_site_id AS blood_site
            , oed.biobank_order_id AS blood_order
            , ssed.test, ssed.status
        FROM biobank_stored_sample ssed
            JOIN biobank_order_identifier edid ON edid.value = ssed.biobank_order_identifier
            JOIN biobank_order oed ON oed.biobank_order_id = edid.biobank_order_id
            JOIN biobank_ordered_sample oeds ON oed.biobank_order_id = oeds.order_id
                AND ssed.test = oeds.test
        WHERE TRUE
            and ssed.biobank_id = :bid_param
            and ssed.test in ("1ED04", "1ED10")
            and ssed.status < 13
        ORDER BY oeds.collected DESC
        """

    @staticmethod
    def usable_saliva_sample():
        return """
            # Max 1SAL2 Sample
            select sssal.biobank_stored_sample_id AS saliva_sample
                , osal.collected_site_id AS saliva_site
                , osal.biobank_order_id AS saliva_order
                , sssal.test, sssal.status
            FROM biobank_order osal
                JOIN biobank_order_identifier salid ON osal.biobank_order_id = salid.biobank_order_id
                JOIN biobank_ordered_sample sal2 ON osal.biobank_order_id = sal2.order_id
                    AND sal2.test = "1SAL2"
                JOIN biobank_stored_sample sssal ON salid.value = sssal.biobank_order_identifier
            WHERE TRUE
                and sssal.biobank_id = :bid_param
                and sssal.status < 13
                and sssal.test = "1SAL2"
                and osal.finalized_time = (
                     SELECT MAX(o.finalized_time)
                     FROM biobank_ordered_sample os
                         JOIN biobank_order o ON o.biobank_order_id = os.order_id
                     WHERE os.test = "1SAL2"
                             AND o.participant_id = :pid_param
                         GROUP BY o.participant_id
                   )
            """

    @staticmethod
    def new_biobank_samples():
        return """
        SELECT DISTINCT
          ss.biobank_id,
          p.participant_id,
          o.biobank_order_id,
          o.collected_site_id,
          mk.state_id,
          ss.biobank_stored_sample_id,
          CASE
            WHEN p.withdrawal_status = :withdrawal_param THEN 1 ELSE 0
          END as valid_withdrawal_status,
          CASE
            WHEN p.suspension_status = :suspension_param THEN 1 ELSE 0
          END as valid_suspension_status,
          CASE
            WHEN ps.consent_for_study_enrollment = :general_consent_param THEN 1 ELSE 0
          END as general_consent_given,
          CASE
            WHEN ps.date_of_birth < DATE_SUB(now(), INTERVAL :dob_param YEAR) THEN 1 ELSE 0
          END AS valid_age,
          CASE
            WHEN c.value = "SexAtBirth_Male" THEN "M"
            WHEN c.value = "SexAtBirth_Female" THEN "F"
            ELSE "NA"
          END as sab,
          CASE
            WHEN ps.consent_for_genomics_ror = 1 THEN 1 ELSE 0
          END AS gror_consent,
          CASE
              WHEN native.participant_id IS NULL THEN 1 ELSE 0
          END AS valid_ai_an,
          ss.status,
          ss.test
        FROM
            biobank_stored_sample ss
            JOIN participant p ON ss.biobank_id = p.biobank_id
            JOIN biobank_order_identifier oi ON ss.biobank_order_identifier = oi.value
            JOIN biobank_order o ON oi.biobank_order_id = o.biobank_order_id
            JOIN participant_summary ps ON ps.participant_id = p.participant_id
            JOIN code c ON c.code_id = ps.sex_id
            LEFT JOIN (
              SELECT ra.participant_id
              FROM participant_race_answers ra
                  JOIN code cr ON cr.code_id = ra.code_id
                      AND SUBSTRING_INDEX(cr.value, "_", -1) = "AIAN"
            ) native ON native.participant_id = p.participant_id
            LEFT JOIN genomic_set_member m ON m.participant_id = ps.participant_id
                    AND m.genomic_workflow_state <> :ignore_param
            LEFT JOIN biobank_mail_kit_order mk ON mk.participant_id = p.participant_id
        WHERE TRUE
            AND ss.test in ('1ED04', '1ED10', '1SAL2')
            AND ss.rdr_created > :from_date_param
            AND ps.consent_cohort = :cohort_3_param
            AND ps.participant_origin != 'careevolution'
            AND m.id IS NULL
        """

    # BEGIN Data Quality Pipeline Report Queries
    @staticmethod
    def dq_report_runs_summary(from_date):

        query_sql = """
            SELECT job_id
                , SUM(IF(run_result = :unset, run_count, 0)) AS 'UNSET'
                , SUM(IF(run_result = :success, run_count, 0)) AS 'SUCCESS'
                , SUM(IF(run_result = :error, run_count, 0)) AS 'ERROR'    
                , SUM(IF(run_result = :no_files, run_count, 0)) AS 'NO_FILES'
                , SUM(IF(run_result = :invalid_name, run_count, 0)) AS 'INVALID_FILE_NAME'
                , SUM(IF(run_result = :invalid_structure, run_count, 0)) AS 'INVALID_FILE_STRUCTURE'
            FROM 
                (
                    SELECT count(id) run_count
                        , job_id
                        , run_result	
                    FROM genomic_job_run
                    WHERE start_time > :from_date
                    group by job_id, run_result
                ) sub
            group by job_id
        """

        query_params = {
            "unset": GenomicSubProcessResult.UNSET.number,
            "success": GenomicSubProcessResult.SUCCESS.number,
            "error": GenomicSubProcessResult.ERROR.number,
            "no_files": GenomicSubProcessResult.NO_FILES.number,
            "invalid_name": GenomicSubProcessResult.INVALID_FILE_NAME.number,
            "invalid_structure": GenomicSubProcessResult.INVALID_FILE_STRUCTURE.number,
            "from_date": from_date
        }
        return query_sql, query_params

    @staticmethod
    def dq_report_ingestions_summary(from_date):
        # TODO: This query only supports the AW1 ingestions
        #  A future PR will expand this support for the AW2 ingestions

        query_sql = """
            SELECT count(distinct raw.id) record_count
                , count(distinct m.id) as ingested_count
                , count(distinct i.id) as incident_count
                , "aw1" as file_type
                , LOWER(SUBSTRING_INDEX(SUBSTRING_INDEX(raw.file_path, "/", -1), "_", 1)) as gc_site_id
                , CASE
                    WHEN SUBSTRING_INDEX(SUBSTRING_INDEX(
                            SUBSTRING_INDEX(raw.file_path, "/", -1), "_", 3), "_", -1
                        ) = "SEQ" 
                    THEN "aou_wgs"
                    WHEN SUBSTRING_INDEX(SUBSTRING_INDEX(
                            SUBSTRING_INDEX(raw.file_path, "/", -1), "_", 3), "_", -1
                        ) = "GEN" 
                    THEN "aou_array"			
                  END AS genome_type
                , raw.file_path
            FROM genomic_aw1_raw raw
                LEFT JOIN genomic_manifest_file mf ON mf.file_path = raw.file_path
                LEFT JOIN genomic_file_processed f ON f.genomic_manifest_file_id = mf.id
                LEFT JOIN genomic_set_member m ON m.aw1_file_processed_id = f.id
                LEFT JOIN genomic_incident i ON i.source_file_processed_id = f.id
            WHERE TRUE
                AND raw.created >=  :from_date
                AND raw.ignore_flag = 0
                AND raw.biobank_id <> ""
            #	AND m.genomic_workflow_state <> 33
            GROUP BY raw.file_path, file_type
        """

        query_params = {
            "from_date": from_date
        }

        return query_sql, query_params
