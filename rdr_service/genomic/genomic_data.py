import sqlalchemy
from sqlalchemy import distinct
from sqlalchemy.orm import aliased

from rdr_service.config import GENOME_TYPE_ARRAY, GENOME_TYPE_WGS
from rdr_service.genomic_enums import GenomicSubProcessResult, GenomicWorkflowState, GenomicManifestTypes, \
    GenomicContaminationCategory
from rdr_service.model.config_utils import get_biobank_id_prefix
from rdr_service.model.genomics import GenomicGCValidationMetrics, GenomicSetMember, GenomicSet, GenomicFileProcessed
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import WithdrawalStatus, SuspensionStatus, QuestionnaireStatus


class GenomicQueryClass:

    def __init__(self, input_manifest=None):
        self.input_manifest = input_manifest

        # Table aliases for tables requiring multiple JOINs
        self.aliases = {
            'gsm': aliased(GenomicSetMember),
        }

        self.subqueries = {
            'aw3_wgs_parent_sample_id': (
                sqlalchemy.select([GenomicSetMember.gcManifestParentSampleId]).select_from(GenomicSetMember).where(
                    (GenomicSetMember.genomeType == "aou_array") &
                    (GenomicSetMember.aw3ManifestJobRunID.isnot(None))
                )
            )
        }

        self.genomic_data_config = {
            GenomicManifestTypes.AW3_ARRAY: (sqlalchemy.select(
                [
                    distinct(GenomicGCValidationMetrics.chipwellbarcode),
                    sqlalchemy.func.concat(get_biobank_id_prefix(), GenomicSetMember.biobankId),
                    GenomicSetMember.sampleId,
                    GenomicSetMember.sexAtBirth,
                    GenomicSetMember.gcSiteId,
                    GenomicGCValidationMetrics.idatRedPath,
                    GenomicGCValidationMetrics.idatRedMd5Path,
                    GenomicGCValidationMetrics.idatGreenPath,
                    GenomicGCValidationMetrics.idatGreenMd5Path,
                    GenomicGCValidationMetrics.vcfPath,
                    GenomicGCValidationMetrics.vcfTbiPath,
                    GenomicGCValidationMetrics.vcfMd5Path,
                    GenomicGCValidationMetrics.callRate,
                    GenomicGCValidationMetrics.sexConcordance,
                    GenomicGCValidationMetrics.contamination,
                    GenomicGCValidationMetrics.processingStatus,
                    Participant.researchId,
                ]
            ).select_from(
                sqlalchemy.join(
                    sqlalchemy.join(
                        sqlalchemy.join(ParticipantSummary,
                                        GenomicSetMember,
                                        GenomicSetMember.participantId == ParticipantSummary.participantId),
                        GenomicGCValidationMetrics,
                        GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
                    ),
                    Participant,
                    Participant.participantId == ParticipantSummary.participantId
                )
            ).where(
                (GenomicGCValidationMetrics.processingStatus == 'pass') &
                (GenomicGCValidationMetrics.ignoreFlag != 1) &
                (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                (GenomicSetMember.genomeType == GENOME_TYPE_ARRAY) &
                (ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN) &
                (ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED) &
                (GenomicGCValidationMetrics.idatRedReceived == 1) &
                (GenomicGCValidationMetrics.idatGreenReceived == 1) &
                (GenomicGCValidationMetrics.idatRedMd5Received == 1) &
                (GenomicGCValidationMetrics.idatGreenMd5Received == 1) &
                (GenomicGCValidationMetrics.vcfReceived == 1) &
                (GenomicGCValidationMetrics.vcfMd5Received == 1) &
                (GenomicSetMember.aw3ManifestJobRunID.is_(None))
            )),
            GenomicManifestTypes.AW3_WGS: (sqlalchemy.select(
                [
                    distinct(sqlalchemy.func.concat(get_biobank_id_prefix(), GenomicSetMember.biobankId)),
                    GenomicSetMember.sampleId,
                    sqlalchemy.func.concat(get_biobank_id_prefix(),
                                           GenomicSetMember.biobankId, '_',
                                           GenomicSetMember.sampleId),
                    GenomicSetMember.sexAtBirth,
                    GenomicSetMember.gcSiteId,
                    GenomicGCValidationMetrics.hfVcfPath,
                    GenomicGCValidationMetrics.hfVcfTbiPath,
                    GenomicGCValidationMetrics.hfVcfMd5Path,
                    GenomicGCValidationMetrics.rawVcfPath,
                    GenomicGCValidationMetrics.rawVcfTbiPath,
                    GenomicGCValidationMetrics.rawVcfMd5Path,
                    GenomicGCValidationMetrics.cramPath,
                    GenomicGCValidationMetrics.cramMd5Path,
                    GenomicGCValidationMetrics.craiPath,
                    GenomicGCValidationMetrics.gvcfPath,
                    GenomicGCValidationMetrics.gvcfMd5Path,
                    GenomicGCValidationMetrics.contamination,
                    GenomicGCValidationMetrics.sexConcordance,
                    GenomicGCValidationMetrics.processingStatus,
                    GenomicGCValidationMetrics.meanCoverage,
                    Participant.researchId,
                    GenomicSetMember.sampleId,
                ]
            ).select_from(
                sqlalchemy.join(
                    ParticipantSummary,
                    GenomicSetMember,
                    GenomicSetMember.participantId == ParticipantSummary.participantId
                ).join(
                    GenomicGCValidationMetrics,
                    GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
                ).join(
                    Participant,
                    Participant.participantId == ParticipantSummary.participantId
                )
            ).where(
                (GenomicGCValidationMetrics.processingStatus == 'pass') &
                (GenomicGCValidationMetrics.ignoreFlag != 1) &
                (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                (GenomicSetMember.genomeType == GENOME_TYPE_WGS) &
                (ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN) &
                (ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED) &
                (GenomicSetMember.aw3ManifestJobRunID.is_(None)) &
                (GenomicGCValidationMetrics.hfVcfReceived == 1) &
                (GenomicGCValidationMetrics.hfVcfTbiReceived == 1) &
                (GenomicGCValidationMetrics.hfVcfMd5Received == 1) &
                (GenomicGCValidationMetrics.cramReceived == 1) &
                (GenomicGCValidationMetrics.cramMd5Received == 1) &
                (GenomicGCValidationMetrics.craiReceived == 1) &
                (GenomicGCValidationMetrics.gvcfReceived == 1) &
                (GenomicGCValidationMetrics.gvcfMd5Received == 1) &
                (GenomicSetMember.gcManifestParentSampleId.in_(self.subqueries['aw3_wgs_parent_sample_id']))
            )),
            GenomicManifestTypes.CVL_W1: (sqlalchemy.select(
                [
                    GenomicSet.genomicSetName,
                    GenomicSetMember.biobankId,
                    GenomicSetMember.sampleId,
                    GenomicSetMember.sexAtBirth,
                    GenomicSetMember.nyFlag,
                    GenomicGCValidationMetrics.siteId,
                    sqlalchemy.bindparam('secondary_validation', None),
                    sqlalchemy.bindparam('date_submitted', None),
                    sqlalchemy.bindparam('test_name', 'aou_wgs'),
                ]
            ).select_from(
                sqlalchemy.join(
                    sqlalchemy.join(GenomicSet, GenomicSetMember,
                                    GenomicSetMember.genomicSetId == GenomicSet.id),
                    GenomicGCValidationMetrics,
                    GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
                )
            ).where(
                (GenomicGCValidationMetrics.processingStatus == 'pass') &
                (GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.CVL_READY) &
                (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                (GenomicSetMember.genomeType == "aou_wgs")
            )),
            GenomicManifestTypes.CVL_W3: (sqlalchemy.select(
                [
                    sqlalchemy.bindparam('value', ''),
                    GenomicSetMember.sampleId,
                    GenomicSetMember.biobankId,
                    GenomicSetMember.collectionTubeId.label("collection_tubeid"),
                    GenomicSetMember.sexAtBirth,
                    sqlalchemy.bindparam('genome_type', 'aou_wgs'),
                    GenomicSetMember.nyFlag,
                    sqlalchemy.bindparam('request_id', ''),
                    sqlalchemy.bindparam('package_id', ''),
                    GenomicSetMember.ai_an,
                    GenomicSetMember.gcSiteId.label('site_id'),
                ]
            ).select_from(
                sqlalchemy.join(
                    GenomicSetMember,
                    ParticipantSummary,
                    GenomicSetMember.participantId == ParticipantSummary.participantId
                )
            ).where(
                (GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.W2) &
                (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                (GenomicSetMember.genomeType == "aou_cvl") &
                (ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED)
            )),
            GenomicManifestTypes.GEM_A1: (sqlalchemy.select(
                [
                    GenomicSetMember.biobankId,
                    GenomicSetMember.sampleId,
                    GenomicSetMember.sexAtBirth,
                    sqlalchemy.func.IF(
                        ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED,
                        sqlalchemy.sql.expression.literal("yes"),
                        sqlalchemy.sql.expression.literal("no")),
                    ParticipantSummary.consentForGenomicsRORAuthored,
                    GenomicGCValidationMetrics.chipwellbarcode,
                    sqlalchemy.func.upper(GenomicSetMember.gcSiteId),
                ]
            ).select_from(
                sqlalchemy.join(
                    sqlalchemy.join(ParticipantSummary,
                                    GenomicSetMember,
                                    GenomicSetMember.participantId == ParticipantSummary.participantId),
                    GenomicGCValidationMetrics,
                    GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
                )
            ).where(
                (GenomicGCValidationMetrics.processingStatus == 'pass') &
                (GenomicGCValidationMetrics.ignoreFlag != 1) &
                (GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.GEM_READY) &
                (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                (GenomicSetMember.genomeType == "aou_array") &
                (ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN) &
                (ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED) &
                (ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED) &
                (ParticipantSummary.participantOrigin != 'careevolution')
            ).group_by(
                GenomicSetMember.biobankId,
                GenomicSetMember.sampleId,
                GenomicSetMember.sexAtBirth,
                sqlalchemy.func.IF(
                    ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED,
                    sqlalchemy.sql.expression.literal("yes"),
                    sqlalchemy.sql.expression.literal("no")),
                ParticipantSummary.consentForGenomicsRORAuthored,
                GenomicGCValidationMetrics.chipwellbarcode,
                sqlalchemy.func.upper(GenomicSetMember.gcSiteId),
            ).order_by(ParticipantSummary.consentForGenomicsRORAuthored).limit(10000)
                                          ),
            GenomicManifestTypes.GEM_A3: (sqlalchemy.select(
                [
                    GenomicSetMember.biobankId,
                    GenomicSetMember.sampleId,
                    sqlalchemy.func.date_format(GenomicSetMember.reportConsentRemovalDate, '%Y-%m-%dT%TZ'),
                ]
            ).select_from(
                sqlalchemy.join(ParticipantSummary,
                                GenomicSetMember,
                                GenomicSetMember.participantId == ParticipantSummary.participantId)
            ).where(
                (GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.GEM_RPT_PENDING_DELETE) &
                (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                (GenomicSetMember.genomeType == "aou_array")
            )
            ),
            GenomicManifestTypes.AW2F: (
                sqlalchemy.select(
                    [
                        GenomicSetMember.packageId,
                        sqlalchemy.func.concat(get_biobank_id_prefix(),
                                               GenomicSetMember.biobankId, "_", GenomicSetMember.sampleId),
                        GenomicSetMember.gcManifestBoxStorageUnitId,
                        GenomicSetMember.gcManifestBoxPlateId,
                        GenomicSetMember.gcManifestWellPosition,
                        GenomicSetMember.sampleId,
                        GenomicSetMember.gcManifestParentSampleId,
                        GenomicSetMember.collectionTubeId,
                        GenomicSetMember.gcManifestMatrixId,
                        sqlalchemy.bindparam('collection_date', ''),
                        GenomicSetMember.biobankId,
                        GenomicSetMember.sexAtBirth,
                        sqlalchemy.bindparam('age', ''),
                        sqlalchemy.func.IF(GenomicSetMember.nyFlag == 1,
                                           sqlalchemy.sql.expression.literal("Y"),
                                           sqlalchemy.sql.expression.literal("N")),
                        sqlalchemy.bindparam('sample_type', 'DNA'),
                        GenomicSetMember.gcManifestTreatments,
                        GenomicSetMember.gcManifestQuantity_ul,
                        GenomicSetMember.gcManifestTotalConcentration_ng_per_ul,
                        GenomicSetMember.gcManifestTotalDNA_ng,
                        GenomicSetMember.gcManifestVisitDescription,
                        GenomicSetMember.gcManifestSampleSource,
                        GenomicSetMember.gcManifestStudy,
                        GenomicSetMember.gcManifestTrackingNumber,
                        GenomicSetMember.gcManifestContact,
                        GenomicSetMember.gcManifestEmail,
                        GenomicSetMember.gcManifestStudyPI,
                        GenomicSetMember.gcManifestTestName,
                        GenomicSetMember.gcManifestFailureMode,
                        GenomicSetMember.gcManifestFailureDescription,
                        GenomicGCValidationMetrics.processingStatus,
                        GenomicGCValidationMetrics.contamination,
                        sqlalchemy.case(
                            [
                                (GenomicGCValidationMetrics.contaminationCategory ==
                                 GenomicContaminationCategory.EXTRACT_WGS, "extract wgs"),

                                (GenomicGCValidationMetrics.contaminationCategory ==
                                 GenomicContaminationCategory.NO_EXTRACT, "no extract"),

                                (GenomicGCValidationMetrics.contaminationCategory ==
                                 GenomicContaminationCategory.EXTRACT_BOTH, "extract both"),

                                (GenomicGCValidationMetrics.contaminationCategory ==
                                 GenomicContaminationCategory.TERMINAL_NO_EXTRACT, "terminal no extract"),
                            ], else_=""
                        ),
                        sqlalchemy.func.IF(ParticipantSummary.consentForGenomicsROR
                                           == QuestionnaireStatus.SUBMITTED,
                                           sqlalchemy.sql.expression.literal("yes"),
                                           sqlalchemy.sql.expression.literal("no")),
                    ]
                ).select_from(
                    sqlalchemy.join(
                        ParticipantSummary,
                        GenomicSetMember,
                        GenomicSetMember.participantId == ParticipantSummary.participantId
                    ).join(
                        GenomicGCValidationMetrics,
                        GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
                    ).join(
                        GenomicFileProcessed,
                        GenomicFileProcessed.id == GenomicSetMember.aw1FileProcessedId
                    )
                ).where(
                    (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                    (GenomicSetMember.aw2fManifestJobRunID.is_(None)) &
                    (GenomicGCValidationMetrics.ignoreFlag == 0) &
                    (GenomicGCValidationMetrics.contamination.isnot(None)) &
                    (GenomicGCValidationMetrics.contamination != '') &
                    (GenomicFileProcessed.genomicManifestFileId == (self.input_manifest and self.input_manifest.id))
                )
            )
        }

    @staticmethod
    def remaining_c2_participants():
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
                            ps.sample_status_1ed10 = :sample_status_param
                            OR
                            ps.sample_status_1sal2 = :sample_status_param
                        )
                    AND ps.consent_cohort = :cohort_2_param
                    AND m.id IS NULL
                    AND ps.participant_origin = "vibrent"
                HAVING TRUE
                    # Validations for Cohort 2
                    AND valid_age = 1
                    AND general_consent_given = 1
                    AND valid_suspension_status = 1
                    AND valid_withdrawal_status = 1
                ORDER BY ps.biobank_id
            """

    @staticmethod
    def remaining_saliva_participants(config):
        is_ror = None
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
        query_sql = """
                # AW1 Ingestions
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
                GROUP BY raw.file_path, file_type
                UNION
                # AW2 Ingestions
                SELECT count(distinct raw.id) record_count
                    , count(distinct m.id) as ingested_count
                    , count(distinct i.id) as incident_count
                    , "aw2" as file_type
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
                FROM genomic_aw2_raw raw
                    LEFT JOIN genomic_manifest_file mf ON mf.file_path = raw.file_path
                    LEFT JOIN genomic_file_processed f ON f.genomic_manifest_file_id = mf.id
                    LEFT JOIN genomic_gc_validation_metrics m ON m.genomic_file_processed_id = f.id
                    LEFT JOIN genomic_incident i ON i.source_file_processed_id = f.id
                WHERE TRUE
                    AND raw.created >=  :from_date
                    AND raw.ignore_flag = 0
                    AND raw.biobank_id <> ""
                GROUP BY raw.file_path, file_type
            """

        query_params = {
            "from_date": from_date
        }

        return query_sql, query_params

    @staticmethod
    def dq_report_incident_detail(from_date):
        query_sql = """
                # Incident Detail Report Query
            SELECT code
                , created
                , biobank_id
                , genomic_set_member_id
                , source_job_run_id
                , source_file_processed_id
            FROM genomic_incident
            WHERE created >= :from_date
            ORDER BY code, created
            """

        query_params = {
            "from_date": from_date
        }

        return query_sql, query_params
