#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from datetime import datetime
from enum import IntEnum

class SchemaID(IntEnum):
    """
    Unique identifiers for each resource schema.  Schema id values should be grouped by schema types.
    These are the ids and names the resource records are stored under in the `resource_data` table.
    These will be seen by consumers of the resource API and should be named appropriately.
    """
    # Codebook schema
    codes = 1001
    # Partner organization schemas
    hpo = 1010
    organization = 1020
    site = 1030
    # Participant schemas
    participant = 2001
    participant_biobank_orders = 2010
    participant_biobank_order_samples = 2020
    participant_physical_measurements = 2030
    participant_gender = 2040
    participant_race = 2050
    participant_sexual_orientation = 2051
    participant_consents = 2060
    participant_modules = 2070
    participant_address = 2080
    participant_pairing_history = 2085
    patient_statuses = 2090
    ehr_recept = 2100
    pdr_participant = 2110
    # Genomic schemas
    genomic_set = 3000
    genomic_set_member = 3010
    genomic_job_run = 3020
    genomic_gc_validation_metrics = 3030
    genomic_file_processed = 3040
    genomic_manifest_file = 3050
    genomic_manifest_feedback = 3060
    genomic_user_event_metrics = 3070
    genomic_informing_loop = 3080
    genomic_cvl_result_past_due = 3090
    genomic_member_report_state = 3100
    genomic_result_viewed = 3110
    genomic_appointment_event = 3120

    # Workbench
    workbench_researcher = 4000
    workbench_institutional_affiliation = 4010
    workbench_workspace = 4020
    workbench_workspace_users = 4030
    workbench_researcher_race = 4040
    workbench_researcher_gender = 4050
    workbench_researcher_sex_at_birth = 4060
    workbench_researcher_degree = 4070
    workbench_workspace_age = 4080
    workbench_workspace_ethnicity = 4090
    workbench_researcher_short_tier_names = 4100
    workbench_researcher_dsv2_ethnic_category = 4110
    workbench_researcher_dsv2_gender_identity = 4120
    workbench_researcher_dsv2_sexual_orientation = 4130

    # Covid study
    biobank_covid_antibody_sample = 5000
    quest_covid_antibody_test = 5010
    quest_covid_antibody_test_result = 5020

    # Metrics
    retention_metrics = 2120

    # Consent Validation Metrics
    consent_metrics = 6000


# Used to calculate participant enrollment cohort.
COHORT_1_CUTOFF = datetime(2018, 4, 24, 0, 0, 0)
COHORT_2_CUTOFF = datetime(2020, 4, 21, 4, 0, 0)

# Workaround:  Allow PDR data rebuild tasks to skip building certain test pids that have so much data associated
# with them (e.g., high numbers of questionnaire responses) that the PDR generator tasks can exceed time or memory
# limits and be terminated abnormally.   For now, there is one pid known to cause such task failures
SKIP_TEST_PIDS_FOR_PDR = [838981439, ]

class ConsentCohortEnum(IntEnum):
    """
    Which cohort does a participant belong too, based on consent date.
    """
    UNSET = 0
    COHORT_1 = 1  # Beta participants.  Consent before April 24, 2018.
    COHORT_2 = 2  # National Launch Participants. Consent between April 24, 2018 and April 21, 2020 (03:59:59 UTC)
    COHORT_3 = 3  # New Participants with consent starting from April 21, 2020 04:00:00 UTC (midnight eastern)


# The PDR version of the RDR Enrollment Status enum. These names line up with program nameing.
# Names should be ordered by value. Insert new status names to keep the correct order.
class PDREnrollmentStatusEnum(IntEnum):
    Unset = 0
    Registered = 10  # EnrollmentStatusV2.REGISTERED
    Participant = 20  # EnrollmentStatusV2.PARTICIPANT
    ParticipantPlusEHR = 30  # EnrollmentStatusV2.FULLY_CONSENTED
    CoreParticipantMinusPM = 40  # EnrollmentStatusV2.CORE_MINUS_PM
    CoreParticipant = 50  # EnrollmentStatusV2.CORE_PARTICIPANT


# Participant Activity Group IDs.
class ActivityGroupEnum(IntEnum):
    Profile = 1  # ParticipantActivity values 1 through 19
    Biobank = 20  # ParticipantActivity values 20 through 29
    QuestionnaireModule = 40  # ParticipantActivity values 40 through 69
    Genomics = 70  # ParticipantActivity values 70 through 99
    EnrollmentStatus = 100 # Part


# An enumeration of all participant activity with in RDR.
class ParticipantEventEnum(IntEnum):
    # Profile Group: 1 - 19
    SignupTime = 1
    PhysicalMeasurements = 3
    EHRFirstReceived = 4
    EHRLastReceived = 5
    CABOR = 6
    Deceased = 18
    Withdrawal = 19

    # Biobank Group: 20 - 29
    BiobankOrder = 20
    BiobankShipped = 21
    BiobankReceived = 22
    BiobankConfirmed = 23

    # Questionnaire Module Group (Names should exactly match module code value name): 40 - 69
    # Initial list based on module responses that can trigger participant status or retention eligibility updates.
    # SNAP modules and some misc. administrative modules not included.
    ConsentPII = 40
    TheBasics = 41
    Lifestyle = 42
    OverallHealth = 43
    EHRConsentPII = 44
    DVEHRSharing = 45
    GROR = 46
    PrimaryConsentUpdate = 47
    ProgramUpdate = 48
    COPE = 49,
    cope_nov = 50,
    cope_dec = 51,
    cope_feb = 52,
    GeneticAncestry = 53
    cope_vaccine1 = 54
    cope_vaccine2 = 55

    # Genomics: 70 - 99

    # Enrollment Status: 100 - 119
    REGISTERED = 100
    PARTICIPANT = 104
    FULLY_CONSENTED = 108
    CORE_MINUS_PM = 112
    CORE_PARTICIPANT = 114


class RetentionStatusEnum(IntEnum):
    """ Whether a participant meets retention-eligible criteria """
    NOT_ELIGIBLE = 1
    ELIGIBLE = 2


class RetentionTypeEnum(IntEnum):
    """ Participant retention type """
    UNSET = 0
    ACTIVE = 1
    PASSIVE = 2
    ACTIVE_AND_PASSIVE = 3
