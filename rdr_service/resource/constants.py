#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
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
    participant_consents = 2060
    participant_modules = 2070
    participant_address = 2080
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

    # Workbench
    workbench_researcher = 4000
    workbench_institutional_affiliation = 4010
    workbench_workspace = 4020
    workbench_workspace_age = 4025
    workbench_workspace_users = 4030

    # Covid study
    biobank_covid_antibody_sample = 5000
    quest_covid_antibody_test = 5010
    quest_covid_antibody_test_result = 5020
