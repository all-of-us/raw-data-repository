#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from .participant import ParticipantSchema
from .code import CodeSchema
from .hpo import HPOSchema
from .organization import OrganizationSchema
from .site import SiteSchema
from .workbench_researcher import WorkbenchResearcherSchema, WorkbenchInstitutionalAffiliationsSchema
from .workbench_workspace import WorkbenchWorkspaceSchema, WorkbenchWorkspaceUsersSchema
from .covid_antibody_study import BiobankCovidAntibodySampleSchema, QuestCovidAntibodyTestResultSchema, \
    QuestCovidAntibodyTestSchema


__all__ = [
    'SchemaUniqueIds',
    'ParticipantSchema',
    'CodeSchema',
    'HPOSchema',
    'OrganizationSchema',
    'SiteSchema',
    'WorkbenchResearcherSchema',
    'WorkbenchInstitutionalAffiliationsSchema',
    'WorkbenchWorkspaceSchema',
    'WorkbenchWorkspaceUsersSchema',
    'BiobankCovidAntibodySampleSchema',
    'QuestCovidAntibodyTestSchema',
    'QuestCovidAntibodyTestResultSchema'
]


#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from enum import IntEnum


class SchemaUniqueIds(IntEnum):
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
    # Genomic schemas
    genomic_set = 3000

    # Workbench
    workbench_researcher = 4000
    workbench_institutional_affiliation = 4010