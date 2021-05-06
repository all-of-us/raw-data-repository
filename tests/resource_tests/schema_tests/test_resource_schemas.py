#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import json

from tests.helpers.unittest_base import BaseTestCase

# -- BQ model imports
from rdr_service.model.bq_code import BQCodeSchema
from rdr_service.model.bq_hpo import BQHPOSchema
from rdr_service.model.bq_organization import BQOrganizationSchema
from rdr_service.model.bq_participant_summary import (
    BQBiobankOrderSchema, BQAddressSchema, BQBiobankSampleSchema, BQGenderSchema, BQRaceSchema,
    BQEhrReceiptSchema, BQPatientStatusSchema, BQConsentSchema, BQModuleStatusSchema,
    BQPhysicalMeasurements, BQParticipantSummarySchema
)
from rdr_service.model.bq_site import BQSiteSchema
from rdr_service.model.bq_genomics import (
    BQGenomicSetSchema, BQGenomicJobRunSchema, BQGenomicFileProcessedSchema, BQGenomicManifestFeedbackSchema,
    BQGenomicManifestFileSchema, BQGenomicSetMemberSchema, BQGenomicGCValidationMetricsSchema
)
from rdr_service.model.bq_workbench_researcher import BQRWBResearcherSchema, BQRWBInstitutionalAffiliationsSchema
from rdr_service.model.bq_workbench_workspace import (
    BQRWBWorkspaceSchema, BQRWBWorkspaceUsersSchema, BQWorkspaceAgeSchema, BQWorkspaceRaceEthnicitySchema
)

# -- Resource Schema Imports
from rdr_service.resource.schemas import CodeSchema
from rdr_service.resource.schemas import HPOSchema
from rdr_service.resource.schemas import OrganizationSchema
from rdr_service.resource.schemas.participant import (
    BiobankOrderSchema, AddressSchema, BiobankSampleSchema, GenderSchema, RaceSchema, EHRReceiptSchema,
    PatientStatusSchema, ConsentSchema, ModuleStatusSchema, PhysicalMeasurementsSchema, ParticipantSchema
)
from rdr_service.resource.schemas import SiteSchema
from rdr_service.resource.schemas.genomics import (
    GenomicSetSchema, GenomicJobRunSchema, GenomicFileProcessedSchema, GenomicManifestFeedbackSchema,
    GenomicManifestFileSchema, GenomicSetMemberSchema, GenomicGCValidationMetricsSchema
)
from rdr_service.resource.schemas.workbench_researcher import (
    WorkbenchResearcherSchema, WorkbenchInstitutionalAffiliationsSchema
)
from rdr_service.resource.schemas.workbench_workspace import (
    WorkbenchWorkspaceSchema, WorkbenchWorkspaceUsersSchema, WorkspaceAgeSchema, WorkspaceRaceEthnicitySchema
)

_excluded_bq_fields = ['id', 'created', 'modified', 'orig_id', 'orig_created', 'orig_modified']


class ResourceSchemaTest(BaseTestCase):
    """
    Test that the resource schema definitions/fields align with the BigQuery schemas
    NOTE:  These tests may be deprecated if use of BigQuery PDR is discontinued
    TODO:  Add more detail about implementing test cases that include handling field name prefixes, exclusions, etc.
    """

    def setup(self):
        super().setup()

    def _verify_resource_schema(self, rsc_schema_obj, bq_schema_obj, bq_prefix='', exclusions=_excluded_bq_fields):

        bq_field_list = sorted(self._get_bq_field_list(bq_schema_obj, bq_prefix=bq_prefix, exclusions=exclusions))
        rsc_field_list = sorted(rsc_schema_obj.fields.keys())
        self.assertListEqual(bq_field_list, rsc_field_list)


    @staticmethod
    def _get_bq_field_list(bq_schema, bq_prefix='', exclusions=[]):
        """ Return a filtered BQ schema column/field name list, with any specified prefix stripped  """
        fields = []
        for field in json.loads(bq_schema.to_json()):
            name = field['name']
            if name not in exclusions:
                fields.append(name[len(bq_prefix):] if name.startswith(bq_prefix) else name)

        return fields

    # -- Create a test for each schema defined in the rdr_service/resource/schemas directory (including sub-schemas)

    # Participant data schemas
    def test_address_resource_schema(self):
        self._verify_resource_schema(AddressSchema(), BQAddressSchema())

    def test_biobank_order_resource_schema(self):
        self._verify_resource_schema(BiobankOrderSchema(), BQBiobankOrderSchema(), bq_prefix='bbo_')

    def test_biobank_sample_resource_schema(self):
        self._verify_resource_schema(BiobankSampleSchema(), BQBiobankSampleSchema(), bq_prefix='bbs_')

    def test_code_resource_schema(self):
        # This BQ table has an additional field not present in the resource schema
        exclusions = _excluded_bq_fields + ['bq_field_name']
        self._verify_resource_schema(CodeSchema(), BQCodeSchema(), exclusions=exclusions)

    def test_consent_resource_schema(self):
        self._verify_resource_schema(ConsentSchema(), BQConsentSchema())

    def test_ehr_receipt_schema(self):
        self._verify_resource_schema(EHRReceiptSchema(), BQEhrReceiptSchema())

    def test_gender_resource_schema(self):
        self._verify_resource_schema(GenderSchema(), BQGenderSchema())

    def test_hpo_resource_schema(self):
        self._verify_resource_schema(HPOSchema(), BQHPOSchema())

    def test_module_status_resource_schema(self):
        self._verify_resource_schema(ModuleStatusSchema(), BQModuleStatusSchema(), bq_prefix='mod_')

    def test_organization_resource_schema(self):
        self._verify_resource_schema(OrganizationSchema(), BQOrganizationSchema())

    def test_patient_status_resource_schema(self):
        self._verify_resource_schema(PatientStatusSchema(), BQPatientStatusSchema())

    def test_participant_resource_schema(self):
        self._verify_resource_schema(ParticipantSchema(), BQParticipantSummarySchema())

    def test_physical_measurements_resource_schema(self):
        self._verify_resource_schema(PhysicalMeasurementsSchema(), BQPhysicalMeasurements(), bq_prefix='pm_')

    def test_race_resource_schema(self):
        self._verify_resource_schema(RaceSchema(), BQRaceSchema())

    def test_site_resource_schema(self):
        self._verify_resource_schema(SiteSchema(), BQSiteSchema())

    # TODO:  Questionnaire-related schemas

    # Genomic-related schemas
    def test_genomic_set_resource_schema(self):
        self._verify_resource_schema(GenomicSetSchema(), BQGenomicSetSchema())

    def test_genomic_set_member_resource_schema(self):
        self._verify_resource_schema(GenomicSetMemberSchema(), BQGenomicSetMemberSchema())

    def test_genomic_job_run_resource_schema(self):
        self._verify_resource_schema(GenomicJobRunSchema(), BQGenomicJobRunSchema())

    def test_genomic_file_processed_resource_schema(self):
        self._verify_resource_schema(GenomicFileProcessedSchema(), BQGenomicFileProcessedSchema())

    def test_genomic_manifest_file_resource_schema(self):
        self._verify_resource_schema(GenomicManifestFileSchema(), BQGenomicManifestFileSchema())

    def test_genomic_manifest_feedback_resource_schema(self):
        self._verify_resource_schema(GenomicManifestFeedbackSchema(), BQGenomicManifestFeedbackSchema())

    def test_genomic_gc_validation_metrics_resource_schema(self):
        self._verify_resource_schema(GenomicGCValidationMetricsSchema(), BQGenomicGCValidationMetricsSchema())

    # Researcher workbench related schemas
    def test_rwb_researcher_resource_schema(self):
        self._verify_resource_schema(WorkbenchResearcherSchema(), BQRWBResearcherSchema())

    def test_rwb_institutional_affiliations_resource_schema(self):
        self._verify_resource_schema(WorkbenchInstitutionalAffiliationsSchema(),
                                     BQRWBInstitutionalAffiliationsSchema())

    def test_workbench_workspace_resource_schema(self):
        self._verify_resource_schema(WorkbenchWorkspaceSchema(), BQRWBWorkspaceSchema())

    def test_workbench_workspace_user_resource_schema(self):
        self._verify_resource_schema(WorkbenchWorkspaceUsersSchema(), BQRWBWorkspaceUsersSchema())

    def test_workbench_workspace_age_resource_schema(self):
        self._verify_resource_schema(WorkspaceAgeSchema(), BQWorkspaceAgeSchema())

    def test_workbench_workspace_race_resource_schema(self):
        self._verify_resource_schema(WorkspaceRaceEthnicitySchema(), BQWorkspaceRaceEthnicitySchema())
