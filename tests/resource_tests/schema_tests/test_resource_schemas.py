#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import json

from tests.helpers.unittest_base import BaseTestCase

# -- BQ model imports
from rdr_service.model import (
    bq_code, bq_genomics, bq_hpo, bq_organization, bq_participant_summary, bq_pdr_participant_summary,
    # bq_questionnaires  <-- to do:  add tests for schemas in these files?
    bq_site, bq_workbench_workspace, bq_workbench_researcher
)

from rdr_service.resource import schemas as rschemas

_excluded_bq_fields = ['id', 'created', 'modified', 'orig_id', 'orig_created', 'orig_modified']


class ResourceSchemaTest(BaseTestCase):
    """
    Test that the resource schema definitions/fields align with the BigQuery schemas
    NOTE:  These tests may be deprecated if use of BigQuery PDR is discontinued
    TODO:  Add more detail about implementing test cases that include handling field name prefixes, exclusions, etc.
    """
    def setup(self):
        super().setup()

    def _verify_resource_schema(self, rsc_name, rsc_schema_obj,
                                bq_schema_obj, bq_prefix='', exclusions=_excluded_bq_fields):

        bq_field_list = sorted(self._get_bq_field_list(bq_schema_obj, bq_prefix=bq_prefix, exclusions=exclusions))
        rsc_field_list = sorted(rsc_schema_obj.fields.keys())
        self.assertListEqual(bq_field_list, rsc_field_list, "\n{0}".format(rsc_name))


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
        self._verify_resource_schema('AddressSchema',
                                     rschemas.participant.AddressSchema(),
                                     bq_participant_summary.BQAddressSchema())

    def test_biobank_order_resource_schema(self):
        self._verify_resource_schema('BiobankOrderSchema',
                                     rschemas.participant.BiobankOrderSchema(),
                                     bq_participant_summary.BQBiobankOrderSchema(), bq_prefix='bbo_')

    def test_biobank_sample_resource_schema(self):
        self._verify_resource_schema('BiobankSampleSchema',
                                     rschemas.participant.BiobankSampleSchema(),
                                     bq_participant_summary.BQBiobankSampleSchema(), bq_prefix='bbs_')

    def test_code_resource_schema(self):
        # This BQ table has an additional field not present in the resource schema
        exclusions = _excluded_bq_fields + ['bq_field_name']
        self._verify_resource_schema('CodeSchema',
                                     rschemas.CodeSchema(),
                                     bq_code.BQCodeSchema(), exclusions=exclusions)

    def test_consent_resource_schema(self):
        self._verify_resource_schema('ConsentSchema',
                                     rschemas.participant.ConsentSchema(),
                                     bq_participant_summary.BQConsentSchema())

    def test_ehr_receipt_schema(self):
        self._verify_resource_schema('EHRReceiptSchema',
                                     rschemas.participant.EHRReceiptSchema(),
                                     bq_participant_summary.BQEhrReceiptSchema())

    def test_gender_resource_schema(self):
        self._verify_resource_schema('GenderSchema',
                                     rschemas.participant.GenderSchema(),
                                     bq_participant_summary.BQGenderSchema())

    def test_hpo_resource_schema(self):
        self._verify_resource_schema('HPOSchema',
                                     rschemas.HPOSchema(),
                                     bq_hpo.BQHPOSchema())

    def test_module_status_resource_schema(self):
        self._verify_resource_schema('ModuleStatusSchema',
                                     rschemas.participant.ModuleStatusSchema(),
                                     bq_participant_summary.BQModuleStatusSchema(), bq_prefix='mod_')

    def test_organization_resource_schema(self):
        self._verify_resource_schema('OrganizationSchema',
                                     rschemas.OrganizationSchema(),
                                     bq_organization.BQOrganizationSchema())

    def test_patient_status_resource_schema(self):
        self._verify_resource_schema('PatientStatusSchema',
                                     rschemas.participant.PatientStatusSchema(),
                                     bq_participant_summary.BQPatientStatusSchema())

    def test_participant_resource_schema(self):
        self._verify_resource_schema('Participant',
                                     rschemas.ParticipantSchema(),
                                     bq_participant_summary.BQParticipantSummarySchema())
        self._verify_resource_schema('PDRParticipant',
                                     rschemas.PDRParticipantSchema(),
                                     bq_pdr_participant_summary.BQPDRParticipantSummarySchema())

    def test_physical_measurements_resource_schema(self):
        self._verify_resource_schema('PhysicalMeasurementsSchema',
                                     rschemas.participant.PhysicalMeasurementsSchema(),
                                     bq_participant_summary.BQPhysicalMeasurements(), bq_prefix='pm_')

    def test_race_resource_schema(self):
        self._verify_resource_schema('RaceSchema',
                                     rschemas.participant.RaceSchema(),
                                     bq_participant_summary.BQRaceSchema())

    def test_site_resource_schema(self):
        self._verify_resource_schema('SiteSchema',
                                     rschemas.SiteSchema(),
                                     bq_site.BQSiteSchema())

    # TODO:  Questionnaire-related schemas

    # Genomic-related schemas
    def test_genomic_set_resource_schema(self):
        self._verify_resource_schema('GenomicSetSchema',
                                     rschemas.GenomicSetSchema(),
                                     bq_genomics.BQGenomicSetSchema())

    def test_genomic_set_member_resource_schema(self):
        self._verify_resource_schema('GenomicSetMemberSchema',
                                     rschemas.GenomicSetMemberSchema(),
                                     bq_genomics.BQGenomicSetMemberSchema())

    def test_genomic_job_run_resource_schema(self):
        self._verify_resource_schema('GenomicJobRunSchema',
                                     rschemas.GenomicJobRunSchema(),
                                     bq_genomics.BQGenomicJobRunSchema())

    def test_genomic_file_processed_resource_schema(self):
        self._verify_resource_schema('GenomicFileProcessedSchema',
                                     rschemas.GenomicFileProcessedSchema(),
                                     bq_genomics.BQGenomicFileProcessedSchema())

    def test_genomic_manifest_file_resource_schema(self):
        self._verify_resource_schema('GenomicManifestFileSchema',
                                     rschemas.GenomicManifestFileSchema(),
                                     bq_genomics.BQGenomicManifestFileSchema())

    def test_genomic_manifest_feedback_resource_schema(self):
        self._verify_resource_schema('GenomicManifestFeedbackSchema',
                                     rschemas.GenomicManifestFeedbackSchema(),
                                     bq_genomics.BQGenomicManifestFeedbackSchema())

    def test_genomic_gc_validation_metrics_resource_schema(self):
        self._verify_resource_schema('GenomicGCValidationMetricsSchema',
                                     rschemas.GenomicGCValidationMetricsSchema(),
                                     bq_genomics.BQGenomicGCValidationMetricsSchema())

    # Researcher workbench related schemas
    def test_rwb_researcher_resource_schema(self):
        self._verify_resource_schema('WorkbenchResearcherSchema',
                                     rschemas.workbench_researcher.WorkbenchResearcherSchema(),
                                     bq_workbench_researcher.BQRWBResearcherSchema())

    def test_rwb_institutional_affiliations_resource_schema(self):
        self._verify_resource_schema('WorkbenchInstitutionalAffiliationsSchema',
                                     rschemas.workbench_researcher.WorkbenchInstitutionalAffiliationsSchema(),
                                     bq_workbench_researcher.BQRWBInstitutionalAffiliationsSchema())

    def test_workbench_workspace_resource_schema(self):
        self._verify_resource_schema('WorkbenchWorkspaceSchema',
                                     rschemas.workbench_workspace.WorkbenchWorkspaceSchema(),
                                     bq_workbench_workspace.BQRWBWorkspaceSchema())

    def test_workbench_workspace_user_resource_schema(self):
        self._verify_resource_schema('WorkbenchWorkspaceUserSchema',
                                     rschemas.workbench_workspace.WorkbenchWorkspaceUsersSchema(),
                                     bq_workbench_workspace.BQRWBWorkspaceUsersSchema())

    def test_workbench_workspace_age_resource_schema(self):
        self._verify_resource_schema('WorkspaceAgeSchema',
                                     rschemas.workbench_workspace.WorkspaceAgeSchema(),
                                     bq_workbench_researcher.BQWorkspaceAgeSchema())

    def test_workbench_workspace_race_resource_schema(self):
        self._verify_resource_schema('WorkspaceRaceEthnicitySchema',
                                     rschemas.workbench_workspace.WorkspaceRaceEthnicitySchema(),
                                     bq_workbench_researcher.BQWorkspaceRaceEthnicitySchema())
