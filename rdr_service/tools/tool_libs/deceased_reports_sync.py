
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase
from rdr_service.services.consent.files import ConsentFileAbstractFactory
from rdr_service.services.consent.validation import ConsentValidator

from rdr_service.model.participant_summary import ParticipantSummary
from typing import List
from rdr_service.storage import GoogleCloudStorageProvider
from pprint import pprint
from rdr_service.model.consent_file import ConsentFile
from rdr_service.participant_enums import QuestionnaireStatus


tool_cmd = 'deceased-sync'
tool_desc = 'Sync deceased reports from Redcap to an environment'


class DeceasedSyncTool(ToolBase):

    @classmethod
    def get_result_output(cls, result: ConsentFile):
        return [
            f'{result.sync_status}',
            f'signature: {"<image>" if result.is_signature_image else result.signature_str}',
            f'expected {result.expected_sign_date}',
            f'actual   {result.signing_date}{"" if result.is_signing_date_valid else " (mismatch)"}',
            f'uploaded {result.file_upload_time}',
            f'other errors: {result.other_errors or ""}'
        ]

    def run(self):
        super(DeceasedSyncTool, self).run()
        storage_provider = GoogleCloudStorageProvider()

        with self.get_session() as session:
            participant_summaries: List[ParticipantSummary] = session.query(ParticipantSummary).filter(
                ParticipantSummary.hpoId == 15,
                ParticipantSummary.organizationId != 97,
                ParticipantSummary.consentForElectronicHealthRecords == 1,
                ParticipantSummary.consentForElectronicHealthRecordsAuthored > '2021-01-01',
                ParticipantSummary.consentForElectronicHealthRecordsAuthored < '2021-05-02'
            ).order_by(ParticipantSummary.participantId).limit(10).all()

        validation_data = {}

        for summary in participant_summaries:
            print(f'checking {summary.participantId}')
            factory = ConsentFileAbstractFactory.get_file_factory(
                participant_id=summary.participantId,
                participant_origin=summary.participantOrigin,
                storage_provider=storage_provider
            )
            validator = ConsentValidator(
                consent_factory=factory,
                participant_summary=summary,
                va_hpo_id=15
            )

            participant_output = {}

            if summary.consentForStudyEnrollment == QuestionnaireStatus.SUBMITTED:
                participant_output['Primary'] = [
                    self.get_result_output(result) for result in validator.get_primary_validation_results()
                ]
            if summary.consentForCABoR == QuestionnaireStatus.SUBMITTED:
                participant_output['Cabor  '] = [
                    self.get_result_output(result) for result in validator.get_cabor_validation_results()
                ]
            if summary.consentForElectronicHealthRecords == QuestionnaireStatus.SUBMITTED:
                participant_output['EHR    '] = [
                    self.get_result_output(result) for result in validator.get_ehr_validation_results()
                ]
            if summary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED:
                participant_output['GROR   '] = [
                    self.get_result_output(result) for result in validator.get_gror_validation_results()
                ]

            validation_data[summary.participantId] = participant_output

        pprint(validation_data)


def run():
    return cli_run(tool_cmd, tool_desc, DeceasedSyncTool)
