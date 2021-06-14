
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase
from rdr_service.services.consent.files import ConsentFileAbstractFactory
from rdr_service.services.consent.validation import ConsentValidator

from rdr_service.model.participant_summary import ParticipantSummary
from typing import List
from rdr_service.storage import GoogleCloudStorageProvider
from pprint import pprint
from rdr_service.model.consent_file import ConsentFile, ConsentSyncStatus
from rdr_service.participant_enums import QuestionnaireStatus


tool_cmd = 'deceased-sync'
tool_desc = 'Sync deceased reports from Redcap to an environment'


class DeceasedSyncTool(ToolBase):

    @classmethod
    def get_result_output(cls, result: ConsentFile):
        if result.signing_date and result.expected_sign_date:
            days_off_str = f' of {(result.signing_date - result.expected_sign_date).days} days'
        else:
            days_off_str = ''
        date_error_str = "" if result.is_signing_date_valid else f" (mismatch{days_off_str})"
        actual_date_str = f'actual   {result.signing_date}{date_error_str}'

        return [
            f'{result.sync_status}',
            f'signature: {"<image>" if result.is_signature_image else result.signature_str}',
            f'expected {result.expected_sign_date}',
            actual_date_str,
            f'uploaded {result.file_upload_time}',
            f'other errors: {result.other_errors or ""}'
        ]

    @classmethod
    def process_results(cls, participant_output, results, type_str):
        if not results:
            participant_output[type_str] = ['NEEDS_CORRECTING: MISSING FILE']
        elif not any([result.sync_status == ConsentSyncStatus.READY_FOR_SYNC for result in results]):
            participant_output[type_str] = [
                cls.get_result_output(result) for result in results
            ]

    def run(self):
        super(DeceasedSyncTool, self).run()
        storage_provider = GoogleCloudStorageProvider()

        with self.get_session() as session:
            participant_summaries: List[ParticipantSummary] = session.query(ParticipantSummary).filter(
                ParticipantSummary.participantId.in_([
                    110543516,
                    112548891,
                    129602493,
                    129794684,
                    132997080,
                    136722557,
                    147201741,
                    148340411,
                    158473360,
                    167821829,
                    168503663,
                    170515639,
                    171103390,
                    201540367,
                    215614627,
                    237096329,
                    242043125,
                    248690967,
                    253515600,
                    262457267,
                    269356887,
                    304620328,
                    326506256,
                    345996416,
                    360566479,
                    394938227,
                    407214542,
                    407812610,
                    422060778,
                    437521908,
                    447597574,
                    449490835,
                    463386363,
                    469544277,
                    480825721,
                    496803347,
                    507576221,
                    507621932,
                    514893319,
                    536311598,
                    541324085,
                    553346633,
                    559184522,
                    560112589,
                    561348603,
                    586697990,
                    602129096,
                    606839622,
                    633396221,
                    634997628,
                    648211058,
                    685493588,
                    691444867,
                    700874324,
                    701833805,
                    717720800,
                    724626862,
                    727052263,
                    735658346,
                    773465501,
                    781213440,
                    806816627,
                    807735649,
                    868313817,
                    875343298,
                    916329064,
                    944420567,
                    103197149,
                    130402095,
                    155341359,
                    156986712,
                    170738226,
                    201190684,
                    215002145,
                    220439737,
                    230718221,
                    238608641,
                    249344865,
                    291034683,
                    295358758,
                    295946478,
                    299438738,
                    317401290,
                    332068734,
                    348895183,
                    352940090,
                    393122191,
                    406612299,
                    412481507,
                    433315578,
                    439578622,
                    441400178,
                    448743569,
                    483972455,
                    500867131,
                    515922901,
                    522274822,
                    526965519,
                    535505334,
                    543356293,
                    551823760,
                    555137030,
                    576136378,
                    623130008,
                    629499218,
                    653817286,
                    655284712,
                    658880485,
                    667250796,
                    680601956,
                    695301427,
                    700647219,
                    703628639,
                    711501509,
                    714612599,
                    738812728,
                    754723708,
                    769745687,
                    772437331,
                    795343095,
                    847913116,
                    848954415,
                    863166236,
                    888368160,
                    891547350,
                    916770642,
                    919989697,
                    928027926,
                    962516560,
                    978025274,
                ])
            ).order_by(ParticipantSummary.participantId).all()

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
                self.process_results(
                    participant_output,
                    results=validator.get_primary_validation_results(),
                    type_str='Primary'
                )
            if summary.consentForCABoR == QuestionnaireStatus.SUBMITTED:
                self.process_results(
                    participant_output,
                    results=validator.get_cabor_validation_results(),
                    type_str='Cabor  '
                )
            if summary.consentForElectronicHealthRecords == QuestionnaireStatus.SUBMITTED:
                self.process_results(
                    participant_output,
                    results=validator.get_ehr_validation_results(),
                    type_str='EHR    '
                )
            if summary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED:
                self.process_results(
                    participant_output,
                    results=validator.get_gror_validation_results(),
                    type_str='GROR   '
                )

            if participant_output:
                validation_data[f'P{summary.participantId}'] = participant_output

        pprint(validation_data)


def run():
    return cli_run(tool_cmd, tool_desc, DeceasedSyncTool)
