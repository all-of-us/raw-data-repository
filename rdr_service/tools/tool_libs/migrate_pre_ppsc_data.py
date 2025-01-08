from collections import defaultdict
import csv
from datetime import datetime
from enum import auto, Enum
from typing import List

from sqlalchemy.orm import joinedload

from rdr_service import code_constants, participant_enums
from rdr_service.api.ppsc_intake_api import PPSCIntakeAPI
from rdr_service.domain_model.response import Response
from rdr_service.model.code import Code
from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.model.organization import Organization
from rdr_service.model.participant import ParticipantHistory
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.repository.questionnaire_response_repository import QuestionnaireResponseRepository
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase


tool_cmd = 'migrate-legacy'
tool_desc = 'Migrate pre-PPSC data into PPSC schema'


class ActivityType(Enum):
    Enrollment = auto()
    Consent = auto()
    SurveyCompletion = auto()
    ProfileUpdates = auto()
    Withdrawal = auto()
    Deactivation = auto
    ParticipantStatus = auto()
    Attribution = auto()
    NphOptIn = auto()


class SurveyEventType(Enum):
    BasicsData = 'Basics Data'
    Behavioral = 'Behavioral Health & Personality'
    Emotional = 'Emotional Health History and Well-Being'
    HealthAccess = 'Health Care Access & Utilization'
    Lifestyle = 'Lifestyle'
    LifeFunctioning = 'Life Functioning Survey'
    OverallHealth = 'Overall Health'
    HealthHistory = 'Personal and Family Health History'
    Social = 'Social Determinants of Health'
    Basics = 'The Basics'


class MigrateLegacyData(ToolBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.intake_api = None
        self.session = None

    def run(self):
        super(MigrateLegacyData, self).run()

        with self.get_session() as session:  # need to start a session before anything else creates one
            self.session = session
            self.intake_api = PPSCIntakeAPI()

            self.migrate_misc_summary_data()
            self.migrate_survey_completions()
            self.migrate_consents()
            self.migrate_basics_data()
            self.migrate_withdrawal_answer()
            self.migrate_ubr_data()

        return 0

    def migrate_consents(self):
        print("\n\n\nmigrating consent data")

        batch_size = 200
        last_participant_id = 0
        summary_list = self.get_next_summary_set(last_participant_id, batch_size)

        skip_code_id = self.session.query(Code.codeId).filter(Code.value == 'PMI_SKIP').scalar()
        code_map = self._build_state_code_map()

        while summary_list:
            print(f'starting batch {summary_list[0].participantId} to {summary_list[-1].participantId}...')

            pid_list = [summary.participantId for summary in summary_list]
            receive_care_map = self._build_receive_care_map(pid_list)

            for summary in summary_list:
                self.migrate_primary_consent(summary, code_map, skip_code_id, receive_care_map)
                if (
                    summary.consentForElectronicHealthRecords
                    and summary.consentForElectronicHealthRecords != participant_enums.QuestionnaireStatus.UNSET
                ):
                    self.migrate_ehr_consent(summary)

            print('... done')
            last_participant_id = summary_list[-1].participantId
            summary_list = self.get_next_summary_set(last_participant_id, batch_size)

    def migrate_misc_summary_data(self):
        print("migrating summary data")

        batch_size = 1000
        last_participant_id = 0
        summary_list = self.get_next_summary_set(last_participant_id, batch_size)

        code_map = self._build_state_code_map()
        org_map = self._build_org_map()

        while summary_list:
            print(f'starting batch {summary_list[0].participantId} to {summary_list[-1].participantId}...')

            pid_list = [summary.participantId for summary in summary_list]
            participant_history_map = self._build_history_map(pid_list)
            deceased_report_map = self._build_deceased_report_map(pid_list)

            for summary in summary_list:
                self.process_profile_data(summary, code_map)
                self.process_nph_optin(summary)
                self.process_withdrawal(summary)
                self.process_deactivation(summary)
                self.process_enrollment_status(summary)
                self.process_test_flag(summary, participant_history_map[summary.participantId])
                self.process_deceased_status(summary, deceased_report_map)
                self.process_attribution(summary, participant_history_map[summary.participantId], org_map)

            print('... done')
            last_participant_id = summary_list[-1].participantId
            summary_list = self.get_next_summary_set(last_participant_id, batch_size)

    def process_test_flag(
        self, summary: ParticipantSummary, history_list: List[ParticipantHistory]
    ):
        if not summary.participant.isTestParticipant:
            return

        first_test_history = None
        for history in history_list:
            if history.isTestParticipant:
                first_test_history = history
                break

        # not all test participants seem to have a history record that they've been marked as test
        modified_test_time = first_test_history.lastModified if first_test_history else summary.lastModified
        self.intake_api.handle_event_insert(req_data={
            'activity': 'Participant Status',
            'eventType': 'Test Account',
            'dataElements': [
                {
                    'dataElementName': 'activity_status',
                    'dataElementValue': 'test'
                },
                {
                    'dataElementName': 'activity_date_time',
                    'dataElementValue': modified_test_time.isoformat()
                }
            ],
            'participantId': f'P{summary.participantId}'
        })

    def process_attribution(
        self, summary: ParticipantSummary, history_list: List[ParticipantHistory], org_map
    ):
        if not summary.organizationId:
            return

        first_history = None
        for history in history_list:
            if history.organizationId == summary.organizationId:
                first_history = history
                break

        self.intake_api.handle_event_insert(req_data={
            'activity': 'Attribution',
            'eventType': 'Org Attribution',
            'dataElements': [
                {
                    'dataElementName': 'activity_status',
                    'dataElementValue': org_map[summary.organizationId]
                },
                {
                    'dataElementName': 'activity_date_time',
                    'dataElementValue': first_history.lastModified.isoformat()
                }
            ],
            'participantId': f'P{summary.participantId}'
        })

    def process_deceased_status(self, summary: ParticipantSummary, deceased_report_map):
        if summary.deceasedStatus == participant_enums.DeceasedStatus.UNSET:
            return

        deceased_report: DeceasedReport = deceased_report_map[summary.participantId].get(summary.deceasedStatus.number)
        match summary.deceasedStatus:
            case participant_enums.DeceasedStatus.PENDING:
                status_str = 'pending'
            case participant_enums.DeceasedStatus.APPROVED:
                status_str = 'deceased'
        if deceased_report:
            match deceased_report.notification:
                case participant_enums.DeceasedNotification.EHR:
                    notification_str = "Electronic Medical Record (EHR)"
                case participant_enums.DeceasedNotification.ATTEMPTED_CONTACT:
                    notification_str = "Attempted to contact participant"
                case participant_enums.DeceasedNotification.NEXT_KIN_HPO:
                    notification_str = "Next of kin contacted HPO"
                case participant_enums.DeceasedNotification.NEXT_KIN_SUPPORT:
                    notification_str = "Next of kin contacted Support Center"
                case participant_enums.DeceasedNotification.OTHER:
                    notification_str = "Other"

        data_elements = [
            {
                'dataElementName': 'activity_status',
                'dataElementValue': status_str
            },
            {
                'dataElementName': 'activity_date_time',
                'dataElementValue': summary.deceasedAuthored.isoformat()
            }
        ]
        if deceased_report:
            data_elements.extend([
                {
                    'dataElementName': 'notification_mechanism',
                    'dataElementValue': notification_str
                },
                {
                    'dataElementName': 'cause_of_death',
                    'dataElementValue': deceased_report.causeOfDeath
                }
            ])
            if deceased_report.dateOfDeath:
                data_elements.append({
                    'dataElementName': 'deceased_datetime',
                    'dataElementValue': deceased_report.dateOfDeath.isoformat()
                })
        self.intake_api.handle_event_insert(req_data={
            'activity': 'Participant Status',
            'eventType': 'Death',
            'dataElements': data_elements,
            'participantId': f'P{summary.participantId}'
        })


    def process_pediatric_flag(self, summary: ParticipantSummary):
        self.intake_api.handle_event_insert(req_data={
            'activity': 'Profile Updates',
            'eventType': 'Account Type',
            'dataElements': [
                {
                    'dataElementName': 'activity_status',
                    'dataElementValue': 'pediatric' if summary.isPediatric else 'adult'
                },
                {
                    'dataElementName': 'activity_date_time',
                    'dataElementValue': summary.signUpTime
                }
            ],
            'participantId': f'P{summary.participantId}'
        })

    def process_nph_optin(self, summary: ParticipantSummary):
        if not summary.consentForNphModule1:
            return

        self.intake_api.handle_event_insert(req_data={
            'activity': 'NPH Opt In',
            'eventType': 'NPH Opt In',
            'dataElements': [
                {
                    'dataElementName': 'activity_status',
                    'dataElementValue': 'submitted_yes'
                },
                {
                    'dataElementName': 'activity_date_time',
                    'dataElementValue': summary.consentForNphModule1Authored.isoformat()
                }
            ],
            'participantId': f'P{summary.participantId}'
        })

    def process_withdrawal(self, summary: ParticipantSummary):
        if summary.withdrawalStatus == participant_enums.WithdrawalStatus.NOT_WITHDRAWN:
            return

        withdrawal_time = summary.withdrawalAuthored or summary.withdrawalTime
        data = {
            'activity': 'Withdrawal',
            'eventType': 'Withdrawal',
            'dataElements': [
                {
                    'dataElementName': 'activity_status',
                    'dataElementValue': 'withdrawn'
                },
                {
                    'dataElementName': 'activity_date_time',
                    'dataElementValue': withdrawal_time.isoformat()
                }
            ],
            'participantId': f'P{summary.participantId}'
        }
        if summary.withdrawalReason:
            match summary.withdrawalReason:
                case participant_enums.WithdrawalReason.FRAUDULENT:
                    reason = 'Fraudulent Account'
                case participant_enums.WithdrawalReason.DUPLICATE:
                    reason = 'Duplicate Account'
                case participant_enums.WithdrawalReason.TEST:
                    reason = 'Other'
            data['dataElements'].append({
                'dataElementName': 'withdrawal_reason',
                'dataElementValue': reason
            })

        self.intake_api.handle_event_insert(req_data=data)

    def process_deactivation(self, summary: ParticipantSummary):
        if summary.suspensionStatus == participant_enums.SuspensionStatus.NOT_SUSPENDED:
            return

        self.intake_api.handle_event_insert(req_data={
            'activity': 'Deactivation',
            'eventType': 'Deactivation',
            'dataElements': [
                {
                    'dataElementName': 'activity_status',
                    'dataElementValue': 'deactivated'
                },
                {
                    'dataElementName': 'activity_date_time',
                    'dataElementValue': summary.suspensionTime.isoformat()
                }
            ],
            'participantId': f'P{summary.participantId}'
        })

    def process_enrollment_status(self, summary: ParticipantSummary):
        data = {
            'activity': 'Participant Status',
            'eventType': 'Enrollment Status',
            'dataElements': [
                {
                    'dataElementName': 'registered',
                    'dataElementValue': summary.signUpTime.isoformat()
                }
            ],
            'participantId': f'P{summary.participantId}'
        }
        status_map = {
            'participant': summary.enrollmentStatusParticipantV3_2Time,
            'participant_ehr_consent': summary.enrollmentStatusParticipantPlusEhrV3_2Time,
            'enrolled': summary.enrollmentStatusEnrolledParticipantV3_2Time,
            'pmb_eligible': summary.enrollmentStatusPmbEligibleV3_2Time,
            'core_minus_pm': summary.enrollmentStatusCoreMinusPmV3_2Time,
            'core_participant': summary.enrollmentStatusCoreV3_2Time
        }
        for element_name, value in status_map.items():
            if value:
                data['dataElements'].append({
                    'dataElementName': element_name,
                    'dataElementValue': value.isoformat()
                })

        self.intake_api.handle_event_insert(req_data=data)

    def process_retention(self, summary: ParticipantSummary):
        if (
            not summary.retentionEligibleStatus
            or not summary.retentionType
            or summary.retentionType == participant_enums.RetentionType.UNSET
        ):
            return

        match summary.retentionEligibleStatus:
            case participant_enums.RetentionStatus.NOT_ELIGIBLE:
                status = 'not_eligible'
            case participant_enums.RetentionStatus.ELIGIBLE:
                status = 'eligible'
        match summary.retentionType:
            case participant_enums.RetentionType.ACTIVE_AND_PASSIVE:
                retention_type = 'Active and Passive'
            case participant_enums.RetentionType.ACTIVE:
                retention_type = 'Active'
            case participant_enums.RetentionType.PASSIVE:
                retention_type = 'Passive'

        data = {
            'activity': 'Participant Status',
            'eventType': 'Retention Status',
            'dataElements': [
                {
                    'dataElementName': 'retention_eligible_status',
                    'dataElementValue': status
                },
                {
                    'dataElementName': 'retention_type',
                    'dataElementValue': retention_type
                }
            ],
            'participantId': f'P{summary.participantId}'
        }

        if summary.retentionEligibleTime:
            data['dataElements'].append({
                'dataElementName': 'retention_eligible_time',
                'dataElementValue': summary.retentionEligibleTime.isoformat()
            })

        self.intake_api.handle_event_insert(req_data=data)

    def process_profile_data(self, summary: ParticipantSummary, state_code_map):
        data = {
            'activity': 'Profile Updates',
            'eventType': 'Profile Data',
            'dataElements': [
                {
                    'dataElementName': 'activity_date_time',
                    'dataElementValue': datetime(2024, 12, 3).isoformat()
                }
            ],
            'participantId': f'P{summary.participantId}'
        }
        data_elements = {
            'piiname_first': summary.firstName,
            'piiname_middle': summary.middleName,
            'piiname_last': summary.lastName,
            'piiaddress_streetaddress': summary.streetAddress,
            'piiaddress_streetaddress2': summary.streetAddress2,
            'streetaddress_piicity': summary.city,
            'streetaddress_piistate': state_code_map[summary.stateId] if summary.stateId else None,
            'streetaddress_piizip': summary.zipCode,
            'piicontactinformation_phone': summary.phoneNumber,
            'piicontactinformation_email': summary.email,
            'piibirthinformation_birthdate': summary.dateOfBirth.isoformat() if summary.dateOfBirth else None,
            'language_preference': summary.primaryLanguage
        }
        for name, value in data_elements.items():
            if value:
                data['dataElements'].append({
                    'dataElementName': name,
                    'dataElementValue': value
                })

        self.intake_api.handle_event_insert(req_data=data)

    def _build_history_map(self, id_list):
        history_list: List[ParticipantHistory] = self.session.query(ParticipantHistory).filter(
            ParticipantHistory.participantId.in_(id_list)
        ).order_by(ParticipantHistory.lastModified).all()

        result = defaultdict(list)
        for history in history_list:
            result[history.participantId].append(history)

        return result

    def _build_receive_care_map(self, id_list):
        result = {}
        all_responses = QuestionnaireResponseRepository.get_responses_to_surveys(
            session=self.session,
            survey_codes=[
                code_constants.CONSENT_FOR_STUDY_ENROLLMENT_MODULE,
                code_constants.PEDIATRIC_PRIMARY_CONSENT_MODULE
            ],
            participant_ids=id_list
        )

        for pid in id_list:
            responses = all_responses[pid]
            for consent_response in reversed(responses.in_authored_order):
                answer = (
                    consent_response.get_answers_for(code_constants.RECEIVE_CARE_STATE)
                    or consent_response.get_answers_for(code_constants.PEDIATRIC_RECEIVE_CARE_STATE)
                )
                if answer:
                    result[pid] = answer[0].value
                    break

        return result

    def _build_deceased_report_map(self, id_list):
        report_list: List[DeceasedReport] = self.session.query(DeceasedReport).filter(
            DeceasedReport.participantId.in_(id_list)
        ).all()

        result = defaultdict(dict)
        for report in report_list:
            result[report.participantId][report.status.number] = report

        return result

    def _build_org_map(self):
        org_list: List[Organization] = self.session.query(
            Organization.organizationId,
            Organization.externalId
        ).all()
        return {
            org.organizationId: org.externalId
            for org in org_list
        }

    def migrate_ubr_data(self):
        print("\n\n\nmigratign ubr data")
        with open('ubr_data_file.csv') as file:
            reader = csv.DictReader(file)

            for record in reader:
                participant_id = record['participant_id']
                data = {
                    'activity': 'Participant Status',
                    'eventType': 'UBR Status',
                    'dataElements': [],
                    'participantId': f'P{participant_id}'
                }

                for field_name in [
                    'ubr_age_at_consent', 'ubr_geography', 'ubr_income', 'ubr_sex', 'ubr_ethnicity',
                    'ubr_education', 'ubr_sexual_orientation', 'ubr_gender_identity',
                    'ubr_sexual_gender_minority', 'ubr_disability', 'ubr_hau'
                ]:
                    ubr_value = record[field_name].lower()
                    if ubr_value == 'notanswer_skip':
                        ubr_value = 'unknown'
                    if ubr_value:
                        data['dataElements'].append({
                            'dataElementName': field_name,
                            'dataElementValue': ubr_value
                        })

                self.intake_api.handle_event_insert(req_data=data)

    def migrate_basics_data(self):
        print("\n\n\nmigrating basics data")

        batch_size = 200
        last_participant_id = 0
        summary_list = self.get_next_summary_set(last_participant_id, batch_size)

        while summary_list:
            print(f'starting batch {summary_list[0].participantId} to {summary_list[-1].participantId}...')

            id_list = [summary.participantId for summary in summary_list]
            basics_responses = QuestionnaireResponseRepository.get_responses_to_surveys(
                session=self.session,
                survey_codes=['TheBasics'],
                participant_ids=id_list
            )

            for summary in summary_list:
                responses = basics_responses.get(summary.participantId)
                if not responses:
                    continue

                self.process_latest_basics_response(
                    participant_id=summary.participantId,
                    response=responses.in_authored_order[-1]
                )

            print('... done')
            last_participant_id = summary_list[-1].participantId
            summary_list = self.get_next_summary_set(last_participant_id, batch_size)

    def migrate_withdrawal_answer(self):
        print("\n\n\nmigrating withdrawal data")

        batch_size = 200
        last_participant_id = 0
        summary_list = self.get_next_summary_set(last_participant_id, batch_size)

        while summary_list:
            print(f'starting batch {summary_list[0].participantId} to {summary_list[-1].participantId}...')
            id_list = [summary.participantId for summary in summary_list]
            withdrawal_responses = QuestionnaireResponseRepository.get_responses_to_surveys(
                session=self.session,
                survey_codes=['StopParticipating', 'withdrawal_intro'],
                participant_ids=id_list
            )

            for summary in summary_list:
                responses = withdrawal_responses.get(summary.participantId)
                if not responses:
                    continue

                self.process_withdrawal_response(
                    participant_id=summary.participantId,
                    response=responses.in_authored_order[-1]
                )

            print('... done')
            last_participant_id = summary_list[-1].participantId
            summary_list = self.get_next_summary_set(last_participant_id, batch_size)

    def get_next_summary_set(self, last_id, batch_size) -> List[ParticipantSummary]:
        return self.session.query(
            ParticipantSummary
        ).filter(
            ParticipantSummary.participantId > last_id
        ).order_by(
            ParticipantSummary.participantId
        ).filter(
            ParticipantSummary.signUpTime < '2024-12-3',
        ).options(
            joinedload(ParticipantSummary.participant),
            joinedload(ParticipantSummary.pediatricData)
        ).limit(batch_size).all()
        # todo: get legacy participants from rdr schema (by signup date?)
        #   todo: make sure to load pediatric data and make sure isPediatric is populated.
        # todo: join load the participant

    def process_withdrawal_response(self, participant_id, response: Response):
        answer = response.get_single_answer_for('withdrawalaianceremony')
        if not answer:
            answer = response.get_single_answer_for('peds_withdrawalaianceremony')

        if not answer:
            return

        answer_str = None
        match answer.value:
            case 'withdrawalaianceremony_yes':
                answer_str = 'yes'
            case 'withdrawalaianceremony_no':
                answer_str = 'no'

        if not answer_str:
            return

        self.intake_api.handle_event_insert(req_data={
            'activity': 'Withdrawal',
            'eventType': 'Withdrawal',
            'dataElements': [
                {
                    'dataElementName': 'aian_ceremony_status',
                    'dataElementValue': answer_str
                }
            ],
            'participantId': f'P{participant_id}'
        })

    def process_latest_basics_response(self, participant_id, response: Response):
        data = {
            'activity': 'Survey Completion',
            'eventType': 'Basics Data',
            'dataElements': [
                {
                    'dataElementName': 'activity_date_time',
                    'dataElementValue': response.authored_datetime.isoformat()
                }
            ],
            'participantId': f'P{participant_id}'
        }

        for question_code in [
            'thebasics_birthplace', 'race_whatraceethnicity', 'whatraceethnicity_raceethnicitynoneofthese',
            'aian_aianspecific', 'asian_asianspecific', 'black_blackspecific', 'hispanic_hispanicspecific',
            'mena_menaspecific', 'nhpi_nhpispecific', 'white_whitespecific', 'gender_genderidentity',
            'gender_closergenderdescription', 'biologicalsexatbirth_sexatbirth', 'thebasics_sexualorientation',
            'genderidentity_sexualitycloserdescription', 'educationlevel_highestgrade',
            'maritalstatus_currentmaritalstatus', 'livingsituation_howmanypeople', 'livingsituation_peopleunder18',
            'disability_deaf', 'disability_blind', 'disability_difficultyconcentrating', 'disability_walkingclimbing',
            'disability_dressingbathing', 'disability_errandsalone', 'employment_employmentstatus',
            'employmentworkaddress_state', 'employmentworkaddress_zipcode', 'income_annualincome',
            'homeown_currenthomeown', 'livingsituation_currentliving', 'livingsituation_howmanylivingyears',
            'livingsituation_stablehouseconcern'
        ]:
            answer_list = response.get_answers_for(question_code)
            if not answer_list:
                continue

            if len(answer_list) == 1:
                new_data_value = answer_list[0].value
            else:
                new_data_value = [answer.value for answer in answer_list]

            data['dataElements'].append({
                'dataElementName': question_code,
                'dataElementValue': new_data_value
            })

        self.intake_api.handle_event_insert(req_data=data)

    def migrate_primary_consent(self, summary: ParticipantSummary, code_map, skip_code_id, receive_care_map):
        consent_data = {
            'activity': "Consent",
            'eventType': "Primary Consent",
            'dataElements': [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "submitted_yes"
                }, {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": summary.consentForStudyEnrollmentFirstYesAuthored.isoformat()
                }
            ],
            'participantId': f'P{summary.participantId}'
        }

        if summary.stateId and summary.stateId != skip_code_id:
            consent_data['dataElements'].append({
                'dataElementName': 'state_of_residence',
                'dataElementValue': code_map[summary.stateId]
            })

        care_state_str = receive_care_map.get(summary.participantId)
        if care_state_str:
            consent_data['dataElements'].append({
                'dataElementName': 'receivecare_piistate',
                'dataElementValue': f'PC_STATE_LIVEHEALTH_{care_state_str[-2:]}'
            })

        self.intake_api.handle_event_insert(req_data=consent_data)

    def migrate_ehr_consent(self, summary: ParticipantSummary):
        if summary.consentForElectronicHealthRecords in [
            participant_enums.QuestionnaireStatus.SUBMITTED,
            participant_enums.QuestionnaireStatus.SUBMITTED_NOT_VALIDATED
        ]:
            consent_response = 'submitted_yes'
        else:
            consent_response = 'submitted_no'

        self.intake_api.handle_event_insert(req_data={
            'activity': "Consent",
            'eventType': "EHR Authorization",
            'dataElements': [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": consent_response
                }, {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": summary.consentForElectronicHealthRecordsAuthored.isoformat()
                }
            ],
            'participantId': f'P{summary.participantId}'
        })

    def _build_state_code_map(self):
        state_code_list = self.session.query(
            Code.codeId,
            Code.value
        ).join(
            ParticipantSummary, ParticipantSummary.stateId == Code.codeId
        ).distinct().all()
        return {
            code.codeId: f'PC_STATE_LIVEHEALTH_{code.value[-2:]}'
            for code in state_code_list
        }

    def migrate_survey_completions(self):
        print("\n\n\nmigrating survey data")

        batch_size = 200
        last_participant_id = 0
        summary_list = self.get_next_summary_set(last_participant_id, batch_size)

        survey_map = {
            'BehavioralHealthAndPersonality': SurveyEventType.Behavioral,
            'EmotionalHealthHistoryAndWellBeing': SurveyEventType.Emotional,
            'HealthcareAccess': SurveyEventType.HealthAccess,
            'Lifestyle': SurveyEventType.Lifestyle,
            'LifeFunctioning': SurveyEventType.LifeFunctioning,
            'OverallHealth': SurveyEventType.OverallHealth,
            'PersonalAndFamilyHealthHistory': SurveyEventType.HealthHistory,
            'SocialDeterminantsOfHealth': SurveyEventType.Social,
            'TheBasics': SurveyEventType.Basics
        }

        while summary_list:
            print(f'starting batch {summary_list[0].participantId} to {summary_list[-1].participantId}...')
            for summary in summary_list:
                for summary_field_name, survey_type in survey_map.items():
                    survey_json = self._build_survey_event_json(summary, survey_type, summary_field_name)
                    if survey_json:
                        self.intake_api.handle_event_insert(req_data=survey_json)

            print('... done')
            last_participant_id = summary_list[-1].participantId
            summary_list = self.get_next_summary_set(last_participant_id, batch_size)

    @classmethod
    def _build_survey_event_json(
        cls, summary: ParticipantSummary, survey_type: SurveyEventType, summary_field_name: str
    ):
        status_map = {
            participant_enums.QuestionnaireStatus.SUBMITTED: 'submitted_complete',
            participant_enums.QuestionnaireStatus.SUBMITTED_INVALID: 'submitted_incomplete'
        }

        summary_status = getattr(summary, f'questionnaireOn{summary_field_name}')
        if summary_status not in status_map:
            return None

        authored_datetime = getattr(summary, f'questionnaireOn{summary_field_name}Authored')
        return {
            'participantId': f'P{summary.participantId}',
            'activity': 'Survey Completion',
            'eventType': survey_type.value,
            'dataElements': [
                {
                    'dataElementName': 'activity_status',
                    'dataElementValue': status_map[summary_status]
                },
                {
                    'dataElementName': 'activity_date_time',
                    'dataElementValue': authored_datetime.isoformat()
                }
            ]
        }


def run():
    cli_run(tool_cmd, tool_desc, MigrateLegacyData)
