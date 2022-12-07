from datetime import datetime

from rdr_service.code_constants import PPI_SYSTEM, WITHDRAWAL_CEREMONY_QUESTION_CODE, WITHDRAWAL_CEREMONY_YES, \
    WITHDRAWAL_CEREMONY_NO, RACE_QUESTION_CODE, RACE_AIAN_CODE
from rdr_service.model.api_user import ApiUser
from rdr_service.model.biobank_mail_kit_order import BiobankMailKitOrder
from rdr_service.model.biobank_order import BiobankSpecimen, BiobankSpecimenAttribute, BiobankOrderHistory, \
    BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSampleHistory, BiobankOrderedSample
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.code import Code, CodeType
from rdr_service.model.consent_file import ConsentFile
from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.model.ehr import ParticipantEhrReceipt
from rdr_service.model.genomic_datagen import GenomicDataGenCaseTemplate, GenomicDataGenManifestSchema, \
    GenomicDataGenOutputTemplate
from rdr_service.model.genomics import GenomicManifestFeedback, GenomicManifestFile, GenomicJobRun, GenomicSet, \
    GenomicSetMember, GenomicAW1Raw, GenomicAW2Raw, GenomicFileProcessed, GenomicIncident, GenomicGCValidationMetrics, \
    GenomicMemberReportState, UserEventMetrics, GenomicInformingLoop, GenomicGcDataFile, GenomicGcDataFileMissing, \
    GenomicResultViewed, GenomicResultWorkflowState, GenomicCVLAnalysis, GenomicCVLSecondSample, GenomicSampleSwap, \
    GenomicSampleSwapMember, GenomicCVLResultPastDue, GenomicW4WRRaw, GenomicW3SCRaw, GenomicAppointmentEvent, \
    GenomicAppointmentEventMetrics
from rdr_service.model.hpo import HPO
from rdr_service.model.hpro_consent_files import HealthProConsentFile
from rdr_service.model.log_position import LogPosition
from rdr_service.model.message_broker import MessageBrokerRecord, MessageBrokerEventData
from rdr_service.model.organization import Organization
from rdr_service.model.participant import Participant, ParticipantHistory
from rdr_service.model.participant_incentives import ParticipantIncentives
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.patient_status import PatientStatus
from rdr_service.model.questionnaire import Questionnaire, QuestionnaireConcept, QuestionnaireHistory, \
    QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponseAnswer, QuestionnaireResponse
from rdr_service.model.site import Site
from rdr_service.model.survey import Survey, SurveyQuestion, SurveyQuestionOption
from rdr_service.offline.biobank_samples_pipeline import _PMI_OPS_SYSTEM
from rdr_service.participant_enums import PatientStatusFlag, QuestionnaireResponseStatus, \
    QuestionnaireResponseClassificationType, UNSET_HPO_ID, WithdrawalStatus, SuspensionStatus, EnrollmentStatus, \
    EnrollmentStatusV30, EnrollmentStatusV31, DeceasedStatus, DeceasedNotification, DeceasedReportStatus,\
    WithdrawalAIANCeremonyStatus


class DataGenerator:
    def __init__(self, session, faker):
        self.session = session
        self.faker = faker
        self._next_unique_participant_id = 900000000
        self._next_unique_research_id = 9000000
        self._next_unique_external_id = 4000000
        self._next_unique_participant_biobank_id = 500000000
        self._next_unique_biobank_order_id = 100000000
        self._next_unique_biobank_stored_sample_id = 800000000
        self._next_unique_questionnaire_response_id = 500000000

        # Set placeholders for withdrawal survey questionnaires
        self.withdrawal_questionnaire = None
        self.ceremony_question_code = None
        self.ceremony_yes_answer_code = None
        self.ceremony_no_answer_code = None

        self.race_question_code = None
        self.native_answer_code = None

    def _commit_to_database(self, model):
        self.session.add(model)
        self.session.commit()

    def create_database_patient_status(self, **kwargs):
        patient_status = self._patient_status(**kwargs)
        self._commit_to_database(patient_status)
        return patient_status

    def _patient_status(self, **kwargs):
        for field, default in [('patientStatus', PatientStatusFlag.YES),
                               ('user', 'test_user')]:
            if field not in kwargs:
                kwargs[field] = default

        if 'hpoId' not in kwargs:
            kwargs['hpoId'] = self.create_database_hpo().hpoId
        if 'organizationId' not in kwargs:
            kwargs['organizationId'] = self.create_database_organization().organizationId
        if 'siteId' not in kwargs:
            kwargs['siteId'] = self.create_database_site().siteId

        return PatientStatus(**kwargs)

    def create_database_questionnaire(self, **kwargs):
        questionnaire = self._questionnaire(**kwargs)
        self._commit_to_database(questionnaire)
        return questionnaire

    @staticmethod
    def _questionnaire(**kwargs):
        for field, default in [('version', 1),
                               ('created', datetime.now()),
                               ('lastModified', datetime.now()),
                               ('resource', '{"version": 1}')]:
            if field not in kwargs:
                kwargs[field] = default

        return Questionnaire(**kwargs)

    def create_database_questionnaire_concept(self, **kwargs):
        questionnaire_concept = self._questionnaire_concept(**kwargs)
        self._commit_to_database(questionnaire_concept)
        return questionnaire_concept

    @staticmethod
    def _questionnaire_concept(**kwargs):
        return QuestionnaireConcept(**kwargs)

    def create_database_questionnaire_history(self, **kwargs):
        questionnaire_history = self._questionnaire_history(**kwargs)
        self._commit_to_database(questionnaire_history)
        return questionnaire_history

    def _questionnaire_history(self, **kwargs):
        for field, default in [('version', 1),
                               ('created', datetime.now()),
                               ('lastModified', datetime.now()),
                               ('resource', 'test')]:
            if field not in kwargs:
                kwargs[field] = default

        if 'questionnaireId' not in kwargs:
            questionnaire = self.create_database_questionnaire()
            kwargs['questionnaireId'] = questionnaire.questionnaireId

        return QuestionnaireHistory(**kwargs)

    def create_database_questionnaire_response_answer(self, **kwargs):
        questionnaire_response_answer = self._questionnaire_response_answer(**kwargs)
        self._commit_to_database(questionnaire_response_answer)
        return questionnaire_response_answer

    @staticmethod
    def _questionnaire_response_answer(**kwargs):
        return QuestionnaireResponseAnswer(**kwargs)

    def create_database_questionnaire_response(self, **kwargs):
        questionnaire_response = self._questionnaire_response(**kwargs)
        self._commit_to_database(questionnaire_response)
        return questionnaire_response

    def _questionnaire_response(self, **kwargs):
        for field, default in [('created', datetime.now()),
                               ('resource', 'test'),
                               ('nonParticipantAuthor', None),
                               ('status', QuestionnaireResponseStatus.COMPLETED),
                               ('classificationType', QuestionnaireResponseClassificationType.COMPLETE)]:
            if field not in kwargs:
                kwargs[field] = default

        if 'questionnaireResponseId' not in kwargs:
            kwargs['questionnaireResponseId'] = self.unique_questionnaire_response_id()
        if 'questionnaireId' not in kwargs:
            questionnaire = self.create_database_questionnaire_history()
            kwargs['questionnaireId'] = questionnaire.questionnaireId
            kwargs['questionnaireVersion'] = questionnaire.version
        if 'participantId' not in kwargs:
            participant = self.create_database_participant()
            kwargs['participantId'] = participant.participantId

        return QuestionnaireResponse(**kwargs)

    def create_database_questionnaire_question(self, **kwargs):
        questionnaire_question = self._questionnaire_question(**kwargs)
        self._commit_to_database(questionnaire_question)
        return questionnaire_question

    def _questionnaire_question(self, **kwargs):
        if 'repeats' not in kwargs:
            kwargs['repeats'] = True

        if 'codeId' not in kwargs:
            code = self.create_database_code()
            kwargs['codeId'] = code.codeId

        return QuestionnaireQuestion(**kwargs)

    def unique_participant_id(self):
        next_participant_id = self._next_unique_participant_id
        self._next_unique_participant_id += 1
        return next_participant_id

    def unique_research_id(self):
        next_research_id = self._next_unique_research_id
        self._next_unique_research_id += 1
        return next_research_id

    def unique_external_id(self):
        next_external_id = self._next_unique_external_id
        self._next_unique_external_id += 1
        return next_external_id

    def unique_participant_biobank_id(self):
        next_biobank_id = self._next_unique_participant_biobank_id
        self._next_unique_participant_biobank_id += 1
        return next_biobank_id

    def unique_biobank_order_id(self):
        next_biobank_order_id = self._next_unique_biobank_order_id
        self._next_unique_biobank_order_id += 1
        return next_biobank_order_id

    def unique_biobank_stored_sample_id(self):
        next_biobank_stored_sameple_id = self._next_unique_biobank_stored_sample_id
        self._next_unique_biobank_stored_sample_id += 1
        return next_biobank_stored_sameple_id

    def unique_questionnaire_response_id(self):
        next_questionnaire_response_id = self._next_unique_questionnaire_response_id
        self._next_unique_questionnaire_response_id += 1
        return next_questionnaire_response_id

    def create_database_site(self, **kwargs):
        site = self._site_with_defaults(**kwargs)
        self._commit_to_database(site)
        return site

    def _site_with_defaults(self, **kwargs):
        defaults = {
            'siteName': 'example_site',
            'googleGroup': self.faker.pystr()
        }
        defaults.update(kwargs)
        return Site(**defaults)

    def create_database_organization(self, **kwargs):
        organization = self._organization_with_defaults(**kwargs)
        self._commit_to_database(organization)
        return organization

    def _organization_with_defaults(self, **kwargs):
        defaults = {
            'displayName': 'example_org_display',
            'externalId': self.faker.pystr()
        }
        defaults.update(kwargs)

        if 'hpoId' not in defaults:
            hpo = self.create_database_hpo()
            defaults['hpoId'] = hpo.hpoId

        return Organization(**defaults)

    def create_database_hpo(self, **kwargs):
        hpo = self._hpo_with_defaults(**kwargs)

        # hpoId is the primary key but is not automatically set when inserting
        if hpo.hpoId is None:
            hpo.hpoId = self.session.query(HPO).count() + 50  # There was code somewhere using lower numbers
        self._commit_to_database(hpo)

        return hpo

    @staticmethod
    def _hpo_with_defaults(**kwargs):
        return HPO(**kwargs)

    def create_database_participant(self, **kwargs):
        participant = self._participant_with_defaults(**kwargs)
        self._commit_to_database(participant)
        return participant

    def _participant_with_defaults(self, **kwargs):
        """Creates a new Participant model, filling in some default constructor args.

        This is intended especially for updates, where more fields are required than for inserts.
        """
        defaults = {
            'hpoId': UNSET_HPO_ID,
            'withdrawalStatus': WithdrawalStatus.NOT_WITHDRAWN,
            'suspensionStatus': SuspensionStatus.NOT_SUSPENDED,
            'participantOrigin': 'example',
            'version': 1,
            'lastModified': datetime.now(),
            'signUpTime': datetime.now(),
            'isTestParticipant': False
        }
        defaults.update(kwargs)

        if 'biobankId' not in defaults:
            defaults['biobankId'] = self.unique_participant_biobank_id()
        if 'participantId' not in defaults:
            defaults['participantId'] = self.unique_participant_id()
        if 'researchId' not in defaults:
            defaults['researchId'] = self.unique_research_id()

        return Participant(**defaults)

    def create_database_participant_summary(self, **kwargs):
        participant_summary = self._participant_summary_with_defaults(**kwargs)
        self._commit_to_database(participant_summary)
        return participant_summary

    def _participant_summary_with_defaults(self, **kwargs):
        participant = kwargs.get('participant')
        if participant is None:
            participant = self.create_database_participant()

        defaults = {
            "participantId": participant.participantId,
            "biobankId": participant.biobankId,
            "hpoId": participant.hpoId,
            "firstName": self.faker.first_name(),
            "lastName": self.faker.last_name(),
            "numCompletedPPIModules": 0,
            "numCompletedBaselinePPIModules": 0,
            "numBaselineSamplesArrived": 0,
            "numberDistinctVisits": 0,
            "withdrawalStatus": WithdrawalStatus.NOT_WITHDRAWN,
            "suspensionStatus": SuspensionStatus.NOT_SUSPENDED,
            "enrollmentStatus": EnrollmentStatus.INTERESTED,
            "enrollmentStatusV3_0": EnrollmentStatusV30.PARTICIPANT,
            "enrollmentStatusV3_1": EnrollmentStatusV31.PARTICIPANT,
            "participantOrigin": participant.participantOrigin,
            "deceasedStatus": DeceasedStatus.UNSET,
            "isEhrDataAvailable": False,
            "wasParticipantMediatedEhrAvailable": False
        }

        defaults.update(kwargs)
        for questionnaire_field in ['consentForStudyEnrollment']:
            if questionnaire_field in defaults:
                if f'{questionnaire_field}Time' not in defaults:
                    defaults[f'{questionnaire_field}Time'] = datetime.now()
                if f'{questionnaire_field}Authored' not in defaults:
                    defaults[f'{questionnaire_field}Authored'] = datetime.now()

        return ParticipantSummary(**defaults)

    def create_database_consent_file(self, **kwargs):
        consent_file = self._consent_file_with_defaults(**kwargs)
        self._commit_to_database(consent_file)
        return consent_file

    def _consent_file_with_defaults(self, **kwargs):
        defaults = {
            'file_exists': True
        }

        defaults.update(kwargs)
        if defaults.get('participant_id') is None:
            defaults['participant_id'] = self.create_database_participant().participantId

        return ConsentFile(**defaults)

    def create_database_biobank_specimen(self, **kwargs):
        specimen = self._biobank_specimen_with_defaults(**kwargs)
        self._commit_to_database(specimen)
        return specimen

    def _biobank_specimen_with_defaults(self, **kwargs):
        defaults = {
            'orderId': 'test_order',
            'rlimsId': self.faker.pystr()
        }
        defaults.update(kwargs)

        if 'biobankId' not in defaults:
            defaults['biobankId'] = self.create_database_participant().biobankId

        return BiobankSpecimen(**defaults)

    def create_database_participant_ehr_receipt(self, **kwargs):
        participant_ehr_receipt = self._participant_ehr_receipt_with_defaults(**kwargs)
        self._commit_to_database(participant_ehr_receipt)
        return participant_ehr_receipt

    @staticmethod
    def _participant_ehr_receipt_with_defaults(**kwargs):
        defaults = {
            'fileTimestamp': datetime.now(),
            'firstSeen': datetime.now(),
            'lastSeen': datetime.now()
        }
        defaults.update(kwargs)

        return ParticipantEhrReceipt(**defaults)

    def create_database_specimen_attribute(self, **kwargs):
        specimen_attribute = self._specimen_attribute_with_defaults(**kwargs)
        self._commit_to_database(specimen_attribute)
        return specimen_attribute

    def _specimen_attribute_with_defaults(self, **kwargs):
        if 'specimen_id' not in kwargs:
            generated_specimen = self.create_database_biobank_specimen()
            kwargs['specimen_id'] = generated_specimen.id
            kwargs['specimen_rlims_id'] = generated_specimen.rlimsId
        return BiobankSpecimenAttribute(**kwargs)

    @staticmethod
    def _participant_history_with_defaults(**kwargs):
        common_args = {
            "hpoId": UNSET_HPO_ID,
            "version": 1,
            "withdrawalStatus": WithdrawalStatus.NOT_WITHDRAWN,
            "suspensionStatus": SuspensionStatus.NOT_SUSPENDED,
            "participantOrigin": "example",
            "isTestParticipant": False
        }
        common_args.update(kwargs)
        return ParticipantHistory(**common_args)

    def create_database_code(self, **kwargs):
        code = self._code(**kwargs)
        self._commit_to_database(code)
        return code

    def _code(self, **kwargs):
        for field, default in [('system', PPI_SYSTEM),
                               ('codeType', 1),
                               ('mapped', False),
                               ('created', datetime.now()),
                               ('value', self.faker.pystr(4, 8))]:
            if field not in kwargs:
                kwargs[field] = default

        return Code(**kwargs)

    def create_database_biobank_order(self, **kwargs):
        biobank_order = self._biobank_order(**kwargs)
        self._commit_to_database(biobank_order)

        order_history = BiobankOrderHistory()
        order_history.fromdict(biobank_order.asdict(follow=["logPosition"]), allow_pk=True)
        self._commit_to_database(order_history)

        return biobank_order

    def _biobank_order(self, log_position=None, **kwargs):
        for field, default in [('version', 1),
                               ('created', datetime.now())]:
            if field not in kwargs:
                kwargs[field] = default

        if 'logPositionId' not in kwargs:
            if log_position is None:
                log_position = self.create_database_log_position()
            kwargs['logPositionId'] = log_position.logPositionId
        if 'biobankOrderId' not in kwargs:
            kwargs['biobankOrderId'] = self.unique_biobank_order_id()
        if 'participantId' not in kwargs:
            kwargs['participantId'] = self.create_database_participant_summary().participantId

        return BiobankOrder(**kwargs)

    def create_database_biobank_mail_kit_order(self, **kwargs):
        biobank_mail_kit_order = self._biobank_mail_kit_order(**kwargs)
        self._commit_to_database(biobank_mail_kit_order)

        return biobank_mail_kit_order

    @staticmethod
    def _biobank_mail_kit_order(**kwargs):
        for field, default in [('version', 1)]:
            if field not in kwargs:
                kwargs[field] = default

        return BiobankMailKitOrder(**kwargs)

    def create_database_biobank_order_identifier(self, **kwargs):
        biobank_order_identifier = self._biobank_order_identifier(**kwargs)
        self._commit_to_database(biobank_order_identifier)
        return biobank_order_identifier

    def _biobank_order_identifier(self, **kwargs):
        for field, default in [('system', _PMI_OPS_SYSTEM)]:
            if field not in kwargs:
                kwargs[field] = default

        if 'biobankOrderId' not in kwargs:
            kwargs['biobankOrderId'] = self.create_database_biobank_order().biobankOrderId

        return BiobankOrderIdentifier(**kwargs)

    def create_database_biobank_ordered_sample(self, **kwargs):
        biobank_ordered_sample = self._biobank_ordered_sample(**kwargs)
        self._commit_to_database(biobank_ordered_sample)

        ordered_sample_history = BiobankOrderedSampleHistory()
        ordered_sample_history.fromdict(biobank_ordered_sample.asdict(), allow_pk=True)
        ordered_sample_history.version = 1
        self._commit_to_database(ordered_sample_history)

        return biobank_ordered_sample

    @staticmethod
    def _biobank_ordered_sample(**kwargs):
        for field, default in [('description', 'test ordered sample'),
                               ('processingRequired', False),
                               ('test', 'C3PO')]:
            if field not in kwargs:
                kwargs[field] = default

        return BiobankOrderedSample(**kwargs)

    def create_database_biobank_stored_sample(self, **kwargs):
        biobank_stored_sample = self._biobank_stored_sample(**kwargs)
        self._commit_to_database(biobank_stored_sample)
        return biobank_stored_sample

    def _biobank_stored_sample(self, **kwargs):
        if 'biobankStoredSampleId' not in kwargs:
            kwargs['biobankStoredSampleId'] = self.unique_biobank_stored_sample_id()

        for field, default in [('biobankOrderIdentifier', self.faker.pystr()),
                               ('test', self.faker.pystr(4))]:
            if field not in kwargs:
                kwargs[field] = default

        return BiobankStoredSample(**kwargs)

    def create_database_log_position(self, **kwargs):
        log_position = self._log_position(**kwargs)
        self._commit_to_database(log_position)
        return log_position

    @staticmethod
    def _log_position(**kwargs):
        return LogPosition(**kwargs)

    def create_database_api_user(self, **kwargs):
        api_user = self._api_user(**kwargs)
        self._commit_to_database(api_user)
        return api_user

    @staticmethod
    def _api_user(**kwargs):
        if 'system' not in kwargs:
            kwargs['system'] = 'unit_test'
        if 'username' not in kwargs:
            kwargs['username'] = 'me@test.com'
        return ApiUser(**kwargs)

    def create_database_deceased_report(self, **kwargs):
        deceased_report = self._deceased_report(**kwargs)
        self._commit_to_database(deceased_report)
        return deceased_report

    def _deceased_report(self, **kwargs):
        if 'participantId' not in kwargs:
            participant = self.create_database_participant()
            kwargs['participantId'] = participant.participantId
        if 'notification' not in kwargs:
            kwargs['notification'] = DeceasedNotification.EHR
        if 'author' not in kwargs:
            kwargs['author'] = self.create_database_api_user()
        if 'authored' not in kwargs:
            kwargs['authored'] = datetime.now()
        if 'status' not in kwargs:
            kwargs['status'] = DeceasedReportStatus.PENDING
        return DeceasedReport(**kwargs)

    def create_database_survey(self, **kwargs):
        survey = self._survey(**kwargs)
        self._commit_to_database(survey)
        return survey

    def _survey(self, **kwargs):
        if 'code' not in kwargs and 'codeId' not in kwargs:
            module_code = self.create_database_code()
            kwargs['codeId'] = module_code.codeId
        return Survey(**kwargs)

    def create_database_survey_question(self, **kwargs):
        survey_question = self._survey_question(**kwargs)
        self._commit_to_database(survey_question)
        return survey_question

    @staticmethod
    def _survey_question(**kwargs):
        return SurveyQuestion(**kwargs)

    def create_database_survey_question_option(self, **kwargs):
        survey_question_option = self._survey_question_option(**kwargs)
        self._commit_to_database(survey_question_option)
        return survey_question_option

    @staticmethod
    def _survey_question_option(**kwargs):
        return SurveyQuestionOption(**kwargs)

    @staticmethod
    def _genomic_manifest_feedback(**kwargs):
        return GenomicManifestFeedback(**kwargs)

    def create_database_genomic_manifest_feedback(self, **kwargs):
        feedback = self._genomic_manifest_feedback(**kwargs)
        self._commit_to_database(feedback)
        return feedback

    def create_database_genomic_manifest_file(self, **kwargs):
        manifest = self._genomic_manifest_file(**kwargs)
        self._commit_to_database(manifest)
        return manifest

    @staticmethod
    def _genomic_manifest_file(**kwargs):
        return GenomicManifestFile(**kwargs)

    def create_database_genomic_job_run(self, **kwargs):
        job_run = self._genomic_job_run(**kwargs)
        self._commit_to_database(job_run)
        return job_run

    @staticmethod
    def _genomic_job_run(**kwargs):
        if 'startTime' not in kwargs:
            kwargs['startTime'] = datetime.utcnow()
        return GenomicJobRun(**kwargs)

    def create_database_genomic_set(self, **kwargs):
        gen_set = self._genomic_set(**kwargs)
        self._commit_to_database(gen_set)
        return gen_set

    @staticmethod
    def _genomic_set(**kwargs):
        return GenomicSet(**kwargs)

    def create_database_genomic_set_member(self, **kwargs):
        m = self._genomic_set_member(**kwargs)
        self._commit_to_database(m)
        return m

    @staticmethod
    def _genomic_set_member(**kwargs):
        return GenomicSetMember(**kwargs)

    def create_database_genomic_aw1_raw(self, **kwargs):
        raw = self._genomic_aw1_raw(**kwargs)
        self._commit_to_database(raw)
        return raw

    @staticmethod
    def _genomic_aw1_raw(**kwargs):
        return GenomicAW1Raw(**kwargs)

    def create_database_genomic_aw2_raw(self, **kwargs):
        raw = self._genomic_aw2_raw(**kwargs)
        self._commit_to_database(raw)
        return raw

    @staticmethod
    def _genomic_aw2_raw(**kwargs):
        return GenomicAW2Raw(**kwargs)

    def create_database_genomic_file_processed(self, **kwargs):
        file = self._genomic_file_processed(**kwargs)
        self._commit_to_database(file)
        return file

    @staticmethod
    def _genomic_file_processed(**kwargs):
        return GenomicFileProcessed(**kwargs)

    def create_database_genomic_incident(self, **kwargs):
        incident = self._genomic_incident(**kwargs)
        self._commit_to_database(incident)
        return incident

    @staticmethod
    def _genomic_incident(**kwargs):
        return GenomicIncident(**kwargs)

    def create_database_genomic_gc_validation_metrics(self, **kwargs):
        metrics = self._genomic_validation_metrics(**kwargs)
        self._commit_to_database(metrics)
        return metrics

    @staticmethod
    def _genomic_validation_metrics(**kwargs):
        return GenomicGCValidationMetrics(**kwargs)

    def create_database_message_broker_record(self, **kwargs):
        records = self._message_broker_records(**kwargs)
        self._commit_to_database(records)
        return records

    @staticmethod
    def _message_broker_records(**kwargs):
        return MessageBrokerRecord(**kwargs)

    def create_database_message_broker_event_data(self, **kwargs):
        records = self._message_broker_event_data(**kwargs)
        self._commit_to_database(records)
        return records

    @staticmethod
    def _message_broker_event_data(**kwargs):
        return MessageBrokerEventData(**kwargs)

    def create_database_genomic_member_report_state(self, **kwargs):
        report_state = self._genomic_member_report_state(**kwargs)
        self._commit_to_database(report_state)
        return report_state

    @staticmethod
    def _genomic_member_report_state(**kwargs):
        return GenomicMemberReportState(**kwargs)

    def create_database_genomic_informing_loop(self, **kwargs):
        informing_loop = self._genomic_informing_loop(**kwargs)
        self._commit_to_database(informing_loop)
        return informing_loop

    @staticmethod
    def _genomic_user_event_metrics(**kwargs):
        return UserEventMetrics(**kwargs)

    def create_database_genomic_user_event_metrics(self, **kwargs):
        event_metrics = self._genomic_user_event_metrics(**kwargs)
        self._commit_to_database(event_metrics)
        return event_metrics

    @staticmethod
    def _genomic_informing_loop(**defaults):
        if 'event_type' not in defaults:
            defaults['event_type'] = 'informing_loop_decision'
        return GenomicInformingLoop(**defaults)

    @staticmethod
    def _genomic_gc_data_file(**kwargs):
        return GenomicGcDataFile(**kwargs)

    def create_database_gc_data_file_record(self, **kwargs):
        records = self._genomic_gc_data_file(**kwargs)
        self._commit_to_database(records)
        return records

    @staticmethod
    def _genomic_gc_data_missing_file(**kwargs):
        return GenomicGcDataFileMissing(**kwargs)

    def create_database_gc_data_missing_file(self, **kwargs):
        records = self._genomic_gc_data_missing_file(**kwargs)
        self._commit_to_database(records)
        return records

    @staticmethod
    def _hpro_consent_file(**kwargs):
        return HealthProConsentFile(**kwargs)

    def create_database_hpro_consent(self, **kwargs):
        consent = self._hpro_consent_file(**kwargs)
        self._commit_to_database(consent)
        return consent

    @staticmethod
    def _participant_incentives(**kwargs):
        return ParticipantIncentives(**kwargs)

    def create_database_participant_incentives(self, **kwargs):
        incentive = self._participant_incentives(**kwargs)
        self._commit_to_database(incentive)
        return incentive

    @staticmethod
    def _genomic_result_viewed(**kwargs):
        return GenomicResultViewed(**kwargs)

    def create_genomic_result_viewed(self, **kwargs):
        result_viewed = self._genomic_result_viewed(**kwargs)
        self._commit_to_database(result_viewed)
        return result_viewed

    @staticmethod
    def _genomic_result_workflow_state(**kwargs):
        return GenomicResultWorkflowState(**kwargs)

    def create_database_genomic_result_workflow_state(self, **kwargs):
        m = self._genomic_result_workflow_state(**kwargs)
        self._commit_to_database(m)
        return m

    @staticmethod
    def _genomic_datagen_template(**kwargs):
        return GenomicDataGenCaseTemplate(**kwargs)

    def create_database_datagen_template(self, **kwargs):
        m = self._genomic_datagen_template(**kwargs)
        self._commit_to_database(m)
        return m

    @staticmethod
    def _genomic_datagen_manifest_schema(**kwargs):
        return GenomicDataGenManifestSchema(**kwargs)

    def create_database_genomic_datagen_manifest_schema(self, **kwargs):
        m = self._genomic_datagen_manifest_schema(**kwargs)
        self._commit_to_database(m)
        return m

    def create_database_genomic_cvl_analysis(self, **kwargs):
        m = self._genomic_cvl_analysis(**kwargs)
        self._commit_to_database(m)
        return m

    @staticmethod
    def _genomic_cvl_analysis(**kwargs):
        return GenomicCVLAnalysis(**kwargs)

    def create_database_genomic_cvl_second_sample(self, **kwargs):
        m = self._genomic_cvl_second_sample(**kwargs)
        self._commit_to_database(m)
        return m

    @staticmethod
    def _genomic_cvl_second_sample(**kwargs):
        return GenomicCVLSecondSample(**kwargs)

    def create_database_genomic_datagen_output_template(self, **kwargs):
        m = self._genomic_datagen_output_template(**kwargs)
        self._commit_to_database(m)
        return m

    @staticmethod
    def _genomic_datagen_output_template(**kwargs):
        return GenomicDataGenOutputTemplate(**kwargs)

    def create_database_genomic_sample_swap(self, **kwargs):
        m = self._genomic_sample_swap(**kwargs)
        self._commit_to_database(m)
        return m

    @staticmethod
    def _genomic_sample_swap(**kwargs):
        return GenomicSampleSwap(**kwargs)

    def create_database_genomic_sample_swap_member(self, **kwargs):
        m = self._genomic_sample_swap_member(**kwargs)
        self._commit_to_database(m)
        return m

    @staticmethod
    def _genomic_sample_swap_member(**kwargs):
        return GenomicSampleSwapMember(**kwargs)

    def create_database_genomic_cvl_past_due(self, **kwargs):
        m = self._genomic_cvl_past_due(**kwargs)
        self._commit_to_database(m)
        return m

    @staticmethod
    def _genomic_cvl_past_due(**kwargs):
        return GenomicCVLResultPastDue(**kwargs)

    def create_database_genomic_w4wr_raw(self, **kwargs):
        m = self._genomic_w4wr_raw(**kwargs)
        self._commit_to_database(m)
        return m

    @staticmethod
    def _genomic_w4wr_raw(**kwargs):
        return GenomicW4WRRaw(**kwargs)

    def create_database_genomic_w3sc_raw(self, **kwargs):
        m = self._genomic_w3sc_raw(**kwargs)
        self._commit_to_database(m)
        return m

    @staticmethod
    def _genomic_w3sc_raw(**kwargs):
        return GenomicW3SCRaw(**kwargs)

    def create_database_genomic_appointment(self, **kwargs):
        m = self._genomic_appointment_event(**kwargs)
        self._commit_to_database(m)
        return m

    @staticmethod
    def _genomic_appointment_event(**kwargs):
        return GenomicAppointmentEvent(**kwargs)

    def create_database_genomic_appointment_metric(self, **kwargs):
        m = self._genomic_appointment_event_metric(**kwargs)
        self._commit_to_database(m)
        return m

    @staticmethod
    def _genomic_appointment_event_metric(**kwargs):
        return GenomicAppointmentEventMetrics(**kwargs)

    def create_withdrawn_participant(self, withdrawal_reason_justification, is_native_american=False,
                                     requests_ceremony=None, withdrawal_time=datetime.utcnow()):
        participant = self.create_database_participant(
            withdrawalTime=withdrawal_time,
            withdrawalStatus=WithdrawalStatus.NO_USE,
            withdrawalReasonJustification=withdrawal_reason_justification
        )

        # Withdrawal report only includes participants that have stored samples
        self.create_database_biobank_stored_sample(biobankId=participant.biobankId, test='test')

        # Create a questionnaire response that satisfies the parameters for the test participant
        questionnaire = self.get_withdrawal_questionnaire()
        answers = []
        for question in questionnaire.questions:
            answer_code_id = None
            if question.codeId == self.race_question_code.codeId and is_native_american:
                answer_code_id = self.native_answer_code.codeId
            elif question.codeId == self.ceremony_question_code.codeId and requests_ceremony:
                if requests_ceremony == WithdrawalAIANCeremonyStatus.REQUESTED:
                    answer_code_id = self.ceremony_yes_answer_code.codeId
                elif requests_ceremony == WithdrawalAIANCeremonyStatus.DECLINED:
                    answer_code_id = self.ceremony_no_answer_code.codeId

            if answer_code_id:
                answers.append(QuestionnaireResponseAnswer(
                    questionId=question.questionnaireQuestionId,
                    valueCodeId=answer_code_id
                ))
        self.create_database_questionnaire_response(
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version,
            answers=answers,
            participantId=participant.participantId
        )

        return participant

    def get_withdrawal_questionnaire(self):
        if self.withdrawal_questionnaire is None:
            self.initialize_common_codes()

            race_question = self.create_database_questionnaire_question(
                codeId=self.race_question_code.codeId
            )
            ceremony_question = self.create_database_questionnaire_question(
                codeId=self.ceremony_question_code.codeId
            )

            self.withdrawal_questionnaire = self.create_database_questionnaire_history(
                # As of writing this, the pipeline only checks for the answers, regardless of questionnaire
                # so putting them in the same questionnaire for convenience of the test code
                questions=[race_question, ceremony_question]
            )

        return self.withdrawal_questionnaire

    def initialize_common_codes(self):
        if self.race_question_code is None:
            self.ceremony_question_code = self.create_database_code(
                value=WITHDRAWAL_CEREMONY_QUESTION_CODE,
                codeType=CodeType.QUESTION,
            )
            self.ceremony_yes_answer_code = self.create_database_code(
                value=WITHDRAWAL_CEREMONY_YES,
                codeType=CodeType.QUESTION,
            )
            self.ceremony_no_answer_code = self.create_database_code(
                value=WITHDRAWAL_CEREMONY_NO,
                codeType=CodeType.QUESTION,
            )

            self.race_question_code = self.create_database_code(
                value=RACE_QUESTION_CODE,
                codeType=CodeType.QUESTION,
                mapped=True
            )
            self.native_answer_code = self.create_database_code(
                value=RACE_AIAN_CODE,
                codeType=CodeType.ANSWER,
                mapped=True
            )
