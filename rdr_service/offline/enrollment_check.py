from datetime import datetime, timedelta

from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, SampleStatus,\
    SelfReportedPhysicalMeasurementsStatus
from rdr_service.services.jira_utils import JiraTicketHandler

PS_DAO = ParticipantSummaryDao()
THIRTYONE_DAYS = datetime.now() - timedelta(days=31)


def check_enrollment(create_ticket=True):
    """For members who are listed as full-member,
     check that necessary requirements are valid"""
    members = get_last_31_days()
    results = None
    if members:
        results = check_requirements(members)
    if results:
        # make a roc ticket
        if create_ticket:
            jira_handler = JiraTicketHandler()
            jira_handler.create_ticket('Participant Status offline job', str(results).strip('[]'), issue_type='Task',
                                       board_id='ROC')
        return False
    else:
        return True


def get_last_31_days():
    """Gets members with full-member (core) status from last 30 days.
    return: members"""
    with PS_DAO.session() as session:
        return session.query(ParticipantSummary).filter(ParticipantSummary.enrollmentStatus == 3) \
            .filter(ParticipantSummary.lastModified >= THIRTYONE_DAYS).all()


def check_requirements(members):
    """ A full participant (core) is defined as:
    completed the primary informed consent process
    HIPAA Authorization/EHR consent
    required PPI modules (Basics, Overall Health, and Lifestyle modules)
    provide physical measurements
    at least one biosample suitable for genetic sequencing.
    """
    result_list = []
    status_results = {}

    for member in members:
        mandatory = check_mandatory(member, status_results)
        optional = check_optional(member, status_results)
        if optional:
            mandatory.update(optional)
        if mandatory:
            result_list.append(mandatory)

    return result_list if result_list else False


def check_mandatory(member, status_results):
    required = {'consentForStudyEnrollment': QuestionnaireStatus.SUBMITTED,
                'questionnaireOnLifestyle': QuestionnaireStatus.SUBMITTED,
                'questionnaireOnOverallHealth': QuestionnaireStatus.SUBMITTED,
                'questionnaireOnTheBasics': QuestionnaireStatus.SUBMITTED,
                'clinicPhysicalMeasurementsStatus': PhysicalMeasurementsStatus.COMPLETED,
                'samplesToIsolateDNA': SampleStatus.RECEIVED}

    physical_measurements = {'clinicPhysicalMeasurementsStatus': PhysicalMeasurementsStatus.COMPLETED,
                  'selfReportedPhysicalMeasurementsStatus': SelfReportedPhysicalMeasurementsStatus.COMPLETED}

    for attribute, value in required.items():
        if attribute not in member.__dict__:
            status_results['participantId'] = member.particpantId
            status_results[attribute] = 'missing'
        if getattr(member, attribute) != value:
            status_results['participantId'] = member.participantId
            status_results[attribute] = getattr(member, attribute)

    pm_values = {}
    for attribute, value in physical_measurements.items():
        if attribute not in member.__dict__:
            status_results['participantId'] = member.participantId
            status_results[attribute] = 'missing'
        if getattr(member, attribute) != value:
            pm_values[attribute] = getattr(member, attribute)

    if len(pm_values) == len(physical_measurements):
        for attribute, value in pm_values.items():
            status_results['participantId'] = member.participantId
            status_results[attribute] = value

    return status_results or None


def check_optional(member, status_results):
    comparable = {'consentForDvElectronicHealthRecordsSharing': QuestionnaireStatus.SUBMITTED,
                  'consentForElectronicHealthRecords': QuestionnaireStatus.SUBMITTED}

    truthy_list = [i in member.__dict__ for i in comparable]
    if not any(truthy_list):
        status_results['participantId'] = member.particpantId
        status_results['consentForDvElectronicHealthRecordsSharing'] = getattr(
            member, 'consentForDvElectronicHealthRecordsSharing')
        status_results['consentForElectronicHealthRecords'] = getattr(member, 'consentForElectronicHealthRecords')

    for attribute, value in comparable.items():
        if getattr(member, attribute) != value and getattr(member, attribute) is not None:
            status_results['participantId'] = member.participantId
            status_results[attribute] = getattr(member, attribute)

    return status_results or None
