from datetime import date
import logging
import pytz
from sqlalchemy import and_, desc, func, or_
from sqlalchemy.orm.exc import DetachedInstanceError
from werkzeug.exceptions import BadRequest, NotFound, Conflict, InternalServerError

from rdr_service import config
from rdr_service.clock import CLOCK
from rdr_service.dao.api_user_dao import ApiUserDao
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.lib_fhir.fhirclient_4_0_0.models.codeableconcept import CodeableConcept
from rdr_service.lib_fhir.fhirclient_4_0_0.models.fhirabstractbase import FHIRValidationError
from rdr_service.lib_fhir.fhirclient_4_0_0.models.fhirdate import FHIRDate
from rdr_service.lib_fhir.fhirclient_4_0_0.models.fhirreference import FHIRReference
from rdr_service.lib_fhir.fhirclient_4_0_0.models.extension import Extension
from rdr_service.lib_fhir.fhirclient_4_0_0.models.humanname import HumanName
from rdr_service.lib_fhir.fhirclient_4_0_0.models.identifier import Identifier
from rdr_service.lib_fhir.fhirclient_4_0_0.models.observation import Observation
from rdr_service.lib_fhir.fhirclient_4_0_0.models.reference import Reference
from rdr_service.model.api_user import ApiUser
from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.model.organization import Organization
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.utils import to_client_participant_id
from rdr_service.participant_enums import DeceasedNotification, DeceasedReportDenialReason, DeceasedReportStatus,\
    DeceasedStatus, SuspensionStatus, WithdrawalStatus


class DeceasedReportDao(UpdatableDao):

    validate_version_match = False
    status_map = {
        'preliminary': DeceasedReportStatus.PENDING,
        'final': DeceasedReportStatus.APPROVED,
        'cancelled': DeceasedReportStatus.DENIED
    }

    def __init__(self):
        super().__init__(DeceasedReport)

    def _is_future_datetime(self, incoming_datetime):
        utc_now = self._convert_to_utc_datetime(CLOCK.now())
        utc_incoming_datetime = self._convert_to_utc_datetime(incoming_datetime)
        return utc_now < utc_incoming_datetime

    @staticmethod
    def _is_future_date(incoming_date):
        return date.today() < incoming_date

    def _read_report_status(self, observation: Observation):
        if observation.status is None:
            raise BadRequest('Missing required field: status')
        if observation.status not in self.status_map:
            raise BadRequest(f'Invalid status "{observation.status}"')

        return self.status_map[observation.status]

    @staticmethod
    def _find_class_in_array(cls, array):
        for item in array:
            if isinstance(item, cls):
                return item
        return None

    def _read_api_request_author(self, observation: Observation):
        user_reference = self._find_class_in_array(Reference, observation.performer)
        if user_reference is None:
            raise BadRequest('Performer reference for authoring user required')
        return ApiUserDao().load_or_init(user_reference.type, user_reference.reference)

    @staticmethod
    def _read_authored_timestamp(observation: Observation):
        if observation.issued is None:
            raise BadRequest('Report issued date is required')

        return observation.issued.date

    @staticmethod
    def _read_encounter(observation: Observation, report):  # Get notification data
        if observation.encounter is None:
            raise BadRequest('Encounter information required for deceased report')
        encounter = observation.encounter
        if encounter.reference is None:
            raise BadRequest('Invalid encounter information')

        report.notification = DeceasedNotification(encounter.reference)
        if report.notification == DeceasedNotification.OTHER:
            if encounter.display is None:
                raise BadRequest('Encounter display description text required when OTHER is set')
            report.notificationOther = encounter.display

    @staticmethod
    def _read_reporter_data(observation: Observation, report):
        extensions = observation.extension

        if extensions is None or\
                not isinstance(extensions, list) or\
                len(extensions) == 0:
            raise BadRequest('Reporter extension data is required')

        extension = extensions[0]
        if extension.valueHumanName is None:
            raise BadRequest('Reporter HumanName data is required')
        human_name = extension.valueHumanName
        if human_name.text is None:
            raise BadRequest('Missing reporter name')
        report.reporterName = human_name.text

        if human_name.extension is None:
            raise BadRequest('Missing reporter extensions')
        reporter_extensions = human_name.extension
        if not isinstance(reporter_extensions, list):
            raise BadRequest('Invalid reporter extensions')
        for reporter_extension in reporter_extensions:
            if reporter_extension.url == 'http://hl7.org/fhir/ValueSet/relatedperson-relationshiptype':
                report.reporterRelationship = reporter_extension.valueCode
            elif reporter_extension.url == 'https://www.pmi-ops.org/email-address':
                report.reporterEmail = reporter_extension.valueString
            elif reporter_extension.url == 'https://www.pmi-ops.org/phone-number':
                report.reporterPhone = reporter_extension.valueString

        if report.reporterRelationship is None:  # If this is unset still then it must have not been provided
            raise BadRequest('Reporter association is required')

    def _read_denial_extension(self, observation: Observation, report):
        if observation.extension is None:
            raise BadRequest('Report denial information missing')

        denial_extension = self._find_class_in_array(Extension, observation.extension)
        if denial_extension.valueReference is None:
            raise BadRequest('Report denial information missing')

        denial_reference = denial_extension.valueReference
        report.denialReason = DeceasedReportDenialReason(denial_reference.reference)
        if report.denialReason == DeceasedReportDenialReason.OTHER:
            report.denialReasonOther = denial_reference.display

    def _load_participant(self, participant_id):
        with self.session() as session:
            participant = session.query(Participant).filter(
                Participant.participantId == participant_id
            ).one_or_none()
            if participant is None:
                raise NotFound(f'Participant P{participant_id} not found.')
            return participant

    @staticmethod
    def _update_participant_summary(session, report: DeceasedReport):
        """
        These are the three fields from the Participant Summary that are affected by deceased reports,
        and explanations of what they will provide and when:

        * deceasedStatus
            Will be UNSET for any participants that have no deceased reports (or only reports that have been denied).
            Is set to PENDING when a participant has a deceased report with a status of *preliminary*.
            And will be APPROVED for participants that have a *final* deceased report.
        * deceasedAuthored
            The most recent **issued** date received for an active deceased report. So for participants with a PENDING
            deceased status this will be the time that an external user created a deceased report for the participant.
            And for participants with an APPROVED status, this will be the time that the report was finalized.
        * dateOfDeath
            Date that the participant passed away if it was provided when creating or reviewing the report (using the
            date from the reviewing request if both requests provided the field).
        """

        participant_summary = session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == report.participantId
        ).one_or_none()
        if participant_summary:
            if report.status == DeceasedReportStatus.DENIED:
                participant_summary.deceasedStatus = DeceasedStatus.UNSET
                participant_summary.deceasedAuthored = None
                participant_summary.dateOfDeath = None
            else:
                participant_summary.deceasedStatus = DeceasedStatus(str(report.status))
                participant_summary.dateOfDeath = report.dateOfDeath

                if report.status == DeceasedReportStatus.APPROVED:
                    participant_summary.deceasedAuthored = report.reviewed
                else:
                    participant_summary.deceasedAuthored = report.authored

    def load_model(self, id_):
        with self.session() as session:
            report = session.query(DeceasedReport).filter(DeceasedReport.id == id_).one_or_none()
        if report is None:
            raise NotFound(f'DeceasedReport with id "{id_}" not found')
        else:
            return report

    # pylint: disable=unused-argument
    def from_client_json(self, resource, participant_id, id_=None, expected_version=None, client_id=None):
        """
        The API takes deceased report data structured as a FHIR Specification 4.0 Observation
        (http://hl7.org/fhir/observation.html). Listed below is an outline of each field, what it means for a deceased
        report, and any requirements for the field.

        .. code-block:: javascript

            {
                // For creating a deceased report, the status must be given as preliminary
                // REQUIRED
                status: "preliminary",

                // CodeableConcept structure defining the observation as a deceased report for the RDR API
                // REQUIRED
                code: {
                    text: "DeceasedReport”
                },

                // The date of death of the participant
                // OPTIONAL
                effectiveDateTime: "2020-01-01",

                // Details for how the user creating the report has become aware of the participant's deceased status
                // REQUIRED
                encounter: {
                    // Must be one of the following: EHR, ATTEMPTED_CONTACT, NEXT_KIN_HPO, NEXT_KIN_SUPPORT, OTHER
                    reference: "OTHER",

                    // Required if reference is given as OTHER
                    display: "Some other reason"
                },

                // The user that has created the deceased report
                // REQUIRED
                performer: [
                    {
                        type: "https://www.pmi-ops.org/healthpro-username",
                        reference: "user.name@pmi-ops.org"
                    }
                ],

                // The timestamp of when the user created the report
                // REQUIRED
                issued: "2020-01-31T08:34:12Z",  // assumed to be UTC if no timezone information is provided

                // Text field for providing the cause of death
                // OPTIONAL
                valueString: "Heart disease",

                // Array providing a single extension with a HumanName value providing information on the person
                //  that has reported that the participant has passed away
                // REQUIRED unless the encounter specifies EHR or OTHER
                extension: [
                    {
                        url: "https://www.pmi-ops.org/deceased-reporter",
                        valueHumanName: {
                            text: "John Doe",
                            extension: [
                                {
                                    // REQUIRED
                                    url: "http://hl7.org/fhir/ValueSet/relatedperson-relationshiptype",
                                    valueCode: "SIB"
                                },
                                {
                                    // OPTIONAL
                                    url: "https://www.pmi-ops.org/email-address",
                                    valueString: "jdoe@yahoo.com"
                                },
                                {
                                    // OPTIONAL
                                    url: "https://www.pmi-ops.org/phone-number",
                                    valueString: "123-456-7890"
                                }
                            ]
                        }
                    }
                ]
            }

        Any participants that are are not paired to an HPO are automatically finalized,
        otherwise the reports remain in the *preliminary* state until they are reviewed by an additional user.
        A review request can set a report as *final* or *cancelled*. Here's a description of the relevant fields
        for reviewing a report:

        .. code-block:: javascript

            {
                // Review status for the deceased report. Can be "final" to finalize a report, or "cancelled" to deny it
                // REQUIRED
                status: "final",

                // REQUIRED
                code: {
                    text: "DeceasedReport”
                },

                // Information for the user that has reviewed the report.
                // REQUIRED
                performer: [
                    {
                        type: "https://www.pmi-ops.org/healthpro-username",
                        reference: "user.name@pmi-ops.org"
                    }
                ],

                // The date of death of the participant. Will replace what is currently on the report.
                // OPTIONAL
                effectiveDateTime: "2020-01-01",

                // The timestamp of when the user reviewed the report
                issued: "2020-01-31T08:34:12Z"  // assumed to be UTC if no timezone information is provided

                // Additional information for defining why the report is cancelled if cancelling the report
                // REQUIRED if providing a status of "cancelled"
                extension: [
                    {
                        url: "https://www.pmi-ops.org/observation-denial-reason",
                        valueReference: {
                            // Must be one of the following: INCORRECT_PARTICIPANT, MARKED_IN_ERROR,
                            //  INSUFFICIENT_INFORMATION, OTHER
                            reference: "OTHER",

                            // Text description of the reason for cancelling
                            // REQUIRED if reference gives OTHER
                            display: "Another reason for denying the report"
                        }
                    }
                ]
            }
        """

        try:
            observation = Observation(resource)
        except FHIRValidationError:
            raise BadRequest('Invalid FHIR Observation structure')

        if observation.performer is None or not isinstance(observation.performer, list):
            raise BadRequest('Performer array is required')

        requested_report_status = self._read_report_status(observation)

        if id_ is None:  # No report was referenced with the request, so the request is to create a new one
            if requested_report_status != DeceasedReportStatus.PENDING:
                raise BadRequest('Status field should be "preliminary" when creating deceased report')
            report = DeceasedReport(participantId=participant_id)

            self._read_encounter(observation, report)

            if report.notification in [DeceasedNotification.ATTEMPTED_CONTACT,
                                       DeceasedNotification.NEXT_KIN_HPO,
                                       DeceasedNotification.NEXT_KIN_SUPPORT]:
                self._read_reporter_data(observation, report)

            report.author = self._read_api_request_author(observation)
            report.authored = self._read_authored_timestamp(observation)

            report.causeOfDeath = observation.valueString
        else:
            report = self.load_model(id_)
            if report.status != DeceasedReportStatus.PENDING:
                raise BadRequest('Can only approve or deny a PENDING deceased report')

            report.reviewer = self._read_api_request_author(observation)
            report.reviewed = self._read_authored_timestamp(observation)

            if requested_report_status == DeceasedReportStatus.DENIED:
                self._read_denial_extension(observation, report)

        report.status = requested_report_status

        if observation.effectiveDateTime is not None:
            date_of_death = observation.effectiveDateTime.date
            if self._is_future_date(date_of_death):
                raise BadRequest(f'Report effective datetime can not be a future date, received {date_of_death}')
            report.dateOfDeath = date_of_death

        return report

    @staticmethod
    def _convert_to_utc_datetime(datetime):
        if datetime.tzinfo is None:
            return pytz.utc.localize(datetime)
        else:
            return datetime.astimezone(pytz.utc)

    def _to_fhir_date(self, datetime):
        utc_datetime = self._convert_to_utc_datetime(datetime)
        fhir_date = FHIRDate()
        fhir_date.date = utc_datetime
        return fhir_date

    def _add_performer_data(self, observation: Observation, user: ApiUser, datetime, is_author):
        performer = FHIRReference()
        performer.type = user.system
        performer.reference = user.username

        # Add extension for details on the user's action
        extension = Extension()
        extension.url = 'https://www.pmi-ops.org/observation/' + ('authored' if is_author else 'reviewed')
        extension.valueDateTime = self._to_fhir_date(datetime)
        performer.extension = [extension]

        observation.performer.append(performer)

    def to_client_json(self, model: DeceasedReport):
        """
        The FHIR Observation fields used for the deceased reports coming from the API are the same as for structures
        sent to the API when creating and reviewing reports with the exceptions outlined below:

        * Participant ID
            The *subject* field will be populated with the ID of deceased report's participant.

            .. code-block:: javascript

                {
                    ...
                    "subject": {
                        "reference": "P000000000"
                    },
                    ...
                }

        * Creator and reviewer
            For reviewed reports, the *performer* array will contain both the author of the report and the reviewer
            along with the dates that they took their actions.

            .. code-block:: javascript

                {
                    ...
                    "performer": [
                        {
                            "type": "https://www.pmi-ops.org/healthpro-username",
                            "reference": "user.name@pmi-ops.org"
                            "extension": [
                                {
                                    "url": "https://www.pmi-ops.org/observation/authored",
                                    "valueDateTime": "2020-01-31T08:34:12Z"
                                }
                            ]
                        },
                        {
                            "type": "https://www.pmi-ops.org/healthpro-username",
                            "reference": "another.user@pmi-ops.org"
                            "extension": [
                                {
                                    "url": "https://www.pmi-ops.org/observation/reviewed",
                                    "valueDateTime": "2020-02-05T09:00:27Z"
                                }
                            ]
                        }
                    ],
                    ...
                }

        """

        status_map = {
            DeceasedReportStatus.PENDING: 'preliminary',
            DeceasedReportStatus.APPROVED: 'final',
            DeceasedReportStatus.DENIED: 'cancelled'
        }

        observation = Observation()

        code = CodeableConcept()
        code.text = 'DeceasedReport'
        observation.code = code

        identifier = Identifier()
        identifier.value = str(model.id)
        observation.identifier = [identifier]

        subject = FHIRReference()
        subject.reference = to_client_participant_id(model.participantId)
        observation.subject = subject

        observation.status = status_map[model.status]

        observation.performer = []
        self._add_performer_data(observation, model.author, model.authored, is_author=True)
        try:
            if model.reviewer:
                self._add_performer_data(observation, model.reviewer, model.reviewed, is_author=False)
        except DetachedInstanceError:
            # With the current structure the reviewer will have been eager-loaded or set on the model,
            # but the model is detached and the reviewer is expected to be None on pending reports.
            # If the reviewer is None, sqlalchemy will try to check the database to see if it shouldn't be
            # and this exception type will result.
            pass

        encounter = FHIRReference()
        encounter.reference = str(model.notification)
        if model.notification == DeceasedNotification.OTHER:
            encounter.display = model.notificationOther
        observation.encounter = encounter

        if model.notification in [DeceasedNotification.NEXT_KIN_SUPPORT,
                                  DeceasedNotification.NEXT_KIN_HPO,
                                  DeceasedNotification.ATTEMPTED_CONTACT]:
            reporter_extension = Extension()
            reporter_extension.url = 'https://www.pmi-ops.org/deceased-reporter'

            human_name = HumanName()
            reporter_extension.valueHumanName = human_name

            human_name.text = model.reporterName

            human_name.extension = []
            relationship_extension = Extension()
            relationship_extension.url = 'http://hl7.org/fhir/ValueSet/relatedperson-relationshiptype'
            relationship_extension.valueCode = model.reporterRelationship
            human_name.extension.append(relationship_extension)

            if model.reporterEmail:
                email_extension = Extension()
                email_extension.url = 'https://www.pmi-ops.org/email-address'
                email_extension.valueString = model.reporterEmail
                human_name.extension.append(email_extension)
            if model.reporterPhone:
                phone_extension = Extension()
                phone_extension.url = 'https://www.pmi-ops.org/phone-number'
                phone_extension.valueString = model.reporterPhone
                human_name.extension.append(phone_extension)

            observation.extension = [reporter_extension]

        if model.status == DeceasedReportStatus.PENDING:
            observation.issued = self._to_fhir_date(model.authored)
        else:
            observation.issued = self._to_fhir_date(model.reviewed)

        date_of_death = FHIRDate()
        date_of_death.date = model.dateOfDeath
        observation.effectiveDateTime = date_of_death

        observation.valueString = model.causeOfDeath

        # Add denial reason extension
        if model.status == DeceasedReportStatus.DENIED:
            denial_reason_extension = Extension()
            denial_reason_extension.url = 'https://www.pmi-ops.org/observation-denial-reason'

            denial_reason_reference = FHIRReference()
            denial_reason_reference.reference = str(model.denialReason)
            if model.denialReason == DeceasedReportDenialReason.OTHER:
                denial_reason_reference.display = model.denialReasonOther
            denial_reason_extension.valueReference = denial_reason_reference

            observation.extension = [denial_reason_extension]

        return observation.as_json()

    @staticmethod
    def _deceased_report_lock_name(participant_id):
        return f'rdr.deceased_report.p{participant_id}'

    @staticmethod
    def _release_report_lock(session, participant_id):
        release_result = session.execute(
            f"SELECT RELEASE_LOCK('{DeceasedReportDao._deceased_report_lock_name(participant_id)}')"
        ).scalar()

        if release_result is None:
            logging.error(f'Deceased report lock did not exist for P{participant_id}!')
        elif release_result == 0:
            logging.error(f'Deceased report lock for P{participant_id} was not taken by this thread!')

    @staticmethod
    def _can_insert_active_report(session, participant_id, lock_timeout_seconds=30):
        # Obtain lock for creating a participant's deceased report
        # If the named lock is free, 1 is returned immediately. If the lock is already taken, then it waits until it's
        # free before making the check. Documentation gives that 'None' is returned in error cases.
        lock_result = session.execute(
            f"SELECT GET_LOCK('{DeceasedReportDao._deceased_report_lock_name(participant_id)}', {lock_timeout_seconds})"
        ).scalar()

        if lock_result == 1:
            # If we have the lock, we know we're the only transaction validating the insert.
            has_active_reports_query = session.query(DeceasedReport).filter(
                DeceasedReport.participantId == participant_id,
                DeceasedReport.status != DeceasedReportStatus.DENIED
            )

            if session.query(has_active_reports_query.exists()).scalar():
                raise Conflict(f'Participant P{participant_id} already has a preliminary or final deceased report')
            else:
                return True
        else:
            # If we got an error from the database or the lock was taken for 30 seconds then something's wrong
            logging.error(f'Database error retrieving named lock for P{participant_id}, '
                          f'received result: "{lock_result}"')
            raise InternalServerError('Unable to create deceased report')

    def is_valid(self, report: DeceasedReport):
        if self._is_future_datetime(report.authored):
            raise BadRequest(f'Report issued date can not be a future date, received {report.authored}')

        if report.notification == DeceasedNotification.NEXT_KIN_SUPPORT:
            if not report.reporterRelationship:
                raise BadRequest(f'Missing reporter relationship')

        return True

    def insert_with_session(self, session, obj: DeceasedReport):
        # Should auto-approve reports for unpaired participants
        participant = self._load_participant(obj.participantId)
        if participant.hpoId == 0:
            obj.status = DeceasedReportStatus.APPROVED
            obj.reviewer = obj.author
            obj.reviewed = obj.authored

        if self.is_valid(obj) and self._can_insert_active_report(session, obj.participantId):
            self._update_participant_summary(session, obj)
            insert_result = super(DeceasedReportDao, self).insert_with_session(session, obj)
            self._release_report_lock(session, obj.participantId)

            return insert_result

    def update_with_session(self, session, obj: DeceasedReport):
        self._update_participant_summary(session, obj)
        return super(DeceasedReportDao, self).update_with_session(session, obj)

    def get_id(self, obj: DeceasedReport):
        return obj.id

    def get_etag(self, id_, participant_id):  # pylint: disable=unused-argument
        return None

    def load_reports(self, participant_id=None, org_id=None, status=None):
        """
        Deceased reports can be listed for for individual participants, or all of the reports matching a given status
        (PENDING, APPROVED, OR DENIED) and/or organization id (such as UNSET). Reports will be listed by date of the
        last action taken on them (authored or reviewed) with the most recent reports appearing at the top.
        """
        ids_ignored_in_filter = config.getSettingJson(config.DECEASED_REPORT_FILTER_EXCEPTIONS, [])

        with self.session() as session:
            # Order reports by newest to oldest based on last date a user modified it
            query = session.query(DeceasedReport).join(Participant).order_by(
                desc(func.coalesce(DeceasedReport.reviewed, DeceasedReport.authored))
            ).filter(
                or_(
                    and_(
                        Participant.suspensionStatus == SuspensionStatus.NOT_SUSPENDED,
                        Participant.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN
                    ),
                    Participant.participantId.in_(ids_ignored_in_filter)
                )

            )
            if participant_id is not None:
                query = query.filter(DeceasedReport.participantId == participant_id)
            else:
                if org_id is not None:
                    if org_id == 'UNSET':
                        query = query.filter(Participant.organizationId.is_(None))
                    else:
                        # Join and filter by the participant's Organization
                        query = query.join(Organization).filter(Organization.externalId == org_id)
                if status is not None:
                    if status not in self.status_map:
                        raise BadRequest(f'Invalid status "{status}"')
                    query = query.filter(DeceasedReport.status == self.status_map[status])
            return query.all()
