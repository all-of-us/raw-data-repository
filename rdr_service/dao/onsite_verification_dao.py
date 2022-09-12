from werkzeug.exceptions import BadRequest
from sqlalchemy import desc

from rdr_service import clock
from rdr_service.dao.database_utils import parse_datetime
from rdr_service.model.utils import from_client_participant_id, to_client_participant_id
from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.onsite_id_verification import OnsiteIdVerification
from rdr_service.dao.site_dao import SiteDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.participant_enums import OnSiteVerificationType, OnSiteVerificationVisitType


class OnsiteVerificationDao(BaseDao):
    def __init__(self):
        super(OnsiteVerificationDao, self).__init__(OnsiteIdVerification)
        self.site_dao = SiteDao()

    def from_client_json(self, resource_json, client_id=None):  # pylint: disable=unused-argument
        self._validate(resource_json)
        participant_id = from_client_participant_id(resource_json.get('participantId'))
        site_google_group = resource_json.get('siteGoogleGroup')
        site = self.site_dao.get_by_google_group(site_google_group) if site_google_group else None
        verification_type = resource_json.get('verificationType') if resource_json.get('verificationType') else 'UNSET'
        visit_type = resource_json.get('visitType') if resource_json.get('visitType') else 'UNSET'
        onsite_id_verification = OnsiteIdVerification(
            participantId=participant_id,
            userEmail=resource_json.get('userEmail'),
            siteId=site.siteId if site else None,
            verifiedTime=parse_datetime(resource_json.get('verifiedTime')),
            verificationType=OnSiteVerificationType(verification_type),
            visitType=OnSiteVerificationVisitType(visit_type),
            resource=resource_json
        )

        return onsite_id_verification

    @staticmethod
    def _validate(resource_json):
        not_null_fields = ['participantId', 'verifiedTime']
        for field_name in not_null_fields:
            if resource_json.get(field_name) is None:
                raise BadRequest(f'{field_name} can not be NULL')


    def get_verification_history(self, participant_id):
        with self.session() as session:
            verification_list = session.query(OnsiteIdVerification)\
                .filter(OnsiteIdVerification.participantId == participant_id)\
                .order_by(desc(OnsiteIdVerification.verifiedTime)).all()
        result = {
            'entry': [
                self.to_client_json(item) for item in verification_list
            ]
        }

        return result

    def to_client_json(self, onsite_id_verification):
        site = self.site_dao.get(onsite_id_verification.siteId)
        verification_type = onsite_id_verification.verificationType if onsite_id_verification.verificationType\
            else 'UNSET'
        visit_type = onsite_id_verification.visitType if onsite_id_verification.visitType else 'UNSET'
        response_json = {
            "participantId": to_client_participant_id(onsite_id_verification.participantId),
            "verifiedTime": onsite_id_verification.verifiedTime,
            "userEmail": onsite_id_verification.userEmail,
            "siteGoogleGroup": site.googleGroup if site else None,
            "siteName": site.siteName if site else None,
            "verificationType": str(OnSiteVerificationType(verification_type)),
            "visitType": str(OnSiteVerificationVisitType(visit_type)),
        }

        return response_json

    def insert(self, obj):
        response = super().insert(obj)

        # update participant summary with id verification timestamp
        participant_summary_dao = ParticipantSummaryDao()
        participant_summary = participant_summary_dao.get_by_participant_id(obj.participantId)
        participant_summary.onsiteIdVerificationTime = obj.verifiedTime
        participant_summary.onsiteIdVerificationType = OnSiteVerificationType(obj.verificationType)
        participant_summary.onsiteIdVerificationVisitType = OnSiteVerificationVisitType(obj.visitType)
        participant_summary.onsiteIdVerificationUser = obj.userEmail
        participant_summary.onsiteIdVerificationSite = obj.siteId
        participant_summary.lastModified = clock.CLOCK.now()
        participant_summary_dao.update(participant_summary)

        return response
