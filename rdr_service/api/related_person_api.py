
from flask_restful import Resource

from rdr_service.api.base_api import ApiUtilMixin
from rdr_service.api_util import PTC
from rdr_service.app_util import auth_required
from rdr_service.clock import CLOCK
from rdr_service.dao.account_link_dao import AccountLinkDao
from rdr_service.lib_fhir.fhirclient_4_0_0.models.relatedperson import RelatedPerson
from rdr_service.model.account_link import AccountLink
from rdr_service.model.utils import from_client_participant_id


class RelatedPersonApi(Resource, ApiUtilMixin):
    @auth_required(PTC)
    def post(self):
        json = self.get_request_json()
        related_person = RelatedPerson(json)

        link_start = related_person.period.start.isostring if related_person.period.start else CLOCK.now()
        link_end = related_person.period.end.isostring if related_person.period.end else None
        child_pid = from_client_participant_id(related_person.patient.reference.split('/')[1])
        guardian_pid = from_client_participant_id(related_person.identifier[0].value)

        AccountLinkDao.save_account_link(
            AccountLink(
                created=CLOCK.now(),
                modified=CLOCK.now(),
                start=link_start,
                end=link_end,
                participant_id=child_pid,
                related_id=guardian_pid
            )
        )

        return json
