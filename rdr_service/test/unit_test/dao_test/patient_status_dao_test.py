import copy

from dateutil.parser import parse

from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.patient_status_dao import PatientStatusDao
from rdr_service.model.participant import Participant
from rdr_service.test.unit_test.unit_test_util import FlaskTestBase


class PatientStatusTestBase(FlaskTestBase):
    def setUp(self, use_mysql=True, with_data=True, with_consent_codes=False):
        super(PatientStatusTestBase, self).setUp(
            use_mysql=use_mysql, with_data=with_data, with_consent_codes=with_consent_codes
        )

        self.test_data = {
            "subject": "Patient/P123456789",
            "awardee": "PITT",
            "organization": "PITT_BANNER_HEALTH",
            "patient_status": "YES",
            "user": "john.doe@pmi-ops.org",
            "site": "hpo-site-monroeville",
            "authored": "2019-04-26T12:11:41Z",
            "comment": "This is comment",
        }

        self.dao = PatientStatusDao()
        self.participant_dao = ParticipantDao()
        self.summary_dao = ParticipantSummaryDao()

        self.participant = Participant(participantId=123456789, biobankId=7)
        self.participant_dao.insert(self.participant)
        self.summary = self.participant_summary(self.participant)
        self.summary_dao.insert(self.summary)

    def test_patient_status(self):

        data = copy.copy(self.test_data)
        model = self.dao.from_client_json(data, participant_id=self.participant.participantId)
        self.dao.insert(model)
        result = self.dao.get(self.participant.participantId, data["organization"])

        self.assertEqual(result["subject"], data["subject"])
        self.assertEqual(result["organization"], data["organization"])
        self.assertEqual(result["site"], data["site"])
        self.assertEqual(parse(result["authored"]), parse(data["authored"]).replace(tzinfo=None))
        self.assertEqual(result["comment"], data["comment"])

        # Test changing site
        data["authored"] = "2019-04-27T16:32:01Z"
        data["comment"] = "saw patient at new site"
        data["site"] = "hpo-site-bannerphoenix"
        model = self.dao.from_client_json(data, participant_id=self.participant.participantId)
        self.dao.update(model)
        result = self.dao.get(self.participant.participantId, data["organization"])

        self.assertEqual(result["subject"], data["subject"])
        self.assertEqual(result["organization"], data["organization"])
        self.assertEqual(result["site"], data["site"])
        self.assertEqual(parse(result["authored"]), parse(data["authored"]).replace(tzinfo=None))
        self.assertEqual(result["comment"], data["comment"])

    def test_patient_status_query(self):

        with self.dao.session() as session:
            query = self.dao._build_response_query(session, self.participant.participantId, 3)
            sql = self.dao.query_to_text(query)
            self.assertIsNotNone(sql)

    # TODO: When new style history tables and triggers have been added to unit tests, test dao.get_history().
