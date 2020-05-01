import datetime

from rdr_service import clock
from rdr_service.dao.biobank_specimen_dao import BiobankSpecimenDao
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.participant import Participant
from rdr_service.model.biobank_order import BiobankSpecimen, BiobankOrderIdentifier, BiobankOrderedSample, BiobankOrder
from tests.helpers.unittest_base import BaseTestCase

TIME_1 = datetime.datetime(2020, 4, 1)
TIME_2 = datetime.datetime(2020, 4, 2)


class BiobankOrderApiTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.participant = Participant(participantId=123, biobankId=555)
        self.participant_dao = ParticipantDao()
        self.participant_dao.insert(self.participant)
        self.summary_dao = ParticipantSummaryDao()
        self.dao = BiobankSpecimenDao()
        self.bo_dao = BiobankOrderDao()
        self.specimen = BiobankSpecimen(rlimsId='sabrina', participantId=self.participant.participantId,
                                        orderId='', testCode='test 1234567', repositoryId='repo id', studyId='study id',
                                        cohortId='cohort id', collectionDate=TIME_1, confirmedDate=TIME_2,
                                        sampleType='sample')
        self.set_status()
        self.set_aliquot()
        self.specimen_path = f"Biobank/specimens/{self.specimen.rlimsId}"

    def set_status(self):
        status = {'status': 'good', 'freezeThawCount': 1,
                  'location': 'Greendale', 'quantity': '1', 'quantityUnits': 'some unit',
                  'processingCompleteDate': TIME_2, 'deviations': 'no deviation'}
        self.specimen.status = status

    def set_aliquot(self):
        aliquot = {}
        self.specimen.aliquots = aliquot

    def _make_biobank_order(self, **kwargs):
        """Makes a new BiobankOrder (same values every time) with valid/complete defaults.

        Kwargs pass through to BiobankOrder constructor, overriding defaults.
        """
        for k, default_value in (
            ("biobankOrderId", "1"),
            ("created", clock.CLOCK.now()),
            ("participantId", self.participant.participantId),
            ("sourceSiteId", 1),
            ("sourceUsername", "fred@pmi-ops.org"),
            ("collectedSiteId", 1),
            ("collectedUsername", "joe@pmi-ops.org"),
            ("processedSiteId", 1),
            ("processedUsername", "sue@pmi-ops.org"),
            ("finalizedSiteId", 2),
            ("finalizedUsername", "bob@pmi-ops.org"),
            ("identifiers", [BiobankOrderIdentifier(system="a", value="c")]),
            (
                "samples",
                [
                    BiobankOrderedSample(
                        biobankOrderId="1",
                        test='2SST8',
                        finalized=TIME_2,
                        description="description",
                        processingRequired=True,
                    )
                ],
            ),
        ):
            if k not in kwargs:
                kwargs[k] = default_value
        return BiobankOrder(**kwargs)

    def test_put_new_specimen(self):
        ParticipantSummaryDao().insert(self.participant_summary(self.participant))
        bio_order = self.bo_dao.insert(self._make_biobank_order(participantId=self.participant.participantId))
        self.specimen.orderId = bio_order.biobankOrderId
        payload = self.dao.to_client_json(self.specimen)
        result = self.send_put(self.specimen_path, request_data=payload, headers={"if-match": 'W/"1"'})

        specimen = self.dao.get((result['id'], result['orderId']))
        self.assertIsNotNone(specimen)

    def test_put_specimen_exists(self):
        ParticipantSummaryDao().insert(self.participant_summary(self.participant))
        bio_order = self.bo_dao.insert(self._make_biobank_order(participantId=self.participant.participantId))
        self.specimen.orderId = bio_order.biobankOrderId
        payload = self.dao.to_client_json(self.specimen)
        result = self.send_put(self.specimen_path, request_data=payload, headers={"if-match": 'W/"1"'})
        new_payload = payload
        new_payload['cohortId'] = 'next cohort'
        new_payload['rlimsId'] = 'next rlimsId'
        print(result)

        # TODO: Are we handling if-match headers? Not in design.
        self.send_put(self.specimen_path, request_data=new_payload, headers={"if-match": 'W/"1"'})
