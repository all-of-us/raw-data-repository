import unittest

import mock
from testlib import testutil

from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.utils import from_client_participant_id
from rdr_service.offline.biobank_samples_pipeline import upsert_from_latest_csv
from rdr_service.participant_enums import SampleStatus
from rdr_service.test.test_data import load_biobank_order_json
from rdr_service.test.unit_test.unit_test_util import FlaskTestBase


def _callthrough(fn, *args, **kwargs):
    fn(*args, **kwargs)


# TODO: represent in new test suite
class DataGenApiTest(testutil.CloudStorageTestBase, FlaskTestBase):
    def setUp(self):
        # Neither CloudStorageTestBase nor our FlaskTestBase correctly calls through to
        # setup(..).setup(..), so explicitly call both here.
        testutil.CloudStorageTestBase.setUp(self)
        FlaskTestBase.setUp(self)

    @unittest.skip("DA-471")
    @mock.patch("google.appengine.ext.deferred.defer", new=_callthrough)
    def test_generate_samples(self):
        participant_id = self.send_post("Participant", {})["participantId"]
        self.send_consent(participant_id)
        self.send_post(
            "Participant/%s/BiobankOrder" % participant_id,
            load_biobank_order_json(from_client_participant_id(participant_id)),
        )

        # Sanity check that the orders were created correctly.
        bo_dao = BiobankOrderDao()
        self.assertEqual(1, bo_dao.count())
        order = bo_dao.get_all()[0]
        self.assertEqual(16, len(bo_dao.get_with_children(order.biobankOrderId).samples))

        self.send_post("DataGen", {"create_biobank_samples": True, "samples_missing_fraction": 0.0})
        upsert_from_latest_csv()  # Run the (usually offline) Biobank CSV import job.

        self.assertEqual(16, BiobankStoredSampleDao().count())
        ps = ParticipantSummaryDao().get(from_client_participant_id(participant_id))
        self.assertEqual(SampleStatus.RECEIVED, ps.samplesToIsolateDNA)
        self.assertEqual(13, ps.numBaselineSamplesArrived)
