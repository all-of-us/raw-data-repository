import datetime
import json

from werkzeug.exceptions import BadRequest, Forbidden

from rdr_service.clock import FakeClock
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.physical_measurements_dao import PhysicalMeasurementsDao
from rdr_service.model.measurements import PhysicalMeasurements
from rdr_service.model.participant import Participant
from rdr_service.participant_enums import PhysicalMeasurementsStatus, WithdrawalStatus
from rdr_service.query import FieldFilter, Operator, Query
from rdr_service.test.test_data import load_measurement_json, load_measurement_json_amendment
from tests.helpers.unittest_base import BaseTestCase

TIME_1 = datetime.datetime(2016, 1, 1)
TIME_2 = datetime.datetime(2016, 1, 2)
TIME_3 = datetime.datetime(2016, 1, 3)
TIME_4 = datetime.datetime(2016, 1, 4)
TIME_5 = datetime.datetime(2016, 1, 5)


class PhysicalMeasurementsDaoTest(BaseTestCase):
    def setUp(self):
        super(PhysicalMeasurementsDaoTest, self).setUp()
        self.participant = Participant(participantId=1, biobankId=2)
        ParticipantDao().insert(self.participant)
        self.dao = PhysicalMeasurementsDao()
        self.participant_summary_dao = ParticipantSummaryDao()
        self.measurement_json = json.dumps(load_measurement_json(self.participant.participantId, TIME_1.isoformat()))
        self.biobank = BiobankOrderDao()

    def test_from_client_json(self):
        measurement = PhysicalMeasurementsDao.from_client_json(json.loads(self.measurement_json))
        self.assertIsNotNone(measurement.createdSiteId)
        self.assertIsNotNone(measurement.finalizedSiteId)

    def _make_physical_measurements(self, **kwargs):
        """Makes a new PhysicalMeasurements (same values every time) with valid/complete defaults.

    Kwargs pass through to PM constructor, overriding defaults.
    """
        for k, default_value in (
            ("physicalMeasurementsId", 1),
            ("participantId", self.participant.participantId),
            ("resource", self.measurement_json),
            ("createdSiteId", 1),
            ("finalizedSiteId", 2),
        ):
            if k not in kwargs:
                kwargs[k] = default_value
        return PhysicalMeasurements(**kwargs)

    def testInsert_noParticipantId(self):
        with self.assertRaises(BadRequest):
            self.dao.insert(self._make_physical_measurements(participantId=None))

    def testInsert_wrongParticipantId(self):
        with self.assertRaises(BadRequest):
            self.dao.insert(self._make_physical_measurements(participantId=2))

    def _with_id(self, resource, id_):
        measurements_json = json.loads(resource)
        measurements_json["id"] = id_
        return json.dumps(measurements_json)

    def testInsert_noSummary(self):
        with self.assertRaises(BadRequest):
            self.dao.insert(self._make_physical_measurements())

    def _make_summary(self):
        self.participant_summary_dao.insert(self.participant_summary(self.participant))

    def testInsert_rightParticipantId(self):
        self._make_summary()
        summary = ParticipantSummaryDao().get(self.participant.participantId)
        self.assertIsNone(summary.physicalMeasurementsStatus)
        with FakeClock(TIME_2):
            measurements = self.dao.insert(self._make_physical_measurements())

        expected_measurements = PhysicalMeasurements(
            physicalMeasurementsId=1,
            participantId=self.participant.participantId,
            resource=self._with_id(self.measurement_json, "1"),
            created=TIME_2,
            finalized=TIME_1,
            final=True,
            logPositionId=1,
            createdSiteId=1,
            finalizedSiteId=2,
        )
        self.assertEqual(expected_measurements.asdict(), measurements.asdict())
        measurements = self.dao.get(measurements.physicalMeasurementsId)
        self.assertEqual(expected_measurements.asdict(), measurements.asdict())
        # Completing physical measurements changes the participant summary status
        summary = ParticipantSummaryDao().get(self.participant.participantId)
        self.assertEqual(PhysicalMeasurementsStatus.COMPLETED, summary.physicalMeasurementsStatus)
        self.assertEqual(TIME_2, summary.physicalMeasurementsTime)
        self.assertEqual(TIME_2, summary.lastModified)

    def test_backfill_is_noop(self):
        self._make_summary()
        measurements_id = self.dao.insert(self._make_physical_measurements()).physicalMeasurementsId
        orig_measurements = self.dao.get_with_children(measurements_id).asdict()
        self.dao.backfill_measurements()
        backfilled_measurements = self.dao.get_with_children(measurements_id).asdict()
        # Formatting of resource gets changed, so test it separately as parsed JSON.
        self.assertEqual(json.loads(orig_measurements["resource"]), json.loads(backfilled_measurements["resource"]))
        del orig_measurements["resource"]
        del backfilled_measurements["resource"]
        self.assertEqual(orig_measurements, backfilled_measurements)

    def testInsert_withdrawnParticipantFails(self):
        self.participant.withdrawalStatus = WithdrawalStatus.NO_USE
        ParticipantDao().update(self.participant)
        self._make_summary()
        summary = ParticipantSummaryDao().get(self.participant.participantId)
        self.assertIsNone(summary.physicalMeasurementsStatus)
        with self.assertRaises(Forbidden):
            self.dao.insert(self._make_physical_measurements())

    def testInsert_getFailsForWithdrawnParticipant(self):
        self._make_summary()
        self.dao.insert(self._make_physical_measurements())
        self.participant.version += 1
        self.participant.withdrawalStatus = WithdrawalStatus.NO_USE
        ParticipantDao().update(self.participant)
        with self.assertRaises(Forbidden):
            self.dao.get(1)
        with self.assertRaises(Forbidden):
            self.dao.query(
                Query([FieldFilter("participantId", Operator.EQUALS, self.participant.participantId)], None, 10, None)
            )

    def testInsert_duplicate(self):
        self._make_summary()
        with FakeClock(TIME_2):
            measurements = self.dao.insert(self._make_physical_measurements())
        with FakeClock(TIME_3):
            measurements_2 = self.dao.insert(self._make_physical_measurements())
        self.assertEqual(measurements.asdict(), measurements_2.asdict())

    def testInsert_amend(self):
        self._make_summary()
        with FakeClock(TIME_2):
            measurements = self.dao.insert(self._make_physical_measurements(physicalMeasurementsId=1))

        amendment_json = load_measurement_json_amendment(
            self.participant.participantId, measurements.physicalMeasurementsId, TIME_2
        )
        with FakeClock(TIME_3):
            new_measurements = self.dao.insert(
                self._make_physical_measurements(physicalMeasurementsId=2, resource=json.dumps(amendment_json))
            )

        measurements = self.dao.get(measurements.physicalMeasurementsId)
        amended_json = json.loads(measurements.resource)
        self.assertEqual("amended", amended_json["entry"][0]["resource"]["status"])
        self.assertEqual("1", amended_json["id"])

        amendment_json = json.loads(new_measurements.resource)
        self.assertEqual("2", amendment_json["id"])
        self.assertTrue(new_measurements.final)
        self.assertEqual(TIME_3, new_measurements.created)
        ps_dao = ParticipantSummaryDao().get(self.participant.participantId)
        # An amendment should not add a distinct visit count to summary
        self.assertEqual(ps_dao.numberDistinctVisits, 1)

    def test_update_with_patch_cancel(self):
        self._make_summary()
        summary = ParticipantSummaryDao().get(self.participant.participantId)
        self.assertIsNone(summary.physicalMeasurementsStatus)
        with FakeClock(TIME_2):
            measurements = self.dao.insert(self._make_physical_measurements())

        cancel = BaseTestCase.get_restore_or_cancel_info()

        with FakeClock(TIME_3):
            with PhysicalMeasurementsDao().session() as session:
                update = self.dao.update_with_patch(measurements.physicalMeasurementsId, session, cancel)
        self.assertEqual(update.status, PhysicalMeasurementsStatus.CANCELLED)
        self.assertEqual(update.reason, cancel["reason"])
        self.assertEqual(update.cancelledSiteId, 1)
        self.assertEqual(update.cancelledTime, TIME_3)
        self.assertEqual(update.cancelledUsername, cancel["cancelledInfo"]["author"]["value"])

        summary = ParticipantSummaryDao().get(self.participant.participantId)
        self.assertEqual(summary.physicalMeasurementsStatus, PhysicalMeasurementsStatus.CANCELLED)
        self.assertEqual(summary.physicalMeasurementsTime, None)
        self.assertEqual(summary.physicalMeasurementsFinalizedTime, None)
        self.assertEqual(summary.physicalMeasurementsCreatedSiteId, 1)
        self.assertEqual(summary.physicalMeasurementsFinalizedSiteId, None)

        with FakeClock(TIME_3):
            measurements = self.dao.insert(self._make_physical_measurements(physicalMeasurementsId=2))

        with FakeClock(TIME_4):
            self.dao.insert(self._make_physical_measurements(physicalMeasurementsId=3))

        summary = ParticipantSummaryDao().get(self.participant.participantId)
        self.assertEqual(summary.physicalMeasurementsStatus, PhysicalMeasurementsStatus.COMPLETED)
        self.assertEqual(summary.physicalMeasurementsTime, TIME_4)
        self.assertEqual(summary.physicalMeasurementsFinalizedTime, TIME_1)
        self.assertEqual(summary.physicalMeasurementsCreatedSiteId, 1)
        self.assertEqual(summary.physicalMeasurementsFinalizedSiteId, 2)
