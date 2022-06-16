import datetime
import json
import mock

from werkzeug.exceptions import BadRequest, Forbidden

from rdr_service.clock import FakeClock
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.physical_measurements_dao import PhysicalMeasurementsDao
from rdr_service.model.measurements import PhysicalMeasurements
from rdr_service.model.participant import Participant
from rdr_service.participant_enums import PhysicalMeasurementsStatus, WithdrawalStatus, \
    PhysicalMeasurementsCollectType, OriginMeasurementUnit
from rdr_service.query import FieldFilter, Operator, Query
from tests.helpers.unittest_base import BaseTestCase
from tests.test_data import data_path

TIME_1 = datetime.datetime(2016, 1, 1)
TIME_2 = datetime.datetime(2016, 1, 2)
TIME_3 = datetime.datetime(2016, 1, 3)
TIME_4 = datetime.datetime(2016, 1, 4)
TIME_5 = datetime.datetime(2016, 1, 5)


def load_measurement_json(participant_id, now=None, alternate=False):
    """
    Loads a PhysicalMeasurement FHIR resource returns it as parsed JSON.
    If alternate is True, loads a different measurement order. Useful for making multiple
    orders to test against when cancelling/restoring. The alternate has less measurements and
    different processed sites and finalized sites.
    """
    if alternate:
        payload = "alternate-measurements-as-fhir.json"
    else:
        payload = "measurements-as-fhir.json"
    with open(data_path(payload)) as measurements_file:
        json_text = measurements_file.read() % {
            "participant_id": participant_id,
            "authored_time": now or datetime.datetime.now().isoformat(),
        }
        return json.loads(json_text)  # deserialize to validate


def load_measurement_json_amendment(participant_id, amended_id, now=None):
    """
    Loads a PhysicalMeasurement FHIR resource and adds an amendment extension.
    """
    with open(data_path("measurements-as-fhir-amendment.json")) as amendment_file:
        extension = json.loads(amendment_file.read() % {"physical_measurement_id": amended_id})
    with open(data_path("measurements-as-fhir.json")) as measurements_file:
        measurement = json.loads(
            measurements_file.read()
            % {"participant_id": participant_id, "authored_time": now or datetime.datetime.now().isoformat()}
        )
    measurement["entry"][0]["resource"].update(extension)
    return measurement



class PhysicalMeasurementsDaoTest(BaseTestCase):
    def setUp(self):
        super(PhysicalMeasurementsDaoTest, self).setUp()
        self.participant = Participant(participantId=1, biobankId=2)
        ParticipantDao().insert(self.participant)

        self.dao = PhysicalMeasurementsDao()
        self.participant_summary_dao = ParticipantSummaryDao()
        self.measurement_json = json.dumps(load_measurement_json(self.participant.participantId, TIME_1.isoformat()))
        self.biobank = BiobankOrderDao()

        # Patching to prevent consent validation checks from running
        build_validator_patch = mock.patch(
            'rdr_service.services.consent.validation.ConsentValidationController.build_controller'
        )
        build_validator_patch.start()
        self.addCleanup(build_validator_patch.stop)

    def test_from_client_json(self):
        measurement = self.dao.from_client_json(json.loads(self.measurement_json))
        self.assertIsNotNone(measurement.createdSiteId)
        self.assertIsNotNone(measurement.finalizedSiteId)


    def test_authored_object(self):
        """
        DA-1435 Test PM document parse supports older incorrect author extension object usage.
        """
        authored_object = [
            {
                "reference": "Practitioner/creator@pmi-ops.org",
                "extension": {
                    "url": "http://terminology.pmi-ops.org/StructureDefinition/authoring-step",
                    "valueCode": "created"
                }
            },
            {
                "reference": "Practitioner/finalizer@pmi-ops.org",
                "extension": {
                    "url": "http://terminology.pmi-ops.org/StructureDefinition/authoring-step",
                    "valueCode": "finalized"
                }
            }
        ]

        resource = json.loads(self.measurement_json)
        resource['entry'][0]['resource']['author'] = authored_object

        result = self.dao.from_client_json(resource, participant_id=1)

        self.assertEqual(result.createdUsername, authored_object[0]['reference'].replace('Practitioner/', ''))
        self.assertEqual(result.finalizedUsername, authored_object[1]['reference'].replace('Practitioner/', ''))


    def test_authored_array(self):
        """
        DA-1435 Test PM document parse supports correct author extension array usage.
        """
        authored_array = [
            {
                "reference": "Practitioner/creator@pmi-ops.org",
                "extension": [{
                    "url": "http://terminology.pmi-ops.org/StructureDefinition/authoring-step",
                    "valueCode": "created"
                }]
            },
            {
                "reference": "Practitioner/finalizer@pmi-ops.org",
                "extension": [{
                    "url": "http://terminology.pmi-ops.org/StructureDefinition/authoring-step",
                    "valueCode": "finalized"
                }]
            }
        ]

        resource = json.loads(self.measurement_json)
        resource['entry'][0]['resource']['author'] = authored_array

        result = self.dao.from_client_json(resource, participant_id=1)

        self.assertEqual(result.createdUsername, authored_array[0]['reference'].replace('Practitioner/', ''))
        self.assertEqual(result.finalizedUsername, authored_array[1]['reference'].replace('Practitioner/', ''))

        self.assertEqual(1, 1)

    def _make_physical_measurements(self, **kwargs):
        """
        Makes a new PhysicalMeasurements (same values every time) with valid/complete defaults.
        Kwargs pass through to PM constructor, overriding defaults.
        """
        resource = json.loads(self.measurement_json)

        if 'resource' in kwargs:
            resource = json.loads(kwargs.pop('resource'))

        for k, default_value in (
            ("physicalMeasurementsId", 1),
            ("participantId", self.participant.participantId),
            ("createdSiteId", 1),
            ("finalizedSiteId", 2),
            ("origin", 'hpro'),
            ("collectType", PhysicalMeasurementsCollectType.SITE),
            ("originMeasurementUnit", OriginMeasurementUnit.UNSET)
        ):
            if k not in kwargs:
                kwargs[k] = default_value

        record = PhysicalMeasurements(**kwargs)
        self.dao.store_record_fhir_doc(record, resource)
        return record

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
            created=TIME_2,
            finalized=TIME_1,
            final=True,
            logPositionId=1,
            createdSiteId=1,
            finalizedSiteId=2,
            collectType=PhysicalMeasurementsCollectType.SITE,
            originMeasurementUnit=OriginMeasurementUnit.UNSET,
            origin='hpro'
        )

        doc = json.loads(self._with_id(self.measurement_json, "1"))
        expected_measurements = self.dao.store_record_fhir_doc(expected_measurements, doc)
        self.assertEqual(expected_measurements.asdict(), measurements.asdict())
        measurements = self.dao.get(measurements.physicalMeasurementsId)

        expected_measurements = self.dao.store_record_fhir_doc(expected_measurements, self.measurement_json)
        self.assertEqual(self.dao._measurements_as_dict(expected_measurements), self.dao._measurements_as_dict(measurements))
        # Completing physical measurements changes the participant summary status
        summary = ParticipantSummaryDao().get(self.participant.participantId)
        self.assertEqual(PhysicalMeasurementsStatus.COMPLETED, summary.physicalMeasurementsStatus)
        self.assertEqual(TIME_2, summary.physicalMeasurementsTime)
        self.assertEqual(TIME_2, summary.lastModified)

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
        self.assertEqual(self.dao._measurements_as_dict(measurements), self.dao._measurements_as_dict(measurements_2))

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
        amended_json, composition = self.dao.load_record_fhir_doc(measurements)
        self.assertEqual("amended", composition["status"])
        self.assertEqual("1", amended_json["id"])

        amendment_json, composition = self.dao.load_record_fhir_doc(new_measurements)
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
            with self.dao.session() as session:
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

    def test_cancel_fhir_doc(self):

        self._make_summary()
        summary = ParticipantSummaryDao().get(self.participant.participantId)
        self.assertIsNone(summary.physicalMeasurementsStatus)
        with FakeClock(TIME_2):
            measurements = self.dao.insert(self._make_physical_measurements())

        cancel = BaseTestCase.get_restore_or_cancel_info()

        with FakeClock(TIME_3):
            with self.dao.session() as session:
                update = self.dao.update_with_patch(measurements.physicalMeasurementsId, session, cancel)

        self.assertEqual(update.status, PhysicalMeasurementsStatus.CANCELLED)

        doc, composition = self.dao.load_record_fhir_doc(update)  # pylint: disable=unused-variable
        count = 0

        for ext in composition['extension']:
            if 'cancelled-site' in ext['url']:
                self.assertEqual(ext['valueInteger'], 1)
                count += 1
            if 'cancelled-time' in ext['url']:
                self.assertEqual(ext['valueString'], TIME_3.isoformat())
                count += 1
            if 'cancelled-username' in ext['url']:
                self.assertEqual(ext['valueString'], 'mike@pmi-ops.org')
                count += 1
            if 'cancelled-reason' in ext['url']:
                self.assertEqual(ext['valueString'], 'a mistake was made.')
                count += 1

        self.assertEqual(count, 4)  # Four cancelled extensions

    def test_restore_fhir_doc(self):

        self._make_summary()
        summary = ParticipantSummaryDao().get(self.participant.participantId)
        self.assertIsNone(summary.physicalMeasurementsStatus)
        with FakeClock(TIME_2):
            measurements = self.dao.insert(self._make_physical_measurements())

        cancel = BaseTestCase.get_restore_or_cancel_info()

        with FakeClock(TIME_3):
            with self.dao.session() as session:
                self.dao.update_with_patch(measurements.physicalMeasurementsId, session, cancel)

        reason = 'really was correct.'
        author = 'rob@pmi-ops.org'
        restore = BaseTestCase.get_restore_or_cancel_info(reason=reason, author=author, status='restored')

        with FakeClock(TIME_4):
            with self.dao.session() as session:
                update = self.dao.update_with_patch(measurements.physicalMeasurementsId, session, restore)

        doc, composition = self.dao.load_record_fhir_doc(update)  # pylint: disable=unused-variable
        count = 0

        self.assertEqual(update.status, PhysicalMeasurementsStatus.UNSET)

        for ext in composition['extension']:
            if 'restore-site' in ext['url']:
                self.assertEqual(ext['valueInteger'], 1)
                count += 1
            if 'restore-time' in ext['url']:
                self.assertEqual(ext['valueString'], TIME_4.isoformat())
                count += 1
            if 'restore-username' in ext['url']:
                self.assertEqual(ext['valueString'], author)
                count += 1
            if 'restore-reason' in ext['url']:
                self.assertEqual(ext['valueString'], reason)
                count += 1

        self.assertEqual(count, 4)  # Four cancelled extensions

    def test_resource_table_column(self):
        """
        Physical Measurements table resource column should save and load the same type.
        """

        self._make_summary()
        summary = ParticipantSummaryDao().get(self.participant.participantId)
        self.assertIsNone(summary.physicalMeasurementsStatus)
        with FakeClock(TIME_2):
            measurements = self.dao.insert(self._make_physical_measurements())

        with self.dao.session() as session:
            record = session.query(PhysicalMeasurements).filter(PhysicalMeasurements.participantId==self.participant.participantId).first()

        self.assertEqual(type(measurements.resource), type(record.resource))
