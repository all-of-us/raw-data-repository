import datetime
import http.client
import json

from rdr_service import main
from rdr_service.clock import FakeClock
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.physical_measurements_dao import PhysicalMeasurementsDao
from rdr_service.model.measurements import Measurement
from rdr_service.model.utils import from_client_participant_id
from rdr_service.participant_enums import UNSET_HPO_ID
from tests.test_data import data_path, load_measurement_json, load_measurement_json_amendment
from tests.helpers.unittest_base import BaseTestCase


class PhysicalMeasurementsApiTest(BaseTestCase):
    def setUp(self):
        super(PhysicalMeasurementsApiTest, self).setUp()
        self.participant_id = self.create_participant()
        self.participant_id_2 = self.create_participant()
        self.time1 = datetime.datetime(2018, 1, 1)
        self.time2 = datetime.datetime(2018, 2, 2)

    def _insert_measurements(self, now=None):
        measurements_1 = load_measurement_json(self.participant_id, now)
        measurements_2 = load_measurement_json(self.participant_id_2, now)
        path_1 = "Participant/%s/PhysicalMeasurements" % self.participant_id
        path_2 = "Participant/%s/PhysicalMeasurements" % self.participant_id_2
        self.send_post(path_1, measurements_1)
        self.send_post(path_2, measurements_2)

    def test_insert_before_consent_fails(self):
        measurements_1 = load_measurement_json(self.participant_id)
        path_1 = "Participant/%s/PhysicalMeasurements" % self.participant_id
        self.send_post(path_1, measurements_1, expected_status=http.client.BAD_REQUEST)

    def test_insert(self):
        self.send_consent(self.participant_id)
        self.send_consent(self.participant_id_2)
        # now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        now = self.time1
        self._insert_measurements(now.isoformat())

        response = self.send_get("Participant/%s/PhysicalMeasurements" % self.participant_id)
        self.assertEqual("Bundle", response["resourceType"])
        self.assertEqual("searchset", response["type"])
        self.assertFalse(response.get("link"))
        self.assertTrue(response.get("entry"))
        self.assertEqual(1, len(response["entry"]))

        physical_measurements_id = response["entry"][0]["resource"]["id"]
        pm_id = int(physical_measurements_id)
        physical_measurements = PhysicalMeasurementsDao().get_with_children(physical_measurements_id)
        self.assertEqual(physical_measurements.createdSiteId, 1)
        self.assertIsNone(physical_measurements.createdUsername)
        self.assertEqual(physical_measurements.finalizedSiteId, 2)
        self.assertIsNone(physical_measurements.finalizedUsername)

        em1 = Measurement(
            measurementId=pm_id * 1000,
            physicalMeasurementsId=pm_id,
            codeSystem="http://terminology.pmi-ops.org/CodeSystem/physical-measurements",
            codeValue="systolic-diastolic-blood-pressure-1",
            measurementTime=now,
            bodySiteCodeSystem="http://snomed.info/sct",
            bodySiteCodeValue="368209003",
        )
        bp1 = Measurement(
            measurementId=pm_id * 1000 + 1,
            physicalMeasurementsId=pm_id,
            codeSystem="http://terminology.pmi-ops.org/CodeSystem/physical-measurements",
            codeValue="systolic-blood-pressure-1",
            measurementTime=now,
            valueDecimal=109.0,
            valueUnit="mm[Hg]",
            parentId=em1.measurementId,
        )
        bp2 = Measurement(
            measurementId=pm_id * 1000 + 2,
            physicalMeasurementsId=pm_id,
            codeSystem="http://loinc.org",
            codeValue="8462-4",
            measurementTime=now,
            valueDecimal=44.0,
            valueUnit="mm[Hg]",
            parentId=em1.measurementId,
        )
        m1 = physical_measurements.measurements[0]
        self.assertEqual(em1.asdict(), m1.asdict())
        self.assertEqual(2, len(m1.measurements))
        self.assertEqual(bp1.asdict(), m1.measurements[0].asdict())
        self.assertEqual(bp2.asdict(), m1.measurements[1].asdict())

        response = self.send_get("Participant/%s/PhysicalMeasurements" % self.participant_id_2)
        self.assertEqual("Bundle", response["resourceType"])
        self.assertEqual("searchset", response["type"])
        self.assertFalse(response.get("link"))
        self.assertTrue(response.get("entry"))
        self.assertEqual(1, len(response["entry"]))

    def test_insert_and_amend(self):
        self.send_consent(self.participant_id)
        measurements_1 = load_measurement_json(self.participant_id)
        path_1 = "Participant/%s/PhysicalMeasurements" % self.participant_id
        response = self.send_post(path_1, measurements_1)
        measurements_2 = load_measurement_json_amendment(self.participant_id, response["id"])
        self.send_post(path_1, measurements_2)

        response = self.send_get("Participant/%s/PhysicalMeasurements" % self.participant_id)
        self.assertEqual(2, len(response["entry"]))
        self.assertEqual("amended", response["entry"][0]["resource"]["entry"][0]["resource"]["status"])

    def test_insert_with_qualifiers(self):
        self.send_consent(self.participant_id)
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        now = self.time2
        with open(data_path("physical_measurements_2.json")) as measurements_file:
            json_text = measurements_file.read() % {
                "participant_id": self.participant_id,
                "authored_time": now.isoformat(),
            }
        measurements_json = json.loads(json_text)
        path_1 = "Participant/%s/PhysicalMeasurements" % self.participant_id
        self.send_post(path_1, measurements_json)

        response = self.send_get("Participant/%s/PhysicalMeasurements" % self.participant_id)
        self.assertEqual("Bundle", response["resourceType"])
        self.assertEqual("searchset", response["type"])
        self.assertFalse(response.get("link"))
        self.assertTrue(response.get("entry"))
        self.assertEqual(1, len(response["entry"]))

        physical_measurements_id = response["entry"][0]["resource"]["id"]
        pm_id = int(physical_measurements_id)
        physical_measurements = PhysicalMeasurementsDao().get_with_children(physical_measurements_id)
        self.assertEqual(physical_measurements.finalizedSiteId, 1)
        self.assertEqual("fred.smith@pmi-ops.org", physical_measurements.finalizedUsername)
        # Site not present in DB, so we don't set it.
        self.assertIsNone(physical_measurements.createdSiteId)
        self.assertEqual("bob.jones@pmi-ops.org", physical_measurements.createdUsername)

        em1_id = pm_id * 1000
        bp1 = Measurement(
            measurementId=pm_id * 1000 + 1,
            physicalMeasurementsId=pm_id,
            codeSystem="http://loinc.org",
            codeValue="8480-6",
            measurementTime=now,
            valueDecimal=109.0,
            valueUnit="mm[Hg]",
            parentId=em1_id,
        )
        bp2 = Measurement(
            measurementId=pm_id * 1000 + 2,
            physicalMeasurementsId=pm_id,
            codeSystem="http://loinc.org",
            codeValue="8462-4",
            measurementTime=now,
            valueDecimal=44.0,
            valueUnit="mm[Hg]",
            parentId=em1_id,
        )
        bp3 = Measurement(
            measurementId=pm_id * 1000 + 3,
            physicalMeasurementsId=pm_id,
            codeSystem="http://terminology.pmi-ops.org/CodeSystem/physical-measurements",
            codeValue="arm-circumference",
            measurementTime=now,
            valueDecimal=32.0,
            valueUnit="cm",
            parentId=em1_id,
        )
        em1 = Measurement(
            measurementId=pm_id * 1000,
            physicalMeasurementsId=pm_id,
            codeSystem="http://loinc.org",
            codeValue="55284-4",
            measurementTime=now,
            bodySiteCodeSystem="http://snomed.info/sct",
            bodySiteCodeValue="368209003",
            measurements=[bp1, bp2, bp3],
        )

        pm_height_system = "http://terminology.pmi-ops.org/CodeSystem/protocol-modifications-height"
        q1 = Measurement(
            measurementId=pm_id * 1000 + 4,
            physicalMeasurementsId=pm_id,
            codeSystem="http://terminology.pmi-ops.org/CodeSystem/physical-measurements",
            codeValue="protocol-modifications-height",
            measurementTime=now,
            valueCodeSystem=pm_height_system,
            valueCodeValue="hair-style",
            valueCodeDescription="Hair style"
        )

        em2 = Measurement(
            measurementId=pm_id * 1000 + 5,
            physicalMeasurementsId=pm_id,
            codeSystem="http://terminology.pmi-ops.org/CodeSystem/physical-measurements",
            codeValue="pre-pregnancy-weight",
            measurementTime=now,
            valueDecimal=28.0,
            valueUnit="kg",
        )
        pm_weight_system = "http://terminology.pmi-ops.org/CodeSystem/protocol-modifications-weight"
        q2 = Measurement(
            measurementId=pm_id * 1000 + 6,
            physicalMeasurementsId=pm_id,
            codeSystem="http://terminology.pmi-ops.org/CodeSystem/physical-measurements",
            codeValue="protocol-modifications-weight",
            measurementTime=now,
            valueCodeSystem=pm_weight_system,
            valueCodeValue="other",
            valueCodeDescription="Participant could not remove boots weight 20 pounds"
        )
        em3 = Measurement(
            measurementId=pm_id * 1000 + 7,
            physicalMeasurementsId=pm_id,
            codeSystem="http://loinc.org",
            codeValue="39156-5",
            measurementTime=now,
            valueDecimal=24.2,
            valueUnit="kg/m2",
        )
        # Skip a bunch -- could add these later
        em4 = Measurement(
            measurementId=pm_id * 1000 + 14,
            physicalMeasurementsId=pm_id,
            codeSystem="http://loinc.org",
            codeValue="8302-2",
            measurementTime=now,
            valueDecimal=111.5,
            valueUnit="cm",
            qualifiers=[q1],
        )
        em5 = Measurement(
            measurementId=pm_id * 1000 + 15,
            physicalMeasurementsId=pm_id,
            codeSystem="http://loinc.org",
            codeValue="29463-7",
            measurementTime=now,
            valueDecimal=30.1,
            valueUnit="kg",
            qualifiers=[q2],
        )

        m = {
            measurement.measurementId: measurement.asdict(follow={"measurements": {}, "qualifiers": {}})
            for measurement in physical_measurements.measurements
        }

        for em in [em1, bp1, bp2, bp3, q1, em2, q2, em3, em4, em5]:
            self.assertEqual(em.asdict(follow={"measurements": {}, "qualifiers": {}}), m.get(em.measurementId))

    def test_physical_measurements_sync(self):
        self.send_consent(self.participant_id)
        self.send_consent(self.participant_id_2)
        sync_response = self.send_get("PhysicalMeasurements/_history")
        self.assertEqual("Bundle", sync_response["resourceType"])
        self.assertEqual("history", sync_response["type"])
        link = sync_response.get("link")
        self.assertIsNone(link)
        self.assertTrue(len(sync_response["entry"]) == 0)

        self._insert_measurements()

        sync_response = self.send_get("PhysicalMeasurements/_history?_count=1")
        self.assertTrue(sync_response.get("entry"))
        link = sync_response.get("link")
        self.assertTrue(link)
        self.assertEqual("next", link[0]["relation"])
        self.assertEqual(1, len(sync_response["entry"]))
        prefix_index = link[0]["url"].index(main.API_PREFIX)
        relative_url = link[0]["url"][prefix_index + len(main.API_PREFIX):]

        sync_response_2 = self.send_get(relative_url)
        self.assertEqual(1, len(sync_response_2["entry"]))
        self.assertNotEqual(sync_response["entry"][0], sync_response_2["entry"][0])

    def test_auto_pair_called(self):
        pid_numeric = from_client_participant_id(self.participant_id)
        participant_dao = ParticipantDao()
        self.send_consent(self.participant_id)
        self.send_consent(self.participant_id_2)
        self.assertEqual(participant_dao.get(pid_numeric).hpoId, UNSET_HPO_ID)
        self._insert_measurements(datetime.datetime.utcnow().isoformat())
        self.assertNotEqual(participant_dao.get(pid_numeric).hpoId, UNSET_HPO_ID)

    def get_composition_resource_from_fhir_doc(self, data):
        """
        Return the Composition object from the PM Bundle
        :param data: dict
        :return: dict
        """
        for entry in data['entry']:
            if entry['resource'].get('resourceType', '') == 'Composition':
                return entry['resource']
        return None


    def test_cancel_a_physical_measurement(self):
        _id = self.participant_id.strip("P")
        self.send_consent(self.participant_id)
        measurement = load_measurement_json(self.participant_id)
        measurement2 = load_measurement_json(self.participant_id)
        path = "Participant/%s/PhysicalMeasurements" % self.participant_id
        response = self.send_post(path, measurement)
        # send another PM
        self.send_post(path, measurement2)
        path = path + "/" + response["id"]
        cancel_info = BaseTestCase.get_restore_or_cancel_info()
        ps = self.send_get("ParticipantSummary?participantId=%s" % _id)
        self.send_patch(path, cancel_info)

        response = self.send_get(path)
        composition = self.get_composition_resource_from_fhir_doc(response)
        self.assertEqual(composition["status"], "entered-in-error")
        count = 0
        for ext in composition['extension']:
            if 'cancelled-site' in ext['url']:
                self.assertEqual(ext['valueInteger'], 1)
                count += 1
            if 'cancelled-username' in ext['url']:
                self.assertEqual(ext['valueString'], 'mike@pmi-ops.org')
                count += 1
            if 'cancelled-reason' in ext['url']:
                self.assertEqual(ext['valueString'], 'a mistake was made.')
                count += 1
            if 'authored-location' in ext['url']:
                self.assertEqual(ext['valueString'], 'Location/hpo-site-monroeville')
                count += 1
            if 'finalized-location' in ext['url']:
                self.assertEqual(ext['valueString'], 'Location/hpo-site-bannerphoenix')
                count += 1
        self.assertEqual(count, 5)

        ps = self.send_get("ParticipantSummary?participantId=%s" % _id)
        # should be completed because of other valid PM
        self.assertEqual(ps["entry"][0]["resource"]["physicalMeasurementsStatus"], "COMPLETED")
        self.assertEqual(ps["entry"][0]["resource"]["physicalMeasurementsCreatedSite"], "hpo-site-monroeville")
        self.assertEqual(ps["entry"][0]["resource"]["physicalMeasurementsFinalizedSite"], "hpo-site-bannerphoenix")

    def test_make_pm_after_cancel_first_pm(self):
        _id = self.participant_id.strip("P")
        self.send_consent(self.participant_id)
        measurement = load_measurement_json(self.participant_id)
        measurement2 = load_measurement_json(self.participant_id)
        path = "Participant/%s/PhysicalMeasurements" % self.participant_id
        response = self.send_post(path, measurement)
        path = path + "/" + response["id"]
        cancel_info = BaseTestCase.get_restore_or_cancel_info()
        self.send_patch(path, cancel_info)
        # send another PM
        path = "Participant/%s/PhysicalMeasurements" % self.participant_id
        self.send_post(path, measurement2)

        path = path + "/" + response["id"]
        response = self.send_get(path)
        composition = self.get_composition_resource_from_fhir_doc(response)
        self.assertEqual(composition["status"], "entered-in-error")
        count = 0
        for ext in composition['extension']:
            if 'cancelled-site' in ext['url']:
                self.assertEqual(ext['valueInteger'], 1)
                count += 1
            if 'cancelled-username' in ext['url']:
                self.assertEqual(ext['valueString'], 'mike@pmi-ops.org')
                count += 1
            if 'cancelled-reason' in ext['url']:
                self.assertEqual(ext['valueString'], 'a mistake was made.')
                count += 1
            if 'authored-location' in ext['url']:
                self.assertEqual(ext['valueString'], 'Location/hpo-site-monroeville')
                count += 1
            if 'finalized-location' in ext['url']:
                self.assertEqual(ext['valueString'], 'Location/hpo-site-bannerphoenix')
                count += 1
        self.assertEqual(count, 5)

        ps = self.send_get("ParticipantSummary?participantId=%s" % _id)
        # should be completed because of other valid PM
        self.assertEqual(ps["entry"][0]["resource"]["physicalMeasurementsStatus"], "COMPLETED")
        self.assertEqual(ps["entry"][0]["resource"]["physicalMeasurementsCreatedSite"], "hpo-site-monroeville")
        self.assertEqual(ps["entry"][0]["resource"]["physicalMeasurementsFinalizedSite"], "hpo-site-bannerphoenix")
        self.assertIsNotNone(ps["entry"][0]["resource"]["physicalMeasurementsTime"])

    def test_make_pm_after_cancel_latest_pm(self):
        _id = self.participant_id.strip("P")
        self.send_consent(self.participant_id)
        measurement = load_measurement_json(self.participant_id)
        measurement2 = load_measurement_json(self.participant_id)
        path = "Participant/%s/PhysicalMeasurements" % self.participant_id
        with FakeClock(self.time1):
            self.send_post(path, measurement)
        with FakeClock(self.time2):
            response2 = self.send_post(path, measurement2)

        # cancel latest PM
        path = path + "/" + response2["id"]
        cancel_info = BaseTestCase.get_restore_or_cancel_info()
        self.send_patch(path, cancel_info)

        response = self.send_get(path)
        composition = self.get_composition_resource_from_fhir_doc(response)
        self.assertEqual(composition["status"], "entered-in-error")
        count = 0
        for ext in composition['extension']:
            if 'cancelled-site' in ext['url']:
                self.assertEqual(ext['valueInteger'], 1)
                count += 1
            if 'cancelled-username' in ext['url']:
                self.assertEqual(ext['valueString'], 'mike@pmi-ops.org')
                count += 1
            if 'cancelled-reason' in ext['url']:
                self.assertEqual(ext['valueString'], 'a mistake was made.')
                count += 1
            if 'authored-location' in ext['url']:
                self.assertEqual(ext['valueString'], 'Location/hpo-site-monroeville')
                count += 1
            if 'finalized-location' in ext['url']:
                self.assertEqual(ext['valueString'], 'Location/hpo-site-bannerphoenix')
                count += 1
        self.assertEqual(count, 5)

        ps = self.send_get("ParticipantSummary?participantId=%s" % _id)

        # should still get first PM in participant summary
        self.assertEqual(ps["entry"][0]["resource"]["physicalMeasurementsStatus"], "COMPLETED")
        self.assertEqual(ps["entry"][0]["resource"]["physicalMeasurementsCreatedSite"], "hpo-site-monroeville")
        self.assertEqual(ps["entry"][0]["resource"]["physicalMeasurementsFinalizedSite"], "hpo-site-bannerphoenix")
        self.assertIsNotNone(ps["entry"][0]["resource"]["physicalMeasurementsTime"])
        self.assertEqual(ps["entry"][0]["resource"]["physicalMeasurementsTime"], self.time1.isoformat())

    def test_cancel_single_pm_returns_cancelled_in_summary(self):
        _id = self.participant_id.strip("P")
        self.send_consent(self.participant_id)
        measurement = load_measurement_json(self.participant_id)
        path = "Participant/%s/PhysicalMeasurements" % self.participant_id
        response = self.send_post(path, measurement)
        ps = self.send_get("ParticipantSummary?participantId=%s" % _id)
        self.assertEqual(ps["entry"][0]["resource"]["physicalMeasurementsStatus"], "COMPLETED")
        self.assertIn("physicalMeasurementsTime", ps["entry"][0]["resource"])

        path = path + "/" + response["id"]
        cancel_info = BaseTestCase.get_restore_or_cancel_info()
        self.send_patch(path, cancel_info)

        response = self.send_get(path)
        composition = self.get_composition_resource_from_fhir_doc(response)
        self.assertEqual(composition["status"], "entered-in-error")
        count = 0
        for ext in composition['extension']:
            if 'cancelled-site' in ext['url']:
                self.assertEqual(ext['valueInteger'], 1)
                count += 1
            if 'cancelled-username' in ext['url']:
                self.assertEqual(ext['valueString'], 'mike@pmi-ops.org')
                count += 1
            if 'cancelled-reason' in ext['url']:
                self.assertEqual(ext['valueString'], 'a mistake was made.')
                count += 1
            if 'authored-location' in ext['url']:
                self.assertEqual(ext['valueString'], 'Location/hpo-site-monroeville')
                count += 1
            if 'finalized-location' in ext['url']:
                self.assertEqual(ext['valueString'], 'Location/hpo-site-bannerphoenix')
                count += 1
        self.assertEqual(count, 5)

        ps = self.send_get("ParticipantSummary?participantId=%s" % _id)
        self.assertEqual(ps["entry"][0]["resource"]["physicalMeasurementsStatus"], "CANCELLED")
        self.assertIn("physicalMeasurementsTime", ps["entry"][0]["resource"])
        self.assertEqual("hpo-site-bannerphoenix", ps["entry"][0]["resource"]["physicalMeasurementsFinalizedSite"])

    def test_restore_a_physical_measurement(self):
        self.send_consent(self.participant_id)
        measurement = load_measurement_json(self.participant_id)
        path = "Participant/%s/PhysicalMeasurements" % self.participant_id
        response = self.send_post(path, measurement)
        ps = self.send_get("Participant/%s/Summary" % self.participant_id)
        path = path + "/" + response["id"]
        self.send_patch(path, BaseTestCase.get_restore_or_cancel_info())
        ps_2 = self.send_get("Participant/%s/Summary" % self.participant_id)
        self.assertEqual(ps_2['physicalMeasurementsStatus'], 'CANCELLED')
        restored_info = BaseTestCase.get_restore_or_cancel_info(reason="need to restore", status="restored", author="me")
        self.send_patch(path, restored_info)

        response = self.send_get(path)
        composition = self.get_composition_resource_from_fhir_doc(response)
        self.assertEqual(composition["status"], "final")
        count = 0
        cancelled = 0
        for ext in composition['extension']:
            if 'restore-site' in ext['url']:
                self.assertEqual(ext['valueInteger'], 1)
                count += 1
            if 'restore-username' in ext['url']:
                self.assertEqual(ext['valueString'], 'me')
                count += 1
            if 'restore-reason' in ext['url']:
                self.assertEqual(ext['valueString'], 'need to restore')
                count += 1
            if 'cancelled' in ext['url']:
                cancelled += 1
        self.assertEqual(count, 3)
        # Response should not contain cancelledInfo
        self.assertEqual(cancelled, 0)

        ps_3 = self.send_get("Participant/%s/Summary" % self.participant_id)
        self.assertEqual(ps_3['physicalMeasurementsStatus'], 'COMPLETED')
        self.assertEqual(ps_3['physicalMeasurementsFinalizedTime'], ps['physicalMeasurementsFinalizedTime'])

    def test_cannot_restore_a_valid_pm(self):
        self.send_consent(self.participant_id)
        measurement = load_measurement_json(self.participant_id)
        path = "Participant/%s/PhysicalMeasurements" % self.participant_id
        response = self.send_post(path, measurement)
        path = path + "/" + response["id"]
        restored_info = BaseTestCase.get_restore_or_cancel_info(reason="need to restore", status="restored", author="me")
        self.send_patch(path, restored_info, expected_status=http.client.BAD_REQUEST)

    def test_cannot_cancel_a_cancelled_pm(self):
        self.send_consent(self.participant_id)
        measurement = load_measurement_json(self.participant_id)
        path = "Participant/%s/PhysicalMeasurements" % self.participant_id
        response = self.send_post(path, measurement)
        path = path + "/" + response["id"]
        self.send_patch(path, BaseTestCase.get_restore_or_cancel_info())
        self.send_patch(path, BaseTestCase.get_restore_or_cancel_info(), expected_status=http.client.BAD_REQUEST)

    def test_cancel_an_ammended_order(self):
        self.send_consent(self.participant_id)
        measurements_1 = load_measurement_json(self.participant_id)
        path = "Participant/%s/PhysicalMeasurements" % self.participant_id
        response = self.send_post(path, measurements_1)
        measurements_2 = load_measurement_json_amendment(self.participant_id, response["id"])
        response_2 = self.send_post(path, measurements_2)

        path = path + "/" + response_2["id"]
        cancel_info = BaseTestCase.get_restore_or_cancel_info()
        self.send_patch(path, cancel_info)
        response = self.send_get(path)
        composition = self.get_composition_resource_from_fhir_doc(response)
        self.assertEqual(composition["status"], "entered-in-error")
        count = 0
        for ext in composition['extension']:
            if 'cancelled-site' in ext['url']:
                self.assertEqual(ext['valueInteger'], 1)
                count += 1
            if 'cancelled-username' in ext['url']:
                self.assertEqual(ext['valueString'], 'mike@pmi-ops.org')
                count += 1
            if 'cancelled-reason' in ext['url']:
                self.assertEqual(ext['valueString'], 'a mistake was made.')
                count += 1
            if 'amends' in ext['url']:
                self.assertIn('PhysicalMeasurements/', ext['valueReference']['reference'])
                count += 1
        self.assertEqual(count, 4)
