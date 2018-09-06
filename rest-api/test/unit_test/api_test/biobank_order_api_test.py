import httplib

import datetime

from dao.biobank_order_dao import BiobankOrderDao
from test.unit_test.unit_test_util import FlaskTestBase
from test.test_data import load_biobank_order_json, load_measurement_json
from model.utils import to_client_participant_id, from_client_participant_id
from model.participant import Participant
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from participant_enums import UNSET_HPO_ID


class BiobankOrderApiTest(FlaskTestBase):
  def setUp(self):
    super(BiobankOrderApiTest, self).setUp()
    self.participant = Participant(participantId=123, biobankId=555)
    self.participant_dao = ParticipantDao()
    self.participant_dao.insert(self.participant)
    self.summary_dao = ParticipantSummaryDao()
    self.bio_dao = BiobankOrderDao()
    self.path = (
        'Participant/%s/BiobankOrder' % to_client_participant_id(self.participant.participantId))

  def test_cancel_order(self):
    self.summary_dao.insert(self.participant_summary(self.participant))
    order_json = load_biobank_order_json(self.participant.participantId,
                                         filename='biobank_order_2.json')
    result = self.send_post(self.path, order_json)
    full_order_json = load_biobank_order_json(self.participant.participantId,
                                              filename='biobank_order_1.json')
    _strip_fields(result)
    _strip_fields(full_order_json)
    self.assertEquals(full_order_json, result)

    biobank_order_id = result['identifier'][1]['value']
    path = self.path + '/' + biobank_order_id
    request_data = {
      "amendedReason": "Its all wrong",
      "cancelledInfo": {
        "author": {
          "system": "https://www.pmi-ops.org/healthpro-username",
          "value": "fred@pmi-ops.org"
        },
        "site": {
          "system": "https://www.pmi-ops.org/site-id",
          "value": "hpo-site-monroeville"
        }
      },
      "status": "cancelled"
    }
    cancelled_order = self.send_patch(path, request_data=request_data, headers={'If-Match':'W/"1"'})
    get_cancelled_order = self.send_get(path)
    self.assertEqual(cancelled_order, get_cancelled_order)

  def test_insert_and_refetch(self):
    self.summary_dao.insert(self.participant_summary(self.participant))
    self.create_and_verify_created_obj(
        self.path, load_biobank_order_json(self.participant.participantId))

  def test_insert_new_order(self):
    self.summary_dao.insert(self.participant_summary(self.participant))
    order_json = load_biobank_order_json(self.participant.participantId,
                                         filename='biobank_order_2.json')
    result = self.send_post(self.path, order_json)
    full_order_json = load_biobank_order_json(self.participant.participantId,
                                              filename='biobank_order_1.json')
    _strip_fields(result)
    _strip_fields(full_order_json)
    self.assertEquals(full_order_json, result)

  def test_error_no_summary(self):
    order_json = load_biobank_order_json(self.participant.participantId)
    self.send_post(self.path, order_json, expected_status=httplib.BAD_REQUEST)

  def test_error_missing_required_fields(self):
    order_json = load_biobank_order_json(self.participant.participantId)
    del order_json['identifier']
    self.send_post(self.path, order_json, expected_status=httplib.BAD_REQUEST)

  def test_no_duplicate_test_within_order(self):
    order_json = load_biobank_order_json(self.participant.participantId)
    order_json['samples'].extend(list(order_json['samples']))
    self.send_post(self.path, order_json, expected_status=httplib.BAD_REQUEST)

  def test_auto_pair_updates_participant_and_summary(self):
    self.summary_dao.insert(self.participant_summary(self.participant))

    # Sanity check: No HPO yet.
    p_unpaired = self.participant_dao.get(self.participant.participantId)
    self.assertEquals(p_unpaired.hpoId, UNSET_HPO_ID)
    self.assertIsNone(p_unpaired.providerLink)
    s_unpaired = self.summary_dao.get(self.participant.participantId)
    self.assertEquals(s_unpaired.hpoId, UNSET_HPO_ID)

    self.send_post(self.path, load_biobank_order_json(self.participant.participantId))

    # Some HPO has been set. (ParticipantDao tests cover more detailed cases / specific values.)
    p_paired = self.participant_dao.get(self.participant.participantId)
    self.assertNotEqual(p_paired.hpoId, UNSET_HPO_ID)
    self.assertIsNotNone(p_paired.providerLink)

    s_paired = self.summary_dao.get(self.participant.participantId)

    self.assertNotEqual(s_paired.hpoId, UNSET_HPO_ID)
    self.assertEqual(s_paired.biospecimenCollectedSiteId, s_paired.siteId)
    self.assertNotEqual(s_paired.biospecimenCollectedSiteId, s_paired.biospecimenFinalizedSiteId)

    self.assertNotEqual(s_paired.siteId, s_paired.physicalMeasurementsCreatedSiteId )
    self.assertNotEqual(s_paired.siteId, s_paired.physicalMeasurementsFinalizedSiteId )

  def test_not_pairing_at_pm_when_has_bio(self):
    self.participant_id = self.create_participant()
    _id = int(self.participant_id[1:])
    self.path = (
      'Participant/%s/BiobankOrder' % to_client_participant_id(_id))
    pid_numeric = from_client_participant_id(self.participant_id)
    self.send_consent(self.participant_id)
    self.send_post(self.path, load_biobank_order_json(pid_numeric))
    participant_paired = self.summary_dao.get(pid_numeric)

    self.assertEqual(participant_paired.siteId, participant_paired.biospecimenCollectedSiteId)
    self.path = (
      'Participant/%s/PhysicalMeasurements' % to_client_participant_id(pid_numeric))
    self._insert_measurements(datetime.datetime.utcnow().isoformat())
    self.assertNotEqual(participant_paired.siteId,
                        participant_paired.physicalMeasurementsFinalizedSiteId)

  def _insert_measurements(self, now=None):
    measurements_1 = load_measurement_json(self.participant_id, now)
    path_1 = 'Participant/%s/PhysicalMeasurements' % self.participant_id
    self.send_post(path_1, measurements_1)


def _strip_fields(order_json):
  if order_json.get('created'):
    del order_json['created']
  if order_json.get('id'):
    del order_json['id']
  for sample in order_json['samples']:
    if sample.get('collected'):
      del sample['collected']
    if sample.get('processed'):
      del sample['processed']
    if sample.get('finalized'):
      del sample['finalized']
