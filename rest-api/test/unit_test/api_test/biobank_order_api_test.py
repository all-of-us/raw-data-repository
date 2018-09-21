import httplib

import datetime

from dao.biobank_order_dao import BiobankOrderDao
from test.unit_test.unit_test_util import FlaskTestBase
from test.test_data import load_biobank_order_json, load_measurement_json
from model.utils import to_client_participant_id, from_client_participant_id
from model.participant import Participant
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from participant_enums import UNSET_HPO_ID, OrderStatus


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
    cancelled_order = self.send_patch(path, request_data=request_data,
                                      headers={'If-Match': 'W/"1"'})
    get_cancelled_order = self.send_get(path)
    get_summary = self.summary_dao.get(self.participant.participantId)

    self.assertEqual(get_summary.biospecimenSourceSiteId, None)
    self.assertEqual(get_summary.biospecimenCollectedSiteId, None)
    self.assertEqual(get_summary.biospecimenOrderTime, None)
    self.assertEqual(get_summary.biospecimenStatus, None)
    self.assertEqual(get_summary.biospecimenFinalizedSiteId, None)
    self.assertEqual(get_summary.biospecimenProcessedSiteId, None)
    self.assertEqual(get_summary.sampleOrderStatus2ED10, None)
    self.assertEqual(get_summary.sampleOrderStatus2ED10Time, None)
    self.assertEqual(get_summary.sampleStatus2ED10, None)
    self.assertEqual(get_summary.sampleStatus2ED10Time, None)
    self.assertEqual(get_summary.sampleOrderStatus1PST8, None)
    self.assertEqual(get_summary.sampleOrderStatus1PST8Time, None)
    self.assertEqual(get_summary.sampleStatus1PST8, None)
    self.assertEqual(get_summary.sampleStatus1PST8Time, None)
    self.assertEqual(get_summary.sampleOrderStatus1PS08, None)
    self.assertEqual(get_summary.sampleOrderStatus1PS08Time, None)
    self.assertEqual(get_summary.sampleStatus1PS08, None)
    self.assertEqual(get_summary.sampleStatus1PS08Time, None)
    self.assertEqual(get_summary.sampleOrderStatus2PST8, None)
    self.assertEqual(get_summary.sampleOrderStatus2PST8Time, None)
    self.assertEqual(get_summary.sampleStatus2PST8, None)
    self.assertEqual(get_summary.sampleStatus2PST8Time, None)
    self.assertEqual(get_summary.sampleOrderStatus1PXR2, None)
    self.assertEqual(get_summary.sampleOrderStatus1PXR2Time, None)
    self.assertEqual(get_summary.sampleStatus1PXR2, None)
    self.assertEqual(get_summary.sampleStatus1PXR2Time, None)
    self.assertEqual(get_summary.sampleOrderStatus1CFD9, None)
    self.assertEqual(get_summary.sampleOrderStatus1CFD9Time, None)
    self.assertEqual(get_summary.sampleStatus1CFD9, None)
    self.assertEqual(get_summary.sampleStatus1CFD9Time, None)
    self.assertEqual(get_summary.sampleOrderStatus1ED02, None)
    self.assertEqual(get_summary.sampleOrderStatus1ED02Time, None)
    self.assertEqual(get_summary.sampleStatus1ED02, None)
    self.assertEqual(get_summary.sampleStatus1ED02Time, None)
    self.assertEqual(cancelled_order, get_cancelled_order)
    self.assertEqual(get_cancelled_order['status'], 'CANCELLED')
    self.assertEqual(get_cancelled_order['amendedReason'], 'Its all wrong')
    self.assertEqual(get_cancelled_order['cancelledInfo']['author']['value'], 'fred@pmi-ops.org')
    self.assertEqual(get_cancelled_order['cancelledInfo']['site']['value'], 'hpo-site-monroeville')

  def test_you_can_not_cancel_a_cancelled_order(self):
    self.summary_dao.insert(self.participant_summary(self.participant))
    order_json = load_biobank_order_json(self.participant.participantId,
                                         filename='biobank_order_2.json')
    result = self.send_post(self.path, order_json)

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
    self.send_patch(path, request_data=request_data, headers={'If-Match': 'W/"1"'})

    self.send_patch(path, request_data=request_data, headers={'If-Match': 'W/"2"'},
                    expected_status=httplib.BAD_REQUEST)

  def test_you_can_not_restore_a_not_cancelled_order(self):
    self.summary_dao.insert(self.participant_summary(self.participant))
    order_json = load_biobank_order_json(self.participant.participantId,
                                         filename='biobank_order_2.json')
    result = self.send_post(self.path, order_json)

    biobank_order_id = result['identifier'][1]['value']
    path = self.path + '/' + biobank_order_id
    request_data = {
      "amendedReason": "Its all wrong",
      "restoredInfo": {
        "author": {
          "system": "https://www.pmi-ops.org/healthpro-username",
          "value": "fred@pmi-ops.org"
        },
        "site": {
          "system": "https://www.pmi-ops.org/site-id",
          "value": "hpo-site-monroeville"
        }
      },
      "status": "restored"
    }
    self.send_patch(path, request_data=request_data, headers={'If-Match': 'W/"1"'},
                    expected_status=httplib.BAD_REQUEST)

  def test_restore_an_order(self):
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
    self.send_patch(path, request_data=request_data, headers={'If-Match': 'W/"1"'})

    request_data = {
      "amendedReason": "I didnt mean to cancel",
      "restoredInfo": {
        "author": {
          "system": "https://www.pmi-ops.org/healthpro-username",
          "value": "fred@pmi-ops.org"
        },
        "site": {
          "system": "https://www.pmi-ops.org/site-id",
          "value": "hpo-site-monroeville"
        }
      },
      "status": "restored"
    }

    self.send_patch(path, request_data=request_data, headers={'If-Match': 'W/"2"'})
    restored_order = self.send_get(path)
    get_summary = self.summary_dao.get(self.participant.participantId)
    self.assertEqual(get_summary.sampleOrderStatus1SST8, OrderStatus.CREATED)
    self.assertEqual(get_summary.sampleOrderStatus2ED10, OrderStatus.CREATED)
    self.assertEqual(get_summary.sampleOrderStatus1SAL, OrderStatus.CREATED)
    self.assertEqual(get_summary.sampleOrderStatus1UR10, OrderStatus.CREATED)
    self.assertEqual(get_summary.sampleOrderStatus1CFD9, OrderStatus.FINALIZED)
    self.assertEqual(get_summary.sampleOrderStatus1ED02, OrderStatus.FINALIZED)
    self.assertEqual(get_summary.sampleOrderStatus2SST8, OrderStatus.FINALIZED)
    self.assertEqual(get_summary.sampleOrderStatus2PST8, OrderStatus.FINALIZED)
    self.assertEqual(restored_order['status'], 'UNSET')
    self.assertEqual(restored_order['restoredInfo']['author']['value'], 'fred@pmi-ops.org')
    self.assertEqual(restored_order['restoredInfo']['site']['value'], 'hpo-site-monroeville')
    self.assertEqual(restored_order['amendedReason'], 'I didnt mean to cancel')

  def test_amending_an_order(self):
    # pylint: disable=unused-variable
    self.summary_dao.insert(self.participant_summary(self.participant))
    order_json = load_biobank_order_json(self.participant.participantId,
                                         filename='biobank_order_2.json')
    result = self.send_post(self.path, order_json)

    biobank_order_id = result['identifier'][1]['value']
    path = self.path + '/' + biobank_order_id
    request_data = {
      "amendedReason": "Its all better",
      "amendedInfo": {
        "author": {
          "system": "https://www.pmi-ops.org/healthpro-username",
          "value": "fred@pmi-ops.org"
        },
        "site": {
          "system": "https://www.pmi-ops.org/site-id",
          "value": "hpo-site-bannerphoenix"
        }
      }
    }

    biobank_order_identifiers = {"created": "2018-02-21T16:25:12",
                "createdInfo": {
                   "author": {
                     "system": "https://www.pmi-ops.org/healthpro-username",
                     "value": "nobody@pmi-ops.org"
                   },
                   "site": {
                     "system": "https://www.pmi-ops.org/site-id",
                     "value": "hpo-site-clinic-phoenix"
                   }
                  }
                 }
    get_order = self.send_get(path)
    full_order = get_order.copy()
    full_order.update(request_data)
    full_order.update(biobank_order_identifiers)

    self.assertEqual(len(full_order['samples']), 16)
    del full_order['samples'][0]

    self.send_put(path, request_data=full_order, headers={'If-Match': 'W/"1"'})

    get_amended_order = self.send_get(path)
    get_summary = self.summary_dao.get(self.participant.participantId)
    self.assertEqual(get_summary.biospecimenProcessedSiteId, 1)
    self.assertEqual(get_summary.biospecimenFinalizedSiteId, 2)
    self.assertEqual(get_summary.biospecimenCollectedSiteId, 1)
    self.assertEqual(get_summary.sampleOrderStatus2PST8, OrderStatus.FINALIZED)
    self.assertEqual(get_summary.sampleOrderStatus1PS08, OrderStatus.FINALIZED)
    self.assertEqual(get_summary.sampleOrderStatus1PST8, OrderStatus.FINALIZED)
    self.assertEqual(get_summary.sampleOrderStatus1SST8, OrderStatus.CREATED)
    self.assertEqual(get_summary.sampleOrderStatus2ED10, OrderStatus.CREATED)
    self.assertEqual(len(get_amended_order['samples']), 15)
    self.assertEqual(get_amended_order['meta'], {'versionId': 'W/"2"'})
    self.assertEqual(get_amended_order['amendedReason'], 'Its all better')
    self.assertEqual(get_amended_order['amendedInfo']['author']['value'], 'fred@pmi-ops.org')
    self.assertEqual(get_amended_order['amendedInfo']['site']['value'], 'hpo-site-bannerphoenix')
    self.assertEqual(get_amended_order['createdInfo']['site']['value'], 'hpo-site-clinic-phoenix')
    self.assertEqual(get_amended_order['createdInfo']['author']['value'], 'nobody@pmi-ops.org')
    self.assertEqual(get_amended_order['created'], "2018-02-21T16:25:12")
    self.assertEqual(get_amended_order['status'], "AMENDED")

  def test_amend_a_restored_order(self):
    self.summary_dao.insert(self.participant_summary(self.participant))
    order_json = load_biobank_order_json(self.participant.participantId,
                                         filename='biobank_order_2.json')
    result = self.send_post(self.path, order_json)
    full_order_json = load_biobank_order_json(self.participant.participantId,
                                              filename='biobank_order_1.json')
    _strip_fields(result)
    _strip_fields(full_order_json)

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
    self.send_patch(path, request_data=request_data, headers={'If-Match': 'W/"1"'})
    self.send_get(path)
    request_data = {
      "amendedReason": "I didnt mean to cancel",
      "restoredInfo": {
        "author": {
          "system": "https://www.pmi-ops.org/healthpro-username",
          "value": "fred@pmi-ops.org"
        },
        "site": {
          "system": "https://www.pmi-ops.org/site-id",
          "value": "hpo-site-monroeville"
        }
      },
      "status": "restored"
    }

    self.send_patch(path, request_data=request_data, headers={'If-Match': 'W/"2"'})

    request_data = {
      "amendedReason": "Its all better",
      "samples": [{
          "test": "1ED10",
          "description": "EDTA 10 mL (1)",
          "processingRequired": False,
          "collected": "2016-01-04T09:45:49Z",
          "finalized": "2016-01-04T10:55:41Z"
        }, {
          "test": "1PST8",
          "description": "Plasma Separator 8 mL",
          "collected": "2016-01-04T09:45:49Z",
          "processingRequired":True,
          "processed": "2016-01-04T10:28:50Z",
          "finalized": "2016-01-04T10:55:41Z"
        }],
      "amendedInfo": {
        "author": {
          "system": "https://www.pmi-ops.org/healthpro-username",
          "value": "mike@pmi-ops.org"
        },
        "site": {
          "system": "https://www.pmi-ops.org/site-id",
          "value": "hpo-site-monroeville"
        }
      }
    }
    get_order = self.send_get(path)
    full_order = get_order.copy()
    full_order.update(request_data)
    self.send_put(path, request_data=full_order, headers={'If-Match': 'W/"3"'})

    get_amended_order = self.send_get(path)
    self.assertEqual(len(get_amended_order['samples']), 2)
    self.assertEqual(get_amended_order['amendedInfo']['author']['value'], 'mike@pmi-ops.org')
    self.assertEqual(get_amended_order['status'], 'AMENDED')
    self.assertEqual(get_amended_order.get('restoredSiteId'), None)
    self.assertEqual(get_amended_order.get('restoredUsername'), None)
    self.assertEqual(get_amended_order.get('restoredTime'), None)
    self.assertEqual(get_amended_order['meta'], {'versionId': 'W/"4"'})

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
  if order_json.get('version'):
    del order_json['version']
  for sample in order_json['samples']:
    if sample.get('collected'):
      del sample['collected']
    if sample.get('processed'):
      del sample['processed']
    if sample.get('finalized'):
      del sample['finalized']
