import datetime
import mock
from sqlalchemy.exc import OperationalError
from werkzeug.exceptions import BadRequest, Forbidden, NotFound, PreconditionFailed, ServiceUnavailable

from rdr_service import participant_enums
from rdr_service.clock import FakeClock
from rdr_service.dao.base_dao import MAX_INSERT_ATTEMPTS
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.participant_dao import ParticipantDao, ParticipantHistoryDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.model.hpo import HPO
from rdr_service.model.participant import Participant
from rdr_service.model.site import Site
from rdr_service.participant_enums import (
    DigitalHealthSharingStatusV31,
    SuspensionStatus,
    UNSET_HPO_ID,
    WithdrawalStatus,
    make_primary_provider_link_for_id,
    make_primary_provider_link_for_name
)
from tests.helpers.unittest_base import BaseTestCase
from tests.helpers.mysql_helper_data import PITT_HPO_ID, PITT_ORG_ID, PITT_SITE_ID, random_ids


class ParticipantDaoTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao = ParticipantDao()
        self.participant_summary_dao = ParticipantSummaryDao()
        self.participant_history_dao = ParticipantHistoryDao()

    def test_get_before_insert(self):
        self.assertIsNone(self.dao.get(1))
        self.assertIsNone(self.participant_summary_dao.get(1))
        self.assertIsNone(self.participant_history_dao.get([1, 1]))

    def test_insert(self):
        p = Participant()
        time = datetime.datetime(2016, 1, 1)
        with random_ids([1, 2, 3]):
            with FakeClock(time):
                self.dao.insert(p)
        expected_participant = self.data_generator._participant_with_defaults(
            participantId=1, researchId=3, version=1, biobankId=2, lastModified=time, signUpTime=time
        )
        self.assertEqual(expected_participant.asdict(), p.asdict())

        p2 = self.dao.get(1)
        self.assertEqual(p.asdict(), p2.asdict())

        # Creating a participant also creates a ParticipantHistory row, but not a ParticipantSummary row
        ps = self.participant_summary_dao.get(1)
        self.assertIsNone(ps)
        ph = self.participant_history_dao.get([1, 1])
        expected_ph = self.data_generator._participant_history_with_defaults(
            participantId=1, biobankId=2, researchId=3, lastModified=time, signUpTime=time
        )
        self.assertEqual(expected_ph.asdict(), ph.asdict())

    def test_insert_with_external_id(self):
        p = Participant(externalId=3)
        time = datetime.datetime(2016, 1, 1)
        with random_ids([1, 2, 3]):
            with FakeClock(time):
                self.dao.insert(p)
        expected_participant = self.data_generator._participant_with_defaults(
            participantId=1, externalId=3, researchId=3, version=1, biobankId=2, lastModified=time, signUpTime=time
        )
        self.assertEqual(expected_participant.asdict(), p.asdict())

        p2 = self.dao.get(1)
        self.assertEqual(p.asdict(), p2.asdict())

        # Creating a participant also creates a ParticipantHistory row, but not a ParticipantSummary row
        ps = self.participant_summary_dao.get(1)
        self.assertIsNone(ps)
        ph = self.participant_history_dao.get([1, 1])
        expected_ph = self.data_generator._participant_history_with_defaults(
            participantId=1, externalId=3, biobankId=2, researchId=3, lastModified=time, signUpTime=time
        )
        self.assertEqual(expected_ph.asdict(), ph.asdict())

    def test_insert_duplicate_participant_id_retry(self):
        p = Participant()
        with random_ids([1, 2, 3]):
            self.dao.insert(p)
        p2 = Participant()
        time = datetime.datetime(2016, 1, 1)
        with random_ids([1, 4, 5, 2, 4, 5]):
            with FakeClock(time):
                p2 = self.dao.insert(p2)
        expected_participant = self.data_generator._participant_with_defaults(
            participantId=2, version=1, biobankId=4, researchId=5, lastModified=time, signUpTime=time
        )
        self.assertEqual(expected_participant.asdict(), p2.asdict())

    def test_insert_duplicate_participant_id_give_up(self):
        p = Participant()
        with random_ids([1, 2, 3]):
            self.dao.insert(p)
        rand_ints = []
        for i in range(0, MAX_INSERT_ATTEMPTS):
            rand_ints.append(1)
            rand_ints.append(i)
            rand_ints.append(3)
        p2 = Participant()
        with random_ids(rand_ints):
            with self.assertRaises(ServiceUnavailable):
                self.dao.insert(p2)

    def test_insert_duplicate_biobank_id_give_up(self):
        p = Participant()
        with random_ids([1, 2, 3]):
            self.dao.insert(p)
        rand_ints = []
        for i in range(0, MAX_INSERT_ATTEMPTS):
            rand_ints.append(i + 2)
            rand_ints.append(2)
            rand_ints.append(3)
        p2 = Participant()
        with random_ids(rand_ints):
            with self.assertRaises(ServiceUnavailable):
                self.dao.insert(p2)

    def test_update_no_expected_version_no_ps(self):
        p = Participant()
        time = datetime.datetime(2016, 1, 1)
        with random_ids([1, 2, 3]):
            with FakeClock(time):
                self.dao.insert(p)

        p.providerLink = make_primary_provider_link_for_name("PITT")
        time2 = datetime.datetime(2016, 1, 2)
        with FakeClock(time2):
            self.dao.update(p)
        # lastModified, hpoId, version is updated on p after being passed in
        p2 = self.dao.get(1)
        expected_participant = self.data_generator._participant_with_defaults(
            participantId=1,
            researchId=3,
            version=2,
            biobankId=2,
            lastModified=time2,
            signUpTime=time,
            hpoId=PITT_HPO_ID,
            providerLink=p2.providerLink,
        )
        self.assertEqual(expected_participant.asdict(), p2.asdict())
        self.assertEqual(p.asdict(), p2.asdict())

        ps = self.participant_summary_dao.get(1)
        self.assertIsNone(ps)

        expected_ph = self.data_generator._participant_history_with_defaults(
            participantId=1, biobankId=2, researchId=3, lastModified=time, signUpTime=time
        )
        # Updating the participant adds a new ParticipantHistory row.
        ph = self.participant_history_dao.get([1, 1])
        self.assertEqual(expected_ph.asdict(), ph.asdict())
        ph2 = self.participant_history_dao.get([1, 2])
        expected_ph2 = self.data_generator._participant_history_with_defaults(
            participantId=1,
            researchId=3,
            version=2,
            biobankId=2,
            lastModified=time2,
            signUpTime=time,
            hpoId=PITT_HPO_ID,
            providerLink=p2.providerLink,
        )
        self.assertEqual(expected_ph2.asdict(), ph2.asdict())

    def test_update_no_expected_version_with_ps(self):
        p = Participant()
        time = datetime.datetime(2016, 1, 1)
        with random_ids([1, 2, 3]):
            with FakeClock(time):
                self.dao.insert(p)
        p.providerLink = make_primary_provider_link_for_name("PITT")
        time2 = datetime.datetime(2016, 1, 2)
        with FakeClock(time2):
            self.dao.update(p)

        summary = self.participant_summary(p)
        summary.enrollmentStatus = participant_enums.EnrollmentStatus.INTERESTED
        summary.enrollmentStatusV3_0 = participant_enums.EnrollmentStatusV30.PARTICIPANT
        summary.enrollmentStatusV3_1 = participant_enums.EnrollmentStatusV31.PARTICIPANT
        self.participant_summary_dao.insert(summary)

        # lastModified, hpoId, version is updated on p after being passed in
        p2 = self.dao.get(1)
        expected_participant = self.data_generator._participant_with_defaults(
            participantId=1,
            researchId=3,
            version=2,
            biobankId=2,
            lastModified=time2,
            signUpTime=time,
            hpoId=PITT_HPO_ID,
            providerLink=p2.providerLink,
        )
        self.assertEqual(expected_participant.asdict(), p2.asdict())
        self.assertEqual(p.asdict(), p2.asdict())

        # Updating the participant provider link also updates the HPO ID on the participant summary.
        ps = self.participant_summary_dao.get(1)
        expected_ps = self.data_generator._participant_summary_with_defaults(
            participantId=1,
            biobankId=2,
            signUpTime=time,
            hpoId=PITT_HPO_ID,
            lastModified=time2,
            firstName=summary.firstName,
            lastName=summary.lastName,
            email=summary.email,
            patientStatus=[],
            retentionEligibleStatus=None,
            wasEhrDataAvailable=False,
            healthDataStreamSharingStatusV3_1=DigitalHealthSharingStatusV31.NEVER_SHARED
        )
        self.assertEqual(expected_ps.asdict(), ps.asdict())

        p2_last_modified = p2.lastModified
        p2.hpoId = 2
        self.dao.update(p2)
        p2_update = self.dao.get(1)
        self.assertNotEqual(p2_last_modified, p2_update.lastModified)
        self.assertEqual(p2_update.lastModified, p2.lastModified)

        expected_ph = self.data_generator._participant_history_with_defaults(
            participantId=1, biobankId=2, researchId=3, lastModified=time, signUpTime=time
        )
        # And updating the participant adds a new ParticipantHistory row.
        ph = self.participant_history_dao.get([1, 1])
        self.assertEqual(expected_ph.asdict(), ph.asdict())
        ph2 = self.participant_history_dao.get([1, 2])
        expected_ph2 = self.data_generator._participant_history_with_defaults(
            participantId=1,
            researchId=3,
            version=2,
            biobankId=2,
            lastModified=time2,
            signUpTime=time,
            hpoId=PITT_HPO_ID,
            providerLink=p2.providerLink,
        )
        self.assertEqual(expected_ph2.asdict(), ph2.asdict())

    def test_update_right_expected_version(self):
        p = Participant()
        time = datetime.datetime(2016, 1, 1)
        with random_ids([1, 2, 3]):
            with FakeClock(time):
                self.dao.insert(p)
        p.version = 1
        p.providerLink = make_primary_provider_link_for_name("PITT")
        time2 = datetime.datetime(2016, 1, 2)
        with FakeClock(time2):
            self.dao.update(p)

        p2 = self.dao.get(1)
        expected_participant = self.data_generator._participant_with_defaults(
            participantId=1,
            researchId=3,
            version=2,
            biobankId=2,
            lastModified=time2,
            signUpTime=time,
            hpoId=PITT_HPO_ID,
            providerLink=p2.providerLink,
        )
        self.assertEqual(expected_participant.asdict(), p2.asdict())

    def test_update_withdraw(self):
        p = Participant()
        time = datetime.datetime(2016, 1, 1)
        with random_ids([1, 2, 3]):
            with FakeClock(time):
                self.dao.insert(p)
        p.version = 1
        p.withdrawalStatus = WithdrawalStatus.NO_USE
        time2 = datetime.datetime(2016, 1, 2)
        with FakeClock(time2):
            self.dao.update(p)

        p2 = self.dao.get(1)
        expected_participant = self.data_generator._participant_with_defaults(
            participantId=1,
            researchId=3,
            version=2,
            biobankId=2,
            lastModified=time2,
            signUpTime=time,
            withdrawalStatus=WithdrawalStatus.NO_USE,
            withdrawalTime=time2,
        )
        self.assertEqual(expected_participant.asdict(), p2.asdict())

        p.version = 2
        p.providerLink = make_primary_provider_link_for_name("PITT")
        p.withdrawalTime = None
        time3 = datetime.datetime(2016, 1, 3)
        with FakeClock(time3):
            self.dao.update(p)

        # Withdrawal time should get copied over.
        p2 = self.dao.get(1)
        expected_participant = self.data_generator._participant_with_defaults(
            participantId=1,
            researchId=3,
            version=3,
            biobankId=2,
            lastModified=time3,
            signUpTime=time,
            withdrawalStatus=WithdrawalStatus.NO_USE,
            withdrawalTime=time2,
            hpoId=PITT_HPO_ID,
            providerLink=p2.providerLink,
        )
        self.assertEqual(expected_participant.asdict(), p2.asdict())

    def test_update_suspend(self):
        p = Participant()
        time = datetime.datetime(2016, 1, 1)
        with random_ids([1, 2, 3]):
            with FakeClock(time):
                self.dao.insert(p)
        p.version = 1
        p.suspensionStatus = SuspensionStatus.NO_CONTACT
        time2 = datetime.datetime(2016, 1, 2)
        with FakeClock(time2):
            self.dao.update(p)

        p2 = self.dao.get(1)
        expected_participant = self.data_generator._participant_with_defaults(
            participantId=1,
            researchId=3,
            version=2,
            biobankId=2,
            lastModified=time2,
            signUpTime=time,
            suspensionStatus=SuspensionStatus.NO_CONTACT,
            suspensionTime=time2,
        )
        self.assertEqual(expected_participant.asdict(), p2.asdict())

        p.version = 2
        p.providerLink = make_primary_provider_link_for_name("PITT")
        p.suspensionTime = None
        time3 = datetime.datetime(2016, 1, 3)
        with FakeClock(time3):
            self.dao.update(p)

        # Withdrawal time should get copied over.
        p2 = self.dao.get(1)
        expected_participant = self.data_generator._participant_with_defaults(
            participantId=1,
            researchId=3,
            version=3,
            biobankId=2,
            lastModified=time3,
            signUpTime=time,
            suspensionStatus=SuspensionStatus.NO_CONTACT,
            suspensionTime=time2,
            hpoId=PITT_HPO_ID,
            providerLink=p2.providerLink,
        )
        self.assertEqual(expected_participant.asdict(), p2.asdict())

    def test_update_multiple_suspend(self):
        p = Participant()
        time = datetime.datetime(2016, 1, 1)
        with random_ids([1, 2, 3]):
            with FakeClock(time):
                self.dao.insert(p)
        p.version = 1
        p.suspensionStatus = SuspensionStatus.NO_CONTACT
        time2 = datetime.datetime(2016, 1, 2)
        with FakeClock(time2):
            self.dao.update(p)

        p.version = 2
        p.providerLink = make_primary_provider_link_for_name("PITT")
        p.suspensionTime = None
        p.suspensionStatus = SuspensionStatus.NOT_SUSPENDED
        time3 = datetime.datetime(2016, 1, 3)
        with FakeClock(time3):
            self.dao.update(p)

        # Withdrawal time should get copied over.
        p2 = self.dao.get(p.participantId)
        expected_participant = self.data_generator._participant_with_defaults(
            participantId=1,
            researchId=3,
            version=3,
            biobankId=2,
            lastModified=time3,
            signUpTime=time,
            suspensionStatus=SuspensionStatus.NOT_SUSPENDED,
            suspensionTime=None,
            hpoId=PITT_HPO_ID,
            providerLink=p2.providerLink,
        )
        self.assertEqual(expected_participant.asdict(), p2.asdict())

        p.version = 3
        p.suspensionStatus = SuspensionStatus.NO_CONTACT
        time4 = datetime.datetime(2016, 1, 4)
        with FakeClock(time4):
            self.dao.update(p)

        p2 = self.dao.get(p.participantId)
        expected_participant = self.data_generator._participant_with_defaults(
            participantId=1,
            researchId=3,
            version=4,
            biobankId=2,
            lastModified=time4,
            signUpTime=time,
            suspensionStatus=SuspensionStatus.NO_CONTACT,
            suspensionTime=time4,
            hpoId=PITT_HPO_ID,
            providerLink=p2.providerLink,
        )
        self.assertEqual(expected_participant.asdict(), p2.asdict())
        p2_summary = self.participant_summary_dao.get(p2.participantId)
        print(p2_summary)

    def test_update_wrong_expected_version(self):
        p = Participant()
        time = datetime.datetime(2016, 1, 1)
        with random_ids([1, 2, 3]):
            with FakeClock(time):
                self.dao.insert(p)

        p.version = 2
        p.providerLink = make_primary_provider_link_for_name("PITT")
        time2 = datetime.datetime(2016, 1, 2)
        with FakeClock(time2):
            with self.assertRaises(PreconditionFailed):
                self.dao.update(p)

    def test_update_withdrawn_hpo_succeeds(self):
        p = Participant(withdrawalStatus=WithdrawalStatus.NO_USE)
        time = datetime.datetime(2016, 1, 1)
        with random_ids([1, 2, 3]):
            with FakeClock(time):
                self.dao.insert(p)

        expected_participant = self.data_generator._participant_with_defaults(
            participantId=1,
            researchId=3,
            version=1,
            biobankId=2,
            lastModified=time,
            signUpTime=time,
            withdrawalStatus=WithdrawalStatus.NO_USE,
        )
        self.assertEqual(expected_participant.asdict(), p.asdict())

        p2 = self.dao.get(1)
        self.assertEqual(p.asdict(), p2.asdict())

        p.version = 1
        p.providerLink = make_primary_provider_link_for_name("PITT")
        self.dao.update(p)

    def test_update_withdrawn_status_fails(self):
        p = Participant(withdrawalStatus=WithdrawalStatus.NO_USE)
        time = datetime.datetime(2016, 1, 1)
        with random_ids([1, 2, 3]):
            with FakeClock(time):
                self.dao.insert(p)

        expected_participant = self.data_generator._participant_with_defaults(
            participantId=1,
            researchId=3,
            version=1,
            biobankId=2,
            lastModified=time,
            signUpTime=time,
            withdrawalStatus=WithdrawalStatus.NO_USE,
        )
        self.assertEqual(expected_participant.asdict(), p.asdict())

        p2 = self.dao.get(1)
        self.assertEqual(p.asdict(), p2.asdict())

        p.version = 1
        p.withdrawalStatus = WithdrawalStatus.NOT_WITHDRAWN
        with self.assertRaises(Forbidden):
            self.dao.update(p)

    @mock.patch('rdr_service.dao.participant_dao.logging')
    def test_error_log_for_incorrect_status_on_participant(self, mock_logging):
        """Participants that have not yet consented should only be withdrawn with the status of EARLY_OUT"""
        participant = self.data_generator.create_database_participant()
        participant.withdrawalStatus = WithdrawalStatus.NO_USE
        self.dao.update(participant)
        mock_logging.error.assert_called_with(
            f'Un-consented participant {participant.participantId} was withdrawn with NO_USE'
        )

    def test_update_not_exists(self):
        p = self.data_generator._participant_with_defaults(participantId=1, biobankId=2)
        with self.assertRaises(NotFound):
            self.dao.update(p)

    def test_bad_hpo_insert(self):
        p = Participant(
            participantId=1, version=1, biobankId=2, providerLink=make_primary_provider_link_for_name("FOO")
        )
        with self.assertRaises(BadRequest):
            self.dao.insert(p)

    def test_bad_hpo_update(self):
        p = Participant(participantId=1, biobankId=2)
        time = datetime.datetime(2016, 1, 1)
        with FakeClock(time):
            self.dao.insert(p)

        p.providerLink = make_primary_provider_link_for_name("FOO")
        with self.assertRaises(BadRequest):
            self.dao.update(p)

    def test_pairs_unset(self):
        participant_id = 22
        self.dao.insert(Participant(participantId=participant_id, biobankId=2))
        refetched = self.dao.get(participant_id)
        self.assertEqual(refetched.hpoId, UNSET_HPO_ID)  # sanity check
        self.participant_summary_dao.insert(self.participant_summary(refetched))

        with self.dao.session() as session:
            self.dao.add_missing_hpo_from_site(session, participant_id, PITT_SITE_ID)

        paired = self.dao.get(participant_id)
        self.assertEqual(paired.hpoId, PITT_HPO_ID)
        self.assertEqual(paired.providerLink, make_primary_provider_link_for_id(PITT_HPO_ID))
        self.assertEqual(self.participant_summary_dao.get(participant_id).hpoId, PITT_HPO_ID)
        self.assertEqual(paired.organizationId, PITT_ORG_ID)
        self.assertEqual(paired.siteId, PITT_SITE_ID)

    def test_overwrite_existing_pairing(self):
        participant_id = 99
        created = self.dao.insert(
            Participant(
                participantId=participant_id,
                biobankId=2,
                hpoId=PITT_HPO_ID,
                providerLink=make_primary_provider_link_for_id(PITT_HPO_ID),
            )
        )
        self.participant_summary_dao.insert(self.participant_summary(created))
        self.assertEqual(created.hpoId, PITT_HPO_ID)  # sanity check

        other_hpo = HPODao().insert(HPO(hpoId=PITT_HPO_ID + 1, name="DIFFERENT_HPO"))
        other_site = SiteDao().insert(
            Site(hpoId=other_hpo.hpoId, siteName="Arbitrary Site", googleGroup="a_site@googlegroups.com")
        )

        with self.dao.session() as session:
            self.dao.add_missing_hpo_from_site(session, participant_id, other_site.siteId)

        # Original Participant + summary is affected.
        refetched = self.dao.get(participant_id)

        self.assertEqual(refetched.hpoId, other_hpo.hpoId)
        self.assertEqual(refetched.providerLink, make_primary_provider_link_for_id(other_hpo.hpoId))
        self.assertEqual(self.participant_summary_dao.get(participant_id).hpoId, other_hpo.hpoId)

    def test_pairing_at_different_levels(self):
        p = Participant()
        time = datetime.datetime(2016, 1, 1)
        with random_ids([1, 2, 3]):
            with FakeClock(time):
                self.dao.insert(p)

        p.version = 1
        p.siteId = 1
        time2 = datetime.datetime(2016, 1, 2)
        with FakeClock(time2):
            self.dao.update(p)

        p2 = self.dao.get(1)
        ep = self.data_generator._participant_with_defaults(
            participantId=1,
            version=2,
            biobankId=2,
            lastModified=time2,
            signUpTime=time,
            hpoId=PITT_HPO_ID,
            siteId=1,
            organizationId=PITT_ORG_ID,
            providerLink=p2.providerLink,
        )
        self.assertEqual(ep.siteId, p2.siteId)
        # ensure that p2 get paired with expected awardee and organization from update().
        self.assertEqual(ep.hpoId, p2.hpoId)
        self.assertEqual(ep.organizationId, p2.organizationId)

    @mock.patch('rdr_service.dao.base_dao.logging')
    def test_inserts_retry_after_lock_wait_timeout(self, mock_logging):
        """
        Check to make sure inserts will retry when encountering a lock wait timeout error.
        Any dao should be able to do this, but this test uses ParticipantDao
        """

        # Lock the participants table and set the lock_wait_timeout low so the test isn't slow
        self.session.execute('set global innodb_lock_wait_timeout = 1')
        self.session.query(Participant).with_for_update().all()

        # Use the error logging to know when the lock timeout was triggered,
        # unlock the participant table after the first failure
        mock_logging.warning.side_effect = lambda *_, **__: self.session.commit()

        test_client_id = 'lock_wait_test'  # Something unique to use to pull this specific participant from the db
        participant = self.data_generator._participant_with_defaults(
            participantId=None,
            biobankId=None,
            clientId=test_client_id
        )
        self.dao.insert(participant)

        # Verify that the participant was inserted
        lock_wait_participant = self.session.query(Participant).filter(
            Participant.clientId == test_client_id
        ).one_or_none()
        self.assertIsNotNone(lock_wait_participant)

    def test_operational_error_messages_passed_out_after_retry_failure(self):
        """
        Check to make sure the retry loop doesn't hide operational error messages
        that would be helpful in diagnosing issues with a request.
        """

        # Try to insert a participant and trigger an operational error on character set incompatibility
        with self.assertRaises(OperationalError) as exc_wrapper:
            new_participant = self.data_generator._participant_with_defaults(
                participantId=None,
                biobankId=None,
                clientId='🐛'
            )
            self.dao.insert(new_participant)

        self.assertIn('Incorrect string value', str(exc_wrapper.exception))

    def test_participant_id_mapping(self):
        """
        Checks that correct Raw sql for export is sent back from dao method
        as well as checking for correct objects being
        sent if is_sql param is false
        """
        num_participants = 10
        for _ in range(num_participants):
            self.data_generator.create_database_participant(
                externalId=self.data_generator.unique_external_id()
            )

        expected_union = "(SELECT participant.participant_id AS p_id, 'r_id' AS id_source, " \
                         "participant.research_id AS id_value \nFROM participant " \
                         "UNION SELECT participant.participant_id AS p_id, 'vibrent_id' " \
                         "AS id_source, participant.external_id AS id_value \nFROM participant)"

        only_sql = self.dao.get_participant_id_mapping(is_sql=True)
        self.assertIn(expected_union, only_sql)

        only_objs = self.dao.get_participant_id_mapping()
        self.assertEqual(len(only_objs), num_participants*2)

        for obj in only_objs:
            p_id = obj[0]
            similar = [obj for obj in only_objs if obj[0] == p_id]
            self.assertEqual(len(similar), 2)
            self.assertTrue(any(obj for obj in similar if obj[1] == 'r_id'))
            self.assertTrue(any(obj for obj in similar if obj[1] == 'vibrent_id'))

    def test_loading_pairing_info_by_ids(self):
        hpo = self.data_generator.create_database_hpo(name='pairing_test')
        first_org = self.data_generator.create_database_organization(externalId='ONE')
        second_org = self.data_generator.create_database_organization(externalId='TWO')

        test_site = self.data_generator.create_database_site(googleGroup='test-site-group')

        participant_one = self.data_generator.create_database_participant(
            hpoId=hpo.hpoId,
            organizationId=first_org.organizationId,
            siteId=test_site.siteId
        )
        participant_two = self.data_generator.create_database_participant(
            hpoId=hpo.hpoId,
            organizationId=second_org.organizationId
        )
        participant_three = self.data_generator.create_database_participant(
            hpoId=hpo.hpoId
        )

        results = self.dao.get_pairing_data_for_ids(
            [participant_one.participantId, participant_two.participantId, participant_three.participantId]
        )
        self.assertEqual([
            (participant_one.participantId, hpo.name, first_org.externalId, test_site.googleGroup),
            (participant_two.participantId, hpo.name, second_org.externalId, None),
            (participant_three.participantId, hpo.name, None, None)
        ], results)

