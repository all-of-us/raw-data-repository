from datetime import datetime
import mock

from rdr_service.model.obfuscation import Obfuscation
from rdr_service.repository.obfuscation_repository import ObfuscationRepository
from tests.helpers.unittest_base import BaseTestCase


class TestObfuscationRepository(BaseTestCase):
    def setUp(self, *args, **kwargs):
        super(TestObfuscationRepository, self).setUp(*args, **kwargs)
        self.repo = ObfuscationRepository()

    def test_retrieving_data(self):
        """Verify loading data that has been previously stored is successful"""
        data_lookup_key = 'test_key'
        self.session.add(
            Obfuscation(
                id=data_lookup_key,
                expires=datetime(2080, 1, 1),
                data={
                    'foo': 3,
                    'bar': 9
                }
            )
        )

        self.assertDictEqual(
            {'bar': 9, 'foo': 3},
            self.repo.get(data_lookup_key, session=self.session)
        )

    def test_storing_data(self):
        """Check that storing data works"""
        data_lookup_key = self.repo.store(
            data={'bar': 9, 'foo': 3},
            expiration=datetime(2080, 1, 1),
            session=self.session
        )
        self.assertIsNotNone(data_lookup_key)  # Check that the function returned something to use to find the data

        stored_object = self.session.query(Obfuscation).filter(
            Obfuscation.id == data_lookup_key
        ).one()
        self.assertDictEqual(
            {'bar': 9, 'foo': 3},
            stored_object.data
        )
        self.assertEqual(datetime(2080, 1, 1), stored_object.expires)

    @mock.patch('rdr_service.repository.obfuscation_repository.ObfuscationRepository._generate_random_key')
    def test_id_collision_retries(self, id_generation_mock):
        """Make sure the storage function retries generating an id if the random value collides with an existing one"""

        # Replace the method for creating the unique id with a mock. Have it generate the same key a few times before
        # it starts generating another
        generation_call_count = 0

        def id_generation_stub():
            nonlocal generation_call_count
            generation_call_count += 1
            if generation_call_count < 4:
                return 'first_key'
            else:
                return 'second_key'
        id_generation_mock.side_effect = id_generation_stub

        # Create an object that uses the first key (making it so that no other object can use that same key)
        first_key_used = self.repo.store(
            data={'bar': 9, 'foo': 3},
            expiration=datetime(2080, 1, 1),
            session=self.session
        )
        self.assertEqual('first_key', first_key_used)

        # Create another object. The id generation mock will try to give the previously used key a few times, but
        # the store method should key trying until it gets a unique key
        second_key_used = self.repo.store(
            data={'bar': 9, 'foo': 3},
            expiration=datetime(2080, 1, 1),
            session=self.session
        )
        self.assertEqual('second_key', second_key_used)

        # TODO: integrate with the ParticipantSummaryDao, and create a cron job that handles expired
