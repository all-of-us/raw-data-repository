from datetime import datetime
import mock

from rdr_service import config
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.offline.retention_eligible_import import _supplement_with_rdr_calculations
from rdr_service.services.retention_calculation import RetentionEligibilityDependencies
from tests.helpers.unittest_base import BaseTestCase


class RetentionCalculationIntegrationTest(BaseTestCase):
    def setUp(self, *args, **kwargs):
        super().setUp(*args, **kwargs)
        self.summary = self.data_generator.create_database_participant_summary()

        # mock the retention calculation to see what it got passed
        retention_calc_patch = mock.patch('rdr_service.offline.retention_eligible_import.RetentionEligibility')
        self.retention_calc_mock = retention_calc_patch.start()
        self.addCleanup(retention_calc_patch.stop)

    def test_get_earliest_dna_sample(self):
        self.temporarily_override_config_setting(
            key=config.DNA_SAMPLE_TEST_CODES,
            value=['1ED04', '1SAL2']
        )

        first_dna_sample_timestamp = datetime(2020, 3, 4)
        for test, timestamp in [
            ('not_dna', datetime(2019, 1, 19)),
            ('1ED04', first_dna_sample_timestamp),
            ('1SAL2', datetime(2021, 4, 2))
        ]:
            self.data_generator.create_database_biobank_stored_sample(
                test=test,
                biobankId=self.summary.biobankId,
                confirmed=timestamp
            )

        retention_parameters = self._get_retention_dependencies_found()
        self.assertEqual(first_dna_sample_timestamp, retention_parameters.dna_samples_timestamp)

    def _get_retention_dependencies_found(self) -> RetentionEligibilityDependencies:
        """
        Call the code responsible for collecting the retention calculation data.
        Return the data in provided to the calculation code.
        """
        _supplement_with_rdr_calculations(
            RetentionEligibleMetrics(participantId=self.summary.participantId)
        )
        return self.retention_calc_mock.call_args[0][0]
