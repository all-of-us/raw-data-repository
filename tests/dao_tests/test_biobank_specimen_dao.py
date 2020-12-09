import mock

from rdr_service.dao.biobank_specimen_dao import BiobankSpecimenDao
from rdr_service.model import config_utils
from rdr_service.model.biobank_order import BiobankSpecimen, BiobankAliquotDatasetItem
from tests.helpers.unittest_base import BaseTestCase


class BiobankSpecimenDaoTest(BaseTestCase):
    def test_preloading_migration_data(self):
        participant = self.data_generator.create_database_participant()

        specimen_rlims_id = 'salem'
        dataset_rlims_id = 'data_one'
        dataset_param_id = 'param_one'
        specimen_json = {
            'rlimsID': specimen_rlims_id,
            'orderID': '25test',
            'participantID': config_utils.to_client_biobank_id(participant.biobankId),
            'testcode': 'test 1234567',
            'attributes': [
                {
                    'name': 'attr_one'
                }
            ],
            'aliquots': [
                {
                    'rlimsID': 'child3',
                    'sampleType': 'first sample',
                    'containerTypeID': 'tube',
                    'datasets': [
                        {
                            'rlimsID': dataset_rlims_id,
                            'datasetItems': [
                                {
                                    'paramID': dataset_param_id
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        # Create the objects in the database using the api
        self.send_put(f"Biobank/specimens", request_data=[specimen_json])
        specimen_object = self.session.query(BiobankSpecimen).filter(BiobankSpecimen.rlimsId == specimen_rlims_id).one()
        dataset_item_object = self.session.query(BiobankAliquotDatasetItem).filter(
            BiobankAliquotDatasetItem.dataset_rlims_id == dataset_rlims_id,
            BiobankAliquotDatasetItem.paramId == dataset_param_id
        ).one()

        # Set up the DAO's preloader
        dao = BiobankSpecimenDao()
        dao.ready_preloader(specimen_json)
        dao.preloader.hydrate(self.session)

        # The preloader should have everything from the database,
        # so there shouldn't need to be any calls to the database
        with mock.patch.object(dao._database, 'session') as mock_session_func:
            parsed_model = dao.from_client_json(specimen_json)

            # Make sure the ids of the objects were retrieved without querying the database
            mock_session = mock_session_func.return_value.__enter__.return_value
            mock_session.query.assert_not_called()

            self.assertEqual(specimen_object.id, parsed_model.id)
            self.assertEqual(dataset_item_object.id, parsed_model.aliquots[0].datasets[0].datasetItems[0].id)

        # TODO: MAKE SURE THAT UPDATING SPECIMENS STILL WORKS!!
        #  I DON'T THINK IT DOES!!!!
