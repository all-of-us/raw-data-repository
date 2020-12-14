from rdr_service.dao.biobank_specimen_dao import RlimsIdLoadingStrategy, SpecimenAttributeLoadingStrategy
from rdr_service.dao.object_preloader import ObjectPreloader
from rdr_service.model.biobank_order import BiobankSpecimen, BiobankSpecimenAttribute
from tests.helpers.unittest_base import BaseTestCase


class ObjectPreloaderTest(BaseTestCase):

    def test_preloading_objects(self):
        # Populate some data in the database
        test_specimen = self.data_generator.create_database_biobank_specimen(rlimsId='test')
        test_attribute = self.data_generator.create_database_specimen_attribute(specimen_id=test_specimen.id,
                                                                                specimen_rlims_id=test_specimen.rlimsId,
                                                                                name='attr_test')

        another_specimen = self.data_generator.create_database_biobank_specimen(rlimsId='another')
        another_attribute = self.data_generator.create_database_specimen_attribute(name='attr_another')

        # Put data in the database that shouldn't be loaded
        not_loaded_specimen = self.data_generator.create_database_biobank_specimen(rlimsId='not_loaded')
        not_loaded_attribute = self.data_generator.create_database_specimen_attribute()

        # Create some objects that won't exist in the database
        specimen_not_in_database = self.data_generator._biobank_specimen_with_defaults()
        attribute_not_in_database = self.data_generator._specimen_attribute_with_defaults()

        # Load the data in batches using the preloader
        preloader = ObjectPreloader({
            BiobankSpecimen: RlimsIdLoadingStrategy,
            BiobankSpecimenAttribute: SpecimenAttributeLoadingStrategy
        })
        preloader.register_for_hydration(test_specimen)
        preloader.register_for_hydration(test_attribute)
        preloader.register_for_hydration(another_specimen)
        preloader.register_for_hydration(another_attribute)
        preloader.register_for_hydration(specimen_not_in_database)
        preloader.register_for_hydration(attribute_not_in_database)
        preloader.hydrate(self.session)

        # Check that the object loader has the objects that it should
        self.assertEqual(test_specimen, preloader.get_object(test_specimen))
        self.assertEqual(test_attribute, preloader.get_object(test_attribute))

        self.assertEqual(another_specimen, preloader.get_object(another_specimen))
        self.assertEqual(another_attribute, preloader.get_object(another_attribute))

        # And that it doesn't have the ones it shouldn't
        self.assertIsNone(preloader.get_object(not_loaded_specimen))
        self.assertIsNone(preloader.get_object(not_loaded_attribute))

        self.assertIsNone(preloader.get_object(specimen_not_in_database))
        self.assertIsNone(preloader.get_object(attribute_not_in_database))
