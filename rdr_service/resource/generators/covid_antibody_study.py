#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging

from sqlalchemy.sql import text

from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.model.covid_antibody_study import BiobankCovidAntibodySample, QuestCovidAntibodyTest, \
    QuestCovidAntibodyTestResult
from rdr_service.resource import generators, schemas


class BiobankCovidAntibodySampleGenerator(generators.BaseGenerator):
    """
    Generate a BiobankCovidAntibodySample BQRecord object
    """
    def make_resource(self, _pk):
        """
        Build a Resource object from the given id.
        :param _pk: Primary key value from biobank_covid_antibody_sample table.
        :return: ResourceDataObject object
        """
        ro_dao = ResourceDataDao(backup=True)
        with ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from biobank_covid_antibody_sample where id = :pk'),
                                     {'pk': _pk}).first()
            data = ro_dao.to_resource_dict(row, schema=schemas.BiobankCovidAntibodySampleSchema)
            return generators.ResourceRecordSet(schemas.BiobankCovidAntibodySampleSchema, data)


class QuestCovidAntibodyTestGenerator(generators.BaseGenerator):
    """
    Generate a QuestCovidAntibodyTest BQRecord object
    """
    def make_resource(self, _pk):
        """
        Build a Resource object from the given id.
        :param _pk: Primary key value from quest_covid_antibody_test table.
        :return: ResourceDataObject object
        """
        ro_dao = ResourceDataDao(backup=True)
        with ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from quest_covid_antibody_test where id = :pk'),
                                     {'pk': _pk}).first()
            data = ro_dao.to_resource_dict(row, schema=schemas.QuestCovidAntibodyTestSchema)
            return generators.ResourceRecordSet(schemas.QuestCovidAntibodyTestSchema, data)


class QuestCovidAntibodyTestResultGenerator(generators.BaseGenerator):
    """
    Generate a QuestCovidAntibodyTestResult BQRecord object
    """
    def make_resource(self, _pk):
        """
        Build a Resource object from the given id.
        :param _pk: Primary key value from quest_covid_antibody_test_result table.
        :return: ResourceDataObject object
        """
        ro_dao = ResourceDataDao(backup=True)
        with ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from quest_covid_antibody_test_result where id = :pk'),
                                     {'pk': _pk}).first()
            data = ro_dao.to_resource_dict(row, schema=schemas.QuestCovidAntibodyTestResultSchema)
            return generators.ResourceRecordSet(schemas.QuestCovidAntibodyTestResultSchema, data)


def rebuild_biobank_covid_antibody_sample_resources_task():
    """
    Cloud Tasks: Generate all new BiobankCovidAntibodySample resource records.
    """
    ro_dao = ResourceDataDao(backup=True)
    with ro_dao.session() as ro_session:
        gen = BiobankCovidAntibodySampleGenerator()
        results = ro_session.query(BiobankCovidAntibodySample.id).all()

    logging.info('BiobankCovidAntibodySample table: rebuilding {0} resource records...'.format(len(results)))
    for row in results:
        res = gen.make_resource(row.id)
        res.save()


def rebuild_quest_covid_antibody_test_resources_task():
    """
    Cloud Tasks: Generate all new QuestCovidAntibodyTest resource records.
    """
    ro_dao = ResourceDataDao(backup=True)
    with ro_dao.session() as ro_session:
        gen = QuestCovidAntibodyTestGenerator()
        results = ro_session.query(QuestCovidAntibodyTest.id).all()

    logging.info('QuestCovidAntibodyTest table: rebuilding {0} resource records...'.format(len(results)))
    for row in results:
        res = gen.make_resource(row.id)
        res.save()


def rebuild_quest_covid_antibody_test_result_resources_task():
    """
    Cloud Tasks: Generate all new QuestCovidAntibodyTestResult resource records.
    """
    ro_dao = ResourceDataDao(backup=True)
    with ro_dao.session() as ro_session:
        gen = QuestCovidAntibodyTestResultGenerator()
        results = ro_session.query(QuestCovidAntibodyTestResult.id).all()

    logging.info('QuestCovidAntibodyTestResult table: rebuilding {0} resource records...'.format(len(results)))
    for row in results:
        res = gen.make_resource(row.id)
        res.save()
