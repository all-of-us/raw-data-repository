from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.model.covid_antibody_study import (
    BiobankCovidAntibodySample,
    QuestCovidAntibodyTest,
    QuestCovidAntibodyTestResult
)


class BiobankCovidAntibodySampleDao(UpdatableDao):

    def __init__(self):
        super(BiobankCovidAntibodySampleDao, self).__init__(BiobankCovidAntibodySample, order_by_ending=["id"])

    def _find_dup_with_session(self, session, biobank_covid_antibody_sample_obj):
        query = (session.query(BiobankCovidAntibodySample)
                 .filter(BiobankCovidAntibodySample.sampleId == biobank_covid_antibody_sample_obj.sampleId))

        record = query.first()
        if record:
            return record.id

    def upsert_all_with_session(self, session, biobank_covid_antibody_samples):
        records = list(biobank_covid_antibody_samples)

        for record in records:
            dup_id = self._find_dup_with_session(session, record)
            if dup_id:
                record.id = dup_id
            session.merge(record)

    def get_biobank_id_by_sample_id_with_session(self, session, sample_id):
        query = (session.query(BiobankCovidAntibodySample)
                 .filter(BiobankCovidAntibodySample.sampleId == sample_id))

        record = query.first()
        if record:
            return record.aouBiobankId if record.aouBiobankId else record.noAouBiobankId
        else:
            return None


class QuestCovidAntibodyTestDao(UpdatableDao):

    def __init__(self):
        super(QuestCovidAntibodyTestDao, self).__init__(QuestCovidAntibodyTest, order_by_ending=["id"])

    def _find_dup_with_session(self, session, quest_covid_antibody_test_obj):
        query = (session.query(QuestCovidAntibodyTest)
                 .filter(QuestCovidAntibodyTest.accession == quest_covid_antibody_test_obj.accession,
                         QuestCovidAntibodyTest.batch == quest_covid_antibody_test_obj.batch))

        record = query.first()
        if record:
            return record.id

    def upsert_all_with_session(self, session, quest_covid_antibody_test):
        records = list(quest_covid_antibody_test)

        for record in records:
            dup_id = self._find_dup_with_session(session, record)
            if dup_id:
                record.id = dup_id
            session.merge(record)


class QuestCovidAntibodyTestResultDao(UpdatableDao):

    def __init__(self):
        super(QuestCovidAntibodyTestResultDao, self).__init__(QuestCovidAntibodyTestResult, order_by_ending=["id"])

    def _find_dup_with_session(self, session, quest_covid_antibody_test_result_obj):
        query = (session.query(QuestCovidAntibodyTestResult)
                 .filter(QuestCovidAntibodyTestResult.accession == quest_covid_antibody_test_result_obj.accession,
                         QuestCovidAntibodyTestResult.resultName == quest_covid_antibody_test_result_obj.resultName,
                         QuestCovidAntibodyTestResult.batch == quest_covid_antibody_test_result_obj.batch
                         ))

        record = query.first()
        if record:
            return record.id

    def upsert_all_with_session(self, session, quest_covid_antibody_test_result):
        records = list(quest_covid_antibody_test_result)

        for record in records:
            dup_id = self._find_dup_with_session(session, record)
            if dup_id:
                record.id = dup_id
            session.merge(record)
