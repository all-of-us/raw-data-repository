#
# Stored Sample data generator.
#
import logging

from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.data_gen.generators.base_gen import BaseGen
from rdr_service.model.biobank_order import BiobankOrder, BiobankOrderedSample
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.participant_summary import ParticipantSummary

_logger = logging.getLogger("rdr_logger")


class StoredSampleGen(BaseGen):
    """
    Fake stored samples generator
    """
    def __init__(self):
        """ initialize stored sample generator """
        super(StoredSampleGen, self).__init__(load_data=False)

        self.sample_dao = BiobankStoredSampleDao()

    def make_stored_sample_for_participant(self, pid):
        """

        :param pid:
        :return:
        """
        # Gets the data to insert into stored sample
        with self.sample_dao.session() as session:
            sample = session.query(
                ParticipantSummary.participantId,
                ParticipantSummary.biobankId,
                BiobankOrder.biobankOrderId,
                BiobankOrderedSample.test,
            ).join(
                BiobankOrder,
                ParticipantSummary.participantId == BiobankOrder.participantId
            ).join(
                BiobankOrderedSample,
                BiobankOrderedSample.biobankOrderId == BiobankOrder.biobankOrderId
            ).filter(
                ParticipantSummary.participantId == pid
            ).first()

        if not sample:
            _logger.error(f"Missing required data for PID {pid}")
            return 1

        # creates the stored sample
        new_stored_sample = BiobankStoredSample(
            biobankStoredSampleId=f'ss-{sample.participantId}-{sample.test}',
            biobankId=sample.biobankId,
            biobankOrderIdentifier=sample.biobankOrderId,
            test=sample.test
        )
        ss = self.sample_dao.insert(new_stored_sample)

        return ss
