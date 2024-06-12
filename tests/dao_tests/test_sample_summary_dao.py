from datetime import datetime

from rdr_service.dao.sample_summary_dao import (
    OrderStatus, SampleOrderStatus, SampleReceiptStatus, SampleStatus, SampleSummaryDao
)
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from tests.helpers.unittest_base import BaseTestCase


class SampleSummaryDaoTest(BaseTestCase):
    def setUp(self, *args, **kwargs):
        super().setUp(*args, **kwargs)
        self.participant = self.data_generator.create_database_participant()

    def test_insert_order_data(self):
        ed_time = datetime(2022, 11, 1)
        sal_time = datetime(2023, 1, 15)
        SampleSummaryDao.upsert_order_data(
            self.participant.participantId, OrderStatus.FINALIZED, '1ED10', ed_time, self.session
        )
        SampleSummaryDao.upsert_order_data(
            self.participant.participantId, OrderStatus.CREATED, '2SAL0', sal_time, self.session
        )
        self.session.commit()

        order_list = self.session.query(SampleOrderStatus).filter(
            SampleOrderStatus.participant_id == self.participant.participantId
        ).all()
        self.assertEqual(2, len(order_list))
        for order in order_list:
            if order.test_code == '1ED10':
                self.assertEqual(ed_time, order.status_time)
                self.assertEqual(OrderStatus.FINALIZED, order.status)
            elif order.test_code == '2SAL0':
                self.assertEqual(sal_time, order.status_time)
                self.assertEqual(OrderStatus.CREATED, order.status)
            else:
                self.fail(f'was not expecting test code "{order.test}"')

    def test_insert_receipt_data(self):
        ed_time = datetime(2022, 11, 1)
        sal_time = datetime(2023, 1, 15)
        SampleSummaryDao.refresh_receipt_data(
            self.participant.participantId,
            BiobankStoredSample(
                test='1ED10',
                status=SampleStatus.RECEIVED,
                confirmed=ed_time
            ),
            self.session
        )
        SampleSummaryDao.refresh_receipt_data(
            self.participant.participantId,
            BiobankStoredSample(
                test='2SAL0',
                status=SampleStatus.SAMPLE_NOT_RECEIVED,
                disposed=sal_time
            ),
            self.session
        )
        self.session.commit()

        receipt_list = self.session.query(SampleReceiptStatus).filter(
            SampleReceiptStatus.participant_id == self.participant.participantId
        ).all()
        self.assertEqual(2, len(receipt_list))
        for receipt in receipt_list:
            if receipt.test_code == '1ED10':
                self.assertEqual(ed_time, receipt.status_time)
                self.assertEqual(SampleStatus.RECEIVED, receipt.status)
            elif receipt.test_code == '2SAL0':
                self.assertEqual(sal_time, receipt.status_time)
                self.assertEqual(SampleStatus.SAMPLE_NOT_RECEIVED, receipt.status)
            else:
                self.fail(f'was not expecting test code "{receipt.test}"')

    def test_update_order_data(self):
        SampleSummaryDao.upsert_order_data(
            self.participant.participantId, OrderStatus.CREATED, '1ED10',
            datetime(2021, 10, 30), self.session
        )
        self.session.commit()

        updated_timestamp = datetime(2022, 11, 1)
        SampleSummaryDao.upsert_order_data(
            self.participant.participantId, OrderStatus.FINALIZED, '1ED10', updated_timestamp, self.session
        )
        self.session.commit()

        order = self.session.query(SampleOrderStatus).filter(
            SampleOrderStatus.participant_id == self.participant.participantId
        ).one()
        self.assertEqual(updated_timestamp, order.status_time)
        self.assertEqual(OrderStatus.FINALIZED, order.status)

    def test_update_receipt_data(self):
        SampleSummaryDao.refresh_receipt_data(
            self.participant.participantId,
            BiobankStoredSample(
                status=SampleStatus.RECEIVED,
                test='1ED10',
                confirmed=datetime(2021, 10, 30)
            ),
            self.session
        )
        self.session.commit()

        updated_timestamp = datetime(2022, 11, 1)
        SampleSummaryDao.refresh_receipt_data(
            self.participant.participantId,
            BiobankStoredSample(
                status=SampleStatus.DISPOSED,
                test='1ED10',
                confirmed=updated_timestamp
            ),
            self.session
        )
        self.session.commit()

        data = self.session.query(SampleReceiptStatus).filter(
            SampleReceiptStatus.participant_id == self.participant.participantId
        ).one()
        self.assertEqual(updated_timestamp, data.status_time)
        self.assertEqual(SampleStatus.DISPOSED, data.status)

    def test_ignore_bad_receipt_update(self):
        """A new sample with a "bad" status should not replace existing data from a "good" status"""
        original_timestamp = datetime(2021, 10, 30)
        sample = self.data_generator.create_database_biobank_stored_sample(
            status=SampleStatus.RECEIVED,
            test='1ED10',
            confirmed=original_timestamp
        )
        SampleSummaryDao.refresh_receipt_data(self.participant.participantId, sample, self.session)
        self.session.commit()

        SampleSummaryDao.refresh_receipt_data(
            self.participant.participantId,
            BiobankStoredSample(
                status=SampleStatus.SAMPLE_NOT_RECEIVED,
                test='1ED10',
                disposed=datetime(2022, 11, 1)
            ),
            self.session
        )
        self.session.commit()

        data = self.session.query(SampleReceiptStatus).filter(
            SampleReceiptStatus.participant_id == self.participant.participantId
        ).one()
        self.assertEqual(original_timestamp, data.status_time)
        self.assertEqual(SampleStatus.RECEIVED, data.status)

    def test_prioritize_good_receipt_update(self):
        """A new sample with a "good" status should replace existing data from a "bad" status"""
        sample = self.data_generator.create_database_biobank_stored_sample(
            status=SampleStatus.SAMPLE_NOT_RECEIVED,
            test='1ED10',
            disposed=datetime(2021, 10, 30)
        )
        SampleSummaryDao.refresh_receipt_data(self.participant.participantId, sample, self.session)
        self.session.commit()

        new_receipt_time = datetime(2022, 11, 1)
        SampleSummaryDao.refresh_receipt_data(
            self.participant.participantId,
            BiobankStoredSample(
                status=SampleStatus.DISPOSED,
                test='1ED10',
                confirmed=new_receipt_time
            ),
            self.session
        )
        self.session.commit()

        data = self.session.query(SampleReceiptStatus).filter(
            SampleReceiptStatus.participant_id == self.participant.participantId
        ).one()
        self.assertEqual(new_receipt_time, data.status_time)
        self.assertEqual(SampleStatus.DISPOSED, data.status)
