from datetime import datetime
from typing import List

from sqlalchemy.orm import Session

from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.sample_order_status import SampleOrderStatus
from rdr_service.model.sample_receipt_status import SampleReceiptStatus
from rdr_service.participant_enums import OrderStatus, SampleStatus


class SampleSummaryDao:
    @classmethod
    def upsert_order_data(cls, participant_id: int, status: OrderStatus, test: str, time: datetime, session: Session):
        order_data = session.query(SampleOrderStatus).filter(
            SampleOrderStatus.participant_id == participant_id,
            SampleOrderStatus.test_code == test
        ).one_or_none()

        if order_data:
            order_data.status = status
            order_data.status_time = time
        else:
            session.add(
                SampleOrderStatus(
                    participant_id=participant_id,
                    test_code=test,
                    status=status,
                    status_time=time
                )
            )

    @classmethod
    def refresh_receipt_data(cls, participant_id: int, new_sample: BiobankStoredSample, session: Session):
        receipt_data = session.query(SampleReceiptStatus).filter(
            SampleReceiptStatus.participant_id == participant_id,
            SampleReceiptStatus.test_code == new_sample.test
        ).one_or_none()

        if receipt_data:
            sample_list = session.query(BiobankStoredSample).filter(
                BiobankStoredSample.biobankId == new_sample.biobankId,
                BiobankStoredSample.test == new_sample.test
            ).all()
            sample_list = [*sample_list, new_sample]

            status_sample = None
            non_disposed_samples = [
                sample for sample in sample_list if sample.status < SampleStatus.SAMPLE_NOT_RECEIVED
            ]
            if non_disposed_samples:
                status_sample = cls._latest_confirmed_sample(non_disposed_samples)
                receipt_data.status = status_sample.status
                receipt_data.status_time = status_sample.confirmed
            else:
                disposed_samples = [
                    sample for sample in sample_list if sample.status >= SampleStatus.SAMPLE_NOT_RECEIVED
                ]
                status_sample = cls._latest_disposed_sample(disposed_samples)
                receipt_data.status = status_sample.status
                receipt_data.status_time = status_sample.disposed
        else:
            timestamp = (
                new_sample.confirmed if new_sample.status < SampleStatus.SAMPLE_NOT_RECEIVED
                else new_sample.disposed
            )
            session.add(
                SampleReceiptStatus(
                    participant_id=participant_id,
                    test_code=new_sample.test,
                    status=new_sample.status,
                    status_time=timestamp
                )
            )

    @classmethod
    def get_receipt_samples(
        cls, participant_id: int, test_code_list: List[str], session: Session
    ) -> List[SampleReceiptStatus]:
        return session.query(SampleReceiptStatus).filter(
            SampleReceiptStatus.participant_id == participant_id,
            SampleReceiptStatus.test_code.in_(test_code_list)
        ).all()

    @classmethod
    def get_order_samples(
        cls, participant_id: int, test_code_list: List[str], session: Session
    ) -> List[SampleOrderStatus]:
        return session.query(SampleOrderStatus).filter(
            SampleOrderStatus.participant_id == participant_id,
            SampleOrderStatus.test_code.in_(test_code_list)
        ).all()

    @classmethod
    def _latest_confirmed_sample(cls, sample_list: List[BiobankStoredSample]) -> BiobankStoredSample:
        timestamp = None
        result = None
        for sample in sample_list:
            if timestamp is None or timestamp < sample.confirmed:
                timestamp = sample.confirmed
                result = sample
        return result

    @classmethod
    def _latest_disposed_sample(cls, sample_list: List[BiobankStoredSample]) -> BiobankStoredSample:
        timestamp = None
        result = None
        for sample in sample_list:
            if timestamp is None or timestamp < sample.disposed:
                timestamp = sample.disposed
                result = sample
        return result
