from datetime import datetime

from sqlalchemy.orm import Session

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
    def upsert_receipt_data(
        cls, participant_id: int, status: SampleStatus, test: str, time: datetime, session: Session
    ):
        receipt_data = session.query(SampleReceiptStatus).filter(
            SampleReceiptStatus.participant_id == participant_id,
            SampleReceiptStatus.test_code == test
        ).one_or_none()

        if receipt_data:
            receipt_data.status = status
            receipt_data.status_time = time
        else:
            session.add(
                SampleReceiptStatus(
                    participant_id=participant_id,
                    test_code=test,
                    status=status,
                    status_time=time
                )
            )
