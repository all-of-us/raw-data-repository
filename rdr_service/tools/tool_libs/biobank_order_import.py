import csv
from dataclasses import dataclass
from datetime import datetime
import pytz
from typing import Dict, List, Optional

from dateutil.parser import parse

from rdr_service.model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from rdr_service.model.log_position import LogPosition
from rdr_service.model.site import Site
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'biobank-order-import'
tool_desc = 'Load Biobank order data from a CSV into a database (DA-4775)'

file_path = 'order_data.csv'


@dataclass()
class OrderInformation:
    participant_id: int
    # biobank_id: int
    order_id: str
    mayo_order_id: str
    created_user: str
    created_site: str
    created_timestamp: str
    collected_user: str
    collected_site: str
    collected_samples: str
    collected_timestamp: str
    collected_notes: str
    processed_user: str
    processed_site: str
    processed_samples: List[str]
    processed_timestamp: Dict[str, int]
    processed_centrifuge_type: str
    processed_notes: str
    finalized_user: str
    finalized_site: str
    finalized_samples: str
    finalized_timestamp: str
    finalized_notes: str
    fedex_tracking: str

    def as_dict(self):
        return self.__dict__


class BiobankOrderImport(ToolBase):
    def run(self):
        super().run()

        file_data = self._load_file()

        print("checking db for orders...")
        order_ids = {order.mayo_order_id for order in file_data}
        with self.get_session() as session:
            db_orders = session.query(BiobankOrder).filter(
                BiobankOrder.biobankOrderId.in_(order_ids)
            ).all()
            print(f"skipping {len(db_orders)} orders that are already in the db")

            found_order_ids = {order.biobankOrderId for order in db_orders}

            import_count = 0
            for order in file_data:
                if order.mayo_order_id in found_order_ids:
                    # skip any orders we already have
                    continue
                if order.mayo_order_id == 'NULL':
                    # skip any orders that are missing a Mayo order id
                    continue

                new_db_order = BiobankOrder(
                    biobankOrderId=order.mayo_order_id,
                    participantId=order.participant_id,
                    logPosition=LogPosition(),
                    created=parse(order.created_timestamp),
                    collectedNote=order.collected_notes,
                    processedNote=order.processed_notes,
                    finalizedNote=order.finalized_notes,
                    finalizedSiteId=self._get_site_id(session, order.finalized_site),
                    finalizedUsername=order.finalized_user,
                    finalizedTime=order.finalized_timestamp,
                    sourceSiteId=self._get_site_id(session, order.created_site),
                    sourceUsername=order.created_user,
                    collectedSiteId=self._get_site_id(session, order.collected_site),
                    collectedUsername=order.collected_user,
                    processedSiteId=self._get_site_id(session, order.processed_site),
                    processedUsername=order.processed_user,
                    version=1,
                    orderOrigin='hpro',
                    identifiers=self._build_order_identifiers(order),
                    samples=self._build_samples(order)
                )
                session.add(new_db_order)
                import_count += 1

        print(f"imported {import_count} orders")

    def _load_file(self) -> List[OrderInformation]:
        result = []
        with open(file_path, 'r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                order = OrderInformation(
                    participant_id=row['participant_id'][1:],
                    order_id=row['order_id'],
                    mayo_order_id=row['mayo_id'],
                    created_user=row['created_user'],
                    created_site=row['created_site'],
                    created_timestamp=row['created_ts'],
                    collected_user=row['collected_user'],
                    collected_site=row['collected_site'],
                    collected_samples=row['collected_samples'],
                    collected_timestamp=row['collected_ts'],
                    collected_notes=self._value_or_none(row['collected_notes']),
                    processed_user=self._value_or_none(row['processed_user']),
                    processed_site=self._value_or_none(row['processed_site']),
                    processed_samples=self._value_or_none(row['processed_samples']),
                    processed_timestamp=self._value_or_none(row['processed_ts']),
                    processed_centrifuge_type=row['processed_centrifuge_type'],
                    processed_notes=self._value_or_none(row['processed_notes']),
                    finalized_user=self._value_or_none(row['finalized_user']),
                    finalized_site=self._value_or_none(row['finalized_site']),
                    finalized_samples=row['finalized_samples'],
                    finalized_timestamp=self._value_or_none(row['finalized_ts']),
                    finalized_notes=self._value_or_none(row['finalized_notes']),
                    fedex_tracking=self._value_or_none(row['fedex_tracking'])
                )
                result.append(order)

        return result

    @classmethod
    def _build_order_identifiers(cls, order: OrderInformation):
        return [
            BiobankOrderIdentifier(
                system='https://orders.mayomedicallaboratories.com',
                value=order.mayo_order_id,
                biobankOrderId=order.mayo_order_id
            ),
            BiobankOrderIdentifier(
                system='https://www.pmi-ops.org',
                value=order.order_id,
                biobankOrderId=order.mayo_order_id
            )
        ]

    @classmethod
    def _build_samples(cls, order: OrderInformation):
        test_codes = eval(order.collected_samples)
        assert test_codes  # need something in the collected test codes, otherwise what are we importing

        processing_times = eval(order.processed_timestamp) if order.processed_timestamp else None
        if order.processed_samples and order.processed_samples != '[]':
            assert processing_times  # if we have processed samples, we should have processing times

        result = []
        for test in test_codes:
            processed_time = None
            if processing_times and test in processing_times:
                processed_time = datetime.fromtimestamp(processing_times[test], tz=pytz.utc)

            ordered_sample = BiobankOrderedSample(
                test=test,
                processingRequired=order.processed_samples and test in order.processed_samples,
                collected=parse(order.collected_timestamp),
                processed=processed_time,
                finalized=order.finalized_timestamp
            )
            result.append(ordered_sample)

        return result

    @classmethod
    def _get_site_id(cls, session, google_group):
        site = session.query(Site.siteId).filter(
            Site.googleGroup == google_group
        ).one_or_none()
        return site.siteId if site else None

    @classmethod
    def _value_or_none(cls, val) -> Optional:
        return val if val and val != 'NULL' else None


def run():
    return cli_run(tool_cmd, tool_desc, BiobankOrderImport)
