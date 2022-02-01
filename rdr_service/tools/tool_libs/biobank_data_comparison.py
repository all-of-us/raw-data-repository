import argparse
from dataclasses import dataclass
from datetime import datetime
from dateutil.parser import parse
from protorpc import messages
from typing import Collection, Dict, List, Optional

from sqlalchemy.orm import Session

from rdr_service.app_util import is_datetime_equal
from rdr_service.model.biobank_order import BiobankSpecimen
from rdr_service.model.biobank_stored_sample import BiobankStoredSample, SampleStatus
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase


tool_cmd = 'api-check'
tool_desc = 'Run a comparison of the API data to the Sample Inventory Report data.'


class DifferenceType(messages.Enum):
    MISSING_FROM_SIR = 1
    MISSING_FROM_API_DATA = 2
    DISPOSAL_DATE = 3
    CONFIRMED_DATE = 4
    BIOBANK_ID = 5
    TEST_CODE = 6
    ORDER_ID = 7
    STATUS = 8


@dataclass
class DifferenceFound:
    report_data: Optional[BiobankStoredSample]
    api_data: Optional[BiobankSpecimen]
    type: int


class BiobankSampleComparator:
    def __init__(self, api_data: Optional[BiobankSpecimen], report_data: Optional[BiobankStoredSample]):
        self.api_data = api_data
        self.report_data = report_data

    def get_differences(self) -> List[DifferenceFound]:
        discrepancies_found: List[DifferenceFound] = []

        if self.report_data is None:
            discrepancies_found.append(DifferenceFound(
                type=DifferenceType.MISSING_FROM_SIR,
                report_data=self.report_data, api_data=self.api_data
            ))
        elif self.api_data is None:
            discrepancies_found.append(DifferenceFound(
                type=DifferenceType.MISSING_FROM_API_DATA,
                report_data=self.report_data, api_data=self.api_data
            ))
        else:
            if self.report_data.biobankId != self.api_data.biobankId:
                discrepancies_found.append(DifferenceFound(
                    type=DifferenceType.BIOBANK_ID,
                    report_data=self.report_data, api_data=self.api_data
                ))
            if self.report_data.test != self.api_data.testCode:
                discrepancies_found.append(DifferenceFound(
                    type=DifferenceType.TEST_CODE,
                    report_data=self.report_data, api_data=self.api_data
                ))
            if self.report_data.biobankOrderIdentifier != self.api_data.orderId:
                discrepancies_found.append(DifferenceFound(
                    type=DifferenceType.ORDER_ID,
                    report_data=self.report_data, api_data=self.api_data
                ))
            if not is_datetime_equal(
                self.report_data.confirmed, self.api_data.confirmedDate, difference_allowed_seconds=3600
            ):
                discrepancies_found.append(DifferenceFound(
                    type=DifferenceType.CONFIRMED_DATE,
                    report_data=self.report_data, api_data=self.api_data
                ))
            if not is_datetime_equal(
                self.report_data.disposed, self.api_data.disposalDate, difference_allowed_seconds=3600
            ):
                discrepancies_found.append((DifferenceFound(
                    type=DifferenceType.DISPOSAL_DATE,
                    report_data=self.report_data, api_data=self.api_data
                )))
            if not self.status_field_match():
                discrepancies_found.append(DifferenceFound(
                    type=DifferenceType.STATUS,
                    report_data=self.report_data, api_data=self.api_data
                ))

        return discrepancies_found

    def status_field_match(self):
        report_status = self.report_data.status
        api_status = self.api_data.status
        api_disposal_reason = self.api_data.disposalReason

        if report_status == SampleStatus.CONSUMED and api_status == 'Disposed' and api_disposal_reason == 'Consumed':
            return True
        elif report_status == SampleStatus.QNS_FOR_PROCESSING and \
                api_status == 'Disposed' and api_disposal_reason == 'QNS for Processing':
            return True
        elif report_status == SampleStatus.UNKNOWN and (
            (api_status == 'Disposed' and api_disposal_reason == 'Could Not Process')
            or (api_status == 'Disposed' and api_disposal_reason == 'Consumed')
            or (api_status == 'Disposed' and api_disposal_reason == 'Damaged')
            or (api_status == 'Disposed' and api_disposal_reason == 'No Consent')
            or (api_status == 'Disposed' and api_disposal_reason == 'Missing')
            or (api_status == 'In Circulation' and self.report_data.test == '1PXR2')
        ):
            return True
        elif report_status == SampleStatus.RECEIVED and api_status == 'In Circulation':
            return True
        # elif report_status == SampleStatus.RECEIVED and api_status == 'Disposed':
        #     # TODO: make sure this is actually something we want to do
        #     #  (ignore samples that haven't been updated in a SIR)
        #     return True
        elif report_status == SampleStatus.QUALITY_ISSUE and (
            api_status == 'Disposed' and api_disposal_reason == 'Quality Issue'
        ):
            return True
        elif report_status == SampleStatus.ACCESSINGING_ERROR and (
            api_status == 'Disposed' and api_disposal_reason == 'Accessioning Error'
        ):
            return True
        elif report_status == SampleStatus.LAB_ACCIDENT and (
            api_status == 'Disposed' and api_disposal_reason == 'Lab Accident'
        ):
            return True
        elif report_status == SampleStatus.DISPOSED and (
            api_status == 'Disposed' and api_disposal_reason == 'Disposed'
        ):
            return True
        elif report_status == SampleStatus.SAMPLE_NOT_PROCESSED and (
            api_status == 'Disposed' and api_disposal_reason == 'Sample Not Processed'
        ):
            return True
        else:
            return False


class BiobankDataCheckTool(ToolBase):
    def run(self):
        super(BiobankDataCheckTool, self).run()

        # Parse supplied date range
        start_date = parse(self.args.start)
        end_date = parse(self.args.end)

        with self.get_session() as session:
            stored_samples = self._get_stored_samples(session=session, start_date=start_date, end_date=end_date)
            specimens_dict = self._get_corresponding_api_data(session=session, stored_samples=stored_samples)
            discrepancies_found: List[DifferenceFound] = []
            for sample in stored_samples:
                specimen = specimens_dict.get(sample.biobankStoredSampleId)
                discrepancies_found.extend(self.check_specimen_against_sample(specimen=specimen, sample=sample))

            specimen_without_inventory = self._get_specimen_without_inventory_counterpart(
                session=session,
                start_date=start_date,
                end_date=end_date
            )
            discrepancies_found.extend([
                DifferenceFound(api_data=specimen, report_data=None, type=DifferenceType.MISSING_FROM_SIR)
                for specimen in specimen_without_inventory
            ])

        for difference in discrepancies_found:
            specimen = difference.api_data
            sample = difference.report_data
            if difference.type == DifferenceType.STATUS:
                print(f'{specimen.rlimsId} STATUS -- '
                      f'API: "{specimen.status}, {specimen.disposalReason}" '
                      f'SIR: "{str(sample.status)}"')
            elif difference.type == DifferenceType.DISPOSAL_DATE:
                if difference.report_data.status == SampleStatus.RECEIVED and difference.api_data.status == 'Disposed':
                    ...
                else:
                    print(f'{specimen.rlimsId} DISPOSAL DATE -- API: {specimen.disposalDate} SIR: {sample.disposed}')
            elif difference.type == DifferenceType.CONFIRMED_DATE:
                print(f'{specimen.rlimsId} CONFIRMED DATE -- API: {specimen.confirmedDate} SIR: {sample.confirmed}')
            else:
                sample_id = sample.biobankStoredSampleId if sample else specimen.rlimsId
                print(f'{sample_id} -- {str(difference.type)}')

        # TODO: samples that are 1SAL2 and disposed on the API but still with a status of 1 on the SIR...
        #       these wouldn't get updated on the SIR when the biobank gets it 6weeks later

    @classmethod
    def _get_stored_samples(cls, session: Session, start_date: datetime, end_date: datetime) \
            -> Collection[BiobankStoredSample]:
        return list(
            session.query(BiobankStoredSample).filter(
                BiobankStoredSample.rdrCreated.between(start_date, end_date)
            )
        )

    @classmethod
    def _get_specimen_without_inventory_counterpart(cls, session: Session, start_date: datetime, end_date: datetime) \
            -> Collection[BiobankSpecimen]:
        return list(
            session.query(BiobankSpecimen).outerjoin(
                BiobankStoredSample,
                BiobankStoredSample.biobankStoredSampleId == BiobankSpecimen.rlimsId
            ).filter(
                BiobankSpecimen.created.between(start_date, end_date),
                BiobankStoredSample.biobankStoredSampleId.is_(None)
            )
        )

    @classmethod
    def _get_corresponding_api_data(cls, session: Session, stored_samples: Collection[BiobankStoredSample])\
            -> Dict[str, BiobankSpecimen]:
        """
        Get the corresponding parent specimen for each stored sample given.
        Return the specimens in a dict that uses the provided rlimsid for a specimen as the key, and the
        matching specimen object as the value.
        """
        stored_sample_ids = [specimen.biobankStoredSampleId for specimen in stored_samples]
        specimen_dict = {sample_id: None for sample_id in stored_sample_ids}

        # Fill in the dictionary with the stored samples we can retrieve from the database
        stored_samples: Collection[BiobankSpecimen] = session.query(
            BiobankSpecimen
        ).filter(
            BiobankSpecimen.rlimsId.in_(stored_sample_ids)
        )
        for specimen in stored_samples:
            specimen_dict[specimen.rlimsId] = specimen

        return specimen_dict


def add_additional_arguments(parser: argparse.ArgumentParser):
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)


def run():
    cli_run(tool_cmd, tool_desc, BiobankDataCheckTool, add_additional_arguments)
