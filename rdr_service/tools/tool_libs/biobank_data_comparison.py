from dataclasses import dataclass
from datetime import datetime
from protorpc import messages
from typing import List, Optional

import argparse
from dateutil.parser import parse
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
class SamplePair:
    report_data: Optional[BiobankStoredSample]
    api_data: Optional[BiobankSpecimen]


@dataclass
class DifferenceFound:
    sample_pair: SamplePair
    type: int


class BiobankSampleComparator:
    def __init__(self, sample_pair: SamplePair):
        self.api_data = sample_pair.api_data
        self.report_data = sample_pair.report_data

    def get_differences(self) -> List[DifferenceFound]:
        discrepancies_found: List[DifferenceFound] = []

        if self.report_data is None:
            discrepancies_found.append(DifferenceFound(
                type=DifferenceType.MISSING_FROM_SIR,
                sample_pair=SamplePair(report_data=self.report_data, api_data=self.api_data)
            ))
        elif self.api_data is None:
            discrepancies_found.append(DifferenceFound(
                type=DifferenceType.MISSING_FROM_API_DATA,
                sample_pair=SamplePair(report_data=self.report_data, api_data=self.api_data)
            ))
        else:
            if self.report_data.biobankId != self.api_data.biobankId:
                discrepancies_found.append(DifferenceFound(
                    type=DifferenceType.BIOBANK_ID,
                    sample_pair=SamplePair(report_data=self.report_data, api_data=self.api_data)
                ))
            if self.report_data.test != self.api_data.testCode:
                discrepancies_found.append(DifferenceFound(
                    type=DifferenceType.TEST_CODE,
                    sample_pair=SamplePair(report_data=self.report_data, api_data=self.api_data)
                ))
            if self.report_data.biobankOrderIdentifier != self.api_data.orderId:
                discrepancies_found.append(DifferenceFound(
                    type=DifferenceType.ORDER_ID,
                    sample_pair=SamplePair(report_data=self.report_data, api_data=self.api_data)
                ))
            if not is_datetime_equal(
                self.report_data.confirmed, self.api_data.confirmedDate, difference_allowed_seconds=3600
            ):
                discrepancies_found.append(DifferenceFound(
                    type=DifferenceType.CONFIRMED_DATE,
                    sample_pair=SamplePair(report_data=self.report_data, api_data=self.api_data)
                ))
            if not is_datetime_equal(
                self.report_data.disposed, self.api_data.disposalDate, difference_allowed_seconds=3600
            ):
                discrepancies_found.append((DifferenceFound(
                    type=DifferenceType.DISPOSAL_DATE,
                    sample_pair=SamplePair(report_data=self.report_data, api_data=self.api_data)
                )))
            if not self.does_status_field_match():
                discrepancies_found.append(DifferenceFound(
                    type=DifferenceType.STATUS,
                    sample_pair=SamplePair(report_data=self.report_data, api_data=self.api_data)
                ))

        return discrepancies_found

    def does_status_field_match(self):
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

        # TODO: samples that are 1SAL2 and disposed on the API but still with a status of 1 on the SIR...
        #       these wouldn't get updated on the SIR when the biobank gets it 6weeks later


class BiobankDataCheckTool(ToolBase):
    def run(self):
        super(BiobankDataCheckTool, self).run()

        # Parse supplied date range
        start_date = parse(self.args.start)
        end_date = parse(self.args.end)

        with self.get_session() as session:
            stored_samples = self._get_report_samples(session=session, start_date=start_date, end_date=end_date)
            api_samples = self._get_api_samples(session=session, start_date=start_date, end_date=end_date)
            sample_pairs = self._get_sample_pairs(
                # Match up the samples by their id, loading any from the database that might
                # have been missed in the date range
                session=session,
                report_samples=stored_samples,
                api_samples=api_samples
            )

            differences_found: List[DifferenceFound] = []
            for pair in sample_pairs:
                comparator = BiobankSampleComparator(pair)
                differences_found.extend(comparator.get_differences())

        self._print_differences_found(differences=differences_found)

    @classmethod
    def _get_report_samples(cls, session: Session, start_date: datetime, end_date: datetime) \
            -> List[BiobankStoredSample]:
        """Get the sample data received from the Sample Inventory Reports"""
        return list(
            session.query(BiobankStoredSample).filter(
                BiobankStoredSample.rdrCreated.between(start_date, end_date)
            )
        )

    @classmethod
    def _get_api_samples(cls, session: Session, start_date: datetime, end_date: datetime) -> List[BiobankSpecimen]:
        """Get the sample data received from the Specimen API"""
        return list(
            session.query(BiobankSpecimen).filter(
                BiobankSpecimen.created.between(start_date, end_date)
            )
        )

    @classmethod
    def _get_sample_pairs(cls, report_samples: List[BiobankStoredSample], api_samples: List[BiobankSpecimen],
                          session: Session) -> List[SamplePair]:
        """
        Creates SamplePairs for all the provided samples.
        If a sample is found in one but not the other, then the database will be checked to see if the counterpart
        might have just been outside the date range.
        """
        sample_pairs = []

        # Create SamplePairs for all the api samples found in the report data
        report_samples_id_map = {sample.biobankStoredSampleId: sample for sample in report_samples}
        for api_sample in api_samples:
            report_sample = None
            if api_sample.rlimsId in report_samples_id_map:
                report_sample = report_samples_id_map[api_sample.rlimsId]
                del report_samples_id_map[api_sample.rlimsId]
            sample_pairs.append(
                SamplePair(api_data=api_sample, report_data=report_sample)
            )

        # Any samples still left in the report map didn't have counter parts in the api data
        for report_sample in report_samples_id_map.values():
            sample_pairs.append(
                SamplePair(api_data=None, report_data=report_sample)
            )

        # Check the database to see if any samples from the report match something from the api
        pairs_missing_api_data_map = {
            pair.report_data.biobankStoredSampleId: pair
            for pair in sample_pairs if pair.api_data is None
        }
        sample_ids = pairs_missing_api_data_map.keys()
        db_api_data: List[BiobankSpecimen] = session.query(BiobankSpecimen).filter(
            BiobankSpecimen.rlimsId.in_(sample_ids)
        ).all()
        for api_sample in db_api_data:
            pair = pairs_missing_api_data_map[api_sample.rlimsId]
            pair.api_data = api_sample

        # ... and check the inverse, making sure any api samples that fell within the time range didn't get missed
        # because of a date difference
        pairs_missing_report_data_map = {
            pair.api_data.rlimsId: pair
            for pair in sample_pairs if pair.report_data is None
        }
        sample_ids = pairs_missing_report_data_map.keys()
        db_report_data: List[BiobankStoredSample] = session.query(BiobankStoredSample).filter(
            BiobankStoredSample.biobankStoredSampleId.in_(sample_ids)
        )
        for report_sample in db_report_data:
            pair = pairs_missing_report_data_map[report_sample.biobankStoredSampleId]
            pair.report_data = report_sample

        return sample_pairs

    @classmethod
    def _print_differences_found(cls, differences: List[DifferenceFound]):
        for diff in differences:
            specimen = diff.sample_pair.api_data
            sample = diff.sample_pair.report_data
            if diff.type == DifferenceType.STATUS:
                print(f'{specimen.rlimsId} STATUS -- '
                      f'API: "{specimen.status}, {specimen.disposalReason}" '
                      f'SIR: "{str(sample.status)}"')
            elif diff.type == DifferenceType.DISPOSAL_DATE:
                if sample.status == SampleStatus.RECEIVED and specimen.status == 'Disposed':
                    ...
                else:
                    print(f'{specimen.rlimsId} DISPOSAL DATE -- API: {specimen.disposalDate} SIR: {sample.disposed}')
            elif diff.type == DifferenceType.CONFIRMED_DATE:
                print(f'{specimen.rlimsId} CONFIRMED DATE -- API: {specimen.confirmedDate} SIR: {sample.confirmed}')
            else:
                sample_id = sample.biobankStoredSampleId if sample else specimen.rlimsId
                print(f'{sample_id} -- {str(diff.type)}')


def add_additional_arguments(parser: argparse.ArgumentParser):
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)


def run():
    cli_run(tool_cmd, tool_desc, BiobankDataCheckTool, add_additional_arguments)
