import abc
from datetime import timedelta
from typing import List, Union

from rdr_service import clock
from rdr_service.api_util import open_cloud_file
from rdr_service.dao.genomics_dao import GenomicIncidentDao, GenomicJobRunDao, GenomicPRReportingDao, \
    GenomicLongReadReportingDao, GenomicRNAReportingDao, GenomicShortReadReportingReadDao
from rdr_service.genomic.genomic_data import GenomicQueryClass
from rdr_service.config import GENOMIC_INGESTION_REPORT_PATH, GENOMIC_INCIDENT_REPORT_PATH, GENOMIC_RESOLVED_REPORT_PATH


class GenomicDataQualityComponentBase:
    """Abstract base class for genomic data quality components"""
    __metaclass__ = abc.ABCMeta

    def __init__(self, controller=None):
        self.controller = controller


class ReportingComponent(GenomicDataQualityComponentBase):
    """
    A data quality reporting component.
    """
    def __init__(self, controller=None):
        super().__init__(controller=controller)
        self.query = GenomicQueryClass()
        self.incident_dao = GenomicIncidentDao()
        self.short_read_reporting_dao = GenomicShortReadReportingReadDao()
        self.pr_reporting_dao = GenomicPRReportingDao()
        self.long_read_reporting_dao = GenomicLongReadReportingDao()
        self.rna_reporting_dao = GenomicRNAReportingDao()
        self.ingestion_type = self.set_ingestion_type()
        self.report_def = None

    class ReportDef:
        def __init__(self,
                     level=None,
                     target=None,
                     time_frame=None,
                     display_name=None,
                     empty_report_string=None,
                     report_type=None):
            self.level = level
            self.target = target
            self.from_date = self.get_from_date(time_frame)
            self.display_name = display_name
            self.empty_report_string = empty_report_string
            self.report_type = report_type
            self.source_data_query = None
            self.source_data_params = None

        def get_sql(self):
            """Returns string representation of source data query"""
            return str(self.source_data_query)

        @staticmethod
        def get_from_date(time_frame):
            """
            Calculates the from_date from a time_frame
            :param time_frame: D or W
            :return:
            """
            interval_mappings = {'D': 1, 'W': 7}
            dd = timedelta(days=interval_mappings[time_frame])
            return clock.CLOCK.now() - dd

    def set_ingestion_type(self) -> Union[str, None]:
        if not self.controller:
            return None
        job_type_list = self.controller.job.name.split('_')
        if job_type_list[-1].lower() == 'ingestions' and len(job_type_list) > 4:
            return job_type_list[2]
        return None

    def get_report_parameters(self, **kwargs):

        def set_target_value() -> str:
            if self.ingestion_type and not kwargs.get('report_target'):
                return self.ingestion_type
            return kwargs.get('report_target', job_type_name_list[-1])

        display_name: str = self.get_report_display_name()
        job_type_name_list: List[str] = self.controller.job.name.split('_')
        target = set_target_value()

        return {
            "level": kwargs.get('report_level', job_type_name_list[1]),
            "time_frame": kwargs.get('time_frame', job_type_name_list[0][0]),
            "target": target,
            "display_name": display_name,
            "empty_report_string": self.get_empty_report_string(display_name),
            "report_type": 'ingestions' if self.ingestion_type else target
        }

    def get_report_display_name(self):
        job_type_list = self.controller.job.name.split('_')
        display_name = f'{job_type_list[0].capitalize()} '
        display_name += f'{job_type_list[-1].capitalize()} '
        if self.ingestion_type:
            display_name += f'({self.ingestion_type.capitalize()}) '
        display_name += job_type_list[1].capitalize()
        return display_name

    @staticmethod
    def get_empty_report_string(display_name):
        return f"No data to display for {display_name}"

    def set_report_def(self, **kwargs):
        """
        Reports are defined in inner class ReportDef objects
        This method returns the ReportDef object based on Target
        """
        report_def = self.ReportDef(**kwargs)

        # Map report targets to source data queries
        target_mappings = {
            ("SUMMARY", "RUNS"): self.query.dq_report_runs_summary(report_def.from_date),
            ("SUMMARY", "INCIDENTS"): self.incident_dao.get_daily_report_incidents(report_def.from_date),
            ("SUMMARY", "RESOLVED"): self.incident_dao.get_daily_report_resolved_manifests(report_def.from_date),
            ("SUMMARY", "SHORTREAD"): self.short_read_reporting_dao.get_reporting_counts(report_def.from_date),
            ("SUMMARY", "PROTEOMICS"): self.pr_reporting_dao.get_reporting_counts(report_def.from_date),
            ("SUMMARY", "LONGREAD"): self.long_read_reporting_dao.get_reporting_counts(report_def.from_date),
            ("SUMMARY", "RNA"): self.rna_reporting_dao.get_reporting_counts(report_def.from_date),
        }

        returned_from_method = target_mappings[(report_def.level, report_def.target)]

        if type(returned_from_method) is tuple:
            report_def.source_data_query, report_def.source_data_params = returned_from_method
        # Leaning into dao method
        else:
            report_def.source_data_query = returned_from_method
        self.report_def = report_def
        return report_def

    def get_report_data(self):
        """
        Returns the report by executing the report definition query
        :return: ResultProxy
        """

        # Execute the query_def's source_data_query
        dao = GenomicJobRunDao()

        with dao.session() as session:
            result = session.execute(
                self.report_def.source_data_query,
                self.report_def.source_data_params
            )

        return result

    def format_report(self, rows):
        """
        Converts the report query ResultProxy object to report string
        :param rows: ResultProxy
        :return: string
        """
        if rows:
            header = rows[0].keys() if not hasattr(rows, 'keys') else rows.keys()
            # Report title
            report_string = "```" + self.report_def.display_name + '\n'
            # Header row
            report_string += "    ".join(header)
            report_string += "\n"
            for row in rows:
                report_string += "    ".join(tuple(map(str, row)))
                report_string += "\n"
            report_string += "```"
        else:
            report_string = self.report_def.empty_report_string

        return report_string

    @staticmethod
    def create_report_file(report_string, display_name, report_type):
        path = {
            'ingestions': GENOMIC_INGESTION_REPORT_PATH,
            'incidents': GENOMIC_INCIDENT_REPORT_PATH,
            'resolved': GENOMIC_RESOLVED_REPORT_PATH
        }[report_type.lower()]

        now_str = clock.CLOCK.now().replace(microsecond=0).isoformat(sep="_", )
        report_file_name = f"{display_name.replace(' ', '_')}_{now_str}.txt"
        file_path = path + report_file_name

        with open_cloud_file(file_path, mode='wt') as cloud_file:
            cloud_file.write(report_string)

        return file_path
