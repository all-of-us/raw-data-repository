import abc
from datetime import timedelta

from rdr_service import clock
from rdr_service.api_util import open_cloud_file
from rdr_service.dao.genomics_dao import GenomicJobRunDao
from rdr_service.genomic.genomic_data import GenomicQueryClass
from rdr_service.config import GENOMIC_REPORT_PATH


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
        self.report_def = None

    class ReportDef:
        def __init__(self, level=None, target=None, time_frame=None,
                     display_name=None, empty_report_string=None):
            self.level = level
            self.target = target
            self.from_date = self.get_from_date(time_frame)

            self.display_name = display_name
            self.empty_report_string = empty_report_string

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
            interval_mappings = {
                'D': 1,
                'W': 7
            }

            dd = timedelta(days=interval_mappings[time_frame])

            return clock.CLOCK.now() - dd

    def get_report_parameters(self, **kwargs):

        # Set report level (SUMMARY, DETAIL, etc)
        try:
            report_level = kwargs['report_level']

        except KeyError:
            report_level = self.controller.job.name.split('_')[1]

        # Set report target (INGESTION, RUNS, etc)
        try:
            report_target = kwargs['report_target']

        except KeyError:
            report_target = self.controller.job.name.split('_')[-1]

        # Set report time frame (D, W, etc.)
        try:
            time_frame = kwargs['time_frame']

        except KeyError:
            time_frame = self.controller.job.name[0]

        display_name = self.get_report_display_name()

        report_params = {
            "level": report_level,
            "target": report_target,
            "time_frame": time_frame,
            "display_name": display_name,
            "empty_report_string": self.get_empty_report_string(display_name),
        }

        return report_params

    def get_report_display_name(self):

        job_name_list = self.controller.job.name.split('_')

        display_name = job_name_list[0].capitalize() + " "
        display_name += job_name_list[-1].capitalize() + " "
        display_name += job_name_list[1].capitalize()

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
            ("SUMMARY", "INGESTIONS"): self.query.dq_report_ingestions_summary(report_def.from_date),
            ("SUMMARY", "INCIDENTS"): self.query.dq_report_incident_detail(report_def.from_date)
        }

        report_def.source_data_query, report_def.source_data_params = target_mappings[
            (report_def.level, report_def.target)
        ]

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
                self.report_def.source_data_query, self.report_def.source_data_params
            )

        return result

    def format_report(self, data):
        """
        Converts the report query ResultProxy object to report string
        :param data: ResultProxy
        :return: string
        """
        rows = data.fetchall()

        if rows:
            # Report title
            report_string = "```" + self.report_def.display_name + '\n'

            # Header row
            report_string += "    ".join(data.keys())
            report_string += "\n"

            for row in rows:
                report_string += "    ".join(tuple(map(str, row)))
                report_string += "\n"

            report_string += "```"

        else:
            report_string = self.report_def.empty_report_string

        return report_string

    def create_report_csv(self, report_string, display_name):
        now_str = clock.CLOCK.now().replace(microsecond=0).isoformat(sep="_", )
        report_file_name = f"{display_name.replace(' ', '_')}_{now_str}.txt"

        path = GENOMIC_REPORT_PATH + report_file_name

        with open_cloud_file(path, mode='wt') as cloud_file:
            cloud_file.write(report_string)

        return path
