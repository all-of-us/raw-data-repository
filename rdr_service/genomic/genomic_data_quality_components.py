import abc
from datetime import timedelta

from rdr_service import clock
from rdr_service.dao.genomics_dao import GenomicJobRunDao
from rdr_service.genomic.genomic_queries import GenomicQueryClass


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

    class ReportDef:
        def __init__(self, level, target, time_frame):
            self.level = level
            self.target = target
            self.from_date = self.get_from_date(time_frame)

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

    def generate_report(self, level, target, time_frame):
        """
        Genearates a report based on target and time frame
        :param level: str, "SUMMARY" or "DETAIL"
        :param target: str, "RUNS", "INGESTIONS" etc.
        :param time_frame: 'd' or 'w'
        :return: dict of report data
        """

        report_def = self.get_report_def(level, target, time_frame)

        sql = report_def.get_sql()

        return self.get_report_data(report_def)

    def set_report_parameters(self, **kwargs):
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

        return report_level, report_target, time_frame

    def get_report_def(self, level, target, time_frame):
        """
        Reports are defined in inner class ReportDef objects
        This method returns the ReportDef object based on Target
        :param level:
        :param target:
        :param time_frame:
        :return: ReportDef object
        """
        report_def = self.ReportDef(level, target, time_frame)

        # Map report targets to source data queries
        target_mappings = {
            ("SUMMARY", "RUNS"): self.query.dq_report_runs_summary(report_def.from_date),
            ("SUMMARY", "INGESTIONS"): self.query.dq_report_ingestions_summary(report_def.from_date)
        }

        report_def.source_data_query, report_def.source_data_params = target_mappings[(level, target)]

        return report_def

    def get_report_data(self, report_def):
        """
        Returns the report by executing the report definition query
        :param report_def: ReportDef object
        :return: dict of report data
        """

        # Execute the query_def's source_data_query
        dao = GenomicJobRunDao()

        with dao.session() as session:
            result = session.execute(
                report_def.source_data_query, report_def.source_data_params
            ).fetchall()

        return result

    def format_report(self, data):
        pass
