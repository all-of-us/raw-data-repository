"""
Workflows for the Genomics Data Quality Pipeline
"""
from rdr_service.genomic.genomic_job_controller import DataQualityJobController
from rdr_service.genomic_enums import GenomicJob


def daily_data_quality_workflow(project=None):
    """
    Entrypoint for a daily quality pipeline job
    :param job: GenomicJob
    :param project: str, for updating job runs in BQ
    """
    for reporting_job in [
        GenomicJob.DAILY_SUMMARY_SHORTREAD_REPORT_INGESTIONS,
        GenomicJob.DAILY_SUMMARY_LONGREAD_REPORT_INGESTIONS,
        GenomicJob.DAILY_SUMMARY_PROTEOMICS_REPORT_INGESTIONS,
        GenomicJob.DAILY_SUMMARY_RNA_REPORT_INGESTIONS
    ]:
        with DataQualityJobController(
            job=reporting_job,
            bq_project_id=project
        ) as controller:
            controller.execute_workflow(slack=True)
