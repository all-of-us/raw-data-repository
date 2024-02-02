"""
Workflows for the Genomics Data Quality Pipeline
"""
from rdr_service.genomic.genomic_job_controller import DataQualityJobController
from rdr_service.genomic_enums import GenomicJob


def daily_data_quality_workflow(**kwargs):
    """
    Entrypoint for a daily quality pipeline job
    """
    if kwargs.get('reporting_job'):
        with DataQualityJobController(
            job=kwargs.get('reporting_job'),
            bq_project_id=kwargs.get('project')
        ) as controller:
            controller.execute_workflow(slack=True)
    else:
        daily_ingestion_list = [
            GenomicJob.DAILY_SUMMARY_SHORTREAD_REPORT_INGESTIONS,
            GenomicJob.DAILY_SUMMARY_LONGREAD_REPORT_INGESTIONS,
            GenomicJob.DAILY_SUMMARY_PROTEOMICS_REPORT_INGESTIONS,
            GenomicJob.DAILY_SUMMARY_RNA_REPORT_INGESTIONS
        ]
        for reporting_job in daily_ingestion_list:
            with DataQualityJobController(
                job=reporting_job,
                bq_project_id=kwargs.get('project')
            ) as controller:
                controller.execute_workflow(slack=True)
