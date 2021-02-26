"""
Workflows for the Genomics Data Quality Pipeline
"""
from rdr_service.genomic.genomic_job_controller import DataQualityJobController


def data_quality_workflow(job, project):
    """
    Entrypoint for a daily quality pipeline job
    :param job: GenomicJob
    :param project: str, for updating job runs in BQ
    """
    with DataQualityJobController(job, bq_project_id=project) as controller:
        controller.execute_workflow()
