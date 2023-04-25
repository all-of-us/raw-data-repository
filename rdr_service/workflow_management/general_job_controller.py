import logging

from protorpc import messages


class GeneralRunResults(messages.Enum):

    UNSET = 0
    SUCCESS = 1
    FAIL = 3


class JobController:
    """
    General Job Controller class
    """

    def __init__(self,
                 job=None,
                 job_run_dao=None,
                 incident_dao=None,
                 run_result_enum=GeneralRunResults,
                 **kwargs):

        # Job attributes
        self.job = job
        self.job_run = None
        self.run_result_enum = run_result_enum
        self.job_run_result = getattr(self.run_result_enum, 'UNSET')
        self.kwargs = kwargs

        # Components
        self.job_run_dao = job_run_dao
        self.incident_dao = incident_dao

    def __enter__(self, **kwargs):
        logging.info(f'Workflow Initiated: {self.job.name}')
        try:
            self.job_run = self.job_run_dao.insert_run_record(self.job, **self.kwargs)
        except AttributeError:
            raise NotImplementedError('job_run_dao class has no insert_run_record method')

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Updates the job_run table with end result"""
        logging.info(f'Workflow Completed: {self.job.name}')
        try:
            self.job_run_dao.update_run_record(self.job_run, self.job_run_result)
        except AttributeError:
            raise NotImplementedError('job_run_dao class has no update_run_record method')

