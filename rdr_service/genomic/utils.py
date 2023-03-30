from typing import Callable

from rdr_service import config, clock
from rdr_service.dao.genomics_dao import GenomicJobRunDao


def check_genomic_cron_job(val) -> Callable:
    def inner_decorator(f):
        def wrapped(*args, **kwargs):
            if not config.getSettingJson(config.GENOMIC_CRON_JOBS).get(val):
                raise RuntimeError(f'Cron job for {val} is currently disabled')
            return f(*args, **kwargs)
        return wrapped
    return inner_decorator


def interval_genomic_run_schedule(job_id, run_type) -> Callable:
    def inner_decorator(f):
        def wrapped(*args, **kwargs):
            interval_run_map = {'bi_week': 14}
            today = clock.CLOCK.now()
            day_interval = interval_run_map.get(run_type)
            job_run_dao = GenomicJobRunDao()
            last_run = job_run_dao.get_last_successful_runtime(job_id)
            if last_run and ((today.date() - last_run.date()).days < day_interval):
                raise RuntimeError(f'Cron job for {job_id.name} is currently disabled for this time')
            return f(*args, **kwargs)
        return wrapped
    return inner_decorator
