#!/usr/bin/env python

"""
Compile the cron YAML file for a project.

see `_compile_job_list` comments for details on merging process.
"""

import argparse
import collections
import os

import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

imap = map

BASE_CRON_NAME = "default"

PROJECT_NAME_MAPPING = {
    'all-of-us-rdr-prod': 'prod',
    'all-of-us-rdr-stable': 'stable',
    'all-of-us-rdr-staging': 'staging',
    'all-of-us-rdr-dryrun': 'dryrun',
    'pmi-drc-api-test': 'test',
    'all-of-us-rdr-sandbox': 'sandbox',
    'all-of-us-rdr-ptsc-1-test': 'ptsc',
    'all-of-us-rdr-ptsc-2-test': 'ptsc',
    'all-of-us-rdr-careevo-test': 'careevo',
}

CRON_SEARCH_LOCATION = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


def _get_cron_names_for_project_name(project_name):
    """
    Make a list of crons to merge for the given project name.
    """
    name = PROJECT_NAME_MAPPING.get(project_name)
    return list(filter(bool, [BASE_CRON_NAME, name]))


def _lazy_chain(iterators):
    """
    Iterate the items from each iterator in turn.

    Different than itertools chain because it can take an iterator of iterators instead of needing
    each iterator argument defined at call time.
    """
    for iterator in iterators:
        for value in iterator:
            yield value


def _get_cron_filename_from_name(name):
    """
    Resolve a `name` to a cron yaml file location
    """
    return os.path.join(CRON_SEARCH_LOCATION, 'cron_{}.yaml'.format(name))


def _load_yaml_file_by_name(name):
    filename = _get_cron_filename_from_name(name)
    if filename and os.path.isfile(filename):
        with open(filename, 'r') as handle:
            return yaml.load(handle, Loader=Loader)


def _compile_job_list(job_iterator):
    """
    Make one unified list of jobs from a job iterator.
    Identify jobs by their `description` or index in the `job_iterator`.
    Keep the last one in the `job_iterator` for each job identifier.
    Remove jobs missing the required fields.
    """
    jobs_dict = collections.OrderedDict()
    for i, job in enumerate(job_iterator):
        job_id = job.get('description', i)
        if 'url' in job and 'schedule' in job:
            jobs_dict[job_id] = job
        elif job_id in jobs_dict:
            del jobs_dict[job_id]
    return list(jobs_dict.values())


def build_cron_yaml(project_name, outstream):
    """
    Compile the cron YAML file for the given project name.
    Write to the `outstream`
    """
    yaml_names = _get_cron_names_for_project_name(project_name)
    config_job_list_iterator = [
        config.get('cron')
        for config
        in map(_load_yaml_file_by_name, yaml_names)
        if config
    ]
    job_iterator = _lazy_chain(config_job_list_iterator)
    compiled_jobs_list = _compile_job_list(job_iterator)
    yaml.dump({
        'cron': compiled_jobs_list
    }, outstream, Dumper=Dumper)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True)
    parser.add_argument('--outfile', '-o', type=argparse.FileType('w'), default='-',
                        help='Default: stdout')
    args = parser.parse_args()
    build_cron_yaml(args.project, args.outfile)
