from typing import List

from google.cloud.storage import Blob

from rdr_service import code_constants
from rdr_service.storage import GoogleCloudStorageProvider
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase


tool_cmd = 'survey-data-import'
tool_desc = 'Import a batch of CSV files provided by PPSC containing survey responses from participants'


SURVEY_CODE_MAP = {  # maps the number at the start of the file to the intended survey
    '2001': code_constants.THE_BASICS_PPI_MODULE,                       # TheBasics
    '2002': code_constants.PEDIATRICS_BASICS,                           # ped_basics
    '2003': code_constants.OVERALL_HEALTH_PPI_MODULE,                   # OverallHealth         (no validation)
    '2004': code_constants.PEDIATRICS_OVERALL_HEALTH,                   # ped_overall_health
    '2005': code_constants.LIFESTYLE_PPI_MODULE,                        # Lifestyle             (no validation)
    # 2006 is undefined
    '2007': code_constants.HEALTHCARE_ACCESS_MODULE,                    # HealthcareAccess      (no validation)
    '2008': code_constants.LIFE_FUNCTIONING_SURVEY,                     # lfs
    '2009': code_constants.REMOTE_PM_MODULE,                            # pm_height_weight
    '2010': code_constants.EMOTIONAL_HEALTH_MODULE,                     # ehhwb
    '2011': code_constants.BEHAVIORAL_HEALTH_MODULE,                    # bhp
    '2012': code_constants.PEDIATRICS_ENVIRONMENTAL_HEALTH,             # ped_environmental_health
    '2013': code_constants.SOCIAL_DETERMINANTS_OF_HEALTH_MODULE,        # sdoh
}


class SurveyDataImport(ToolBase):
    def run(self):
        super().run()

        for blob in self._get_response_blobs():
            print()
            print(blob)
            print(blob.name)

    def _get_response_blobs(self) -> List[Blob]:
        directory_path = self.args.path
        path_parts = directory_path.split('/')
        storage_provider = GoogleCloudStorageProvider()

        results = []
        for blob in storage_provider.list(bucket_name=path_parts[0], prefix='/'.join(path_parts[1:])):
            if blob.name.endswith('_data.csv'):
                results.append(blob)

        return results


def add_additional_arguments(parser):
    parser.add_argument(
        '--path',
        required=True,
        help="Directory containing the CSV files to import (it'll have a bunch "
             "of folders named a number from 2001 to 2013)"
    )


def run():
    return cli_run(tool_cmd, tool_desc, SurveyDataImport, add_additional_arguments)
