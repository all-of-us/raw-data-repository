import argparse
import sys
import logging
from os.path import isabs, exists
from typing import Dict, Any, Iterator, Optional
from io import StringIO
from csv import DictReader

from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.model.study_nph import Site
from rdr_service.dao.study_nph_dao import NphSiteDao


_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "study_nph_ingest_awardee_data_from_file"
tool_desc = "NPH Study Ingest Awardee Data"

def read_csv(filepath: str) -> Iterator[Dict[str, Any]]:

    def _decode_utf8_sig_content(csv_fp) -> StringIO:
        content = csv_fp.read()
        content = content.decode("utf-8-sig")
        return StringIO(content)

    with open(filepath, "rb") as csv_fp:
        csv_file_content = _decode_utf8_sig_content(csv_fp)
        csv_dict_reader = DictReader(csv_file_content)
        for row in csv_dict_reader:
            yield row


def _convert_csv_obj_to_site_obj(awardee_data_obj: Dict[str, Any]) -> Dict[str, Any]:

    return {
        "external_id": awardee_data_obj["healthpro_site_id"],
        "name": awardee_data_obj["site_name"],
        "awardee_external_id": awardee_data_obj["nph_awardee_id"],
        "organization_external_id": awardee_data_obj["organization_id"],
    }


def create_sites_from_csv(csv_filepath: str):
    nph_site_dao = NphSiteDao()
    for row in read_csv(csv_filepath):
        site_obj = _convert_csv_obj_to_site_obj(row)
        nph_site: Optional[Site] = nph_site_dao.get_site_using_params(**site_obj)
        if nph_site:
            _logger.info(f"An nph site already exists with {site_obj}. Skipping ...")
        else:
            _logger.info(f"Inserting a new nph site with {site_obj} fields")
            nph_site = Site(**site_obj)
            nph_site_dao.insert(nph_site)


def main():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument(
        "--awardee-file-path",
        help="Absolute File path to xlsx file with NPH Awardee data",
        type=str
    )
    args = parser.parse_args()

    if not isabs(args.awardee_file_path):
        raise ValueError(f"'{args.awardee_file_path}' is not an absolute file path")

    if not exists(args.awardee_file_path):
        raise IOError(f"'{args.awardee_file_path}' filepath does not exist")

    create_sites_from_csv(csv_filepath=args.awardee_file_path)


if __name__=="__main__":
    main()
