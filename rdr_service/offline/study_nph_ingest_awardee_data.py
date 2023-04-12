import logging
from typing import Dict, Any, Iterator, Optional
from csv import DictReader

from rdr_service.model.study_nph import Site
from rdr_service.dao.study_nph_dao import NphSiteDao


_logger = logging.getLogger("rdr_logger")


def read_csv(filepath: str) -> Iterator[Dict[str, Any]]:
    with open(filepath, "r", encoding="utf-8-sig") as csv_fp:
        csv_dict_reader = DictReader(csv_fp)
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
