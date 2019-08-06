import logging

from rdr_service.dao.genomics_dao import GenomicSetDao
from rdr_service.genomic import genomic_biobank_menifest_handler, genomic_set_file_handler, \
  validation
from rdr_service.model.genomics import GenomicSetStatus


def process_genomic_water_line():
  """
  Entrypoint, executed as a cron job
  """
  genomic_set_id = genomic_set_file_handler.read_genomic_set_from_bucket()
  if genomic_set_id is not None:
    logging.info('Read input genomic set file successfully.')
    dao = GenomicSetDao()
    validation.validate_and_update_genomic_set_by_id(genomic_set_id, dao)
    genomic_set = dao.get(genomic_set_id)
    if genomic_set.genomicSetStatus == GenomicSetStatus.VALID:
      genomic_biobank_menifest_handler\
        .create_and_upload_genomic_biobank_manifest_file(genomic_set_id)
      logging.info('Validation passed, generate biobank manifest file successfully.')
    else:
      logging.info('Validation failed.')
    genomic_set_file_handler.create_genomic_set_status_result_file(genomic_set_id)
  else:
    logging.info('No file found or nothing read from genomic set file')

  genomic_biobank_menifest_handler.process_genomic_manifest_result_file_from_bucket()
