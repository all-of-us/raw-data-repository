import logging
from genomic import genomic_set_file_handler, validation, genomic_biobank_menifest_handler
from dao.genomics_dao import GenomicSetDao
from model.genomics import GenomicSetStatus
from code_constants import GENOME_TYPE

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
      for genome_type in GENOME_TYPE:
        genomic_biobank_menifest_handler\
          .create_and_upload_genomic_biobank_manifest_file(genomic_set_id, genome_type)
      logging.info('Validation passed, generate biobank manifest file successfully.')
    else:
      logging.info('Validation failed.')
    genomic_set_file_handler.create_genomic_set_status_result_file(genomic_set_id)
  else:
    logging.info('No file found or nothing read from genomic set file')
