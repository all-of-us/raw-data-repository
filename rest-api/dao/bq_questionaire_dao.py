import datetime
import logging

from sqlalchemy import text

from dao.bigquery_sync_dao import BigQuerySyncDao, BigQueryGenerator
from model.bq_base import BQRecord
from model.bq_questionnaires import BQPDRTheBasics, BQPDRConsentPII, BQPDRLifestyle, \
        BQPDROverallHealth, BQPDRDVEHRSharing, BQPDREHRConsentPII


class BQPDRQuestionnaireResponseGenerator(BigQueryGenerator):
  """
  Generate a questionnaire module response BQRecord
  """
  dao = None

  def make_bqrecord(self, p_id, module_id, latest=False, convert_to_enum=False):
    """
    Generate a list of questionnaire module BQRecords for the given participant id.
    :param p_id: participant id
    :param module_id: A questionnaire module id, IE: 'TheBasics'.
    :param latest: only process the most recent response if True
    :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
    :return: BQTable object, List of BQRecord objects
    """
    if not self.dao:
      self.dao = BigQuerySyncDao()

    if module_id == 'TheBasics':
      table = BQPDRTheBasics
    elif module_id == 'ConsentPII':
      table = BQPDRConsentPII
    elif module_id == 'Lifestyle':
      table = BQPDRLifestyle
    elif module_id == 'OverallHealth':
      table = BQPDROverallHealth
    elif module_id == 'DVEHRSharing':
      table = BQPDRDVEHRSharing
    elif module_id == 'EHRConsentPII':
      table = BQPDREHRConsentPII
    else:
      logging.error('Generator: unknown or unsupported questionnaire module id [{0}].'.format(module_id))
      return None, list()

    qnans = self.dao.call_proc('sp_get_questionnaire_answers', args=[module_id, p_id])
    if not qnans or len(qnans) == 0:
      return None, list()

    bqrs = list()
    for qnan in qnans:
      bqr = BQRecord(schema=table().get_schema(), data=qnan, convert_to_enum=convert_to_enum)
      bqr.participant_id = p_id  # reset participant_id.

      fields = bqr.get_fields()
      for field in fields:
        fld_name = field['name']
        if fld_name in (
            'id', 'created', 'modified', 'authored', 'language', 'participant_id', 'questionnaire_response_id'):
          continue

        fld_value = getattr(bqr, fld_name, None)
        if fld_value is None:  # Let empty strings pass.
          continue
        # question responses values need to be coerced to a String type.
        if isinstance(fld_value, (datetime.date, datetime.datetime)):
          setattr(bqr, fld_name, fld_value.isoformat())
        else:
          setattr(bqr, fld_name, str(fld_value))

        # Truncate zip codes to 3 digits
        if fld_name in ('StreetAddress_PIIZIP', 'EmploymentWorkAddress_ZipCode') and len(fld_value) > 2:
          setattr(bqr, fld_name, fld_value[:3])

      bqrs.append(bqr)
      if latest:
        break

    return table, bqrs


def deferred_bq_questionnaire_update(p_id, qr_id):
  """
  Generate a BQ questionnaire response record from the given p_id and questionnaire response id.
  :param p_id: participant id
  :param qr_id: A questionnaire response id.
  """
  # determine what the module id is for the given questionnaire response id.
  sql = text("""
    select c.value from
        questionnaire_response qr inner join questionnaire_concept qc on qr.questionnaire_id = qc.questionnaire_id
        inner join code c on qc.code_id = c.code_id
    where qr.questionnaire_response_id = :qr_id
  """)

  dao = BigQuerySyncDao()
  qr_gen = BQPDRQuestionnaireResponseGenerator()

  with dao.session() as session:

    results = session.execute(sql, {'qr_id': qr_id})
    if results:
      for row in results:
        module_id = row.value
        break

      table, bqrs = qr_gen.make_bqrecord(p_id, module_id, latest=True)
      for bqr in bqrs:
        qr_gen.save_bqrecord(qr_id, bqr, bqtable=table, dao=dao, session=session)
