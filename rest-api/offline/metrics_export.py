
import clock
import executors

from offline.sql_exporter import SqlExporter
from dao.code_dao import CodeDao
from dao.database_utils import replace_isodate
from model.base import get_column_name
from model.participant_summary import ParticipantSummary
from code_constants import QUESTIONNAIRE_MODULE_FIELD_NAMES, PPI_SYSTEM
from code_constants import UNMAPPED, RACE_QUESTION_CODE
from offline.metrics_config import ANSWER_FIELD_TO_QUESTION_CODE
from offline.metrics_pipeline import MetricsPipeline

# TODO: filter out participants that have withdrawn in here

_PARTICIPANTS_CSV = "participants_%d.csv"
_HPO_IDS_CSV = "hpo_ids_%d.csv"
_ANSWERS_CSV = "answers_%d.csv"
_ALL_CSVS = [_PARTICIPANTS_CSV, _HPO_IDS_CSV, _ANSWERS_CSV]

_QUEUE_NAME = 'metrics-pipeline'

_PARTICIPANT_SQL_TEMPLATE = (
"SELECT p.participant_id, ps.date_of_birth date_of_birth, "
+ "(SELECT ISODATE[MIN(bo.created)] FROM biobank_order bo "
+ "  WHERE bo.participant_id = p.participant_id) first_order_date, "
+ "(SELECT ISODATE[MIN(bs.confirmed)] FROM biobank_stored_sample bs "
+ "  WHERE bs.biobank_id = p.biobank_id) first_samples_arrived_date, "
+ "(SELECT ISODATE[MIN(pm.created)] FROM physical_measurements pm "
+ "  WHERE pm.participant_id = p.participant_id) first_physical_measurements_date, {}"
+ " FROM participant p, participant_summary ps "
+ "WHERE p.participant_id = ps.participant_id"
+ "  AND p.participant_id % :num_shards = :shard_number")

# Find HPO ID changes in participant history.
_HPO_ID_QUERY = (
"SELECT ph.participant_id participant_id, hpo.name hpo, "
+" ISODATE[ph.last_modified] last_modified "
+" FROM participant_history ph, hpo "
+ "WHERE ph.participant_id % :num_shards = :shard_number"
+ "  AND ph.hpo_id = hpo.hpo_id "
+ "  AND NOT EXISTS (SELECT * from participant_history ph_prev WHERE "
+ "   ph_prev.participant_id = ph.participant_id AND ph_prev.version = ph.version - 1"
+ "     AND ph_prev.hpo_id = ph.hpo_id) ")

_ANSWER_QUERY = (
"SELECT qr.participant_id participant_id, ISODATE[qr.created] start_time, "
+ "qc.value question_code, "
+ "(SELECT CASE WHEN ac.mapped THEN ac.value ELSE :unmapped END FROM Code ac "
+"   WHERE ac.code_id = qra.value_code_id) answer_code, "
+ "qra.value_string answer_string "
+ "FROM questionnaire_response_answer qra, questionnaire_response qr, questionnaire_question qq, "
+ " code qc "
+ "WHERE qra.questionnaire_response_id = qr.questionnaire_response_id "
+ "  AND qra.question_id = qq.questionnaire_question_id "
+ "  AND qq.code_id = qc.code_id "
+ "  AND qq.code_id in ({})"
+ "  AND qr.participant_id % :num_shards = :shard_number "
+ "ORDER BY qr.participant_id, qr.created, qc.value"
)

def _get_participant_sql(num_shards, shard_number):
  module_time_fields = ['ISODATE[ps.{0}] {0}'.format(get_column_name(ParticipantSummary,
                                                            field_name + 'Time'))
                        for field_name in QUESTIONNAIRE_MODULE_FIELD_NAMES]
  modules_sql = ', '.join(module_time_fields)
  return (replace_isodate(_PARTICIPANT_SQL_TEMPLATE.format(modules_sql)),
          {"num_shards": num_shards, "shard_number": shard_number})

def _get_hpo_id_sql(num_shards, shard_number):
  return (replace_isodate(_HPO_ID_QUERY),
          {"num_shards": num_shards, "shard_number": shard_number})

def _get_answer_sql(num_shards, shard_number):
  code_dao = CodeDao()
  code_ids = []
  question_codes = list(ANSWER_FIELD_TO_QUESTION_CODE.values())
  question_codes.append(RACE_QUESTION_CODE)
  for code_value in question_codes:
    code = code_dao.get_code(PPI_SYSTEM, code_value)
    code_ids.append(str(code.codeId))
  return (replace_isodate(_ANSWER_QUERY.format((",".join(code_ids)))),
          {"unmapped": UNMAPPED,
           "num_shards": num_shards,
           "shard_number": shard_number})

class MetricsExport(object):
  """Exports data from the database needed to generate metrics.

  Exports are performed in a chain of tasks, each of which can run for up to 10 minutes.
  A configurable number of shards allows each data set being exported to be broken up into pieces
  that can complete in time; sharded output also makes MapReduce on the result run faster.

  When the last task is done, the MapReduce pipeline for metrics is kicked off.
  """

  @classmethod
  def _export_participants(self, bucket_name, filename_prefix, num_shards, shard_number):
    sql, params = _get_participant_sql(num_shards, shard_number)
    SqlExporter(bucket_name).run_export(filename_prefix + _PARTICIPANTS_CSV % shard_number,
                                        sql, **params)

  @classmethod
  def _export_hpo_ids(self, bucket_name, filename_prefix, num_shards, shard_number):
    sql, params = _get_hpo_id_sql(num_shards, shard_number)
    SqlExporter(bucket_name).run_export(filename_prefix + _HPO_IDS_CSV % shard_number,
                                        sql, **params)

  @classmethod
  def _export_answers(self, bucket_name, filename_prefix, num_shards, shard_number):
    sql, params = _get_answer_sql(num_shards, shard_number)
    SqlExporter(bucket_name).run_export(filename_prefix + _ANSWERS_CSV % shard_number,
                                        sql, **params)

  @staticmethod
  def start_export_tasks(bucket_name, num_shards):
    '''Entry point to exporting data for use by the metrics pipeline. Begins the export of
    the first shard of the participant data.'''
    filename_prefix = "%s/" % clock.CLOCK.now().isoformat()
    executors.defer(MetricsExport._start_participant_export, bucket_name, filename_prefix,
                    num_shards, 0)

  @staticmethod
  def _start_export(bucket_name, filename_prefix, num_shards, shard_number, export_methodname,
                    next_shard_methodname, next_type_methodname, finish_methodname=None):
    getattr(MetricsExport, export_methodname)(bucket_name, filename_prefix,
                                              num_shards, shard_number)
    shard_number += 1
    if shard_number == num_shards:
      if next_type_methodname:
        executors.defer(getattr(MetricsExport, next_type_methodname), bucket_name, filename_prefix,
                        num_shards, 0)
      else:
        getattr(MetricsExport, finish_methodname)(bucket_name, filename_prefix, num_shards)
    else:
      executors.defer(getattr(MetricsExport, next_shard_methodname), bucket_name, filename_prefix,
                      num_shards, shard_number)


  @classmethod
  def _start_participant_export(cls, bucket_name, filename_prefix, num_shards, shard_number):
    MetricsExport._start_export(bucket_name, filename_prefix, num_shards, shard_number,
                                "_export_participants", "_start_participant_export",
                                "_start_hpo_id_export")

  @classmethod
  def _start_hpo_id_export(cls, bucket_name, filename_prefix, num_shards, shard_number):
    MetricsExport._start_export(bucket_name, filename_prefix, num_shards, shard_number,
                                "_export_hpo_ids", "_start_hpo_id_export",
                                "_start_answers_export")
  @classmethod
  def _start_answers_export(cls, bucket_name, filename_prefix, num_shards, shard_number):
    MetricsExport._start_export(bucket_name, filename_prefix, num_shards, shard_number,
                                "_export_answers", "_start_answers_export", None,
                                "_start_metrics_pipeline")

  @classmethod
  def _start_metrics_pipeline(cls, bucket_name, filename_prefix, num_shards):
    input_files = []
    for csv_filename in _ALL_CSVS:
      input_files.extend([filename_prefix + csv_filename % shard for shard
                          in range(0, num_shards)])
    pipeline = MetricsPipeline(bucket_name, clock.CLOCK.now(), input_files)
    pipeline.start(queue_name=_QUEUE_NAME)
