
import executors

from offline.sql_exporter import SqlExporter
from dao.code_dao import CodeDao
from dao.database_factory import replace_isodate
from code_constants import FIELD_TO_QUESTIONNAIRE_MODULE_CODE, PPI_SYSTEM
from code_constants import METRIC_FIELD_TO_QUESTION_CODE

# TODO: filter out participants that have withdrawn in here

PARTICIPANTS_CSV = "participants_%d.csv"
HPO_IDS_CSV = "hpo_ids_%d.csv"
ANSWERS_CSV = "answers_%d.csv"

PARTICIPANT_SQL_TEMPLATE = (
"SELECT ps.date_of_birth date_of_birth, "
+ "(SELECT ISODATE[MIN(bo.created)] FROM biobank_order bo "
+ "  WHERE bo.participant_id = p.participant_id) first_order_date, "
+ "(SELECT ISODATE[MIN(bs.confirmed)] FROM biobank_stored_sample bs "
+ "  WHERE bs.biobank_id = p.biobank_id) first_samples_arrived_date, "
+ "(SELECT ISODATE[MIN(pm.created)] FROM physical_measurements pm "
+ "  WHERE pm.participant_id = p.participant_id) first_physical_measurements_date, {}"
+ " FROM participant p, participant_summary ps "
+ "WHERE p.participant_id = ps.participant_id"
+ "  AND p.participant_id % :num_shards = :shard_number")

MODULE_SQL_TEMPLATE = (
"(SELECT ISODATE[MIN(qr.created)] FROM questionnaire_response qr, "
+ "questionnaire_concept qc "
+ " WHERE qr.questionnaire_id = qc.questionnaire_id and qc.code_id = {}) {}")

HPO_ID_QUERY = (
"SELECT participant_id, hpo_id, ISODATE[last_modified] last_modified "
+" FROM participant_history "
+ "WHERE participant_id % :num_shards = :shard_number")

ANSWER_QUERY = (
"SELECT qr.participant_id participant_id, ISODATE[qr.created] start_time, "
+ "ISODATE[qra.end_time] end_time, "
+ "qc.value question_code, ac.value answer_code "
+ "FROM questionnaire_response_answer qra, questionnaire_response qr, questionnaire_question qq, "
+ " code qc, code ac "
+ "WHERE qra.questionnaire_response_id = qr.questionnaire_response_id "
+ "  AND qra.question_id = qq.questionnaire_question_id "
+ "  AND qq.code_id = qc.code_id "
+ "  AND qra.value_code_id = ac.code_id"
+ "  AND qq.code_id in ({})"
+ "  AND qr.participant_id % :num_shards = :shard_number"
)

def get_participant_sql(num_shards, shard_number):
  modules_statements = []
  code_dao = CodeDao()
  for field_name, code_value in FIELD_TO_QUESTIONNAIRE_MODULE_CODE.iteritems():
    code = code_dao.get_code(PPI_SYSTEM, code_value)
    modules_statements.append(MODULE_SQL_TEMPLATE.format(code.codeId, field_name))
  modules_sql = ', '.join(modules_statements)
  return (replace_isodate(PARTICIPANT_SQL_TEMPLATE.format(modules_sql)),
          {"num_shards": num_shards, "shard_number": shard_number })

def get_hpo_id_sql(num_shards, shard_number):
  return (replace_isodate(HPO_ID_QUERY),
          {"num_shards": num_shards, "shard_number": shard_number})

def get_answer_sql(num_shards, shard_number):
  code_dao = CodeDao()
  code_ids = []
  for code_value in METRIC_FIELD_TO_QUESTION_CODE.values():
    code = code_dao.get_code(PPI_SYSTEM, code_value)
    code_ids.append(str(code.codeId))
  return (replace_isodate(ANSWER_QUERY.format((",".join(code_ids)))),
          {"num_shards": num_shards,
           "shard_number": shard_number})

class MetricsExport(object):
  """Exports data from the database needed to generate metrics.

  Exports are performed in a chain of tasks, each of which can run for up to 10 minutes.
  A configurable number of shards allows each data set being exported to be broken up into pieces
  that can complete in time; sharded output also makes MapReduce on the result run faster.

  When the last task is done, the MapReduce pipeline for metrics is kicked off.
  """

  @classmethod
  def export_participants(self, bucket_name, filename_prefix, num_shards, shard_number):
    sql, params = get_participant_sql(num_shards, shard_number)
    SqlExporter(bucket_name).run_export(filename_prefix + PARTICIPANTS_CSV % shard_number,
                                        sql, **params)

  @classmethod
  def export_hpo_ids(self, bucket_name, filename_prefix, num_shards, shard_number):
    sql, params = get_hpo_id_sql(num_shards, shard_number)
    SqlExporter(bucket_name).run_export(filename_prefix + HPO_IDS_CSV % shard_number,
                                        sql, **params)

  @classmethod
  def export_answers(self, bucket_name, filename_prefix, num_shards, shard_number):
    sql, params = get_answer_sql(num_shards, shard_number)
    SqlExporter(bucket_name).run_export(filename_prefix + ANSWERS_CSV % shard_number,
                                        sql, **params)

  @staticmethod
  def start_export_tasks(bucket_name, now, num_shards):
    filename_prefix = "%s/" % now.isoformat()
    executors.defer(MetricsExport.start_participant_export, bucket_name, filename_prefix,
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
  def start_participant_export(cls, bucket_name, filename_prefix, num_shards, shard_number):
    MetricsExport._start_export(bucket_name, filename_prefix, num_shards, shard_number,
                                "export_participants", "start_participant_export",
                                "start_hpo_id_export")

  @classmethod
  def start_hpo_id_export(cls, bucket_name, filename_prefix, num_shards, shard_number):
    MetricsExport._start_export(bucket_name, filename_prefix, num_shards, shard_number,
                                "export_hpo_ids", "start_hpo_id_export",
                                "start_answers_export")
  @classmethod
  def start_answers_export(cls, bucket_name, filename_prefix, num_shards, shard_number):
    MetricsExport._start_export(bucket_name, filename_prefix, num_shards, shard_number,
                                "export_answers", "start_answers_export", None,
                                "start_metrics_pipeline")

  @classmethod
  def start_metrics_pipeline(cls, bucket_name, filename_prefix, num_shards):
    pass

