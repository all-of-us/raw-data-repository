import executors
import logging

from offline.sql_exporter import SqlExporter
from config import get_db_config
from dao.code_dao import CodeDao
from code_constants import FIELD_TO_QUESTIONNAIRE_MODULE_CODE, PPI_SYSTEM 
from code_constants import METRIC_FIELD_TO_QUESTION_CODE
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from google.appengine.api import app_identity
from werkzeug.exceptions import InternalServerError

# TODO: filter out participants that have withdrawn in here

PARTICIPANTS_CSV = "participants.csv"
HPO_IDS_CSV = "hpo_ids.csv"
ANSWERS_CSV = "answers.csv"

PARTICIPANT_SQL_TEMPLATE = (
"SELECT ps.date_of_birth date_of_birth, " 
+ "(SELECT MIN(bo.created) FROM biobank_order bo "
+ "  WHERE bo.participant_id = p.participant_id) first_order_date, "
+ "(SELECT MIN(bs.confirmed) FROM biobank_stored_sample bs "
+ "  WHERE bs.biobank_id = p.biobank_id) first_samples_arrived_date, "
+ "(SELECT MIN(pm.created) FROM physical_measurements pm "
+ "  WHERE pm.participant_id = p.participant_id) first_physical_measurements_date, %s"
+ " FROM participant p, participant_summary ps "
+ "WHERE p.participant_id = ps.participant_id")

MODULE_SQL_TEMPLATE = (
"(SELECT MIN(qr.created) FROM questionnaire_response qr, questionnaire_concept qc "
+ " WHERE qr.questionnaire_id = qc.questionnaire_id and qc.code_id = %d) %s")

HPO_ID_QUERY = "SELECT participant_id, hpo_id, last_modified FROM participant_history"

ANSWER_QUERY_TEMPLATE = (
"SELECT qr.participant_id participant_id, qr.created start_time, qra.end_time end_time, "
+ "qc.value question_code, ac.value answer_code "
+ "FROM questionnaire_response_answer qra, questionnaire_response qr, questionnaire_question qq, "
+ " code qc, code ac "
+ "WHERE qra.questionnaire_response_id = qr.questionnaire_response_id "
+ "  AND qra.question_id = qq.questionnaire_question_id "
+ "  AND qq.code_id = qc.code_id "
+ "  AND qra.value_code_id = ac.code_id"
+ "  AND qq.code_id IN (%s)"
)

def get_participant_sql():
  modules_statements = []
  code_dao = CodeDao()
  for field_name, code_value in FIELD_TO_QUESTIONNAIRE_MODULE_CODE.iteritems():            
    code = code_dao.get_code(PPI_SYSTEM, code_value)
    modules_statements.append(MODULE_SQL_TEMPLATE % (code.codeId, field_name))
  modules_sql = ', '.join(modules_statements)
  return PARTICIPANT_SQL_TEMPLATE % modules_sql

def get_answer_sql():
  code_dao = CodeDao()
  code_ids = []
  for code_value in METRIC_FIELD_TO_QUESTION_CODE.values():
    code = code_dao.get_code(PPI_SYSTEM, code_value)
    code_ids.append(str(code.codeId))  
  return ANSWER_QUERY_TEMPLATE % (",".join(code_ids))

class MetricsExport(object):
  """Exports data from the database needed to generate metrics.
  
  Exports are performed in a chain of tasks, each of which can run for up to 10 minutes.  
  When the last task is done, the MapReduce pipeline for metrics is kicked off.
  """  
  def __init__(self, bucket_name, filename_prefix):
    self.bucket_name = bucket_name
    self.filename_prefix = filename_prefix     
            
                   
  def export_participants(self):
    SqlExporter(self.bucket_name).run_export(self.filename_prefix + PARTICIPANTS_CSV, 
                                        get_participant_sql())
                                              
  def export_hpo_ids(self):
    SqlExporter(self.bucket_name).run_export(self.filename_prefix + HPO_IDS_CSV, HPO_ID_QUERY)
  
  def export_answers(self):
    SqlExporter(self.bucket_name).run_export(self.filename_prefix + ANSWERS_CSV, get_answer_sql())              

  @staticmethod
  def start_export_tasks(bucket_name, now):
    filename_prefix = "%s/" % now.isoformat()
    executors.defer(MetricsExport.start_participant_export, bucket_name, filename_prefix)
      
  @classmethod
  def start_participant_export(cls, bucket_name, filename_prefix):
    MetricsExport(bucket_name, filename_prefix).export_participants()
    executors.defer(MetricsExport.start_hpo_id_export, bucket_name, filename_prefix)
    
  @classmethod
  def start_hpo_id_export(cls, bucket_name, filename_prefix):
    MetricsExport(bucket_name, filename_prefix).export_hpo_ids()
    executors.defer(MetricsExport.start_answers_export, bucket_name, filename_prefix)
        
  @classmethod
  def start_answers_export(cls, bucket_name, filename_prefix):
    MetricsExport(bucket_name, filename_prefix).export_answers()
