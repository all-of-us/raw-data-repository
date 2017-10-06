"""Generates a JSON questionnaire response answer spec based off data in the database."""
import csv

from dao import database_factory
from pprint import pprint
from sqlalchemy import text
from decimal import Decimal
from main_util import get_parser

_BATCH_SIZE = 1000

_QUESTION_SPEC_SQL = """ 
  SELECT     
    module,
    question_code,
    COUNT(DISTINCT participant_id) num_participants,
    COUNT(DISTINCT questionnaire_response_id) num_questionnaire_responses,
    COUNT(*) num_answers, 
    MIN(answer_date) - interval (ROUND(RAND() * 7)) day min_date_answer, 
    MAX(answer_date) + interval (ROUND(RAND() * 7)) day max_date_answer, 
    MIN(answer_datetime) - interval (ROUND(RAND() * 7)) day min_datetime_answer, 
    MAX(answer_datetime) + interval (ROUND(RAND() * 7)) day max_datetime_answer, 
    MIN(answer_decimal) min_decimal_answer, 
    MAX(answer_decimal) max_decimal_answer, 
    MIN(answer_integer) min_integer_answer, 
    MAX(answer_integer) max_integer_answer, 
    GROUP_CONCAT(DISTINCT answer_code) code_answers,
    SUM(answer_date IS NOT NULL) date_answer_count,
    SUM(answer_datetime IS NOT NULL) datetime_answer_count,
    SUM(answer_decimal IS NOT NULL) decimal_answer_count,
    SUM(answer_integer IS NOT NULL) integer_answer_count,
    SUM(answer_code IS NOT NULL) code_answer_count,
    SUM(answer_boolean IS NOT NULL) boolean_answer_count, 
    SUM(answer_string IS NOT NULL) string_answer_count, 
    SUM(answer_uri IS NOT NULL) uri_answer_count    
  FROM questionnaire_response_answer_view 
  WHERE answer_end_time is NULL 
  GROUP BY module, question_code 
  ORDER BY module, question_code
"""


def main(args):  
  with open(args.output_file, 'wb') as csvfile:
    writer = csv.writer(csvfile)
    with database_factory.get_database().session() as session:    
      cursor = session.execute(text(_QUESTION_SPEC_SQL))            
      try:            
        writer.writerow(cursor.keys()) 
        results = cursor.fetchall()
        for result in results:
          writer.writerow(result)
      finally:      
        cursor.close()
          
if __name__ == '__main__':
  parser = get_parser()
  parser.add_argument('--output_file', help='File to write the spec to', required=True)
  main(parser.parse_args())
