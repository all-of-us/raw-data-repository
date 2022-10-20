"""Generates 2 CSV files using the Data Analysis team's swoop & n3c query and drops the files in a google bucket"""
from os.path import exists as file_exists
from os import remove

import subprocess
import csv

from sqlalchemy import text

from rdr_service.dao import database_factory
from rdr_service.main_util import get_parser


_BATCH_SIZE = 1000
# TODO: Optimize Queries?
_SWOOP_SQL = """
    SELECT
        ps.participant_id,
        ps.first_name,
        ps.last_name,
        CASE
        WHEN c.display = 'Female' then 'F'
        WHEN c.display = 'Male' then 'M'
        ELSE 'U'
        END as sex_at_birth,
        ps.date_of_birth
      FROM participant_summary as ps
      left join participant as p on p.participant_id=ps.participant_id
      left join code as c on ps.sex_id = c.code_id
      WHERE p.withdrawal_status = 1 and
       p.hpo_id != 21 and p.is_ghost_id is null
       and ps.suspension_status=1 and
       (ps.email NOT LIKE '%@example.com' or ps.email IS NULL) and
       (REPLACE(REPLACE(REPLACE(REPLACE(ps.phone_number, '(', ''), ')', ''), '-', ''), ' ', '') not like '4442%'
       or ps.phone_number IS NULL) and
       (REPLACE(REPLACE(REPLACE(REPLACE(ps.login_phone_number, '(', ''), ')', ''), '-', ''), ' ', '') not like '4442%'
       or ps.login_phone_number IS NULL)
      AND sex_id IS NOT NULL
"""
_N3C_SQL = """
SELECT ps.participant_id, last_name, first_name, CASE sex_id WHEN 302 THEN 'M' WHEN 303 THEN 'F' ELSE 'U' END as SAB,
date_of_birth, ssn_answer.value_string as ssn, LEFT(zip_code, 3) as zip3, zip_code,
IFNULL(login_phone_number, phone_number) as phone, MONTH(date_of_birth) as month, YEAR(date_of_birth) as year, email
FROM participant_summary ps
left join participant as p on p.participant_id=ps.participant_id
inner join (
   select qr.participant_id, qra.value_string
   from questionnaire_response_answer qra
            inner join questionnaire_question qq on qq.questionnaire_question_id = qra.question_id
            inner join code c on c.code_id = qq.code_id
            inner join questionnaire_response qr on qr.questionnaire_response_id = qra.questionnaire_response_id
   where c.value = 'SocialSecurity_SocialSecurityNumber'
) ssn_answer on ssn_answer.participant_id = ps.participant_id
WHERE consent_for_study_enrollment = 1 and consent_for_study_enrollment_time >= '06/30/2022' and
   p.withdrawal_status = 1 and
   p.hpo_id != 21 and p.is_ghost_id is null
   and ps.suspension_status=1 and
   (ps.email NOT LIKE '%@example.com' or ps.email IS NULL)
"""


def main(args):
    # Download data from SQL to local
    for i, f in enumerate(['swoop_data.csv', "n3c_data.csv"]):
        with open(f, "w") as csvfile:
            writer = csv.writer(csvfile)
            with database_factory.get_database().session() as session:
                if i == 0:
                    cursor = session.execute(text(_SWOOP_SQL))
                elif i == 1:
                    cursor = session.execute(text(_N3C_SQL))
                try:
                    writer.writerow(list(cursor.keys()))
                    results = cursor.fetchall()
                    for result in results:
                        writer.writerow(result)
                finally:
                    cursor.close()

        # Copy file to bucket, and delete file from local
        if args.project.lower() == 'all-of-us-rdr-sandbox':
            bucket = 'gs://test_data_transfer_1'
        elif args.project.lower() == 'all-of-us-rdr-prod':
            # TODO: ENTER CORRECT BUCKET FOR PROD
            bucket = 'gs://MISSING_BUCKET'
        result = subprocess.run(['gsutil', 'cp', f, bucket], stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        print(result.stdout)

        if file_exists(f):
            try:
                remove(f)
                print(f'Removed {f} from local')
            except FileNotFoundError as e:
                print(f'File {f} does not exist \n {e}')


if __name__ == "__main__":
    parser = get_parser()
    parser.add_argument("--project", help="CHOOSE BETWEEN: all-of-us-rdr-prod OR all-of-us-rdr-sandbox", required=True)
    main(parser.parse_args())
