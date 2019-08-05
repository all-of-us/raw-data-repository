from dao.patient_status_dao import PatientStatusDao
from model.patient_status import PatientStatus


def backfill_patient_status():
  """ cron job to backfill patient status """
  dao = PatientStatusDao()

  with dao.session() as session:
    results = session.query(PatientStatus.participantId).distinct(PatientStatus.participantId).all()

    for row in results:
      dao.update_participant_summary(row.participantId)

