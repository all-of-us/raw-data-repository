from collections import defaultdict
from datetime import datetime

from sqlalchemy import or_

from rdr_service.participant_enums import PhysicalMeasurementsCollectType, PhysicalMeasurementsStatus,\
    SelfReportedPhysicalMeasurementsStatus
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.measurements import PhysicalMeasurements
from rdr_service.services.system_utils import list_chunks
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'pm_backfill'
tool_desc = 'Run backfill to correct how self-reported physical measurement fields were set in the participant summary.'


class PmFix(ToolBase):
    logger_name = None
    participant_ids = set()

    def run(self):
        super(PmFix, self).run()

        with self.get_session() as session:
            # Get list of summaries that have clinic physical measurements recorded so we can check to see that they
            # shouldn't have the self-reported fields set instead.
            summary_list = session.query(ParticipantSummary).filter(
                ParticipantSummary.clinicPhysicalMeasurementsStatus != PhysicalMeasurementsStatus.UNSET
            ).all()

            for summary_chunk in list_chunks(summary_list, 500):
                # for each participant that has physical measurements:
                # load their physical measurements (both clinic and self-reported)
                participant_id_list = [summary.participantId for summary in summary_chunk]
                self_measurement_collection = self._get_self_pm(
                    session=session,
                    participant_id_list=participant_id_list
                )
                clinic_measurement_collection = self._get_clinic_pm(
                    session=session,
                    participant_id_list=participant_id_list
                )

                # loop through each summary and check that the clinic fields are set as expected,
                # as well as their self reported fields. If any of the fields aren't correct, set them.
                for summary in summary_chunk:
                    self_measurement_list = self_measurement_collection.get(summary.participantId)
                    clinic_measurement_list = clinic_measurement_collection.get(summary.participantId)

                    expected_clinic_measurement = clinic_measurement_list[0] if clinic_measurement_list else None
                    if not self._has_clinic_data_set(summary, expected_clinic_measurement):
                        self._set_clinic_measurement_fields(summary, expected_clinic_measurement)

                    expected_self_measurement = self_measurement_list[0] if self_measurement_list else None
                    if not self._has_self_data_set(summary, expected_self_measurement):
                        self._set_self_measurement_fields(summary, expected_self_measurement)

    @classmethod
    def _get_clinic_pm(cls, session, participant_id_list):
        query = session.query(PhysicalMeasurements).filter(
            PhysicalMeasurements.participantId.in_(participant_id_list),
            PhysicalMeasurements.finalized.isnot(None),
            or_(
                PhysicalMeasurements.status != PhysicalMeasurementsStatus.CANCELLED,
                PhysicalMeasurements.status.is_(None)
            ),
            or_(
                PhysicalMeasurements.collectType != PhysicalMeasurementsCollectType.SELF_REPORTED,
                PhysicalMeasurements.collectType.is_(None)
            )
        ).order_by(PhysicalMeasurements.finalized.desc())

        participant_measurement_map = defaultdict(list)
        for physical_measurement in query.all():
            participant_measurement_map[physical_measurement.participantId].append(physical_measurement)

        return participant_measurement_map

    @classmethod
    def _get_self_pm(cls, session, participant_id_list):
        query = session.query(PhysicalMeasurements).filter(
            PhysicalMeasurements.participantId.in_(participant_id_list),
            PhysicalMeasurements.collectType == PhysicalMeasurementsCollectType.SELF_REPORTED
        ).order_by(PhysicalMeasurements.finalized.desc())

        participant_measurement_map = defaultdict(list)
        for physical_measurement in query.all():
            participant_measurement_map[physical_measurement.participantId].append(physical_measurement)

        return participant_measurement_map

    @classmethod
    def _has_clinic_data_set(cls, summary: ParticipantSummary, clinic_measurement: PhysicalMeasurements):
        if (
            summary.clinicPhysicalMeasurementsFinalizedTime is not None
            and summary.clinicPhysicalMeasurementsFinalizedTime < datetime(2022, 6, 15)
        ):
            return True

        if not clinic_measurement:
            return summary.clinicPhysicalMeasurementsFinalizedTime is None

        return summary.clinicPhysicalMeasurementsFinalizedTime == clinic_measurement.finalized

    @classmethod
    def _has_self_data_set(cls, summary: ParticipantSummary, self_measurement: PhysicalMeasurements):
        if self_measurement is None:
            return summary.selfReportedPhysicalMeasurementsAuthored is None

        return summary.selfReportedPhysicalMeasurementsAuthored == self_measurement.finalized

    @classmethod
    def _set_clinic_measurement_fields(cls, summary: ParticipantSummary, clinic_measurement: PhysicalMeasurements):
        if clinic_measurement:
            summary.clinicPhysicalMeasurementsStatus = PhysicalMeasurementsStatus.COMPLETED
            summary.clinicPhysicalMeasurementsTime = clinic_measurement.created
            summary.clinicPhysicalMeasurementsFinalizedTime = clinic_measurement.finalized
            summary.clinicPhysicalMeasurementsCreatedSiteId = clinic_measurement.createdSiteId
            summary.clinicPhysicalMeasurementsFinalizedSiteId = clinic_measurement.finalizedSiteId
        else:
            # Clear any data set by self-reported measurements
            summary.clinicPhysicalMeasurementsStatus = PhysicalMeasurementsStatus.UNSET
            summary.clinicPhysicalMeasurementsTime = None
            summary.clinicPhysicalMeasurementsFinalizedTime = None

    @classmethod
    def _set_self_measurement_fields(cls, summary: ParticipantSummary, self_measurement: PhysicalMeasurements):
        # By default the self-reported fields are empty, if we're needing to set them
        # then they're empty and something is missing
        summary.selfReportedPhysicalMeasurementsStatus = SelfReportedPhysicalMeasurementsStatus.COMPLETED
        summary.selfReportedPhysicalMeasurementsAuthored = self_measurement.finalized


def run():
    return cli_run(tool_cmd, tool_desc, PmFix)
