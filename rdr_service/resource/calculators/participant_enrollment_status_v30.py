#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import datetime

from rdr_service.code_constants import (
    CONSENT_PERMISSION_YES_CODE,
    DVEHRSHARING_CONSENT_CODE_YES
)
from rdr_service.participant_enums import QuestionnaireResponseClassificationType
from rdr_service.resource.calculators import EnrollmentStatusCalculator
from rdr_service.resource.constants import PDREnrollmentStatusEnum
from rdr_service.resource.constants import ParticipantEventEnum, ConsentCohortEnum


class EnrollmentStatusInfo:
    """ Information about Enrollment Status events """
    calculated = False  # False = No, True = Yes
    first_ts = None  # First timestamp seen.
    last_ts = None  # Last timestamp seen.
    values = None  # List of related events.

    def add_value(self, value):
        """ Save a relevant datum to the values list. """
        if self.values is None:
            self.values = list()
        self.values.append(value)


class EnrollmentStatusCalculator_v3_0(EnrollmentStatusCalculator):
    """
    Calculate participant enrollment status.
    Changes:
        * Implement Participant PM&B Eligible status
        * Never downgrade participant status
        * (v3.1 Feature) : If participant has "Ever" consented to EHR sharing, participant EHR sharing stays "Yes"
                           even if there is a negative EHR consent later.
    """
    # Additional properties to support v3.0 calculations.
    _thebasics_module = None
    participant_pmb_eligible_time = None

    def calculate_from_events(self, events):
        """
        Use the events list to calculate the participant enrolment status and status timestamps.
        :param events: List of events to use in calculations
        """
        # Get each datum needed for calculating the enrollment status.
        signed_up = self.calc_signup(events)
        consented, cohort = self.calc_consent(events)
        ehr_consented = self.calc_ehr_consent(events)
        gror_received = self.calc_gror_received(events)
        biobank_samples = self.calc_biobank_samples(events)
        physical_measurements = self.calc_physical_measurements(events)
        thebasics_module = self.calc_thebasics_modules(events)
        baseline_modules = self.calc_baseline_modules(events)

        if not self.cohort:
            self.cohort = cohort
        # Calculate enrollment status
        status = PDREnrollmentStatusEnum.Unset
        if signed_up:
            status = PDREnrollmentStatusEnum.Registered
        if status == PDREnrollmentStatusEnum.Registered and consented:
            status = PDREnrollmentStatusEnum.Participant
        if status == PDREnrollmentStatusEnum.Participant and ehr_consented:
            status = PDREnrollmentStatusEnum.ParticipantPlusEHR
        if status == PDREnrollmentStatusEnum.ParticipantPlusEHR and thebasics_module and \
                thebasics_module.values:
            status = PDREnrollmentStatusEnum.ParticipantPMBEligible
        if status == PDREnrollmentStatusEnum.ParticipantPMBEligible and biobank_samples and \
                (cohort != ConsentCohortEnum.COHORT_3 or gror_received) and \
                (baseline_modules and len(baseline_modules.values) >= len(self._module_enums)):
            status = PDREnrollmentStatusEnum.CoreParticipantMinusPM
        if status == PDREnrollmentStatusEnum.CoreParticipantMinusPM and \
                physical_measurements and \
                (cohort != ConsentCohortEnum.COHORT_3 or gror_received):
            status = PDREnrollmentStatusEnum.CoreParticipant

        # Only move forward with enrollment status changes, do not downgrade.
        if status > self.status:
            self.status = status

        # Save the timestamp when each status was reached.
        self.save_status_timestamp(status)

    def save_status_timestamp(self, status):
        """
        Save the status timestamp when we first see that status achieved.
        :param status: Current calculated enrollment status.
        """
        super().save_status_timestamp(status)

        if not self.participant_pmb_eligible_time and self._thebasics_module:
            self.participant_pmb_eligible_time = self._thebasics_module.first_ts

    def calc_ehr_consent(self, events):
        """
        Determine if participant has an EHR Consent.
        Criteria:
          - Use first "Yes" consent submission, ignore all negative "No" consent submissions.
        :param events: List of events
        :return: EnrollmentStatusInfo object
        """
        info = EnrollmentStatusInfo()
        for ev in events:
            if ev.event in [ParticipantEventEnum.EHRConsentPII, ParticipantEventEnum.DVEHRSharing]:
                # See if we should set the consent info.
                if info.calculated is False and \
                        ev.answer in [CONSENT_PERMISSION_YES_CODE, DVEHRSHARING_CONSENT_CODE_YES]:
                    info.calculated = True
                    info.first_ts = ev.last_ts = ev.timestamp
                    info.add_value(ev)
                    break

        return self.save_calc('_ehr_consented', info)

    def calc_thebasics_modules(self, events):
        """
        Find TheBasics module the participant has submitted.
        Criteria:
          - TheBasics submissions
        :param events: List of events
        :return: EnrollmentStatusInfo object
        """
        info = EnrollmentStatusInfo()
        info.first_ts = datetime.datetime.max
        info.last_ts = datetime.datetime.min

        def module_type_stored(mod_ev_):
            """ See if we have stored that module type already. """
            if isinstance(info.values, list):
                for ev_ in info.values:
                    if mod_ev_ == ev_.event:
                        return True
            return False

        # Find the TheBasics module events.
        for ev in events:
            # Make sure we are saving a distinct list of baseline module events that have a COMPLETE classification
            if (ev.event == ParticipantEventEnum.TheBasics
                    and ev.classification_type == str(QuestionnaireResponseClassificationType.COMPLETE)
                    and module_type_stored(ParticipantEventEnum.TheBasics) is False):
                if ev.timestamp < info.first_ts:
                    info.first_ts = ev.timestamp
                if ev.timestamp > info.last_ts:
                    info.last_ts = ev.timestamp
                info.add_value(ev)
                info.calculated = True
                break

        return self.save_calc('_thebasics_module', info)
