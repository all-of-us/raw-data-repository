#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import datetime

from rdr_service.code_constants import (
    CONSENT_PERMISSION_YES_CODE,
    CONSENT_PERMISSION_NO_CODE,
    DVEHRSHARING_CONSENT_CODE_YES,
    EHR_CONSENT_EXPIRED_YES,
    DVEHRSHARING_CONSENT_CODE_NO, CONSENT_GROR_NO_CODE, CONSENT_GROR_NOT_SURE, CONSENT_GROR_YES_CODE
)
from rdr_service import config
from rdr_service.resource.constants import PDREnrollmentStatusEnum
from rdr_service.resource.constants import ParticipantEventEnum, COHORT_1_CUTOFF, \
    COHORT_2_CUTOFF, ConsentCohortEnum
from rdr_service.services.system_utils import JSONObject


class EnrollmentStatusInfo:
    """ Information about Enrollment Status events """
    calculated = False  # False = No, True = Yes
    first_ts = None  # First timestamp seen.
    last_ts = None  # Last timestamp seen.
    values: list = list()  # List of related events.


class EnrollmentStatusCalculator:
    """
    Calculate participant enrollment status.
    """
    status: PDREnrollmentStatusEnum = PDREnrollmentStatusEnum.Unset
    # events = None  # List of EnrollmentStatusEvent objects.
    activity = None  # List of activity created from Participant generator.

    cohort = None

    # First info object for each part used in calculating enrollment status.
    _signup = None
    _consented = None
    _ehr_consented = None
    _gror_consented = None
    _biobank_samples = None
    _physical_measurements = None
    _baseline_modules = None

    # Timestamps for when each status was achieved, these are set in the self.save_calc() method.
    registered_time = None
    participant_time = None
    participant_plus_ehr_time = None
    core_participant_minus_pm_time = None
    core_participant_time = None

    def run(self, activity: list):
        """
        :param activity: A list of activity dictionary objects created by the ParticipantSummaryGenerator.
        """
        self.activity = sorted([JSONObject(r) for r in activity if r['timestamp']], key=lambda i: i.timestamp)
        # Work through activity by slicing to determine current enrollment status.
        # This method allows us to iterate once through the data and still catch participants
        # that might have been considered a Core Participant at one point, but would not by
        # looking at their current state.
        for x in range(1, len(self.activity)+1):
            events = self.activity[0:x]

            # Get each datum needed for calculating the enrollment status.
            signed_up = self.calc_signup(events)
            consented, cohort = self.calc_consent(events)
            ehr_consented = self.calc_ehr_consent(events)
            gror_consented = self.calc_gror_consent(events)
            biobank_samples = self.calc_biobank_samples(events)
            physical_measurements = self.calc_physical_measurements(events)
            modules = self.calc_baseline_modules(events)

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
            if status == PDREnrollmentStatusEnum.ParticipantPlusEHR and biobank_samples and \
                    (modules and len(modules.values) >= 3) and \
                    (cohort != ConsentCohortEnum.COHORT_3 or gror_consented):
                status = PDREnrollmentStatusEnum.CoreParticipantMinusPM
            if status == PDREnrollmentStatusEnum.CoreParticipantMinusPM and \
                    physical_measurements and \
                    (cohort != ConsentCohortEnum.COHORT_3 or gror_consented):
                status = PDREnrollmentStatusEnum.CoreParticipant

            # Set the permanent enrollment status value if needed. Enrollment status can go down
            # unless the enrollment status has reached a 'Core' status.
            if status > self.status or self.status < PDREnrollmentStatusEnum.CoreParticipantMinusPM:
                self.status = status

            # Save the timestamp when each status was reached.
            self.save_status_timestamp(status)

    def save_status_timestamp(self, status):
        """
        Save the status timestamp when we first see that status achieved.
        :param status: Current calculated enrollment status.
        """
        # Set the first timestamp for each status the participant reaches.
        if status == PDREnrollmentStatusEnum.Registered and not self.registered_time:
            self.registered_time = self._signup.first_ts
        if status == PDREnrollmentStatusEnum.Participant and not self.participant_time:
            self.participant_time = self._consented.first_ts
        if status == PDREnrollmentStatusEnum.ParticipantPlusEHR and not self.participant_plus_ehr_time:
            self.participant_plus_ehr_time = self._ehr_consented.first_ts
        if status == PDREnrollmentStatusEnum.CoreParticipantMinusPM and not self.core_participant_minus_pm_time:
            self.core_participant_minus_pm_time = max([self._biobank_samples.first_ts, self._baseline_modules.last_ts])
        if status == PDREnrollmentStatusEnum.CoreParticipant and not self.core_participant_time:
            self.core_participant_time = \
                max([self._biobank_samples.first_ts, self._baseline_modules.last_ts,
                        self._physical_measurements.first_ts])
            # If we jumped over core minus pm status, just make it the same timestamp as core.
            if not self.core_participant_minus_pm_time:
                self.core_participant_minus_pm_time = self.core_participant_time

    def save_calc(self, key, info):
        """
        Save first calculated info object for the given key.
        :param key: Property name.
        :param info: EnrollmentStatusInfo object
        :return: EnrollmentStatusInfo object
        """
        if info.calculated is False:
            return None

        obj = getattr(self, key)
        if not obj or obj.calculated is False:
            setattr(self, key, info)
        return info

    def calc_signup(self, events):
        """
        Determine when participant signed up.
        Criteria:
          - Establish participant sign up timestamp.
        :param events: List of events
        :return: EnrollmentStatusInfo object
        """
        info = EnrollmentStatusInfo()
        for ev in events:
            if ev.event == ParticipantEventEnum.SignupTime:
                info.calculated = True
                info.first_ts = ev.last_ts = ev.timestamp
                info.values.append(ev)
                break
        return self.save_calc('_signup', info)

    def calc_consent(self, events):
        """
        Determine if participant has consented.
        Criteria:
          - ConsentPII has been submitted.
        :param events: List of events
        :return: EnrollmentStatusInfo object
        """
        info = EnrollmentStatusInfo()
        for ev in events:
            if ev.event == ParticipantEventEnum.ConsentPII:
                info.calculated = True
                info.first_ts = ev.last_ts = ev.timestamp
                info.values.append(ev)
                break

        # Calculate consent cohort group
        cohort = None
        if info.calculated is True:
            if info.first_ts < COHORT_1_CUTOFF:
                cohort = ConsentCohortEnum.COHORT_1
            elif COHORT_1_CUTOFF <= info.first_ts <= COHORT_2_CUTOFF:
                cohort = ConsentCohortEnum.COHORT_2
            else:
                cohort = ConsentCohortEnum.COHORT_3

        return self.save_calc('_consented', info), cohort

    def calc_ehr_consent(self, events):
        """
        Determine if participant has an EHR Consent.
        Criteria:
          - A positive EHR or DVEHR Consent submission has been received.
          - Use first Yes Consent submission after most recent No/Expired consent.
        :param events: List of events
        :return: EnrollmentStatusInfo object
        """
        info = EnrollmentStatusInfo()
        for ev in events:
            if ev.event == ParticipantEventEnum.EHRConsentPII or ev.event == ParticipantEventEnum.DVEHRSharing:
                # See if we need to reset the info object.
                if ev.answer in [CONSENT_PERMISSION_NO_CODE, DVEHRSHARING_CONSENT_CODE_NO, EHR_CONSENT_EXPIRED_YES]:
                    info = EnrollmentStatusInfo()
                    continue
                # See if we should set the consent info.
                if info.calculated is False and \
                        ev.answer in [CONSENT_PERMISSION_YES_CODE, DVEHRSHARING_CONSENT_CODE_YES]:
                    info.calculated = True
                    info.first_ts = ev.last_ts = ev.timestamp
                    info.values.append(ev)

        return self.save_calc('_ehr_consented', info)

    def calc_gror_consent(self, events):
        """
        Determine if participant has consented to GROR.
        Criteria:
          - GROR consented has been submitted with a CheckDNA_Yes answer.
        :param events: List of events
        :return: EnrollmentStatusInfo object
        """
        info = EnrollmentStatusInfo()
        for ev in events:
            if ev.event == ParticipantEventEnum.GROR:
                # See if we need to reset the info object.
                if ev.answer in [CONSENT_GROR_NO_CODE, CONSENT_GROR_NOT_SURE]:
                    info = EnrollmentStatusInfo()
                    continue
                # See if we should set the consent info.
                if info.calculated is False and ev.answer == CONSENT_GROR_YES_CODE:
                    info.calculated = True
                    info.first_ts = ev.last_ts = ev.timestamp
                    info.values.append(ev)

        return self.save_calc('_gror_consented', info)

    def calc_biobank_samples(self, events):
        """
        Determine if biobank has confirmed DNA test for participant.
        Criteria:
          - First time DNA tests have been confirmed by the BioBank.
        :param events: List of events
        :return: EnrollmentStatusInfo object
        """
        info = EnrollmentStatusInfo()
        for ev in events:
            if ev.event == ParticipantEventEnum.BiobankConfirmed and ev.dna_tests > 0:
                info.calculated = True
                info.first_ts = ev.last_ts = ev.timestamp
                info.values.append(ev)
                break

        return self.save_calc('_biobank_samples', info)

    def calc_physical_measurements(self, events):
        """
        Determine if biobank has confirmed DNA test for participant.
        Criteria:
          - Physical Measurements have been received and finalized.
        :param events: List of events
        :return: EnrollmentStatusInfo object
        """
        info = EnrollmentStatusInfo()
        for ev in events:
            if info.calculated is False and ev.event == ParticipantEventEnum.PhysicalMeasurements and \
                    ev.finalized is not None:
                info.calculated = True
                info.first_ts = ev.last_ts = ev.timestamp
                info.values.append(ev)
                break

        return self.save_calc('_physical_measurements', info)

    def calc_baseline_modules(self, events):
        """
        Find the baseline modules the participant has submitted.
        Criteria:
          - First TheBasics, Lifestyle and OverallHealth submissions
        :param events: List of events
        :return: EnrollmentStatusInfo object
        """
        info = EnrollmentStatusInfo()
        info.first_ts = datetime.datetime.max
        info.last_ts = datetime.datetime.min

        # Create a list of the baseline module enumerations from the config file.
        module_enums = [ParticipantEventEnum[mod.replace('questionnaireOn', '')]
                                for mod in config.getSettingList('baseline_ppi_questionnaire_fields')]
        mod_events = list()

        # Find the baseline module events.
        for ev in events:
            for mod_ev in module_enums:
                if ev.event == mod_ev:
                    if ev.timestamp < info.first_ts:
                        info.first_ts = ev.timestamp
                    if ev.timestamp > info.last_ts:
                        info.last_ts = ev.timestamp
                    mod_events.append(ev)

        info.values = mod_events
        if info.values:
            info.calculated = True

        return self.save_calc('_baseline_modules', info)
