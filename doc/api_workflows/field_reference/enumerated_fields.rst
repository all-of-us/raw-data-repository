============================================================
Enumerated Field Options Reference
============================================================
Below is a reference to all available field options for fields with defined choices.

Participant Summary Field Options
============================================================

.. _age_range:

ageRange
------------------------------------------------------------
  * 0-17
  * 18-25
  * 26-35
  * 36-45
  * 46-55
  * 56-65
  * 66-75
  * 76-85
  * 86+

.. _gender_identity:

genderIdentity
------------------------------------------------------------

  * UNSET
  * PMI_Skip
  * GenderIdentity_Man
  * GenderIdentity_Woman
  * GenderIdentity_NonBinary
  * GenderIdentity_Transgender
  * GenderIdentity_AdditionalOptions
  * GenderIdentity_MoreThanOne
  * PMI_PreferNotToAnswer

.. _race:

race
------------------------------------------------------------
  * UNSET
  * PMI_Skip
  * AMERICAN_INDIAN_OR_ALASKA_NATIVE
  * BLACK_OR_AFRICAN_AMERICAN
  * ASIAN
  * NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER
  * WHITE
  * HISPANIC_LATINO_OR_SPANISH
  * MIDDLE_EASTER_OR_NORTH_AFRICAN
  * HLS_AND_WHITE
  * HSL_AND_BLACK
  * HLS_AND_ONE_OTHER_RACE
  * HLS_AND_MORE_THAN_ONE_OTHER_RACE
  * MORE_THAN_ONE_RACE
  * OTHER_RACE
  * PREFER_NOT_TO_SAY


.. _enrollment_status:

enrollmentStatus
------------------------------------------------------------

  * INTERESTED
  * MEMBER
  * FULL_PARTICIPANT

.. _ehr_status:

ehrStatus
------------------------------------------------------------
  * NOT_PRESENT
  * PRESENT

.. _questionnaire_status:

consentFor\*, questionnaireOn\*
------------------------------------------------------------
Below are the available options for consent forms and questionnaires:

consentFor[StudyEnrollment, ElectronicHealthRecords, DVElectronicHealthRecordsSharing, CABor, GenomicsROR]

questionnaireOn[TheBasics, OverallHealth, Lifestyle, HealthcareAccess, MedicalHistory, Medications, FamilyHealth, CopeMay, CopeJune,
CopeJuly, DnaProgram ]

  * UNSET
  * SUBMITTED
  * SUBMITTED_NO_CONSENT
  * SUBMITTED_NOT_SURE
  * SUBMITTED_INVALID

.. _consent_cohort:

consentCohort
------------------------------------------------------------
  * COHORT_1
  * COHORT_2
  * COHORT_3

.. _cohort_2_pilot_flag:

cohort2PilotFlag
------------------------------------------------------------
  * UNSET
  * COHORT_2_PILOT

.. _physical_measurements_status:

physicalMeasurementsStatus
------------------------------------------------------------

  * UNSET
  * COMPLETED
  * CANCELLED

.. _biospecimen_status:

biospecimenStatus
------------------------------------------------------------
  * UNSET
  * CREATED
  * COLLECTED
  * PROCESSED
  * FINALIZED

.. _sample_status:

samplesToIsolateDNA, sampleStatus\*
------------------------------------------------------------
Below are the available options for samplesToIsolateDNA and sampleStatus[1SS08, 1SST8, 2SST8, 1PS08, 1PST8, 2PST8, 1HEP4,
1ED04, 1ED10, 2ED10, 1UR10, 1UR90, 1ED02, 1CFD9, 1PXR2, 1SAL, 1SAL2, DV1SAL2]

  * UNSET
  * RECEIVED
  * DISPOSED
  * CONSUMED
  * UNKNOWN
  * SAMPLE_NOT_RECEIVED
  * SAMPLE_NOT_PROCESSED
  * ACCESSINGING_ERROR
  * LAB_ACCIDENT
  * QNS_FOR_PROCESSING
  * QUALITY_ISSUE

.. _sample_order_status:

sampleOrderStatus\*
------------------------------------------------------------
Below are the available options for sampleOrderStatus[1SST8, 1PST8, 1HEP4, 1ED04, 1ED10, 2ED10, 1UR10, 1UR90, 1ED02, 1CFD9, 1PXR2, 1SAL, 1SAL2]

  * UNSET
  * CREATED
  * COLLECTED
  * PROCESSED
  * FINALIZED

.. _withdrawal_status:

withdrawalStatus
------------------------------------------------------------

  * NOT_WITHDRAWN
  * NO_USE
  * EARLY_OUT

.. _withdrawal_reason:

withdrawalReason
------------------------------------------------------------

  * UNSET
  * FRAUDULENT
  * DUPLICATE
  * TEST

.. _suspension_status:

suspensionStatus
------------------------------------------------------------

  * NOT_SUSPENDED
  * NO_CONTACT

.. _ehr_consent_expire_status:

ehrConsentExpireStatus
------------------------------------------------------------
  * UNSET
  * NOT_EXPIRED
  * EXPIRED

DeceasedStatus
------------------------------------------------------------
  * UNSET
  * PENDING
  * APPROVED

DeceasedNotification
------------------------------------------------------------
  * EHR
  * ATTEMPTED_CONTACT
  * NEXT_KIN_HPO
  * NEXT_KIN_SUPPORT
  * OTHER

DeceasedReportStatus
------------------------------------------------------------
  * PENDING
  * APPROVED
  * DENIED

DeceasedReportDenialReason
------------------------------------------------------------
  * INCORRECT_PARTICIPANT
  * MARKED_IN_ERROR
  * INSUFFICIENT_INFORMATION
  * OTHER

.. _retention_status:

RetentionStatus
------------------------------------------------------------
  * NOT_ELIGIBLE
  * ELIGIBLE
