from sqlalchemy import Column, BigInteger, Integer, String, Date, event
from sqlalchemy.dialects.mysql import TINYINT, JSON
from rdr_service.model.base import (
    model_insert_listener,
    model_update_listener,
    PPSCBase,
)
from rdr_service.model.utils import UTCDateTime


class AwardeeInSite(PPSCBase):
    __tablename__ = "awardee_insite"

    id = Column("id", Integer, autoincrement=True, primary_key=True)
    created = Column("created", UTCDateTime)
    modified = Column("modified", UTCDateTime)
    ignoreFlag = Column("ignore_flag", TINYINT, default=0)

    participantId = Column("participant_id", BigInteger, nullable=False)

    firstName = Column("first_name", String(255), nullable=True)
    middleName = Column("middle_name", String(255), nullable=True)
    lastName = Column("last_name", String(255), nullable=True)
    zipCode = Column("zip_code", String(10), nullable=True)
    state = Column("state", String(255), nullable=True)
    city = Column("city", String(255), nullable=True)
    streetAddress = Column("street_address", String(255), nullable=True)
    streetAddress2 = Column("street_address2", String(255), nullable=True)
    phoneNumber = Column("phone_number", String(80), nullable=True)
    email = Column("email", String(255), nullable=True)
    dateOfBirth = Column("date_of_birth", Date, nullable=True)

    organization = Column("organization", String(255), nullable=True)

    withdrawalStatus = Column(
        "withdrawal_status", String(32), nullable=True, default="NOT_WITHDRAWN"
    )
    withdrawalTime = Column("withdrawal_time", UTCDateTime, nullable=True)

    deactivationStatus = Column(
        "deactivation_status", String(32), nullable=True, default="NOT_DEACTIVATED"
    )
    deactivationTime = Column("deactivation_time", UTCDateTime, nullable=True)

    deceasedStatus = Column("deceased_status", String(32), nullable=True)
    deceasedAuthored = Column("deceased_authored", UTCDateTime, nullable=True)

    clinicPhysicalMeasurementsStatus = Column(
        "clinic_physical_measurements_status", String(32), nullable=True
    )
    clinicPhysicalMeasurementsFinalizedTime = Column(
        "clinic_physical_measurements_finalized_time", UTCDateTime, nullable=True
    )
    clinicPhysicalMeasurementsFinalizedSite = Column(
        "clinic_physical_measurements_finalized_site", String(255), nullable=True
    )

    selfReportedPhysicalMeasurementsStatus = Column(
        "self_reported_physical_measurements_status", String(32), nullable=True
    )
    selfReportedPhysicalMeasurementsAuthored = Column(
        "self_reported_physical_measurements_authored", UTCDateTime, nullable=True
    )

    consentForElectronicHealthRecords = Column(
        "consent_for_electronic_health_records", String(10), default="NO"
    )
    consentForElectronicHealthRecordsAuthored = Column(
        "consent_for_electronic_health_records_authored", UTCDateTime, nullable=True
    )
    consentForElectronicHealthRecordsFirstYesAuthored = Column(
        "consent_for_electronic_health_records_first_yes_authored",
        UTCDateTime,
        nullable=True,
    )
    firstEhrReceiptTime = Column("first_ehr_receipt_time", UTCDateTime, nullable=True)
    latestEhrReceiptTime = Column("latest_ehr_receipt_time", UTCDateTime, nullable=True)

    consentForStudyEnrollment = Column(
        "consent_for_study_enrollment", String(10), default="NO"
    )
    consentForStudyEnrollmentAuthored = Column(
        "consent_for_study_enrollment_authored", UTCDateTime, nullable=True
    )

    patientStatus = Column("patientStatus", JSON, nullable=True, default=list())

    enrollmentStatus = Column("enrollmentStatus", String(32), nullable=True)

    biospecimenSourceSite = Column(
        "biospecimen_source_site", String(255), nullable=True
    )
    biospecimenOrderTime = Column("biospecimen_order_time", UTCDateTime, nullable=True)
    biospecimenStatus = Column("biospecimen_status", String(255), nullable=True)

    sample1SAL2CollectionMethod = Column(
        "sample_1sal2_collection_method", String(255), nullable=True
    )
    sampleStatus1SAL2 = Column("sample_status_1sal2", String(255), nullable=True)
    sampleOrderStatus1SAL2 = Column(
        "sample_order_status_1sal2", String(255), nullable=True
    )
    sampleOrderStatus1SAL2Time = Column(
        "sample_order_status_1sal2_time", UTCDateTime, nullable=True
    )


event.listen(AwardeeInSite, "before_insert", model_insert_listener)
event.listen(AwardeeInSite, "before_update", model_update_listener)
