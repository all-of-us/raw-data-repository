from sqlalchemy import Column, BigInteger, Integer, String, Date, event
from sqlalchemy.dialects.mysql import JSON

from rdr_service.model.utils import UTCDateTime
from rdr_service.model.base import (
    model_insert_listener,
    model_update_listener,
    PPSCBase,
)


class AwardeeInSite(PPSCBase):
    __tablename__ = "awardee_insite"

    internal_fields = ["id", "created", "modified"]

    id = Column("id", Integer, autoincrement=True, primary_key=True)
    created = Column("created", UTCDateTime)
    modified = Column("modified", UTCDateTime)

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
        "withdrawal_status", String(32), nullable=False, default="not_withdrawn"
    )
    withdrawalTime = Column("withdrawal_time", UTCDateTime, nullable=True)

    deactivationStatus = Column(
        "deactivation_status", String(32), nullable=False, default="not_deactivated"
    )
    deactivationTime = Column("deactivation_time", UTCDateTime, nullable=True)

    deceasedStatus = Column(
        "deceased_status", String(32), nullable=False, default="unset"
    )
    deceasedAuthored = Column("deceased_authored", UTCDateTime, nullable=True)

    clinicPhysicalMeasurementsStatus = Column(
        "clinic_physical_measurements_status",
        String(32),
        nullable=False,
        default="unset",
    )
    clinicPhysicalMeasurementsFinalizedTime = Column(
        "clinic_physical_measurements_finalized_time", UTCDateTime, nullable=True
    )
    clinicPhysicalMeasurementsFinalizedSite = Column(
        "clinic_physical_measurements_finalized_site", String(255), nullable=True
    )

    selfReportedPhysicalMeasurementsStatus = Column(
        "self_reported_physical_measurements_status",
        String(32),
        nullable=False,
        default="unset",
    )
    selfReportedPhysicalMeasurementsAuthored = Column(
        "self_reported_physical_measurements_authored", UTCDateTime, nullable=True
    )

    consentForElectronicHealthRecords = Column(
        "consent_for_electronic_health_records",
        String(10),
        nullable=False,
        default="no",
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
        "consent_for_study_enrollment", String(10), nullable=False, default="no"
    )
    consentForStudyEnrollmentAuthored = Column(
        "consent_for_study_enrollment_authored", UTCDateTime, nullable=True
    )

    patientStatus = Column("patient_status", JSON, nullable=True, default=list())

    enrollmentStatus = Column("enrollment_status", String(32), nullable=True)

    biospecimenSourceSite = Column(
        "biospecimen_source_site", String(255), nullable=True
    )
    biospecimenOrderTime = Column("biospecimen_order_time", UTCDateTime, nullable=True)
    biospecimenStatus = Column(
        "biospecimen_status", String(255), nullable=False, default="unset"
    )

    sample1SAL2CollectionMethod = Column(
        "sample_1sal2_collection_method", String(255), nullable=False, default="unset"
    )
    sampleStatus1SAL2 = Column(
        "sample_status_1sal2", String(255), nullable=False, default="unset"
    )
    sampleOrderStatus1SAL2 = Column(
        "sample_order_status_1sal2", String(255), nullable=False, default="unset"
    )
    sampleOrderStatus1SAL2Time = Column(
        "sample_order_status_1sal2_time", UTCDateTime, nullable=True
    )

    @classmethod
    def create_surrogate_key_sql(cls) -> str:
        """
        Generates a SQL string for computing a hash of concatenated column values.

        :return: A SQL string that computes the hash of the concatenated column values.
            Eg: >>> generate_surrogate_sql()
            "FARM_FINGERPRINT(CONCAT(COALESCE(CAST(col1 AS STRING), ''), '|' , COALESCE(CAST(col2 AS STRING), '') ))"
        """
        keys = [
            column.key
            for column in AwardeeInSite.__table__.columns
            if column.key not in AwardeeInSite.internal_fields
        ]
        sql_string = (
            "FARM_FINGERPRINT(CONCAT("
            + ", ".join(
                (
                    f"COALESCE(CAST({key} AS STRING), ''), '|' "
                    if key != "patient_status"
                    else "COALESCE(TO_JSON_STRING(patient_status), '[]'), '|' "
                )
                for key in keys
            ).rstrip(", '|' ")
            + "))"
        )
        return sql_string


event.listen(AwardeeInSite, "before_insert", model_insert_listener)
event.listen(AwardeeInSite, "before_update", model_update_listener)
