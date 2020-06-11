from protorpc import messages

from sqlalchemy import Column, event, String, UniqueConstraint, BigInteger, SmallInteger

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import UTCDateTime6


class ResourceTypeEnum(messages.Enum):
    """
    IDs for Resource Type table.  Must match Resource API URI pattern minus forward slashes.
    """
    # Awardee Resources
    AwardeeHPO = 1000
    AwardeeOrganization = 1001
    AwardeeSite = 1002

    # Codebook Code Resources
    Codes = 1010

    # Participant Resources
    Participant = 2000
    ParticipantProfile = 2001
    ParticipantActivity = 2002
    ParticipantAddresses = 2003
    ParticipantRaces = 2004
    ParticipantGenders = 2005
    ParticipantConsents = 2006

    ParticipantBiobankOrders = 2020
    ParticipantBiobankOrdersActivity = 2021
    ParticipantBiobankOrderSamples = 2022

    ParticipantPhysicalMeasurementsActivity = 2030
    ParticipantPhysicalMeasurementMeasurements = 2031

    ParticipantModulesActivity = 2040
    ParticipantModules = 2041
    ParticipantModuleAnswers = 2042

    # Research Workbench
    ResearchWorkbenchWorkspaces = 3000
    ResearchWorkbenchWorkspaceActivity = 3001
    ResearchWorkbenchWorkspaceRaces = 3002
    ResearchWorkbenchWorkspaceAges = 3003

    ResearchWorkbenchResearchers = 3010
    ResearchWorkbenchResearcherActivity = 3011
    ResearchWorkbenchResearcherRaces = 3012
    ResearchWorkbenchResearcherGenders = 3013
    ResearchWorkbenchResearcherSexAtBirth = 3014

    ResearchWorkbenchInstitutionalAffiliations = 3020


class ResourceType(Base):
    """
    Resource Type Model
    """
    __tablename__ = "resource_type"

    # Primary Key
    id = Column("id", BigInteger, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", UTCDateTime6, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", UTCDateTime6, nullable=True)
    resourceURI = Column("resource_uri", String(80), nullable=False)
    resourcePKField = Column("resource_pk_field", String(65), nullable=False)
    typeName = Column("type_name", String(80), nullable=False)
    typeUID = Column("type_uid", SmallInteger, nullable=False)

    __table_args__ = (
        UniqueConstraint("resource_uri"),
        UniqueConstraint("type_uid", "type_name"),
    )


event.listen(ResourceType, "before_insert", model_insert_listener)
event.listen(ResourceType, "before_update", model_update_listener)
