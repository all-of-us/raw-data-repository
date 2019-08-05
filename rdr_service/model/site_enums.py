from protorpc import messages

class SiteStatus(messages.Enum):
  """ The active scheduling status of a site. """
  UNSET = 0
  ACTIVE = 1
  INACTIVE = 2

class EnrollingStatus(messages.Enum):
  """ The actively enrolling status of a site. """
  UNSET = 0
  ACTIVE = 1
  INACTIVE = 2

class DigitalSchedulingStatus(messages.Enum):
  """ The status of a sites digital scheduling capability. """
  UNSET = 0
  ACTIVE = 1
  INACTIVE = 2

class ObsoleteStatus(messages.Enum):
  """ If an organization is obsolete but referenced in other tables. """
  ACTIVE = 0
  OBSOLETE = 1
