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
