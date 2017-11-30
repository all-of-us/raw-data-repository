from protorpc import messages

class SiteStatus(messages.Enum):
  """The state of a site."""
  UNSET = 0
  ACTIVE = 1
  INACTIVE = 2
