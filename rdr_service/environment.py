
from rdr_service.config import GAE_PROJECT


class EnvironmentManager:
    target_project_id = GAE_PROJECT
    """
    This is the GAE project that the code should be operating on.
    Meaning that the project's database would be used by default, any tasks would be queued here,
    etc.
    """
