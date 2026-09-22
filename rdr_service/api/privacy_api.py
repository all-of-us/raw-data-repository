import logging

from werkzeug.exceptions import BadRequest, NotFound
from sqlalchemy import text

from rdr_service.api.base_api import BaseApi
from rdr_service.app_util import auth_required, restrict_to_gae_project
from rdr_service.dao.database_factory import get_database
from rdr_service.services.gcp_config import RdrEnvironment


class PrivacyLookupApi(BaseApi):
    """
    Lightweight API for looking up privacy-related data from a different schema.
    Only available in sandbox environments.

    This API doesn't use the standard DAO/ORM approach since it queries tables
    in a different schema that were created independently.
    """

    method_decorators = [
        auth_required(['rdr']),
        restrict_to_gae_project([RdrEnvironment.SANDBOX.value, 'sandbox'])
    ]

    def __init__(self):
        # Don't call parent __init__ since we don't use a DAO
        super(PrivacyLookupApi, self).__init__(dao=None)

    def get(self, concept_id=None, participant_id=None):
        """
        GET endpoint to retrieve privacy data by concept_id.

        Args:
            concept_id: The concept ID to look up (from URL parameter)
            participant_id: Not used, included for BaseApi compatibility

        Returns:
            JSON data from the privacy schema table
        """
        if not concept_id:
            raise BadRequest("concept_id parameter is required")

        try:
            # Get database session
            db = get_database()
            session = db.make_session()

            try:
                row = session.execute(
                    text("""
                         SELECT
                            privacy_risk_id,
                            concept_id,
                            privacy_entity_id,
                            source_origin,
                            workflow_status,
                            final_decision,
                            rule_version
                         FROM
                             `privacy`.privacy_risk_lookup
                         WHERE concept_id = :concept_id
                         """),
                    {"concept_id": concept_id}
                ).fetchone()

                # Check if result exists before parsing
                if not row:
                    raise NotFound(f"Privacy data not found for concept_id {concept_id}")

                # Parse the Row object into a dictionary
                result = {
                    "concept_id": row.concept_id,
                    "data": {
                        "privacy_risk_id": row.privacy_risk_id,
                        "privacy_entity_id": row.privacy_entity_id,
                        "source_origin": row.source_origin,
                        "workflow_status": row.workflow_status,
                        "final_decision": row.final_decision,
                        "rule_version": row.rule_version
                    }
                }

                return result

            finally:
                session.close()

        except NotFound:
            raise
        except Exception as e:
            logging.error(f"Error retrieving privacy data: {str(e)}", exc_info=True)
            raise NotFound(f"Error retrieving privacy data for concept_id {concept_id}")
