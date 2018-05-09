from code_constants import UNSET
from dao.cache_all_dao import CacheAllDao
from dao.base_dao import FhirMixin, FhirProperty
from dao.organization_dao import _FhirOrganization, OrganizationDao
from model.hpo import HPO
from model.organization import Organization
from singletons import HPO_CACHE_INDEX
from sqlalchemy.orm import subqueryload
from fhirclient.models.domainresource import DomainResource

class _FhirAwardee(FhirMixin, DomainResource):
  """FHIR client definition of the expected JSON structure for an Awardee resource."""
  resource_name = 'Awardee'
  _PROPERTIES = [
    FhirProperty('display_name', str, required=True),
    FhirProperty('type', str, required=True),
    FhirProperty('organizations', _FhirOrganization, is_list=True)
  ]

# Sort order for HPOs.
_ORDER_BY_ENDING = ('name')

class HPODao(CacheAllDao):

  def __init__(self):
    super(HPODao, self).__init__(HPO, cache_index=HPO_CACHE_INDEX,
                                 cache_ttl_seconds=600, index_field_keys=['name'],
                                 order_by_ending=_ORDER_BY_ENDING)

  def _validate_update(self, session, obj, existing_obj):
    # HPOs aren't versioned; suppress the normal check here.
    pass

  def get_id(self, obj):
    return obj.hpoId

  def get_by_name(self, name):
    return self._get_cache().index_maps['name'].get(name)

  def get_with_children_in_session(self, session, obj_id):
    return (session.query(HPO)
        .options(subqueryload(HPO.organizations).subqueryload(Organization.sites))
        .get(obj_id))

  def get_with_children(self, obj_id):
    with self.session() as session:
      return self.get_with_children_in_session(session, obj_id)

  def _make_query(self, session, query_def):  # pylint: disable=unused-argument
    # For now, no filtering, ordering, or pagination is supported; fetch child organizations and
    # sites.
    return (session.query(HPO).options(subqueryload(HPO.organizations)
                                       .subqueryload(Organization.sites))
                              .order_by(HPO.name),
            _ORDER_BY_ENDING)

  def to_client_json(self, model, inactive_sites):
    return HPODao._to_json(model, inactive_sites)

  @staticmethod
  def _to_json(model, inactive_sites=False):
    resource = _FhirAwardee()
    resource.id = model.name
    resource.display_name = model.displayName
    if model.organizationType:
      resource.type = str(model.organizationType)
    else:
      resource.type = UNSET
    resource.organizations = [OrganizationDao._to_json(organization, inactive_sites)
                              for organization
                              in model.organizations]
    json = resource.as_json()
    del json['resourceType']
    return json


  def _do_update(self, session, obj, existing_obj):
    super(HPODao, self)._do_update(session, obj, existing_obj)
    if obj.hpoId != existing_obj.hpoId:
      # from participant_dao import make_primary_provider_link_for_id
      # provider_link = make_primary_provider_link_for_id(obj.hpoId)
      provider_link = "'NOT A PROVIDER'"

      participant_sql = """ 
            UPDATE participant 
            SET hpo_id = {},
                last_modified = now(),
                provider_link = {}
            WHERE hpo_id = {};
            
            """ .format(obj.hpoId, provider_link, existing_obj.hpoId)

      participant_summary_sql = """ 
            UPDATE participant_summary
            SET hpo_id = {},
                last_modified = now()
            WHERE hpo_id = {};
            
            """ .format(obj.hpoId, existing_obj.hpoId)

      participant_history_sql = """ 
            UPDATE participant_history 
            SET hpo_id = {},
                last_modified = now(),
                provider_link = {}
            WHERE hpo_id = {};
            
            """ .format(obj.hpoId, provider_link, existing_obj.hpoId)

      with self.session() as session:
        session.execute(participant_sql)
        session.execute(participant_summary_sql)
        session.execute(participant_history_sql)
