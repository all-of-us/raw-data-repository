from test.unit_test.unit_test_util import FlaskTestBase

def _make_awardee_dict(awardee_id, display_name, org_type):
  return { 'fullUrl': 'http://localhost/rdr/v1/Awardee/%s' % awardee_id,
           'resource': {
             'displayName': display_name,
             'id': awardee_id,
             'type': org_type
           }}


class AwardeeApiTest(FlaskTestBase):
  def setUp(self):
    super(AwardeeApiTest, self).setUp()

  def test_get_awardees_no_organizations(self):
    result = self.send_get('Awardee')
    self.assertEquals(2, len(result['entry']))
    self.assertEquals(_make_awardee_dict('PITT', 'Pittsburgh', 'HPO'), result['entry'][0])
    self.assertEquals(_make_awardee_dict('UNSET', 'Unset', 'UNSET'), result['entry'][1])



