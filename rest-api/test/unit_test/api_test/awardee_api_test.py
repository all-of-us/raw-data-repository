from test.unit_test.unit_test_util import FlaskTestBase

def _make_awardee_dict(id, display_name, type):
  return { 'fullUrl': 'http://localhost/rdr/v1/Awardee/%s' % id,
           'resource': {
             'displayName': display_name,
             'id': id,
             'type': type
           }}


class AwardeeApiTest(FlaskTestBase):
  def setUp(self):
    super(AwardeeApiTest, self).setUp()
    
  def test_get_awardees_no_organizations(self):
    result = self.send_get('Awardee')
    self.assertEquals(2, len(result['entry']))
    self.assertEquals(_make_awardee_dict('PITT', 'Pittsburgh', 'HPO'), result['entry'][0])
    self.assertEquals(_make_awardee_dict('UNSET', 'Unset', 'UNSET'), result['entry'][1])
  
    
  
  