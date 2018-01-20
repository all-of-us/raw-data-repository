from test.unit_test.unit_test_util import FlaskTestBase

class MetricsFieldsApiTest(FlaskTestBase):

  def test_metrics_fields(self):
    response = self.send_get('MetricsFields')
    fields_dict = {item['name']: item['values'] for item in response}
    # Rather than asserting all the fields (replicating the config),
    # assert just a couple.
    self.assertEquals(['UNSET', 'MIDWEST', 'NORTHEAST', 'SOUTH', 'WEST'],
                      fields_dict.get('Participant.censusRegion'))
    self.assertEquals(['UNSET', 'PITT', 'AZ_TUCSON'], fields_dict.get('Participant.hpoId'))
