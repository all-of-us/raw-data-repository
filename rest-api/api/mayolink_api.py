import urllib2
from xml.etree import ElementTree as ET

from api.base_api import UpdatableApi
from api_util import RDR
from app_util import auth_required


mayo_test_api = 'https://test.orders.mayocliniclabs.com/orders/create.xml'
# get response
response = urllib2.urlopen(mayo_test_api)
root = ET.parse(response).getroot()

# iterate
items = root.findall('orders/order')
for item in items:
  print item


class MayoLinkApi(UpdatableApi):

  def __init__(self):
    super(MayoLinkApi, self).__init__(MayoLinkApi(), get_returns_children=True)
    self.namespace = 'http://orders.mayomedicallaboratories.com'

  @auth_required(RDR)
  def post(self, order):
    pass
