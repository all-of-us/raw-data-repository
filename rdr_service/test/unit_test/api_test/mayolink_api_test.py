from rdr_service.test.unit_test.unit_test_util import FlaskTestBase


class MayoLinkApiTest(FlaskTestBase):
    def setUp(self):
        super(MayoLinkApiTest, self).setUp(use_mysql=True)
        self.path = "https://test.orders.mayomedicallaboratories.com/api/orders.xml"

    def test_send_post(self):
        # self.send_post()
        # will make tests next
        pass
