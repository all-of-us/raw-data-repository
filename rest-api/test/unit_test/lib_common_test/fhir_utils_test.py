import unittest

from fhir_utils import SimpleFhirR4Reader


EXAMPLE_SUPPLY_REQUEST = {
  "authoredOn": "2019-02-12",
  "contained": [
    {
      "id": "supplier-1",
      "name": "GenoTek",
      "resourceType": "Organization"
    },
    {
      "deviceName": [
        {
          "name": "GenoTek DNA Kit",
          "type": "manufacturer-name"
        }
      ],
      "id": "device-1",
      "identifier": [
        {
          "code": "4081",
          "system": "SKU"
        },
        {
          "code": "SNOMED CODE TBD",
          "system": "SNOMED"
        }
      ],
      "resourceType": "Device"
    },
    {
      "address": [
        {
          "city": "FakeVille",
          "line": [
            "123 Fake St"
          ],
          "postalCode": "22155",
          "state": "VA",
          "type": "postal",
          "use": "home"
        }
      ],
      "id": "patient-1",
      "identifier": [
        {
          "system": "participantId",
          "value": "P748018940"
        }
      ],
      "resourceType": "Patient"
    }
  ],
  "deliverFrom": {
    "reference": "#supplier-1"
  },
  "deliverTo": {
    "reference": "Patient/#patient-1"
  },
  "extension": [
    {
      "url": "http://vibrenthealth.com/fhir/order-type",
      "valueString": "Salivary Pilot"
    },
    {
      "url": "http://vibrenthealth.com/fhir/fullfilment-status",
      "valueString": "pending"
    }
  ],
  "identifier": [
    {
      "code": "99993",
      "system": "orderId"
    }
  ],
  "itemReference": {
    "reference": "#device-1"
  },
  "quantity": {
    "value": 1
  },
  "requester": {
    "reference": "Patient/#patient-1"
  },
  "resourceType": "SupplyRequest",
  "status": "active",
  "supplier": {
    "reference": "#supplier-1"
  },
  "text": {
    "div": "....",
    "status": "generated"
  }
}


class SimpleFhirR4ReaderBasicTestCase(unittest.TestCase):

  def test_dict_lookup(self):
    fhir = SimpleFhirR4Reader({'a': 'foo'})
    self.assertEqual(fhir.get('a'), 'foo')

  def test_nested_dict_lookup(self):
    fhir = SimpleFhirR4Reader({'a': {'b': 'bar'}})
    self.assertEqual(fhir.get('a', 'b'), 'bar')

  def test_list_lookup(self):
    fhir = SimpleFhirR4Reader([
      {'a': 'foo', 'b': 0},
      {'a': 'bar', 'b': 1},
    ])
    self.assertEqual(fhir.get(dict(a='bar'), 'b'), 1)

  def test_attribute_lookup(self):
    fhir = SimpleFhirR4Reader({
      'foo': {
        'bar': {
          'baz': 123
        }
      }
    })
    self.assertEqual(fhir.foo.bar.baz, 123)


class SimpleFhirR4ReaderSupplyRequestTestCase(unittest.TestCase):

  def setUp(self):
    self.fhir = SimpleFhirR4Reader(EXAMPLE_SUPPLY_REQUEST)

  def test_lookup_patient_id(self):
    self.assertEqual(
      self.fhir.contained.get(resourceType='Patient').identifier.get(system='participantId').value,
      'P748018940'
    )
