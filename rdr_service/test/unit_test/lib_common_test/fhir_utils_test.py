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
      "url": "http://vibrenthealth.com/fhir/fulfillment-status",
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

  def test_list_item_at_index(self):
    fhir = SimpleFhirR4Reader({
      'a': [2, 4, 6, 8]
    })
    self.assertEqual(fhir.get('a', 1), 4)

  def test_dict_key_lookup(self):
    fhir = SimpleFhirR4Reader({'a': 'foo'})
    self.assertEqual(fhir['a'], 'foo')

  def test_list_key_lookup(self):
    fhir = SimpleFhirR4Reader(['foo', 'bar', 'baz'])
    self.assertEqual(fhir[1], 'bar')
    self.assertEqual(fhir[-1], 'baz')

  def test_list_key_range(self):
    fhir = SimpleFhirR4Reader(['foo', 'bar', 'baz'])
    self.assertEqual(fhir[1:], ['bar', 'baz'])
    self.assertEqual(fhir[:-1], ['foo', 'bar'])

  def test_list_filter_function_key(self):
    fhir = SimpleFhirR4Reader(list(range(10)))
    odds_only_filter = lambda x: x % 2
    self.assertEqual(list(fhir.get(odds_only_filter)), [1,3,5,7,9])

  def test_dict_filter_function_key(self):
    fhir = SimpleFhirR4Reader({'a': 0, 'b': 1, 'c': 2, 'd': 3})
    odd_values_only_filter = lambda x: x[1] % 2
    self.assertEqual(list(fhir.get(odd_values_only_filter)), [('b', 1), ('d', 3)])

  def test_dict_exceptions(self):
    fhir = SimpleFhirR4Reader({'a': 'foo'})
    with self.assertRaises(AttributeError):
      fhir.b
    with self.assertRaises(KeyError):
      fhir['b']
    with self.assertRaises(KeyError):
      fhir[0]
    with self.assertRaises(KeyError):
      fhir[-1]
    with self.assertRaises(TypeError):
      fhir[1:]

  def test_list_exceptions(self):
    fhir = SimpleFhirR4Reader([0,1])
    with self.assertRaises(AttributeError):
      fhir.foo
    with self.assertRaises(IndexError):
      fhir[2]
    with self.assertRaises(IndexError):
      fhir['b']

  def test_looks_up_top_level_references(self):
    fhir = SimpleFhirR4Reader({
      'contained': [
        {'id': 'some-foo', 'value': 123}
      ],
      'foo': {
        'reference': '#some-foo'
      }
    })
    self.assertEqual(fhir.foo.value, 123)

  def test_looks_up_deep_references(self):
    fhir = SimpleFhirR4Reader({
      'contained': [
        {'id': 'some-foo', 'value': 123}
      ],
      'bar': {
        'foo': {
          'reference': '#some-foo'
        }
      }
    })
    self.assertEqual(fhir.bar.foo.value, 123)


class SimpleFhirR4ReaderSupplyRequestTestCase(unittest.TestCase):

  def setUp(self):
    self.fhir = SimpleFhirR4Reader(EXAMPLE_SUPPLY_REQUEST)

  def test_lookup_patient_id(self):
    self.assertEqual(
      self.fhir.contained.get(resourceType='Patient').identifier.get(system='participantId').value,
      'P748018940'
    )

  def test_lookup_reference(self):
    self.assertEqual(self.fhir.deliverTo.id, 'patient-1')
    self.assertEqual(self.fhir.itemReference.id, 'device-1')
