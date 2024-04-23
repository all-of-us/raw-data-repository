#
# Physical Measurements Generator
#
import json
import logging
import os.path
import random
import string

from werkzeug.exceptions import BadRequest

from rdr_service import clock
from rdr_service.concepts import Concept
from rdr_service.data_gen.generators.base_gen import BaseGen
from rdr_service.data_gen.generators.hpo import HPOGen

_logger = logging.getLogger("rdr_logger")


class PhysicalMeasurementsGen(BaseGen):
    """
  Fake physical measurements data generator
  ref: fake_participant_generator.py:492
  """

    _participant_id = None
    _site = None

    _measurement_specs = None
    _qualifier_map = None

    def __init__(self, load_data=False):
        """
    Initialize physical measurements generator.
    """
        super(PhysicalMeasurementsGen, self).__init__(load_data=load_data)

        qualifier_concepts = set()
        file_path = os.path.join(os.path.dirname(__file__), '../../app_data/measurement_specs.json')
        with open(file_path) as f:
            measurement_specs = json.load(f)
        for measurement in measurement_specs:
            for qualifier in measurement["qualifiers"]:
                qualifier_concepts.add(Concept(qualifier["system"], qualifier["code"]))
        measurement_map = {
            Concept(measurement["code"]["system"], measurement["code"]["code"]): measurement
            for measurement in measurement_specs
        }
        self._measurement_specs = [
            measurement
            for measurement in measurement_specs
            if Concept(measurement["code"]["system"], measurement["code"]["code"]) not in qualifier_concepts
        ]
        self._qualifier_map = {
            qualifier_concept: measurement_map[qualifier_concept] for qualifier_concept in qualifier_concepts
        }

    def new(self, participant_id, site=None):
        """
    Return a new PhysicalMeasurementsGen object
    :param participant_id: participant id
    :param site: HPOSiteGen object
    :return: json string
    """
        clone = self.__class__()
        clone._participant_id = participant_id
        if site:
            clone._site = site
        else:
            clone._site = HPOGen().get_random_site()

        return clone

    def make_fhir_document(self, force_measurement=False):
        """
    build a FHIR bundle with physical measurement resource objects.
    :return: FHIR bundle object
    """
        doc = self._make_physical_measurements(force_measurement)
        return doc

    def _make_full_url(self, concept):
        return "urn:example:%s" % concept["code"]

    def _make_base_measurement_resource(self, measurement, mean, measurement_count):

        system = "https://terminology.pmi-ops.org/CodeSystem/physical-measurements"

        resource = {
            "code": {
                "coding": [
                    {
                        "code": measurement["code"]["code"],
                        "display": "measurement",
                        "system": measurement["code"]["system"],
                    }
                ],
                "text": "text",
            }
        }
        pmi_code = measurement.get("pmiCode")
        if pmi_code:
            pmi_code_json = {"code": pmi_code, "display": "measurement", "system": system}
            resource["code"]["coding"].append(pmi_code_json)
        else:
            pmi_code_prefix = measurement.get("pmiCodePrefix")
            if pmi_code_prefix:
                if mean:
                    code_suffix = "mean"
                else:
                    code_suffix = str(measurement_count)
                pmi_code_json = {"code": pmi_code_prefix + code_suffix, "display": "measurement", "system": system}
                resource["code"]["coding"].append(pmi_code_json)
        return resource

    def _make_measurement_resource(self, measurement, qualifier_set, previous_resource, measurement_count):
        resource = self._make_base_measurement_resource(measurement, False, measurement_count)
        if "decimal" in measurement["types"]:
            previous_value = None
            previous_unit = None
            if previous_resource:
                previous_value_quantity = previous_resource["valueQuantity"]
                previous_unit = previous_value_quantity["unit"]
                previous_value = previous_value_quantity["value"]

            # Arguably min and max should vary with units, but in our data there's only one unit
            # so we won't bother for now.
            min_value = measurement["min"]
            max_value = measurement["max"]
            if previous_unit:
                unit = previous_unit
            else:
                unit = random.choice(measurement["units"])
            if min_value == int(min_value) and max_value == int(max_value):
                # Assume int min and max means an integer
                if previous_value:
                    # Find a value that is up to some percent of the possible range higher or lower.
                    delta = int(0.01 * (max_value - min_value))
                    value = random.randint(previous_value - delta, previous_value + delta)
                else:
                    value = random.randint(min_value, max_value)
            else:
                # Otherwise assume a floating point number with one digit after the decimal place
                if previous_value:
                    delta = 0.01 * (max_value - min_value)
                    value = round(random.uniform(previous_value - delta, previous_value + delta), 1)
                else:
                    value = round(random.uniform(min_value, max_value), 1)
            resource["valueQuantity"] = {
                "code": unit,
                "system": "http://unitsofmeasure.org",
                "unit": unit,
                "value": value,
            }
        if "string" in measurement["types"]:
            resource["valueString"] = "".join([random.choice(string.ascii_lowercase) for _ in range(20)])
        if measurement["valueCodes"]:
            value_code = random.choice(measurement["valueCodes"])
            resource["valueCodeableConcept"] = {
                "coding": [{"system": value_code["system"], "code": value_code["code"], "display": "value"}],
                "text": "value text",
            }
        if measurement["qualifiers"]:
            if random.random() <= 0.2:
                qualifiers = random.sample(
                    measurement["qualifiers"], random.randint(1, len(measurement["qualifiers"]))
                )
                qualifier_set.update(Concept(qualifier["system"], qualifier["code"]) for qualifier in qualifiers)
                resource["related"] = [
                    {"type": "qualified-by", "target": {"reference": self._make_full_url(qualifier)}}
                    for qualifier in qualifiers
                ]
        return resource

    def _get_measurement_values(self, measurement_resources):
        return [resource["valueQuantity"]["value"] for resource in measurement_resources]

    def _get_related(self, measurement_resources):
        related_list = []
        related_urls = set()
        for measurement_resource in measurement_resources:
            for related in measurement_resource.get("related", []):
                # Make sure we don't repeat references to the same qualifier.
                url = related["target"]["reference"]
                if url not in related_urls:
                    related_list.append(related)
                    related_urls.add(url)
        return related_list

    def _mean(self, values):
        return round(sum(values) / len(values), 1)

    def _find_closest_two_mean(self, measurement_values):
        # Find the mean of the two closest values; or if they are equally distant from each other,
        # find the mean of all three.
        delta_0_1 = abs(measurement_values[0] - measurement_values[1])
        delta_0_2 = abs(measurement_values[0] - measurement_values[2])
        delta_1_2 = abs(measurement_values[1] - measurement_values[2])
        if delta_0_1 < delta_0_2:
            if delta_0_1 < delta_1_2:
                return self._mean(measurement_values[0:2])
            else:
                return self._mean(measurement_values[1:])
        elif delta_0_1 == delta_0_2:
            return self._mean(measurement_values)
        else:
            if delta_0_2 < delta_1_2:
                return self._mean([measurement_values[0], measurement_values[2]])
            else:
                return self._mean(measurement_values[1:])

    def _calculate_last_two_mean(self, measurement_values):
        return self._mean(measurement_values[1:])

    def _find_components_by_coding(self, measurement_resources, submeasurement_coding):
        components = []
        for resource in measurement_resources:
            for subcomponent in resource.get("component", []):
                for coding in subcomponent["code"]["coding"]:
                    if (
                        coding["system"] == submeasurement_coding["system"]
                        and coding["code"] == submeasurement_coding["code"]
                    ):
                        components.append(subcomponent)
        return components

    def _make_mean_resource(self, mean_type, measurement, measurement_resources):
        resource = self._make_base_measurement_resource(measurement, True, None)
        if measurement_resources[0].get("valueQuantity"):
            measurement_values = self._get_measurement_values(measurement_resources)
            if len(measurement_values) < 3:
                raise BadRequest("Bad measurement resources: %s" % measurement_resources)
            elif mean_type == "closestTwo":
                value = self._find_closest_two_mean(measurement_values)
            elif mean_type == "lastTwo":
                value = self._calculate_last_two_mean(measurement_values)
            else:
                raise BadRequest("Unknown meanType: %s" % mean_type)
            unit = measurement_resources[0]["valueQuantity"]["unit"]
            resource["valueQuantity"] = {
                "code": unit,
                "system": "http://unitsofmeasure.org",
                "unit": unit,
                "value": value,
            }
        # Include all qualifiers on the measurements being averaged.
        related = self._get_related(measurement_resources)
        if related:
            resource["related"] = related
        return resource

    def _make_mean_entry(self, measurement, time_str, participant_id, measurement_resources):
        mean_type = measurement.get("meanType")
        resource = self._make_mean_resource(mean_type, measurement, measurement_resources)
        self._populate_measurement_entry(resource, time_str, participant_id)
        body_site = measurement_resources[0].get("bodySite")
        if body_site:
            resource["bodySite"] = body_site
        if measurement["submeasurements"]:
            components = []
            for submeasurement in measurement["submeasurements"]:
                measurement_components = self._find_components_by_coding(measurement_resources, submeasurement["code"])
                if measurement_components:
                    components.append(self._make_mean_resource(mean_type, submeasurement, measurement_components))
            if components:
                resource["component"] = components
        return {"fullUrl": self._make_full_url(measurement["code"]), "resource": resource}

    def _populate_measurement_entry(self, resource, time_str, participant_id):
        resource["effectiveDateTime"] = time_str
        resource["resourceType"] = "Observation"
        resource["status"] = "final"
        resource["subject"] = {"reference": "Patient/%s" % participant_id}

    def _make_measurement_entry(
        self, measurement, time_str, participant_id, qualifier_set, previous_resource=None, measurement_count=None
    ):
        resource = self._make_measurement_resource(measurement, qualifier_set, previous_resource, measurement_count)
        self._populate_measurement_entry(resource, time_str, participant_id)
        if measurement["bodySites"]:
            if previous_resource:
                resource["bodySite"] = previous_resource["bodySite"]
            else:
                body_site = random.choice(measurement["bodySites"])
                resource["bodySite"] = {
                    "coding": [{"code": body_site["code"], "display": "body site", "system": body_site["system"]}],
                    "text": "text",
                }
        if measurement["submeasurements"]:
            components = []
            for submeasurement in measurement["submeasurements"]:
                previous_component = None
                if previous_resource:
                    subcomponents = self._find_components_by_coding([previous_resource], submeasurement["code"])
                    if subcomponents:
                        previous_component = subcomponents[0]
                components.append(
                    self._make_measurement_resource(
                        submeasurement, qualifier_set, previous_component, measurement_count
                    )
                )
            if components:
                resource["component"] = components
        return {"fullUrl": self._make_full_url(measurement["code"]), "resource": resource}

    def _make_author(self, username, authoring_step):

        reference = {
            "reference": "{0}{1}".format("Practitioner/", username),
            "extension": {
                "url": "http://terminology.pmi-ops.org/StructureDefinition/authoring-step",
                "valueCode": authoring_step,
            },
        }
        return reference

    def _make_physical_measurements(self, force_measurement=False):
        """
    Create a FHIR bundle resource and populate it with measurment resources.
    :param force_measurement: if True, do not randomly skip measurements.
    :return: bundle FHIR document
    """

        created_ext = "http://terminology.pmi-ops.org/StructureDefinition/authored-location"
        finalized_ext = "http://terminology.pmi-ops.org/StructureDefinition/finalized-location"

        now = clock.CLOCK.now().isoformat()

        entries = [
            {
                "fullUrl": "urn:example:report",
                "resource": {
                    "author": [
                        self._make_author("creator@pmi-ops.org", "created"),
                        self._make_author("finalizer@pmi-ops.org", "finalized"),
                    ],
                    "extension": [
                        {"url": created_ext, "valueReference": "{0}{1}".format("Location/", self._site.name)},
                        {"url": finalized_ext, "valueReference": "{0}{1}".format("Location/", self._site.name)},
                    ],
                    "date": now,
                    "resourceType": "Composition",
                    "section": [{"entry": [{"reference": "urn:example:blood-pressure-1"}]}],
                    "status": "final",
                    "subject": {"reference": "Patient/%s" % self._participant_id},
                    "title": "PMI Intake Evaluation",
                    "type": {
                        "coding": [
                            {
                                "code": "intake-exam-v0.0.1",
                                "display": "PMI Intake Evaluation v0.0.1",
                                "system": "http://terminology.pmi-ops.org/CodeSystem/document-type",
                            }
                        ],
                        "text": "PMI Intake Evaluation v0.0.1",
                    },
                },
            }
        ]

        qualifier_set = set()
        for measurement in self._measurement_specs:
            if random.random() <= 0.1 and not force_measurement:
                continue
            num_measurements = 1
            num_measurements_str = measurement.get("numMeasurements")
            if num_measurements_str is not None:
                num_measurements = int(num_measurements_str)
            measurement_resources = []
            first_entry = self._make_measurement_entry(measurement, now, self._participant_id, qualifier_set, None, 1)
            entries.append(first_entry)
            if num_measurements > 1:
                measurement_resources.append(first_entry["resource"])
                for i in range(1, num_measurements):
                    entry = self._make_measurement_entry(
                        measurement, now, self._participant_id, qualifier_set, first_entry["resource"], i + 1
                    )
                    measurement_resources.append(entry["resource"])
                    entries.append(entry)
                entries.append(self._make_mean_entry(measurement, now, self._participant_id, measurement_resources))

        # Add any qualifiers that were specified for other measurements.
        for qualifier in qualifier_set:
            qualifier_measurement = self._qualifier_map[qualifier]
            entry = self._make_measurement_entry(qualifier_measurement, now, self._participant_id, qualifier_set)
            entries.append(entry)
        return {"resourceType": "Bundle", "type": "document", "entry": entries}
