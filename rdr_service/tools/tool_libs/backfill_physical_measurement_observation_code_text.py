from datetime import datetime

from rdr_service.dao.physical_measurements_dao import _OBSERVATION_RESOURCE_TYPE, PhysicalMeasurementsDao
from rdr_service.lib_fhir.fhirclient_1_0_6.models import observation as fhir_observation
from rdr_service.model.measurements import Measurement, PhysicalMeasurements
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase, logger

tool_cmd = 'measurement-back-fill'
tool_desc = 'back fill the valueCodeDescription for meausurements'


class MeasurementBackFill(ToolBase):
    def run(self):
        super(MeasurementBackFill, self).run()

        latest_id = -10
        with self.get_session() as session:
            found_measurements = True
            while found_measurements:
                found_measurements = False
                physical_measurements_query = session.query(
                    PhysicalMeasurements.physicalMeasurementsId,
                    PhysicalMeasurements.resource
                ).filter(
                    PhysicalMeasurements.physicalMeasurementsId > latest_id
                ).order_by(PhysicalMeasurements.physicalMeasurementsId).limit(500)

                for physical_measurement_id, measurements_json in physical_measurements_query:
                    found_measurements = True

                    for entry_json in measurements_json['entry']:
                        entry_resource = entry_json.get('resource')
                        if entry_resource and entry_resource.get('resourceType') == _OBSERVATION_RESOURCE_TYPE:
                            observation_obj = fhir_observation.Observation(entry_resource)
                            if observation_obj.valueCodeableConcept is not None:
                                value_coding = PhysicalMeasurementsDao.get_preferred_coding(
                                    observation_obj.valueCodeableConcept
                                )
                                measurement = session.query(Measurement).filter(
                                    Measurement.physicalMeasurementsId == physical_measurement_id,
                                    Measurement.valueCodeValue == value_coding.code,
                                    Measurement.valueCodeSystem == value_coding.system,
                                    Measurement.valueCodeDescription.is_(None)
                                ).all()[0]  # Should fail if nothing is found, gets the first one if there are multiple
                                measurement.valueCodeDescription = observation_obj.valueCodeableConcept.text

                    latest_id = physical_measurement_id
                    logger.info(f'got to {latest_id}')

                if found_measurements:
                    logger.info(f'got to {latest_id}')
                    logger.info(datetime.now())
                    logger.info('committing')
                    session.commit()


def run():
    return cli_run(tool_cmd, tool_desc, MeasurementBackFill)
