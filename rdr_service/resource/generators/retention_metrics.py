#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging

from sqlalchemy.sql import text
from werkzeug.exceptions import NotFound

from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.resource import generators, schemas
from rdr_service.resource.constants import RetentionStatusEnum, RetentionTypeEnum


class RetentionEligibleMetricGenerator(generators.BaseGenerator):
    """
    Generate a Retention Metric resource object
    """
    ro_dao = None

    def make_resource(self, p_id, backup=False):
        """
        Build a resource object from the given primary key id.
        :param p_id: Participant ID.
        :param backup: if True, get from backup database instead of Primary.
        :return: resource object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(
                    text('select * from retention_eligible_metrics where participant_id = :pid'), {'pid': p_id}).first()
            data = self.ro_dao.to_dict(row)

            if not data:
                msg = f'Participant P{p_id} not found in retention_eligible_metrics table.'
                logging.error(msg)
                raise NotFound(msg)

            # Populate Enum fields. Note: When Enums have a possible zero value, explicitly check for None.
            if data['retention_eligible_status'] is not None:
                data['retention_eligible_status'] = data['retention_eligible_status_id'] = \
                       RetentionStatusEnum(data['retention_eligible_status'])
            if data['retention_type'] is not None:
                data['retention_type'] = data['retention_type_id'] = RetentionTypeEnum(data['retention_type'])

            return generators.ResourceRecordSet(schemas.RetentionMetricSchema, data)
