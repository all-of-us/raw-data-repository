
from rdr_service.dao.bigquery_sync_dao import BigQueryGenerator, BigQuerySyncDao
from rdr_service.dao.bq_participant_summary_dao import BQParticipantSummaryGenerator
from rdr_service.model.bq_base import BQRecord
from rdr_service.model.bq_participant_summary import BQStreetAddressTypeEnum
from rdr_service.model.bq_pdr_participant_summary import BQPDRParticipantSummarySchema
from rdr_service.participant_enums import OrderStatus


class BQPDRParticipantSummaryGenerator(BigQueryGenerator):
    """
    Generate a PDR Participant Summary BQRecord object.
    This is a Participant Summary record without PII.
    Note: Logic to create a PDR Participant Summary is in bq_participant_summary_dao:rebuild_bq_participant.
    """
    ro_dao = None
    rural_zipcodes = None

    def make_bqrecord(self, p_id, convert_to_enum=False, ps_bqr=None):
        """
        Build a Participant Summary BQRecord object for the given participant id.
        :param p_id: participant id
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :param ps_bqr: A BQParticipantSummary BQRecord object.
        :return: BQRecord object
        """
        if not self.ro_dao:
            self.ro_dao = BigQuerySyncDao(backup=True)

        # Since we are primarily a subset of the Participant Summary, call the full Participant Summary generator
        # and take what we need from it.
        if not ps_bqr:
            ps_bqr = BQParticipantSummaryGenerator().make_bqrecord(p_id, convert_to_enum=convert_to_enum)
        bqr = BQRecord(schema=BQPDRParticipantSummarySchema, data=ps_bqr.to_dict(), convert_to_enum=convert_to_enum)

        summary = bqr.to_dict()
        data = {}
        # Populate BQPDRBiospecimenSchema if there are biobank orders.
        # TODO:  Deprecate this BQPDRBiospecimenSchema and transition PDR users to utilize BQBiobankOrderSchema data
        if hasattr(ps_bqr, 'biobank_orders'):
            data['biospec'] = list()
            for order in ps_bqr.biobank_orders:
                # Count the number of DNA and Baseline tests in this order.
                dna_tests = 0
                dna_tests_confirmed = 0
                baseline_tests = 0
                baseline_tests_confirmed = 0
                for test in order.get('bbo_samples', list()):
                    if test['bbs_dna_test'] == 1:
                        dna_tests += 1
                        if test['bbs_confirmed']:
                            dna_tests_confirmed += 1
                    # PDR-134:  Add baseline tests counts
                    if test['bbs_baseline_test'] == 1:
                        baseline_tests += 1
                        if test['bbs_confirmed']:
                            baseline_tests_confirmed += 1

                # PDR-243:  Use an OrderStatus (not the order's BiobankOrderStatus value)
                # to align with the RDR biospecimen fields in participant summary.  TODO:  the BQPDRBiospecimenSchema
                # will be deprecated once PDR users migrate to using the BQBiobankOrderSchema data for queries
                data['biospec'].append({
                    'biosp_status': str(OrderStatus(order.get('bbo_finalized_status', OrderStatus.UNSET))),
                    'biosp_status_id': int(OrderStatus(order.get('bbo_finalized_status_id', OrderStatus.UNSET))),
                    'biosp_order_time': order.get('bbo_created', None),
                    'biosp_isolate_dna': dna_tests,
                    'biosp_isolate_dna_confirmed': dna_tests_confirmed,
                    'biosp_baseline_tests': baseline_tests,
                    'biosp_baseline_tests_confirmed': baseline_tests_confirmed,
                })

        if hasattr(ps_bqr, 'addresses') and isinstance(ps_bqr.addresses, list):
            for addr in ps_bqr.addresses:
                if addr['addr_type_id'] == BQStreetAddressTypeEnum.RESIDENCE.value:
                    data['addr_state'] = addr['addr_state']
                    data['addr_zip'] = addr['addr_zip'][:3] if addr['addr_zip'] else None

        summary = self._merge_schema_dicts(summary, data)

        # Calculate contact information
        summary = self._merge_schema_dicts(summary, self._set_contact_flags(ps_bqr))

        bqr = BQRecord(schema=BQPDRParticipantSummarySchema, data=summary, convert_to_enum=convert_to_enum)
        return bqr

    def _set_contact_flags(self, ps_bqr):
        """
        Determine if an email or phone number is available.
        :param ps_bqr: A BQParticipantSummary BQRecord object
        :return: dict
        """
        data = {
            'email_available': 1 if ps_bqr.email else 0,
            'phone_number_available': 1 if (getattr(ps_bqr, 'login_phone_number', None) or
                                            getattr(ps_bqr, 'phone_number', None)) else 0
        }
        return data
