import logging

from rdr_service import config
from rdr_service.dao.genomics_dao import GenomicCVLResultPastDueDao
from rdr_service.services.email_service import Email, EmailService
from rdr_service.genomic_enums import GenomicJob


class GenomicCVLReconcile:
    def __init__(self, reconcile_type):
        self.reconcile_type = reconcile_type
        self.result_dao = GenomicCVLResultPastDueDao()

    def __get_reconcile_method(self):
        return {
            GenomicJob.RECONCILE_CVL_HDR_RESULTS: self.set_samples_for_reconcile,
            GenomicJob.RECONCILE_CVL_PGX_RESULTS: self.set_samples_for_reconcile,
            GenomicJob.RECONCILE_CVL_ALERTS: self.send_reconcile_alerts,
            GenomicJob.RECONCILE_CVL_RESOLVE: self.resolve_reconciled_samples
        }[self.reconcile_type]

    def run_reconcile(self):
        run_method = self.__get_reconcile_method()
        run_method()

    def set_samples_for_reconcile(self):
        ...

    def send_reconcile_alerts(self):

        def _build_message(*, samples, results_types):
            message = 'The following sample IDs are past due:\n'
            for results_type in results_types:
                message += f'{results_type}\n'
                current_samples = [obj for obj in samples if obj.results_type.lower() == results_type.lower()]
                for sample in current_samples:
                    message += f'{sample.sample_id}\n'
            return message

        email_config = config.getSettingJson(config.GENOMIC_CVL_RECONCILE_EMAILS, {})
        if not email_config or \
                (email_config and not email_config.get('send_emails')):
            return

        reconcile_alerts = self.result_dao.get_samples_for_notifications()
        if not reconcile_alerts:
            logging.info(f'There are no CVL reconciled alerts to send')
            return

        logging.info(f'{len(reconcile_alerts)} alerts found. Sending...')

        cvl_sites = {obj.cvl_site_id for obj in reconcile_alerts}
        recipients, cc_recipients = email_config.get('recipients'), email_config.get('cc_recipients')

        for site in cvl_sites:
            site_samples = list(filter(lambda x: x.cvl_site_id == site, reconcile_alerts))
            message = _build_message(
                samples=site_samples,
                results_types={obj.results_type for obj in site_samples}
            )
            email_message = Email(
                recipients=recipients.get(site),
                cc_recipients=cc_recipients,
                subject="GHR3 Weekly Past Due Samples Report",
                plain_text_content=message
            )
            EmailService.send_email(email_message)

            logging.info(f'{len(reconcile_alerts)} alerts sent for {site.upper()}')

            self.result_dao.batch_update_samples(
                update_type=self.reconcile_type,
                _ids=[obj.id for obj in site_samples]
            )

    def resolve_reconciled_samples(self):
        samples_to_resolve = self.result_dao.get_samples_to_resolve()
        if not samples_to_resolve:
            logging.info(f'There are no CVL reconciled samples to resolve')
            return

        logging.info(f'Resolving {len(samples_to_resolve)} past due samples')
        self.result_dao.batch_update_samples(
            update_type=self.reconcile_type,
            _ids=[obj.id for obj in samples_to_resolve]
        )

