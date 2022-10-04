import logging

from rdr_service import config, clock
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

        def _process_sample_data(*, dao, sample):
            sample = sample._asdict()
            sample['cvlSiteId'] = 'co' if sample.get('cvlSiteId') == 'bi' else sample.get('cvlSiteId')
            past_due_sample_obj = dao.get_model_obj_from_items(
                {self.result_dao.camel_to_snake(k): v for k, v in sample.items()}.items()
            )
            past_due_sample_obj.created = clock.CLOCK.now()
            past_due_sample_obj.modified = clock.CLOCK.now()
            return past_due_sample_obj

        past_due_samples = self.result_dao.get_past_due_samples(
            result_type=self.reconcile_type
        )
        if not past_due_samples:
            logging.info('There are no CVL past due samples to flag')
            return

        logging.info(f'Flagging/Storing {len(past_due_samples)} past due samples')
        batch_size, item_count, batch = 100, 0, []

        for sample in past_due_samples:
            batch.append(
                _process_sample_data(dao=self.result_dao, sample=sample)
            )
            item_count += 1

            if item_count == batch_size:
                with self.result_dao.session() as session:
                    session.bulk_save_objects(batch)
                item_count = 0
                batch.clear()

        if item_count:
            with self.result_dao.session() as session:
                session.bulk_save_objects(batch)

    def send_reconcile_alerts(self):

        def _build_message(*, samples, results_types):
            returned_message = 'The following sample IDs are past due:\n'
            for results_type in results_types:
                returned_message += f'{results_type}\n'
                current_samples = [obj for obj in samples if obj.results_type.lower() == results_type.lower()]
                for sample in current_samples:
                    returned_message += f'{sample.sample_id}\n'
            return returned_message

        email_config = config.getSettingJson(config.GENOMIC_CVL_RECONCILE_EMAILS, {})
        if not email_config or \
                (email_config and not email_config.get('send_emails')):
            return

        reconcile_alerts = self.result_dao.get_samples_for_notifications()
        if not reconcile_alerts:
            logging.info('There are no CVL reconciled alerts to send')
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

            logging.info(f'{len(site_samples)} alerts sent for {site.upper()}')

            self.result_dao.batch_update_samples(
                update_type=self.reconcile_type,
                _ids=[obj.id for obj in site_samples]
            )

    def resolve_reconciled_samples(self):
        samples_to_resolve = self.result_dao.get_samples_to_resolve()
        if not samples_to_resolve:
            logging.info('There are no CVL reconciled samples to resolve')
            return

        logging.info(f'Resolving {len(samples_to_resolve)} past due samples')
        self.result_dao.batch_update_samples(
            update_type=self.reconcile_type,
            _ids=[obj.id for obj in samples_to_resolve]
        )
