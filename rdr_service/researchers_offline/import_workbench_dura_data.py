from datetime import datetime, timedelta
import logging
from sqlalchemy.exc import IntegrityError
from werkzeug.exceptions import HTTPException

from rdr_service import clock, config
from rdr_service.dao.workbench_dao import WorkbenchInstitutionalDuraDao
from rdr_service.model.workbench_researcher import WorkbenchInstitutionalDura
from rdr_service.services.redcap_client import RedcapClient

USER_SYSTEM = 'https://www.pmi-ops.org/redcap'


class WorkbenchDuraImporter:
    """Functionality for importing Workbench DURA data from REDCap"""

    def __init__(self):
        self.redcap_api_key = config.getSettingJson(config.WB_INSTITUTIONAL_DURA_REDCAP_TOKEN)
        self.workbench_dura_dao = WorkbenchInstitutionalDuraDao()

    def get_records(self, project_api_token, datetime_range_begin: datetime = None):
        request_parameters = {
            'action': 'export',
            'type': 'flat',
            'csvDelimiter': '',
            'rawOrLabel': 'raw',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': True,
            'exportSurveyFields': False,
            'exportDataAccessGroups': False,
        }
        if datetime_range_begin:
            request_parameters['dateRangeBegin'] = datetime_range_begin.strftime('%Y-%m-%d %H:%M:%S')
        redcap = RedcapClient()
        return redcap.send_request(project_api_token, 'record', request_parameters)

    def create_dura_record(self, record, current_timestamp: str):
        return WorkbenchInstitutionalDura(
            created=current_timestamp,
            modified=current_timestamp,
            record_id=record.get('record_id'),
            access_method=record.get('access_method'),
            access_method_rt=record.get('access_method_rt'),
            access_team_review_status=record.get('access_team_review_status'),
            additionalrequesters=record.get('additionalrequesters'),
            additional_viewers=record.get('additional_viewers'),
            agreement_auto_renew=record.get('agreement_auto_renew'),
            agreement_end_date=record.get('agreement_end_date'),
            agreement_expired=record.get('agreement_expired'),
            agreement_renewal_info=record.get('agreement_renewal_info'),
            agreement_start_date=record.get('agreement_start_date'),
            agreementstatus_id=record.get('agreementstatus'),
            agreementtype_id=record.get('agreementtype'),
            ambiguous_reason=record.get('ambiguous_reason'),
            available_rh=record.get('available_rh'),
            available_rw=record.get('available_rw'),
            biovu_used=record.get('biovu_used'),
            carnegieclass=record.get('carnegieclass'),
            closed_reason_ct=record.get('closed_reason_ct'),
            closed_reason=record.get('closed_reason'),
            collaborator_personnel=record.get('collaborator_personnel'),
            commercial_subcategories=record.get('commercial_subcategories'),
            commercial_subcategory_other=record.get('commercial_subcategory_other'),
            consortiummember_id=record.get('consortiummember_id'),
            contact_person_email=record.get('contact_person_email'),
            contact_person_phone=record.get('contact_person_phone'),
            contact_person=record.get('contact_person'),
            contractoutcome=record.get('contractoutcome'),
            country_institution_code=record.get('country_institution'),
            country_institution_other=record.get('country_institution_other'),
            ct_requester_date=record.get('ct_requester_date'),
            ctrequester_email=record.get('ctrequester_email'),
            ctrequester=record.get('ctrequester'),
            ct_request_source=record.get('ct_request_source'),
            data_description=record.get('data_description'),
            data_exists=record.get('data_exists'),
            data_set_transferred=record.get('data_set_transferred'),
            departmental_contact=record.get('departmental_contact'),
            department=record.get('department'),
            description_of_the_data=record.get('description_of_the_data'),
            disposition_of_data=record.get('disposition_of_data'),
            document_status___1=record.get('document_status___1'),
            document_status___2=record.get('document_status___2'),
            draft_contact=record.get('draft_contact'),
            edlevel=record.get('edlevel'),
            ein_and_uei_number_complete=record.get('ein_and_uei_number_complete'),
            ein_and_uei_number_timestamp=record.get('ein_and_uei_number_timestamp'),
            ein_first_name=record.get('ein_first_name'),
            ein_institutional_role=record.get('ein_institutional_role'),
            ein_last_name=record.get('ein_last_name'),
            ein_notification=record.get('ein_notification'),
            eligibility_check1=record.get('eligibility_check1'),
            eligibility_countries_of_concern=record.get('eligibility_countries_of_concern'),
            eligibility_dura=record.get('eligibility_dura'),
            eligibility_idme=record.get('eligibility_idme'),
            eligibility_idme_persistent=record.get('eligibility_idme_persistent'),
            eligibility_institution_type=record.get('eligibility_institution_type'),
            eligibility_notifications=record.get('eligibility_notifications'),
            eligible_country_concern_persistent=record.get('eligible_country_concern_persistent'),
            email1_ct=record.get('email1_ct'),
            email1=record.get('email1'),
            email2_ct=record.get('email2_ct'),
            email2=record.get('email2'),
            email3_ct=record.get('email3_ct'),
            email3=record.get('email3'),
            email4=record.get('email4'),
            emaildomainlist_ct=record.get('emaildomainlist_ct'),
            emaildomainlist=record.get('emaildomainlist'),
            email_notification=record.get('email_notification'),
            era_commons=record.get('era_commons'),
            executed_dura_acceptable_domains_user_reporting_complete=record.get(
                'executed_dura_acceptable_domains_user_reporting_complete'),
            executeddura=record.get('executeddura'),
            external_party=record.get('external_party'),
            family_name=record.get('idua7'),
            file=record.get('file'),
            finalization_formdate=record.get('finalization_formdate'),
            first_individual=record.get('first_individual'),
            foreign_interaction=record.get('foreign_interaction'),
            forwarded_request_ct=record.get('forwarded_request_ct'),
            fqhc=record.get('fqhc'),
            given_name=record.get('idua6'),
            hbcu=record.get('hbcu'),
            how_learn_aou_website=record.get('idua11___1'),
            how_learn_describe=record.get('idua11a'),
            how_learn_friends=record.get('idua11___6'),
            how_learn_journal_news=record.get('idua11___4'),
            how_learn_other=record.get('idua11___7'),
            how_learn_other_website=record.get('idua11___2'),
            how_learn_presentation_demonstration=record.get('idua11___5'),
            how_learn_social_media=record.get('idua11___3'),
            hsi=record.get('hsi'),
            human_subjects_originate=record.get('human_subjects_originate'),
            idua_internal_tracking_complete=record.get('idua_internal_tracking_complete'),
            individual_name=record.get('individual_name'),
            information_outside_us=record.get('information_outside_us'),
            informed_consent=record.get('informed_consent'),
            inst_email=record.get('idua9'),
            institutional_dua_request_for_the_all_of_us_resear_comple=record.get(
                'institutional_dua_request_for_the_all_of_us_resear_comple'),
            institutioncategory=record.get('institutioncategory'),
            institution_website=record.get('institution_website'),
            inst_name=record.get('inst_name'),
            intro_letter=record.get('intro_letter'),
            location_external_party=record.get('location_external_party'),
            medical_diagnostics_institution=record.get('medical_diagnostics_institution'),
            method_data_transfer=record.get('method_data_transfer'),
            multi_national_internal=record.get('multi_national_internal'),
            multi_national_request_form=record.get('multi_national_request_form'),
            nih_funding=record.get('nih_funding'),
            ocmcontact_ct=record.get('ocmcontact_ct'),
            ocm_draft_template=record.get('ocm_draft_template'),
            ocmsubmission_ct=record.get('ocmsubmission_ct'),
            ocmsubmissiondate_ct=record.get('ocmsubmissiondate_ct'),
            opeid=record.get('opeid'),
            original_contact_email=record.get('original_contact_email'),
            original_contact_name=record.get('original_contact_name'),
            original_peerconfirmation=record.get('original_peerconfirmation'),
            originalrequestinfo=record.get('originalrequestinfo'),
            originalrequest=record.get('originalrequest'),
            other_materials=record.get('other_materials'),
            other_party_address=record.get('other_party_address'),
            other_party_email=record.get('other_party_email'),
            other_party_name=record.get('other_party_name'),
            other_party_phone=record.get('other_party_phone'),
            other_reason_ct=record.get('other_reason_ct'),
            other_reason=record.get('other_reason'),
            partner_other=record.get('additional_information_notes'),
            peerconfirmationdate=record.get('peerconfirmationdate'),
            peerconfirmation=record.get('peerconfirmation'),
            peer_integration_complete=record.get('peer_integration_complete'),
            peersubmissionstatusdate=record.get('peersubmissionstatusdate'),
            peersubmissionstatus=record.get('peersubmissionstatus'),
            phone_number=record.get('phone_number'),
            preapproval_discussion_date=record.get('preapproval_discussion_date'),
            pre_approval_notes=record.get('pre_approval_notes'),
            preapproval_projectcreation_date=record.get('preapproval_projectcreation_date'),
            preapprovalregistration2=record.get('preapprovalregistration2'),
            preapprovalregistration=record.get('preapprovalregistration'),
            preapproval_requestform_date=record.get('preapproval_requestform_date'),
            preapproval_reviewer_email_2=record.get('preapproval_reviewer_email_2'),
            preapproval_reviewer_email_3=record.get('preapproval_reviewer_email_3'),
            preapproval_reviewer_email_4=record.get('preapproval_reviewer_email_4'),
            preapproval_reviewer_email=record.get('preapproval_reviewer_email'),
            preapproval_reviewer=record.get('preapproval_reviewer'),
            preapproval_tier___1=record.get('preapproval_tier___1'),
            preapproval_tier___2=record.get('preapproval_tier___2'),
            pre_approval_type=record.get('pre_approval_type'),
            presentation_date=record.get('presentation_date'),
            presentation_source_aahd=record.get('presentation_source___1'),
            presentation_source_ahc=record.get('presentation_source___11'),
            presentation_source_baylor_college_of_medicine=record.get('presentation_source___2'),
            presentation_source_ctsa_pacer_community_network=record.get('presentation_source___13'),
            presentation_source_dref=record.get('presentation_source___3'),
            presentation_source_fiftyforward=record.get('presentation_source___8'),
            presentation_source_hunter_college_cuny=record.get('presentation_source___12'),
            presentation_source_nahh=record.get('presentation_source___4'),
            presentation_source_nnlm=record.get('presentation_source___5'),
            presentation_source_other=record.get('presentation_source___10'),
            presentation_source_pridenet=record.get('presentation_source___9'),
            presentation_source_pyxis_partners=record.get('presentation_source___6'),
            presentation_source_rti_international=record.get('presentation_source___7'),
            presentation_text=record.get('presentation_source_other'),
            principal_investigator=record.get('principal_investigator'),
            program_partners_aahd=record.get('program_partners___1'),
            program_partners_ahc=record.get('program_partners___11'),
            program_partners_aises=record.get('program_partners___21'),
            program_partners_arizona=record.get('program_partners___23'),
            program_partners_ctsa_pacer_community_network=record.get('program_partners___13'),
            program_partners_drc=record.get('program_partners___16'),
            program_partners_dref=record.get('program_partners___3'),
            program_partners_evenings_with_grp=record.get('program_partners___2'),
            program_partners_fiftyforward=record.get('program_partners___8'),
            program_partners_ignite=record.get('program_partners___12'),
            program_partners_marshfield=record.get('program_partners___22'),
            program_partners_nahh=record.get('program_partners___4'),
            program_partners_nln=record.get('program_partners___24'),
            program_partners_nbc=record.get('program_partners___17'),
            program_partners_nnlm=record.get('program_partners___5'),
            program_partners_none_of_above=record.get('program_partners___14'),
            program_partners_other=record.get('program_partners___10'),
            program_partners_pridenet=record.get('program_partners___9'),
            program_partners_program_staff=record.get('program_partners___15'),
            program_partners_pyxis_partners=record.get('program_partners___6'),
            program_partners_rti_international=record.get('program_partners___7'),
            program_partners_scripps=record.get('program_partners___18'),
            program_partners_utah=record.get('program_partners___19'),
            project_title_peer=record.get('project_title_peer'),
            provided_ein_number=record.get('provided_ein_number'),
            provided_uei_number=record.get('provided_uei_number'),
            registrationformsame=record.get('registrationformsame'),
            related_to_existing_contract=record.get('related_to_existing_contract'),
            reportingemail=record.get('reportingemail'),
            reportingemail_2=record.get('reportingemail_2'),
            reportingemail_3=record.get('reportingemail_3'),
            reportingemail_4=record.get('reportingemail_4'),
            reportingname=record.get('reportingname'),
            requestcompletion_ct=record.get('requestcompletion_ct'),
            requestcompletion=record.get('requestcompletion'),
            request_ct=record.get('request_ct'),
            request_type=record.get('request_type'),
            reviewconfirmationdate=record.get('reviewconfirmationdate'),
            reviewconfirmation=record.get('reviewconfirmation'),
            reviewrequest_ct=record.get('reviewrequest_ct'),
            reviewrequest_ct_2=record.get('reviewrequest_ct_2'),
            reviewrequestdate_ct=record.get('reviewrequestdate_ct'),
            reviewrequestdate_ct_2=record.get('reviewrequestdate_ct_2'),
            riderstatus_ct_id=record.get('riderstatus_ct'),
            role_for_dura=record.get('idua8'),
            ruralhealth=record.get('ruralhealth'),
            separate_domains_ct=record.get('separate_domains_ct'),
            signedriderdate_ct=record.get('signedriderdate_ct'),
            signing_family_name=record.get('idua2'),
            signing_given_name=record.get('idua1'),
            signing_inst_email=record.get('idua3'),
            signing_official_contact_date=record.get(''),
            signing_phone_number=record.get('idua4'),
            signing_role=record.get('idua5'),
            source_of_data=record.get('source_of_data'),
            state_initials_code=record.get('state_initials'),
            submit_peer=record.get('submit_peer'),
            tier_access___1=record.get('tier_access___1'),
            tier_access___2=record.get('tier_access___2'),
            typeoforganization_academic=record.get('typeoforganization___1'),
            typeoforganization_for_profit=record.get('typeoforganization___4'),
            typeoforganization_government=record.get('typeoforganization___6'),
            typeoforganization_hci=record.get('typeoforganization___2'),
            typeoforganization_non_profit=record.get('typeoforganization___3'),
            typeoforganization_other_desc=record.get('typeoforganization_other'),
            typeoforganization_other=record.get('typeoforganization___5'),
            type=record.get('type'),
            us_ein_number=record.get('us_ein_number'),
            website_url=record.get('website_url'),
            whitelisted_emails_ct=record.get('whitelisted_emails_ct'),
            whitelisted_emails_rt=record.get('whitelisted_emails_rt'),
            whose_data_shared=record.get('whose_data_shared'),
            zip=record.get('zip')
        )

    def import_reports(self, since: datetime = None):
        """
        :param since: DateTime to use as start of date range request. Will import all reports created or modified
            after the given date. Defaults to the start (midnight) of yesterday.
        """
        if since is None or since == "":
            now_yesterday = datetime.now() - timedelta(days=1)
            since = datetime(now_yesterday.year, now_yesterday.month, now_yesterday.day)

        records = self.get_records(self.redcap_api_key, since)

        with self.workbench_dura_dao.session() as session:
            for record in records:
                try:
                    now = clock.CLOCK.now()
                    record_id = record['record_id']
                    dura_record = self.create_dura_record(self.workbench_dura_dao.transform_rows(record), now)
                    self.workbench_dura_dao.insert_with_session(session, dura_record)
                except IntegrityError:
                    session.rollback()
                    logging.error(f'DURA record_id: {record_id} import encountered a database error', exc_info=True)
                except (HTTPException, KeyError, ValueError):
                    logging.error(f'DURA record_id: {record_id}  import encountered an error', exc_info=True)

        logging.info('Workbench Institutional DURA import complete')
