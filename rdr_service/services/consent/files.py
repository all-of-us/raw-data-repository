from abc import ABC, abstractmethod
from datetime import datetime
from dateutil import parser
from io import BytesIO
from os.path import basename
from typing import List, Optional, Union

from geometry import Rect
from google.cloud.storage.blob import Blob
from pdfminer.high_level import extract_pages, extract_text
from pdfminer.layout import LTChar, LTCurve, LTFigure, LTImage, LTTextBox

from rdr_service import config
from rdr_service.storage import GoogleCloudStorageProvider


class ConsentFileAbstractFactory(ABC):
    @classmethod
    def get_file_factory(cls, participant_id: int, participant_origin: str,
                         storage_provider: GoogleCloudStorageProvider) -> 'ConsentFileAbstractFactory':
        origin_factory_class_map = {
            'vibrent': VibrentConsentFactory,
            'careevolution': CeConsentFactory
        }

        if participant_origin in origin_factory_class_map:
            return origin_factory_class_map[participant_origin](participant_id, storage_provider)
        else:
            raise Exception(f'Unsupported participant origin {participant_origin}')

    def __init__(self, participant_id: int, storage_provider: GoogleCloudStorageProvider):
        # Get the PDF Blobs from Google's API for the participant's consent files
        factory_consent_bucket = self._get_source_bucket()
        participant_path_prefix = self._get_source_prefix()
        file_blobs = storage_provider.list(
            bucket_name=factory_consent_bucket,
            prefix=f'{participant_path_prefix}/P{participant_id}'
        )
        self.consent_blobs: List[_ConsentBlobWrapper] = [
            _ConsentBlobWrapper(blob) for blob in file_blobs if blob.name.endswith('.pdf')
        ]
        self._storage_provider = storage_provider

    def get_consent_for_path(self, file_path) -> 'ConsentFile':
        bucket_name, *blob_name_parts = file_path.split('/')
        blob = self._storage_provider.get_blob(bucket_name=bucket_name, blob_name='/'.join(blob_name_parts))
        blob_wrapper = _ConsentBlobWrapper(blob)

        if self._is_primary_consent(blob_wrapper):
            return self._build_primary_consent(blob_wrapper)
        elif self._is_cabor_consent(blob_wrapper):
            return self._build_cabor_consent(blob_wrapper)
        elif self._is_ehr_consent(blob_wrapper):
            return self._build_ehr_consent(blob_wrapper)
        elif self._is_gror_consent(blob_wrapper):
            return self._build_gror_consent(blob_wrapper)

    def get_primary_consents(self) -> List['PrimaryConsentFile']:
        return [
            self._build_primary_consent(blob_wrapper)
            for blob_wrapper in self.consent_blobs
            if self._is_primary_consent(blob_wrapper)
        ]

    def get_cabor_consents(self) -> List['CaborConsentFile']:
        return [
            self._build_cabor_consent(blob_wrapper)
            for blob_wrapper in self.consent_blobs
            if self._is_cabor_consent(blob_wrapper)
        ]

    def get_ehr_consents(self) -> List['EhrConsentFile']:
        return [
            self._build_ehr_consent(blob_wrapper)
            for blob_wrapper in self.consent_blobs
            if self._is_ehr_consent(blob_wrapper)
        ]

    def get_gror_consents(self) -> List['GrorConsentFile']:
        return [
            self._build_gror_consent(blob_wrapper)
            for blob_wrapper in self.consent_blobs
            if self._is_gror_consent(blob_wrapper)
        ]

    def get_primary_update_consents(self, consent_date: datetime) -> List['PrimaryConsentUpdateFile']:
        return [
            self._build_primary_update_consent(blob_wrapper, consent_date)
            for blob_wrapper in self.consent_blobs
            if self._is_primary_update_consent(blob_wrapper, consent_date)
        ]

    def get_wear_consents(self) -> List['WearConsentFile']:
        return [
            self._build_wear_consent(blob_wrapper)
            for blob_wrapper in self.consent_blobs
            if self._is_wear_consent(blob_wrapper)
        ]

    def get_from_path(self, file_path: str, consent_date=None) -> 'ConsentFile':
        wrapper = None
        for consent in self.consent_blobs:
            if file_path in consent.blob.public_url:
                wrapper = consent
                break
        if self._is_primary_consent(wrapper):
            return self._build_primary_consent(wrapper)
        elif self._is_cabor_consent(wrapper):
            return self._build_cabor_consent(wrapper)
        elif self._is_ehr_consent(wrapper):
            return self._build_ehr_consent(wrapper)
        elif self._is_gror_consent(wrapper):
            return self._build_gror_consent(wrapper)
        elif self._is_primary_update_consent(wrapper, consent_date=consent_date):
            return self._build_primary_update_consent(wrapper, consent_date=consent_date)
        elif self._is_wear_consent(wrapper):
            return self._build_wear_consent(wrapper)

    @abstractmethod
    def _is_primary_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> bool:
        ...

    @abstractmethod
    def _is_cabor_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> bool:
        ...

    @abstractmethod
    def _is_ehr_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> bool:
        ...

    @abstractmethod
    def _is_gror_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> bool:
        ...

    @abstractmethod
    def _is_primary_update_consent(self, blob_wrapper: '_ConsentBlobWrapper', consent_date: datetime) -> bool:
        ...

    @abstractmethod
    def _is_wear_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> bool:
        ...

    @abstractmethod
    def _build_primary_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> 'PrimaryConsentFile':
        ...

    @abstractmethod
    def _build_cabor_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> 'CaborConsentFile':
        ...

    @abstractmethod
    def _build_ehr_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> 'EhrConsentFile':
        ...

    @abstractmethod
    def _build_gror_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> 'GrorConsentFile':
        ...

    @abstractmethod
    def _build_primary_update_consent(self, blob_wrapper: '_ConsentBlobWrapper', consent_date: datetime) \
            -> 'PrimaryConsentUpdateFile':
        ...

    @abstractmethod
    def _build_wear_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> 'WearConsentFile':
        ...

    @abstractmethod
    def _get_source_bucket(self) -> str:
        ...

    @abstractmethod
    def _get_source_prefix(self) -> str:
        ...


class VibrentConsentFactory(ConsentFileAbstractFactory):
    CABOR_TEXT = (
        'California Experimental Subject’s Bill of Rights',
        'Declaración de Derechos del Sujeto de Investigación Experimental'
    )

    def _is_primary_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> bool:
        name = blob_wrapper.blob.name
        if not basename(name).startswith('ConsentPII'):
            return False

        pdf = blob_wrapper.get_parsed_pdf()
        return pdf.get_page_number_of_text([self.CABOR_TEXT]) is None

    def _is_cabor_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> bool:
        name = blob_wrapper.blob.name
        if not basename(name).startswith('ConsentPII'):
            return False

        pdf = blob_wrapper.get_parsed_pdf()
        return pdf.get_page_number_of_text([self.CABOR_TEXT]) is not None

    def _is_ehr_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> bool:
        return basename(blob_wrapper.blob.name).startswith('EHRConsentPII')

    def _is_gror_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> bool:
        return basename(blob_wrapper.blob.name).startswith('GROR')

    def _is_primary_update_consent(self, blob_wrapper: '_ConsentBlobWrapper', consent_date) -> bool:
        return (
            basename(blob_wrapper.blob.name).startswith('PrimaryConsentUpdate')
            and (
                PrimaryConsentUpdateFile.pdf_has_update_text(blob_wrapper.get_parsed_pdf())
                or consent_date < VibrentPrimaryConsentUpdateFile.FIRST_VERSION_END_DATE
            )
        )

    def _is_wear_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> bool:
        raise NotImplementedError('Wear consent validation not implemented for Vibrent')

    def _build_primary_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> 'PrimaryConsentFile':
        return VibrentPrimaryConsentFile(pdf=blob_wrapper.get_parsed_pdf(), blob=blob_wrapper.blob)

    def _build_cabor_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> 'CaborConsentFile':
        return VibrentCaborConsentFile(pdf=blob_wrapper.get_parsed_pdf(), blob=blob_wrapper.blob)

    def _build_ehr_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> 'EhrConsentFile':
        return VibrentEhrConsentFile(pdf=blob_wrapper.get_parsed_pdf(), blob=blob_wrapper.blob)

    def _build_gror_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> 'GrorConsentFile':
        return VibrentGrorConsentFile(pdf=blob_wrapper.get_parsed_pdf(), blob=blob_wrapper.blob)

    def _build_primary_update_consent(self, blob_wrapper: '_ConsentBlobWrapper', consent_date: datetime) \
            -> 'PrimaryConsentUpdateFile':
        return VibrentPrimaryConsentUpdateFile(
            pdf=blob_wrapper.get_parsed_pdf(),
            blob=blob_wrapper.blob,
            consent_date=consent_date
        )

    def _build_wear_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> 'WearConsentFile':
        raise NotImplementedError('Wear consent validation not implemented for Vibrent')

    def _get_source_bucket(self) -> str:
        return config.getSettingJson(config.CONSENT_PDF_BUCKET)['vibrent']

    def _get_source_prefix(self) -> str:
        return 'Participant'


class CeConsentFactory(ConsentFileAbstractFactory):
    # Text being checked is based on the content of the files found at
    # https://joinallofus.atlassian.net/wiki/spaces/PROT/pages/1251180906/Consents+and+Authorizations

    def _is_primary_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> bool:
        pdf = blob_wrapper.get_parsed_pdf()
        return pdf.has_text([(
            'Consent to Join the All of Us Research Program',
            'Consentimiento para Participar en el Programa Científico',
            'Consentimiento para Participar en el\nPrograma Cientíﬁco'
        )])

    def _is_cabor_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> bool:
        pdf = blob_wrapper.get_parsed_pdf()
        return pdf.has_text([(
            "California Experimental Subject's Bill of Rights",
            'Declaración de Derechos del Sujeto de Investigación Experimental'
        )])

    def _is_ehr_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> bool:
        pdf = blob_wrapper.get_parsed_pdf()
        return pdf.has_text([
            (
                'HIPAA Authorization for Research EHR',
                'Autorización para Investigación de HIPAA',
                'Authorization to Share My EHRs for Research',
                'Autorización para compartir mis EHR para propósitos de investigación'
            )
        ])

    def _is_gror_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> bool:
        pdf = blob_wrapper.get_parsed_pdf()
        return pdf.has_text([
            (
                'Consent to Receive DNA Results',
                'Consentimiento para Recibir Resultados de ADN',
                'Consent to Get DNA Results'
            )
        ])

    def _is_primary_update_consent(self, blob_wrapper: '_ConsentBlobWrapper', consent_date: datetime) -> bool:
        return False  # CE doesn't have cohort 1 participants to have needed re-consents

    def _is_wear_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> bool:
        pdf = blob_wrapper.get_parsed_pdf()
        return pdf.has_text([(
            'All of Us WEAR Study',
            'All of Us WEAR\nStudy',
            'el Estudio WEAR de All of Us',
            'Estudio del uso desensores portátiles',
            'Estudio\ndel uso de sensores portátiles',
            'All of Us Wearable Study',
            'All of Us Wearable\nStudy'
        )])

    def _build_primary_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> 'PrimaryConsentFile':
        return CePrimaryConsentFile(pdf=blob_wrapper.get_parsed_pdf(), blob=blob_wrapper.blob)

    def _build_cabor_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> 'CaborConsentFile':
        return CeCaborConsentFile(pdf=blob_wrapper.get_parsed_pdf(), blob=blob_wrapper.blob)

    def _build_ehr_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> 'EhrConsentFile':
        return CeEhrConsentFile(pdf=blob_wrapper.get_parsed_pdf(), blob=blob_wrapper.blob)

    def _build_gror_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> 'GrorConsentFile':
        return CeGrorConsentFile(pdf=blob_wrapper.get_parsed_pdf(), blob=blob_wrapper.blob)

    def _build_primary_update_consent(self, blob_wrapper: '_ConsentBlobWrapper', consent_date: datetime) \
            -> 'PrimaryConsentUpdateFile':
        pass  # CE doesn't have cohort 1 participants to have needed re-consents

    def _build_wear_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> 'WearConsentFile':
        return CeWearConsentFile(pdf=blob_wrapper.get_parsed_pdf(), blob=blob_wrapper.blob)

    def _get_source_bucket(self) -> str:
        return config.getSettingJson(config.CONSENT_PDF_BUCKET)['careevolution']

    def _get_source_prefix(self) -> str:
        return 'Participants'


class _ConsentBlobWrapper:
    def __init__(self, blob: Blob):
        self.blob = blob
        self._parsed_pdf = None

    def get_parsed_pdf(self) -> 'Pdf':
        if self._parsed_pdf is None:
            self._parsed_pdf = Pdf.from_google_storage_blob(self.blob)

        return self._parsed_pdf


class ConsentFile(ABC):
    def __init__(self, pdf: 'Pdf', blob: Blob):
        self.pdf = pdf
        self.upload_time = blob.updated
        self.file_path = f'{blob.bucket.name}/{blob.name}'
        self._blob = blob

    def get_signature_on_file(self):
        signature_elements = self._get_signature_elements()
        for element in signature_elements:
            first_child = Pdf.get_first_child_of_element(element)

            if isinstance(first_child, LTChar):
                possible_signature = ''.join([char_child.get_text() for char_child in element]).strip()
                if possible_signature != '':
                    return possible_signature

            elif isinstance(first_child, LTImage):
                return True

    def get_date_signed(self):
        date_str = self._get_date_signed_str()
        if date_str:
            try:
                return parser.parse(date_str).date()
            except parser.ParserError:
                ...

        return None

    def get_printed_name(self):
        printed_name_elements = self._get_printed_name_elements()
        for element in printed_name_elements:
            if isinstance(element, LTFigure) and len(element) > 0:
                return ''.join([char_child.get_text() for char_child in element]).strip()

    def _get_date_signed_str(self):
        date_elements = self._get_date_elements()
        for element in date_elements:
            if isinstance(element, LTFigure) and len(element) > 0:
                # Some files have empty Figures in the same place as the date
                return ''.join([char_child.get_text() for char_child in element]).strip()

    def _get_signature_elements(self):
        return []

    def _get_date_elements(self):
        return []

    def _get_printed_name_elements(self):
        return []


class PrimaryConsentFile(ConsentFile, ABC):
    def get_is_va_consent(self):
        return self.pdf.get_page_number_of_text([
            (
                'you will get care at a VA facility',
                'Si necesita atención de seguimiento para su lesión, VA lo cubrirá.'
            )
        ]) is not None


class CaborConsentFile(ConsentFile, ABC):
    ...


class EhrConsentFile(ConsentFile, ABC):
    def get_is_va_consent(self):
        return self.pdf.get_page_number_of_text([
            (
                'We may ask you to go to a local clinic to be measured',
                'Es posible que le pidamos que visite a una clínica local para dar sus muestras \nbiológicas.'
            )
        ]) is not None

    def is_sensitive_form(self):
        return False

    def has_valid_sensitive_form_initials(self):
        return False


class GrorConsentFile(ConsentFile, ABC):
    def is_confirmation_selected(self):
        for element in self._get_confirmation_check_elements():
            for child in element:
                if isinstance(child, LTCurve):
                    return True

        return False

    @abstractmethod
    def _get_confirmation_check_elements(self):
        ...


class PrimaryConsentUpdateFile(PrimaryConsentFile, ABC):
    """
    Updated consent file received for cohort 1 participants that
    needed to agree to (or decline) new wording for DNA data
    """

    @abstractmethod
    def is_agreement_selected(self):
        ...

    @classmethod
    def pdf_has_update_text(cls, pdf: 'Pdf'):
        # Text being checked is based on the F1.20a.C1U.0915.Eng/Esp and later versions of the Cohort 1 Update consent
        # file. Found at https://joinallofus.atlassian.net/wiki/spaces/PROT/pages/2678587466/
        # Primary+Consent#Primary-Consent-Form-(Appendix-F1)
        update_agreement_page_number = pdf.get_page_number_of_text([
            (
                'Do you agree to this updated consent?',
                '¿Está de acuerdo con este consentimiento actualizado?'
            )
        ])
        return update_agreement_page_number is not None


class WearConsentFile(PrimaryConsentFile, ABC):
    ...


class VibrentPrimaryConsentFile(PrimaryConsentFile):
    def _get_signature_page(self):
        return self.pdf.get_page_number_of_text([
            ('I freely and willingly choose', 'Decido participar libremente y por voluntad propia'),
            ('sign your full name', 'Firme con su nombre completo')
        ])

    def _get_signature_elements(self):
        signature_page = self._get_signature_page()

        elements = self.pdf.get_elements_intersecting_box(
            Rect.from_edges(left=125, right=500, bottom=155, top=160),
            page=signature_page
        )
        if not elements:  # old style consent
            elements = self.pdf.get_elements_intersecting_box(
                Rect.from_edges(left=220, right=500, bottom=590, top=600), page=signature_page)

        return elements

    def _get_date_elements(self):
        signature_page = self._get_signature_page()

        elements = self.pdf.get_elements_intersecting_box(
            Rect.from_edges(left=125, right=200, bottom=110, top=120),
            page=signature_page
        )
        if not elements:  # old style consent
            elements = self.pdf.get_elements_intersecting_box(
                Rect.from_edges(left=110, right=200, bottom=570, top=580),
                page=signature_page
            )

        return elements

    def _get_printed_name_elements(self):
        signature_page = self._get_signature_page()

        return self.pdf.get_elements_intersecting_box(
            Rect.from_edges(left=350, right=500, bottom=45, top=50),
            page=signature_page
        )


class VibrentCaborConsentFile(CaborConsentFile):
    def _get_signature_elements(self):
        elements = self.pdf.get_elements_intersecting_box(Rect.from_edges(left=200, right=400, bottom=110, top=115))
        if not elements:  # old style cabor have signature higher up on the page
            elements = self.pdf.get_elements_intersecting_box(Rect.from_edges(left=200, right=400, bottom=165, top=170))

        return elements

    def _get_date_elements(self):
        elements = self.pdf.get_elements_intersecting_box(Rect.from_edges(left=520, right=570, bottom=110, top=115))
        if not elements:  # old style cabor have signature higher up on the page
            elements = self.pdf.get_elements_intersecting_box(Rect.from_edges(left=520, right=570, bottom=165, top=170))

        return elements

    def _get_printed_name_elements(self):
        return self.pdf.get_elements_intersecting_box(
            Rect.from_edges(left=350, right=500, bottom=45, top=50)
        )


class VibrentEhrConsentFile(EhrConsentFile):
    def _get_signature_elements(self):
        signature_page_number = self._get_signature_page_number()
        return self.pdf.get_elements_intersecting_box(
            Rect.from_edges(left=130, right=250, bottom=160, top=165),
            page=signature_page_number
        )

    def _get_date_elements(self):
        signature_page_number = self._get_signature_page_number()
        return self.pdf.get_elements_intersecting_box(
            Rect.from_edges(left=130, right=250, bottom=110, top=115),
            page=signature_page_number
        )

    def _get_printed_name_elements(self):
        signature_page_number = self._get_signature_page_number()
        return self.pdf.get_elements_intersecting_box(
            Rect.from_edges(left=350, right=500, bottom=45, top=55),
            page=signature_page_number
        )

    def is_sensitive_form(self):
        return self.pdf.get_page_number_of_text([(
            'I agree to release sensitive information from my EHRs',
            'Acepto compartir información confidencial de mis EHR'
        )]) is not None

    def _get_signature_page_number(self):
        return self.pdf.get_page_number_of_text([(
            'You will have access to a signed copy of this form',
            'Usted tendrá acceso a una copia firmada de este documento'
        )])

    def has_valid_sensitive_form_initials(self):
        initial_location_list = [
            Rect.from_edges(left=82, right=130, bottom=345, top=350),
            Rect.from_edges(left=82, right=130, bottom=390, top=395),
            Rect.from_edges(left=82, right=130, bottom=445, top=450),
            Rect.from_edges(left=82, right=130, bottom=490, top=495),
            Rect.from_edges(left=82, right=130, bottom=540, top=545),
        ]
        initial_text_found = None

        for location in initial_location_list:
            element_list = self.pdf.get_elements_intersecting_box(location, page=7)

            initial_text_at_location = None
            for element in element_list:
                first_child = Pdf.get_first_child_of_element(element)

                if isinstance(first_child, LTChar):
                    initial_from_element = ''.join([char_child.get_text() for char_child in element]).strip()
                    if not initial_from_element:
                        # Move to the next element to check if nothing was found here
                        continue

                    initial_text_found = initial_from_element
                    initial_text_at_location = initial_from_element
                    break

            if not initial_text_at_location:
                # If a given location doesn't have an initial, return that the initials are invalid
                return False

        return bool(initial_text_found)


class VibrentGrorConsentFile(GrorConsentFile):
    _SIGNATURE_PAGE = 9

    def _get_signature_elements(self):
        return self.pdf.get_elements_intersecting_box(
            Rect.from_edges(left=150, right=400, bottom=155, top=160),
            page=self._SIGNATURE_PAGE
        )

    def _get_date_elements(self):
        return self.pdf.get_elements_intersecting_box(
            Rect.from_edges(left=130, right=400, bottom=110, top=115),
            page=self._SIGNATURE_PAGE
        )

    def _get_confirmation_check_elements(self):
        spanish_signature_text = '¿Desea conocer alguno de sus resultados de ADN?'
        if self.pdf.get_page_number_of_text([spanish_signature_text]) is not None:
            # Spanish versions of the the GROR have the checkmark a bit more to the left
            search_box = Rect.from_edges(left=33, right=36, bottom=480, top=485)
        else:
            search_box = Rect.from_edges(left=70, right=73, bottom=475, top=478)

        return self.pdf.get_elements_intersecting_box(search_box, page=self._SIGNATURE_PAGE)

    def _get_printed_name_elements(self):
        return self.pdf.get_elements_intersecting_box(
            Rect.from_edges(left=350, right=500, bottom=45, top=50),
            page=self._SIGNATURE_PAGE
        )


class VibrentPrimaryConsentUpdateFile(PrimaryConsentUpdateFile):
    FIRST_VERSION_END_DATE = datetime(2020, 11, 1)

    def __init__(self, *args, consent_date: datetime, **kwargs):
        super(VibrentPrimaryConsentUpdateFile, self).__init__(*args, **kwargs)

        # In Sep 2020, the content of the update consent changed. Versions prior ot that were essentially just
        # copies of the Primary consent file. If the given file is close enough to the switch-over date then
        # treat it as a normal consent.
        self.wrapped_consent_file = None
        if consent_date < self.FIRST_VERSION_END_DATE and not PrimaryConsentUpdateFile.pdf_has_update_text(self.pdf):
            self.wrapped_consent_file = VibrentPrimaryConsentFile(*args, **kwargs)

    def _get_signature_page(self):
        return self.pdf.get_page_number_of_text([
            ('Do you agree to this updated consent?', '¿Está de acuerdo con este consentimiento actualizado?')
        ])

    def _get_signature_elements(self):
        if self.wrapped_consent_file:
            return self.wrapped_consent_file._get_signature_elements()
        else:
            return self.pdf.get_elements_intersecting_box(
                Rect.from_edges(left=150, right=400, bottom=155, top=160),
                page=self._get_signature_page()
            )

    def _get_date_elements(self):
        if self.wrapped_consent_file:
            return self.wrapped_consent_file._get_date_elements()
        else:
            return self.pdf.get_elements_intersecting_box(
                Rect.from_edges(left=130, right=400, bottom=110, top=115),
                page=self._get_signature_page()
            )

    def is_agreement_selected(self):
        if self.wrapped_consent_file:
            return True  # TODO: implement and use checkbox validation of Primary consent
        else:
            agreement_elements = self.pdf.get_elements_intersecting_box(
                Rect.from_edges(left=38, right=40, bottom=676, top=678),
                page=self._get_signature_page()
            )

            for element in agreement_elements:
                for child in element:
                    if isinstance(child, LTChar) and child.get_text() == '4':
                        return True

            return False

    def _get_printed_name_elements(self):
        return self.pdf.get_elements_intersecting_box(
            Rect.from_edges(left=350, right=500, bottom=45, top=50),
            page=self._get_signature_page()
        )


class CeFileWrapper:
    def __init__(self, pdf):
        self.pdf = pdf
        self.shift_params = (8, 10)

    def _try_to_parse_signature(self, footer_text):
        signature_page = self._get_last_page()
        signature_footer_location = self._get_location_of_string(signature_page, footer_text)

        if signature_footer_location:
            signature_string_list = self._text_in_bounds(
                search_rect=self._rect_shifted_up_from_footer(signature_footer_location),
                element=signature_page
            )
            if signature_string_list:
                return signature_string_list[0]

        return None

    def get_signature_on_file(self):
        signature_offsets = [
            (8, 15),
            (73, 80)
        ]

        signature_label_strings = [
            "Participant's Name (printed)",
            "'s Name (printed)",
            "Name (printed)",
            'Nombre'
        ]

        for offset in signature_offsets:
            for label_string in signature_label_strings:
                self.shift_params = offset
                signature_str = self._try_to_parse_signature(label_string)
                if signature_str:
                    return signature_str

    def get_date_signed_str(self):
        signature_page = self._get_last_page()
        signature_footer_location = self._get_location_of_string(signature_page, 'Date')
        if not signature_footer_location:
            signature_footer_location = self._get_location_of_string(signature_page, 'Fecha')

        if signature_footer_location:
            date_string_list = self._text_in_bounds(
                search_rect=self._rect_shifted_up_from_footer(signature_footer_location),
                element=signature_page
            )
            if date_string_list:
                return date_string_list[0]

        return None

    def _get_last_page(self):
        last_page_index = len(self.pdf.pages) - 1
        return self.pdf.pages[last_page_index]

    def _rect_shifted_up_from_footer(self, footer_rect: Rect) -> Rect:
        shift_bottom, shift_top = self.shift_params
        return Rect.from_edges(
            left=footer_rect.left - 3,
            right=footer_rect.right + 200,
            bottom=footer_rect.top + shift_bottom,
            top=footer_rect.top + shift_top
        )

    def _text_in_bounds(self, element, search_rect: Rect) -> List[str]:
        if hasattr(element, 'get_text') and hasattr(element, 'x0') and \
                Pdf.rect_for_element(element).intersection(search_rect) is not None:
            return [element.get_text().strip()]
        elif hasattr(element, '__iter__'):
            strings = []
            characters_for_next_string = []
            for child in element:
                if isinstance(child, LTChar):
                    if Pdf.rect_for_element(child).intersection(search_rect) is not None:
                        characters_for_next_string.append(child.get_text())
                else:
                    if characters_for_next_string:
                        strings.append(''.join(characters_for_next_string))
                        characters_for_next_string = []
                    strings.extend(self._text_in_bounds(element=child, search_rect=search_rect))

            if characters_for_next_string:
                strings.append(''.join(characters_for_next_string))

            return strings

        return []

    def _get_location_of_string(self, element, string: str) -> Optional[Rect]:
        char_index_to_match = 0
        location_rect = None
        if hasattr(element, '__iter__'):
            for child in element:
                if isinstance(child, LTChar) and child.get_text() == string[char_index_to_match]:
                    char_location = Pdf.rect_for_element(child)
                    if location_rect is None:
                        location_rect = char_location
                    else:
                        location_rect = location_rect.union(char_location)
                    char_index_to_match += 1
                    if char_index_to_match == len(string):
                        return location_rect
                elif location_rect is None:  # Only check children if we haven't started the string yet
                    children_location = self._get_location_of_string(child, string)
                    if children_location is not None:
                        return children_location
                else:  # reset the box if we've only matched on part of a string
                    char_index_to_match = 0
                    location_rect = None

        return location_rect


class CePrimaryConsentFile(PrimaryConsentFile):
    def __init__(self, *args, **kwargs):
        super(CePrimaryConsentFile, self).__init__(*args, **kwargs)
        self.pdf_wrapper = CeFileWrapper(self.pdf)

    def get_signature_on_file(self):
        return self.pdf_wrapper.get_signature_on_file()

    def _get_date_signed_str(self):
        return self.pdf_wrapper.get_date_signed_str()


class CeCaborConsentFile(CaborConsentFile):
    def __init__(self, *args, **kwargs):
        super(CeCaborConsentFile, self).__init__(*args, **kwargs)
        self.pdf_wrapper = CeFileWrapper(self.pdf)

    def get_signature_on_file(self):
        return self.pdf_wrapper.get_signature_on_file()

    def _get_date_signed_str(self):
        return self.pdf_wrapper.get_date_signed_str()


class CeEhrConsentFile(EhrConsentFile):
    def __init__(self, *args, **kwargs):
        super(CeEhrConsentFile, self).__init__(*args, **kwargs)
        self.pdf_wrapper = CeFileWrapper(self.pdf)

    def get_signature_on_file(self):
        return self.pdf_wrapper.get_signature_on_file()

    def _get_date_signed_str(self):
        return self.pdf_wrapper.get_date_signed_str()


class CeGrorConsentFile(GrorConsentFile):
    def __init__(self, *args, **kwargs):
        super(CeGrorConsentFile, self).__init__(*args, **kwargs)
        self.pdf_wrapper = CeFileWrapper(self.pdf)

    def get_signature_on_file(self):
        return self.pdf_wrapper.get_signature_on_file()

    def _get_date_signed_str(self):
        return self.pdf_wrapper.get_date_signed_str()

    def is_confirmation_selected(self):
        # CE GROR files don't contain a checkmark to have selected or not
        return True

    def _get_confirmation_check_elements(self):
        return []


class CeWearConsentFile(WearConsentFile):
    def __init__(self, *args, **kwargs):
        super(CeWearConsentFile, self).__init__(*args, **kwargs)
        self.pdf_wrapper = CeFileWrapper(self.pdf)

    def get_signature_on_file(self):
        return self.pdf_wrapper.get_signature_on_file()

    def _get_date_signed_str(self):
        return self.pdf_wrapper.get_date_signed_str()


class Pdf:

    def __init__(self, pages, blob: Blob):
        self.pages = pages
        self._pdf_text = None
        self._blob = blob

    @classmethod
    def from_google_storage_blob(cls, blob: Blob):
        file_bytes = BytesIO(blob.download_as_string())
        pages = list(extract_pages(file_bytes))
        return Pdf(pages, blob)

    @classmethod
    def rect_for_element(cls, element) -> Optional[Rect]:
        return Rect.from_edges(
            left=element.x0,
            right=element.x1,
            bottom=element.y0,
            top=element.y1
        )

    def get_elements_intersecting_box(self, search_box: Rect, page=0):
        if page is None or len(self.pages) <= page:
            return []

        elements = []
        page = self.pages[page]
        for element in page:
            element_rect = self.rect_for_element(element)
            if element_rect.intersection(search_box) is not None:
                elements.append(element)

        return elements

    def get_page_number_of_text(self, search_str_list: List[Union[str, tuple]]):
        for page_number, page in enumerate(self.pages):
            all_strings_found_in_page = True
            for search_str in search_str_list:
                if isinstance(search_str, str):
                    string_found_in_page = self._is_string_in_page(search_str, page)
                else:
                    # Using tuples for translations
                    string_found_in_page = False
                    for search_str_translation in search_str:
                        if self._is_string_in_page(search_str_translation, page):
                            string_found_in_page = True
                            break

                if not string_found_in_page:
                    all_strings_found_in_page = False
                    break

            if all_strings_found_in_page:
                return page_number

        return None

    def has_text(self, search_strings):
        if self._pdf_text is None:
            file_bytes = BytesIO(self._blob.download_as_string())
            self._pdf_text = extract_text(file_bytes)[:200]

        for search_token in search_strings:
            found_token_in_page = False
            for translation in search_token:
                if translation in self._pdf_text:
                    found_token_in_page = True

            if not found_token_in_page:
                return False

        return True

    @classmethod
    def get_first_child_of_element(cls, element):
        try:
            return list(element)[0]
        except (IndexError, TypeError):
            return None

    def _is_string_in_page(self, search_str, page):
        for element in page:
            if self._is_text_in_layout_element(element, search_str):
                return True

        return False

    def _is_text_in_layout_element(self, element, search_str):
        if hasattr(element, 'get_text'):
            if self._is_text_match(element.get_text(), search_str):
                return True

        if isinstance(element, LTTextBox):
            for child_text in element:
                if self._is_text_in_layout_element(child_text, search_str):
                    return True

        return False

    @classmethod
    def _is_text_match(cls, element_text, search_text):
        return cls._get_text_for_comparison(search_text) in cls._get_text_for_comparison(element_text)

    @classmethod
    def _get_text_for_comparison(cls, text: str):
        return ''.join(text.lower().split())
