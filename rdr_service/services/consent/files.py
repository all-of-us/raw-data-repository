from abc import ABC, abstractmethod
from dateutil import parser
from io import BytesIO
from os.path import basename
from typing import List

from geometry import Rect
from google.cloud.storage.blob import Blob
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTChar, LTCurve, LTFigure, LTImage, LTTextBox

from rdr_service.storage import GoogleCloudStorageProvider


class ConsentFileAbstractFactory(ABC):
    @classmethod
    def get_file_factory(cls, participant_id: int, participant_origin: str,
                         storage_provider: GoogleCloudStorageProvider) -> 'ConsentFileAbstractFactory':
        if participant_origin == 'vibrent':
            return VibrentConsentFactory(participant_id, storage_provider)

        raise Exception(f'Unsupported participant origin {participant_origin}')

    def __init__(self, participant_id: int, storage_provider: GoogleCloudStorageProvider):
        # Get the PDF Blobs from Google's API for the participant's consent files
        factory_consent_bucket = self._get_source_bucket()
        file_blobs = storage_provider.list(
            bucket_name=factory_consent_bucket,
            prefix=f'Participant/P{participant_id}'
        )
        self.pdf_blobs = [blob for blob in file_blobs if blob.name.endswith('.pdf')]

    @abstractmethod
    def get_primary_consents(self) -> List['PrimaryConsentFile']:
        ...

    @abstractmethod
    def get_cabor_consents(self) -> List['CaborConsentFile']:
        ...

    @abstractmethod
    def get_ehr_consents(self) -> List['EhrConsentFile']:
        ...

    @abstractmethod
    def get_gror_consents(self) -> List['GrorConsentFile']:
        ...

    @abstractmethod
    def _get_source_bucket(self) -> str:
        ...


class VibrentConsentFactory(ConsentFileAbstractFactory):
    CABOR_TEXT = 'California Experimental Subject’s Bill of Rights'

    def get_primary_consents(self) -> List['PrimaryConsentFile']:
        primary_consents = []
        for blob in self._get_consent_pii_blobs():
            pdf_data = Pdf.from_google_storage_blob(blob)
            if pdf_data.get_page_number_of_text([self.CABOR_TEXT]) is None:
                primary_consents.append(VibrentPrimaryConsentFile(pdf=pdf_data, blob=blob))

        return primary_consents

    def get_cabor_consents(self) -> List['CaborConsentFile']:
        cabor_consents = []
        for blob in self._get_consent_pii_blobs():
            pdf_data = Pdf.from_google_storage_blob(blob)
            if pdf_data.get_page_number_of_text([self.CABOR_TEXT]) is not None:
                cabor_consents.append(VibrentCaborConsentFile(pdf=pdf_data, blob=blob))

        return cabor_consents

    def get_ehr_consents(self) -> List['EhrConsentFile']:
        ehr_consents = []
        for blob in self.pdf_blobs:
            if basename(blob.name).startswith('EHRConsentPII'):
                ehr_consents.append(VibrentEhrConsentFile(pdf=Pdf.from_google_storage_blob(blob), blob=blob))

        return ehr_consents

    def get_gror_consents(self) -> List['GrorConsentFile']:
        gror_consents = []
        for blob in self.pdf_blobs:
            if basename(blob.name).startswith('GROR'):
                gror_consents.append(VibrentGrorConsentFile(pdf=Pdf.from_google_storage_blob(blob), blob=blob))

        return gror_consents

    def _get_source_bucket(self) -> str:
        return 'ptc-uploads-all-of-us-rdr-prod'

    def _get_consent_pii_blobs(self):
        def is_consent_pii_blob(blob):
            return basename(blob.name).startswith('ConsentPII') and blob.name.endswith('.pdf')
        return [blob for blob in self.pdf_blobs if is_consent_pii_blob(blob)]


class ConsentFile(ABC):
    def __init__(self, pdf: 'Pdf', blob: Blob):
        self.pdf = pdf
        self.upload_time = blob.updated
        self.file_path = f'{blob.bucket.name}/{blob.name}'

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
        date_elements = self._get_date_elements()
        for element in date_elements:
            if isinstance(element, LTFigure):
                date_str = ''.join([char_child.get_text() for char_child in element]).strip()
                if date_str:
                    return parser.parse(date_str).date()

    @abstractmethod
    def _get_signature_elements(self):
        ...

    @abstractmethod
    def _get_date_elements(self):
        ...


class PrimaryConsentFile(ConsentFile, ABC):
    def get_is_va_consent(self):
        return self.pdf.get_page_number_of_text(['you will get care at a VA facility']) is not None


class CaborConsentFile(ConsentFile, ABC):
    ...


class EhrConsentFile(ConsentFile, ABC):
    ...


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


class VibrentPrimaryConsentFile(PrimaryConsentFile):
    def _get_signature_page(self):
        return self.pdf.get_page_number_of_text([
            'I freely and willingly choose',
            'sign your full name'
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


class VibrentCaborConsentFile(CaborConsentFile):
    def _get_signature_elements(self):
        return self.pdf.get_elements_intersecting_box(Rect.from_edges(left=200, right=400, bottom=110, top=115))

    def _get_date_elements(self):
        return self.pdf.get_elements_intersecting_box(Rect.from_edges(left=520, right=570, bottom=110, top=115))


class VibrentEhrConsentFile(EhrConsentFile):
    def _get_signature_elements(self):
        return self.pdf.get_elements_intersecting_box(Rect.from_edges(left=130, right=250, bottom=160, top=165), page=6)

    def _get_date_elements(self):
        return self.pdf.get_elements_intersecting_box(Rect.from_edges(left=130, right=250, bottom=110, top=115), page=6)


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


class Pdf:

    def __init__(self, pages):
        self.pages = pages

    @classmethod
    def from_google_storage_blob(cls, blob: Blob):
        file_bytes = BytesIO(blob.download_as_string())
        pages = list(extract_pages(file_bytes))
        return Pdf(pages)

    def get_elements_intersecting_box(self, search_box: Rect, page=0):
        if page is None or len(self.pages) <= page:
            return []

        elements = []
        page = self.pages[page]
        for element in page:
            element_rect = Rect.from_edges(
                left=element.x0,
                right=element.x1,
                bottom=element.y0,
                top=element.y1
            )
            if element_rect.intersection(search_box) is not None:
                elements.append(element)

        return elements

    def get_page_number_of_text(self, search_str_list: List[str]):
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
