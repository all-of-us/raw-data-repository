from abc import ABC, abstractmethod
from dateutil import parser
from geometry import Rect
from google.cloud.storage.blob import Blob
from io import BytesIO
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTChar, LTCurve, LTFigure, LTImage, LTTextBox
from typing import List


class ConsentFile(ABC):
    def __init__(self, pdf: 'Pdf'):
        self.pdf = pdf

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
    def _get_signature_elements(self):
        return self.pdf.get_elements_intersecting_box(Rect.from_edges(left=150, right=400, bottom=155, top=160), page=9)

    def _get_date_elements(self):
        return self.pdf.get_elements_intersecting_box(Rect.from_edges(left=130, right=400, bottom=110, top=115), page=9)

    def _get_confirmation_check_elements(self):
        return self.pdf.get_elements_intersecting_box(Rect.from_edges(left=70,  right=73, bottom=475, top=478), page=9)


class Pdf:

    def __init__(self, pages):
        self.pages = pages

    @classmethod
    def from_google_storage_blob(cls, blob: Blob):
        file_bytes = BytesIO(blob.download_as_string())
        pages = extract_pages(file_bytes)
        return Pdf(pages)

    def get_elements_intersecting_box(self, search_box: Rect, page=0):
        if page is None:
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
