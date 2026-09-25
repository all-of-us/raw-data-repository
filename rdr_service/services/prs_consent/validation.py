from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from io import BytesIO
from typing import List

from dateutil.parser import parse
from geometry import Rect

from rdr_service.services.consent.files import Pdf


class PdfParsingError(Exception):
    pass


@dataclass
class BaseExpectedData:
    signed_date: date


class _Validator(ABC):
    @classmethod
    def validate(cls, file_data_stream: BytesIO, expected_data: BaseExpectedData):
        pdf_obj = Pdf.from_stream(file_data_stream)
        return cls._check_pdf(pdf_obj, expected_data)

    @classmethod
    @abstractmethod
    def _check_pdf(cls, pdf: Pdf, expected_data: BaseExpectedData):
        pass


@dataclass
class G2pConsentExpectedData(BaseExpectedData):
    received_help: bool
    helper_name: str


class G2pConsentValidator(_Validator):
    @classmethod
    def _check_pdf(cls, pdf: Pdf, expected_data: G2pConsentExpectedData) -> List[str]:
        errors = []

        signature_page_num = pdf.get_page_number_of_text(['Sign Your Full Name'])

        signature_bounds = Rect.from_edges(320, 500, 290, 310)
        has_signature_image = pdf.has_image_at(signature_bounds, signature_page_num)
        if not has_signature_image:
            errors.append('missing signature')

        consent_check_bounds = Rect.from_edges(38, 40, 375, 380)
        has_consent_checked = pdf.has_x_stroke_at(consent_check_bounds, signature_page_num)
        if not has_consent_checked:
            errors.append('missing consent checkmark')

        date_bounds = Rect.from_edges(330, 380, 210, 215)
        date_str = pdf.get_text_at(date_bounds, signature_page_num)  # '541822131'
        try:
            signed_date = parse(date_str).date()
        except TypeError:
            raise PdfParsingError(f'unable to parse date string "{date_str}"')
        if signed_date != expected_data.signed_date:
            errors.append('signing date mismatched')

        help_check_bounds = Rect.from_edges(38, 40, 159, 161)
        has_helped_checked = pdf.has_x_stroke_at(help_check_bounds, signature_page_num)
        if expected_data.received_help != has_helped_checked:
            errors.append('help received mismatched')

        help_name_bounds = Rect.from_edges(330, 380, 130, 135)
        helper_name = pdf.get_text_at(help_name_bounds, signature_page_num)
        if expected_data.helper_name != helper_name:
            errors.append('helper name mismatched')

        return errors
