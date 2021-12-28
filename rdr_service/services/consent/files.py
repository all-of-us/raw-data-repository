from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from dateutil import parser
from io import BytesIO
from os.path import basename
from typing import List, Optional, Union

from geometry import Rect
from google.cloud.storage.blob import Blob
from pdfminer.high_level import extract_text
from pdfminer.converter import PDFPageAggregator
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdftypes import PDFObjRef
from pdfminer.utils import open_filename
from pdfminer.layout import LAParams, LTChar, LTCurve, LTFigure, LTImage, LTTextBox

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
            'Consentimiento para Participar en el Programa Científico'
        )])

    def _is_cabor_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> bool:
        pdf = blob_wrapper.get_parsed_pdf()
        return pdf.has_text([(
            "California Experimental Subject's Bill of Rights",
            'Declaración de Derechos del Sujeto de Investigación Experimental, de California'
        )])

    def _is_ehr_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> bool:
        pdf = blob_wrapper.get_parsed_pdf()
        return pdf.has_text([(
            'HIPAA Authorization for Research EHR',
            'Autorización para Investigación de HIPAA'
        )])

    def _is_gror_consent(self, blob_wrapper: '_ConsentBlobWrapper') -> bool:
        pdf = blob_wrapper.get_parsed_pdf()
        return pdf.has_text([(
            'Consent to Receive DNA Results',
            'Consentimiento para Recibir Resultados de ADN'
        )])

    def _is_primary_update_consent(self, blob_wrapper: '_ConsentBlobWrapper', consent_date: datetime) -> bool:
        return False  # CE doesn't have cohort 1 participants to have needed re-consents

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
        if signature_elements:
            for element in signature_elements:
                first_child = Pdf.get_first_child_of_element(element)

                if isinstance(first_child, LTChar):
                    possible_signature = ''.join([char_child.get_text() for char_child in element]).strip()
                    if possible_signature != '':
                        return possible_signature

                elif isinstance(first_child, LTImage):
                    return True

        return None

    def get_date_signed(self):
        date_str = self._get_date_signed_str()
        if date_str:
            try:
                return parser.parse(date_str).date()
            except parser.ParserError:
                ...

        return None

    def _get_date_signed_str(self):
        date_elements = self._get_date_elements()
        for element in date_elements:
            if isinstance(element, LTFigure):
                return ''.join([char_child.get_text() for char_child in element]).strip()

    def _get_signature_elements(self):
        return []

    def _get_date_elements(self):
        return []


class PrimaryConsentFile(ConsentFile, ABC):
    def get_is_va_consent(self):
        return self.pdf.get_page_number_of_text(['you will get care at a VA facility']) is not None


class CaborConsentFile(ConsentFile, ABC):
    ...


class EhrConsentFile(ConsentFile, ABC):
    def get_is_va_consent(self):
        return self.pdf.get_page_number_of_text(['We may ask you to go to a local clinic to be measured']) is not None


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


class VibrentConsentFile(ConsentFile):
    def get_signature_on_file(self):
        signature_from_elements = super(VibrentConsentFile, self).get_signature_on_file()
        if signature_from_elements:
            return signature_from_elements
        else:
            # No elements found worked, so check annotations
            return self._get_signature_from_annotation()  # Either gets the signature or returns None

    @abstractmethod
    def _get_signature_page(self):
        ...

    def _get_signature_from_annotation(self):
        signature_annotation = self.pdf.get_annotation(
            page_no=self._get_signature_page(),
            annotation_name='ParticipantTypedinSignature'
        )
        if signature_annotation and 'V' in signature_annotation:
            return signature_annotation['V'].decode('latin_1').strip()
        else:
            return None

    def _search_for_signature(self, content_variations: List['ContentVariation']):
        page_no = self._get_signature_page()
        page = self.pdf.pages[page_no]

        for variant in content_variations:
            signature_label_location = self.pdf.location_of_text(page, variant.text_of_signature_label)
            date_label_location = self.pdf.location_of_text(page, variant.text_of_date_label)
            if signature_label_location and date_label_location:
                for layout in variant.layout_variations:
                    if (
                        signature_label_location.is_inside_of(layout.signature_label_location)
                        and date_label_location.is_inside_of(layout.date_label_location)
                    ):
                        self.date_search_box = layout.date_search_box
                        return self.pdf.get_elements_intersecting_box(
                            search_box=layout.signature_search_box,
                            page=page_no
                        )
                return None

        return None

    def _get_date_signed_str(self):
        signature_page = self._get_signature_page()

        if self.date_search_box:
            possible_elements = self.pdf.get_elements_intersecting_box(self.date_search_box, page=signature_page)
            date_figures = [
                element for element in possible_elements
                if isinstance(element, LTFigure) and not any([isinstance(child, LTImage) for child in element])
            ]

            if date_figures:
                for date_figure in date_figures:
                    return ''.join([char_child.get_text() for char_child in date_figure]).strip()
            else:
                date_annotation = self.pdf.get_annotation(page_no=signature_page, annotation_name='date')
                if date_annotation and 'V' in date_annotation:
                    return date_annotation['V'].decode('latin_1').strip()

        return None


class VibrentPrimaryConsentFile(PrimaryConsentFile, VibrentConsentFile):
    def __init__(self, *args, **kwargs):
        super(VibrentPrimaryConsentFile, self).__init__(*args, **kwargs)
        self.date_search_box = None

    def _get_signature_page(self):
        return self.pdf.get_page_number_of_text([
            ('I freely and willingly choose', 'Decido participar libremente y por voluntad propia'),
            ('sign your full name', 'Firme con su nombre completo')
        ])

    def _get_signature_elements(self):
        return self._search_for_signature(
            content_variations=[
                ContentVariation(
                    text_of_signature_label='Sign Your Full Name:',
                    text_of_date_label='Date: \n',
                    layout_variations=[
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=80, right=218, bottom=170, top=185),
                            date_label_location=Rect.from_edges(left=80, right=118, bottom=120, top=135),
                            signature_search_box=Rect.from_edges(left=120, right=500, bottom=148, top=152),
                            date_search_box=Rect.from_edges(left=120, right=500, bottom=95, top=100)
                        )
                    ]
                ),
                ContentVariation(
                    text_of_signature_label='Sign your full name ____',
                    text_of_date_label='Date ____',
                    layout_variations=[
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=71, right=482, bottom=500, top=513),
                            date_label_location=Rect.from_edges(left=72, right=404, bottom=456, top=470),
                            signature_search_box=Rect.from_edges(left=80, right=400, bottom=505, top=510),
                            date_search_box=Rect.from_edges(left=80, right=380, bottom=465, top=468)
                        ),
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=71, right=482, bottom=590, top=603),
                            date_label_location=Rect.from_edges(left=72, right=404, bottom=546, top=560),
                            signature_search_box=Rect.from_edges(left=80, right=400, bottom=593, top=598),
                            date_search_box=Rect.from_edges(left=80, right=380, bottom=550, top=555)
                        )
                    ]
                ),
                ContentVariation(
                    text_of_signature_label='Sign Your  \nFull Name: \n',
                    text_of_date_label='Date: \n',
                    layout_variations=[
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=80, right=152, bottom=171, top=203),
                            date_label_location=Rect.from_edges(left=80, right=118, bottom=120, top=135),
                            signature_search_box=Rect.from_edges(left=120, right=400, bottom=150, top=153),
                            date_search_box=Rect.from_edges(left=120, right=400, bottom=98, top=102)
                        )
                    ]
                ),
                ContentVariation(
                    text_of_signature_label='Firme con su \nnombre completo',
                    text_of_date_label='Fecha ',
                    layout_variations=[
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=75, right=172, bottom=518, top=547),
                            date_label_location=Rect.from_edges(left=76, right=117, bottom=475, top=488),
                            signature_search_box=Rect.from_edges(left=120, right=400, bottom=150, top=153),
                            date_search_box=Rect.from_edges(left=150, right=400, bottom=525, top=530)
                        )
                    ]
                ),
                ContentVariation(
                    text_of_signature_label='Firme con su  \nnombre completo: \n',
                    text_of_date_label='Fecha ',
                    layout_variations=[
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=44, right=161, bottom=170, top=203),
                            date_label_location=Rect.from_edges(left=44, right=92, bottom=120, top=135),
                            signature_search_box=Rect.from_edges(left=120, right=400, bottom=150, top=153),
                            date_search_box=Rect.from_edges(left=120, right=400, bottom=98, top=102)
                        )
                    ]
                ),
                ContentVariation(
                    text_of_signature_label='Firme con su nombre completo: \n',
                    text_of_date_label='Fecha: \n',
                    layout_variations=[
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=44, right=246, bottom=170, top=185),
                            date_label_location=Rect.from_edges(left=44, right=91, bottom=119, top=135),
                            signature_search_box=Rect.from_edges(left=120, right=400, bottom=150, top=153),
                            date_search_box=Rect.from_edges(left=120, right=400, bottom=98, top=102)
                        )
                    ]
                )
            ]
        )


class VibrentCaborConsentFile(CaborConsentFile, VibrentConsentFile):
    def __init__(self, *args, **kwargs):
        super(VibrentCaborConsentFile, self).__init__(*args, **kwargs)
        self.date_search_box = None

    def _get_signature_page(self):
        return 0

    def _get_signature_elements(self):
        return self._search_for_signature(
            content_variations=[
                ContentVariation(
                    text_of_signature_label='Firma \n',
                    text_of_date_label='Fecha \n',
                    layout_variations=[
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=224, right=264, bottom=73, top=88),
                            date_label_location=Rect.from_edges(left=500, right=544, bottom=73, top=88),
                            signature_search_box=Rect.from_edges(left=120, right=400, bottom=110, top=115),
                            date_search_box=Rect.from_edges(left=520, right=570, bottom=110, top=115)
                        ),
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=236, right=269, bottom=161, top=174),
                            date_label_location=Rect.from_edges(left=459, right=491, bottom=161, top=174),
                            signature_search_box=Rect.from_edges(left=90, right=400, bottom=178, top=185),
                            date_search_box=Rect.from_edges(left=430, right=510, bottom=178, top=185)
                        )
                    ]
                ),
                ContentVariation(
                    text_of_signature_label='Signature \n',
                    text_of_date_label='Date \n',
                    layout_variations=[
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=211, right=276, bottom=73, top=89),
                            date_label_location=Rect.from_edges(left=504, right=539, bottom=74, top=89),
                            signature_search_box=Rect.from_edges(left=120, right=400, bottom=110, top=115),
                            date_search_box=Rect.from_edges(left=520, right=570, bottom=110, top=115)
                        )
                    ]
                ),
                ContentVariation(
                    text_of_signature_label='Signature  \n',
                    text_of_date_label='Date \n',
                    layout_variations=[
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=229, right=282, bottom=138, top=151),
                            date_label_location=Rect.from_edges(left=469, right=495, bottom=138, top=151),
                            signature_search_box=Rect.from_edges(left=120, right=400, bottom=160, top=170),
                            date_search_box=Rect.from_edges(left=440, right=570, bottom=160, top=170)
                        )
                    ]
                )
            ]
        )


class VibrentEhrConsentFile(EhrConsentFile, VibrentConsentFile):
    def _get_signature_page(self):
        signature_page_number = self.pdf.get_page_number_of_text([
            ('Please print your name and sign below', 'Por favor escriba su nombre y firme en la parte de abajo')
        ])
        if not signature_page_number:
            signature_page_number = self.pdf.get_page_number_of_text([
                (
                    'By signing this form, I voluntarily authorize my healthcare providers',
                    'Al firmar este documento, autorizo voluntariamente a mis proveedores'
                )
            ])

        return signature_page_number

    def _get_signature_elements(self):
        return self._search_for_signature(
            content_variations=[
                ContentVariation(
                    text_of_signature_label='Firme con su \nnombre completo: \n',
                    text_of_date_label='Fecha de hoy:',
                    layout_variations=[
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=72, right=172, bottom=589, top=617),
                            date_label_location=Rect.from_edges(left=72, right=152, bottom=550, top=564),
                            signature_search_box=Rect.from_edges(left=180, right=500, bottom=593, top=598),
                            date_search_box=Rect.from_edges(left=155, right=500, bottom=553, top=558)
                        ),
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=68, right=168, bottom=567, top=594),
                            date_label_location=Rect.from_edges(left=68, right=148, bottom=523, top=536),
                            signature_search_box=Rect.from_edges(left=172, right=500, bottom=575, top=580),
                            date_search_box=Rect.from_edges(left=152, right=500, bottom=528, top=532)
                        )
                    ]
                ),
                ContentVariation(
                    text_of_signature_label='Firme con su  \nnombre completo: \n',
                    text_of_date_label='Fecha de hoy: \n',
                    layout_variations=[
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=44, right=161, bottom=172, top=204),
                            date_label_location=Rect.from_edges(left=44, right=137, bottom=121, top=136),
                            signature_search_box=Rect.from_edges(left=116, right=500, bottom=150, top=155),
                            date_search_box=Rect.from_edges(left=116, right=500, bottom=100, top=105)
                        )
                    ]
                ),
                ContentVariation(
                    text_of_signature_label='Firme con su nombre completo:  \n',
                    text_of_date_label='Fecha de hoy: \n',
                    layout_variations=[
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=72, right=248, bottom=326, top=339),
                            date_label_location=Rect.from_edges(left=72, right=152, bottom=282, top=295),
                            signature_search_box=Rect.from_edges(left=260, right=500, bottom=330, top=335),
                            date_search_box=Rect.from_edges(left=170, right=500, bottom=285, top=290)
                        )
                    ]
                ),
                ContentVariation(
                    text_of_signature_label='Sign your full name: \n',
                    text_of_date_label='Today’s date: \n',
                    layout_variations=[
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=72, right=200, bottom=566, top=581),
                            date_label_location=Rect.from_edges(left=72, right=160, bottom=518, top=533),
                            signature_search_box=Rect.from_edges(left=220, right=450, bottom=570, top=575),
                            date_search_box=Rect.from_edges(left=170, right=450, bottom=525, top=530)
                        ),
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=72, right=200, bottom=545, top=560),
                            date_label_location=Rect.from_edges(left=72, right=160, bottom=497, top=512),
                            signature_search_box=Rect.from_edges(left=220, right=450, bottom=538, top=553),
                            date_search_box=Rect.from_edges(left=170, right=450, bottom=500, top=505)
                        ),
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=72, right=200, bottom=535, top=550),
                            date_label_location=Rect.from_edges(left=72, right=160, bottom=486, top=501),
                            signature_search_box=Rect.from_edges(left=220, right=450, bottom=540, top=545),
                            date_search_box=Rect.from_edges(left=170, right=450, bottom=490, top=495)
                        )
                    ]
                ),
                ContentVariation(
                    text_of_signature_label='Sign your full name:  \n',
                    text_of_date_label='Today’s date: \n',
                    layout_variations=[
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=72, right=184, bottom=239, top=252),
                            date_label_location=Rect.from_edges(left=72, right=148, bottom=195, top=208),
                            signature_search_box=Rect.from_edges(left=200, right=450, bottom=243, top=248),
                            date_search_box=Rect.from_edges(left=200, right=450, bottom=198, top=203)
                        )
                    ]
                ),
                ContentVariation(
                    text_of_signature_label='Sign Your  \nFull Name: \n',
                    text_of_date_label='Today’s date: \n',
                    layout_variations=[
                        LayoutVariation(
                            signature_label_location=Rect.from_edges(left=80, right=152, bottom=172, top=204),
                            date_label_location=Rect.from_edges(left=80, right=169, bottom=121, top=136),
                            signature_search_box=Rect.from_edges(left=116, right=500, bottom=150, top=155),
                            date_search_box=Rect.from_edges(left=116, right=500, bottom=100, top=105)
                        )
                    ]
                ),
            ]
        )


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
        self.shift_params = (8, 10)
        signature_str = self._try_to_parse_signature("Participant's Name (printed)")
        if signature_str:
            return signature_str

        self.shift_params = (73, 75)
        signature_str = self._try_to_parse_signature("Participant's Name (printed)")
        if signature_str:
            return signature_str

        self.shift_params = (8, 10)
        signature_str = self._try_to_parse_signature("'s Name (printed)")
        if signature_str:
            return signature_str

        self.shift_params = (73, 75)
        signature_str = self._try_to_parse_signature("'s Name (printed)")
        if signature_str:
            return signature_str

        self.shift_params = (8, 10)
        signature_str = self._try_to_parse_signature("Name (printed)")
        if signature_str:
            return signature_str

        self.shift_params = (73, 75)
        signature_str = self._try_to_parse_signature("Name (printed)")
        if signature_str:
            return signature_str

    def get_date_signed_str(self):
        signature_page = self._get_last_page()
        signature_footer_location = self._get_location_of_string(signature_page, 'Date')

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


class Pdf:

    def __init__(self, pages, blob: Blob, pdf=None, raw_pages=None):
        self.pages = pages
        self._pdf_text = None
        self._blob = blob
        self._pdf_structure = pdf
        self._raw_pages = raw_pages

    @classmethod
    def from_google_storage_blob(cls, blob: Blob):
        file_bytes = BytesIO(blob.download_as_string())
        # page_layouts = list(extract_pages(file_bytes))

        # Extracting pages using the same algorithm as pdfminer.high_level.extract_pages
        raw_pages = []
        page_layouts = []
        with open_filename(file_bytes, 'rb') as pdf_file_pointer:
            resource_manager = PDFResourceManager()
            device = PDFPageAggregator(resource_manager, laparams=LAParams())
            interpreter = PDFPageInterpreter(resource_manager, device)
            for page in PDFPage.get_pages(pdf_file_pointer):
                raw_pages.append(page)

                interpreter.process_page(page)
                layout = device.get_result()
                page_layouts.append(layout)

        return Pdf(page_layouts, blob, raw_pages=raw_pages)

    @classmethod
    def rect_for_element(cls, element) -> Optional[Rect]:
        return Rect.from_edges(
            left=element.x0,
            right=element.x1,
            bottom=element.y0,
            top=element.y1
        )

    @classmethod
    def location_of_text(cls, root_element, search_text):
        if hasattr(root_element, '__iter__'):
            for child in root_element:
                if hasattr(child, 'get_text') and search_text in child.get_text():
                    return cls.rect_for_element(child)
                else:
                    location_in_children = cls.location_of_text(child, search_text)
                    if location_in_children:
                        return location_in_children

        return None

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

    def get_annotation(self, page_no: int, annotation_name: str):
        raw_page = self._raw_pages[page_no]
        annots = raw_page.annots
        if isinstance(annots, PDFObjRef):
            annots = annots.resolve()

        if annots:  # Annotations can sometimes resolve to None
            for annotation_pointer in annots:
                annotation = annotation_pointer.resolve()
                if annotation['T'].decode('ascii').lower() == annotation_name.lower():
                    return annotation

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


@dataclass
class ContentVariation:
    text_of_signature_label: str
    text_of_date_label: str
    layout_variations: List['LayoutVariation']


@dataclass
class LayoutVariation:
    signature_label_location: Rect
    date_label_location: Rect
    signature_search_box: Rect
    date_search_box: Rect

