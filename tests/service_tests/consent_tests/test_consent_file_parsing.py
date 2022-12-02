from dataclasses import dataclass, field
from datetime import date, datetime
from geometry import Rect
import mock
from pdfminer.layout import LTChar, LTCurve, LTFigure, LTImage, LTPage, LTTextBoxHorizontal, LTTextLineHorizontal
from typing import Collection, List

from rdr_service.services.consent import files
from tests.helpers.unittest_base import BaseTestCase


class ConsentFileParsingTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(ConsentFileParsingTest, self).__init__(*args, **kwargs)
        self.uses_database = False

    def test_vibrent_primary_consent(self):
        for consent_example in self._get_vibrent_primary_test_data():
            consent_file = consent_example.file
            self.assertEqual(consent_example.expected_signature, consent_file.get_signature_on_file())
            self.assertEqual(consent_example.expected_sign_date, consent_file.get_date_signed())
            self.assertEqual(consent_example.expected_to_be_va_file, consent_file.get_is_va_consent())

    def test_ce_primary_consent(self):
        for consent_example in self._get_ce_primary_test_data():
            consent_file = consent_example.file
            self.assertEqual(consent_example.expected_signature, consent_file.get_signature_on_file())
            self.assertEqual(consent_example.expected_sign_date, consent_file.get_date_signed())

    def test_vibrent_cabor_consent(self):
        for consent_example in self._get_vibrent_cabor_test_data():
            consent_file = consent_example.file
            self.assertEqual(consent_example.expected_signature, consent_file.get_signature_on_file())
            self.assertEqual(consent_example.expected_sign_date, consent_file.get_date_signed())

    def test_ce_cabor_consent(self):
        for consent_example in self._get_ce_cabor_test_data():
            consent_file = consent_example.file
            self.assertEqual(consent_example.expected_signature, consent_file.get_signature_on_file())
            self.assertEqual(consent_example.expected_sign_date, consent_file.get_date_signed())

    def test_vibrent_ehr_consent(self):
        for consent_example in self._get_vibrent_ehr_test_data():
            consent_file = consent_example.file
            self.assertEqual(consent_example.expected_signature, consent_file.get_signature_on_file())
            self.assertEqual(consent_example.expected_sign_date, consent_file.get_date_signed())
            self.assertEqual(consent_example.expected_to_be_va_file, consent_file.get_is_va_consent())
            self.assertFalse(consent_file.is_sensitive_form())

    def test_ce_ehr_consent(self):
        for consent_example in self._get_ce_ehr_test_data():
            consent_file = consent_example.file
            self.assertEqual(consent_example.expected_signature, consent_file.get_signature_on_file())
            self.assertEqual(consent_example.expected_sign_date, consent_file.get_date_signed())

    def test_vibrent_gror_consent(self):
        for consent_example in self._get_vibrent_gror_test_data():
            consent_file = consent_example.file
            self.assertEqual(consent_example.expected_signature, consent_file.get_signature_on_file())
            self.assertEqual(consent_example.expected_sign_date, consent_file.get_date_signed())
            self.assertEqual(consent_example.has_yes_selected, consent_file.is_confirmation_selected())

    def test_ce_gror_consent(self):
        for consent_example in self._get_ce_gror_test_data():
            consent_file = consent_example.file
            self.assertEqual(consent_example.expected_signature, consent_file.get_signature_on_file())
            self.assertEqual(consent_example.expected_sign_date, consent_file.get_date_signed())

    def test_vibrent_primary_update_consent(self):
        for consent_example in self._get_vibrent_primary_update_test_data():
            consent_file = consent_example.file
            self.assertEqual(consent_example.expected_signature, consent_file.get_signature_on_file())
            self.assertEqual(consent_example.expected_sign_date, consent_file.get_date_signed())
            self.assertEqual(consent_example.has_yes_selected, consent_file.is_agreement_selected())
            self.assertEqual(consent_example.expected_to_be_va_file, consent_file.get_is_va_consent())

    def test_vibrent_etm_consent(self):
        for consent_example in self._get_vibrent_etm_test_data():
            consent_file = consent_example.file
            self.assertEqual(consent_example.expected_signature, consent_file.get_signature_on_file())
            self.assertEqual(consent_example.expected_sign_date, consent_file.get_date_signed())

    def test_detection_of_sensitive_ehr_form(self):
        sensitive_ehr = self._build_sensitive_ehr(with_initials=False)
        self.assertTrue(sensitive_ehr.is_sensitive_form())

    def test_finding_sensitive_signatures(self):
        pdf_without_initials = self._build_sensitive_ehr(with_initials=False)
        self.assertFalse(pdf_without_initials.has_valid_sensitive_form_initials())

        pdf_with_initials = self._build_sensitive_ehr(with_initials=True)
        self.assertTrue(pdf_with_initials.has_valid_sensitive_form_initials())

    def _get_primary_consent_elements(self):
        return [
            self._build_pdf_element(
                cls=LTTextBoxHorizontal,
                children=[
                    self._build_pdf_element(
                        cls=LTTextLineHorizontal,
                        text='understand the information in this form. All of my questions\n'
                    ),
                    self._build_pdf_element(
                        cls=LTTextLineHorizontal,
                        text='have been answered. I freely and willingly choose to take part in\n'
                    ),
                    self._build_pdf_element(
                        cls=LTTextLineHorizontal,
                        text='the All of Us Research Program.\n'
                    )
                ]
            ),
            self._build_pdf_element(
                cls=LTTextBoxHorizontal,
                children=[
                    self._build_pdf_element(cls=LTTextLineHorizontal, text='Sign Your Full Name: \n')
                ]
            )
        ]

    def _get_vibrent_primary_test_data(self) -> List['PrimaryConsentTestData']:
        """
        Builds a list of PDFs that represent the different layouts of Vibrent's primary consent
        that have been encountered. Add to this if the code incorrectly parses any Vibrent primary pdf
        """
        test_data = []

        # elements that usually appear on the signature page
        description_elements = self._get_primary_consent_elements()

        # Build basic file with signature of Test Name and signing date of August 17, 2019
        pdf = self._build_pdf(pages=[
            [
                *description_elements,
                self._build_form_element(text='Test Name', bbox=(116, 147, 517, 169)),
                self._build_form_element(text='Aug 17, 2019', bbox=(116, 97, 266, 119))
            ]
        ])
        test_data.append(
            PrimaryConsentTestData(
                file=files.VibrentPrimaryConsentFile(pdf=pdf, blob=mock.MagicMock()),
                expected_signature='Test Name',
                expected_sign_date=date(2019, 8, 17)
            )
        )

        # Build an older style of primary layout, with signature box higher up on the page
        pdf = self._build_pdf(pages=[
            [
                *description_elements,
                self._build_form_element(text='Nick', bbox=(116, 585, 517, 605)),
                self._build_form_element(text='Dec 25, 2017', bbox=(116, 565, 266, 585))
            ]
        ])
        test_data.append(
            PrimaryConsentTestData(
                file=files.VibrentPrimaryConsentFile(pdf=pdf, blob=mock.MagicMock()),
                expected_signature='Nick',
                expected_sign_date=date(2017, 12, 25)
            )
        )

        # Build basic VA primary file
        pdf = self._build_pdf(pages=[
            [
                self._build_pdf_element(
                    cls=LTTextBoxHorizontal,
                    children=[
                        self._build_pdf_element(cls=LTTextLineHorizontal, text='you will get care at a VA facility')
                    ]
                )
            ]
        ])
        test_data.append(
            PrimaryConsentTestData(
                file=files.VibrentPrimaryConsentFile(pdf=pdf, blob=mock.MagicMock()),
                expected_signature=None,
                expected_sign_date=None,
                expected_to_be_va_file=True
            )
        )

        # Build file with an empty text element instead of a signature and date
        pdf = self._build_pdf(pages=[
            [
                *description_elements,
                self._build_form_element(text='', bbox=(116, 147, 521, 171)),
                self._build_form_element(text='', bbox=(116, 97, 266, 119))
            ]
        ])
        test_data.append(
            PrimaryConsentTestData(
                file=files.VibrentPrimaryConsentFile(pdf=pdf, blob=mock.MagicMock()),
                expected_signature=None,
                expected_sign_date=None
            )
        )

        # Build consent with an image instead of a typed signature
        pdf = self._build_pdf(pages=[
            [
                *description_elements,
                self._build_form_element(
                    bbox=(200, 125, 400, 191),
                    children=[
                        self._build_pdf_element(cls=LTImage, bbox=(200, 125, 400, 191))
                    ]
                ),
                self._build_form_element(text='December 7, 2018', bbox=(116, 97, 266, 119))
            ]
        ])
        test_data.append(
            PrimaryConsentTestData(
                file=files.VibrentPrimaryConsentFile(pdf=pdf, blob=mock.MagicMock()),
                expected_signature=True,
                expected_sign_date=date(2018, 12, 7)
            )
        )

        # Build older style consent with different signature description formatting
        pdf = self._build_pdf(pages=[
            [
                self._build_pdf_element(
                    cls=LTTextBoxHorizontal,
                    children=[
                        self._build_pdf_element(
                            cls=LTTextLineHorizontal,
                            text='this form. All of my questions have been answered. I freely and\n'
                        ),
                        self._build_pdf_element(
                            cls=LTTextLineHorizontal,
                            text='willingly choose to take part in the All of Us Research Program.\n'
                        ),
                    ]
                ),
                self._build_pdf_element(
                    cls=LTTextBoxHorizontal,
                    children=[
                        self._build_pdf_element(
                            cls=LTTextLineHorizontal,
                            children=[
                                self._build_pdf_element(LTTextLineHorizontal, text='Sign Your \n'),
                                self._build_pdf_element(LTTextLineHorizontal, text='Full Name: \n')
                            ]
                        )
                    ]
                ),
                self._build_form_element(text='2018 Participant', bbox=(116, 147, 521, 171)),
                self._build_form_element(text='Feb 19, 2018', bbox=(116, 96, 521, 120))
            ]
        ])
        test_data.append(
            PrimaryConsentTestData(
                file=files.VibrentPrimaryConsentFile(pdf=pdf, blob=mock.MagicMock()),
                expected_signature='2018 Participant',
                expected_sign_date=date(2018, 2, 19)
            )
        )

        # Build Spanish version of the Primary file
        pdf = self._build_pdf(pages=[
            [
                self._build_pdf_element(
                    cls=LTTextBoxHorizontal,
                    children=[
                        self._build_pdf_element(
                            cls=LTTextLineHorizontal,
                            text='Decido participar libremente y por voluntad propia'
                        )
                    ]
                ),
                self._build_pdf_element(
                    cls=LTTextBoxHorizontal,
                    children=[
                        self._build_pdf_element(
                            cls=LTTextLineHorizontal,
                            children=[
                                self._build_pdf_element(LTTextLineHorizontal, text='Firme con su nombre completo:')
                            ]
                        )
                    ]
                ),
                self._build_form_element(text='Spanish Participant', bbox=(116, 147, 517, 169)),
                self._build_form_element(text='Mar 3, 2021', bbox=(116, 97, 266, 119))
            ]
        ])
        test_data.append(
            PrimaryConsentTestData(
                file=files.VibrentPrimaryConsentFile(pdf=pdf, blob=mock.MagicMock()),
                expected_signature='Spanish Participant',
                expected_sign_date=date(2021, 3, 3)
            )
        )

        return test_data

    def _get_ce_primary_test_data(self):
        basic_pdf = self._build_ce_pdf(pages=[
            CePdfPage(), CePdfPage(), CePdfPage(), CePdfPage(), CePdfPage(),
            CePdfPage(
                [
                    CePdfText(string='Test Name', starting_at=Rect.from_edges(52, 60, 757, 766)),
                    CePdfText(string="Participant's Name (printed)", starting_at=Rect.from_edges(52, 60, 747, 756)),
                    CePdfText(string='10/31/2021', starting_at=Rect.from_edges(392, 400, 757, 766)),
                    CePdfText(string='Date', starting_at=Rect.from_edges(392, 400, 747, 756))
                ]
            )
        ])
        basic_expected_data = PrimaryConsentTestData(
            file=files.CePrimaryConsentFile(pdf=basic_pdf, blob=mock.MagicMock()),
            expected_signature='Test Name',
            expected_sign_date=date(2021, 10, 31)
        )

        missing_signature_and_date = self._build_ce_pdf(pages=[
            CePdfPage(), CePdfPage(), CePdfPage(), CePdfPage(), CePdfPage(),
            CePdfPage()
        ])
        missing_expected_data = PrimaryConsentTestData(
            file=files.CePrimaryConsentFile(pdf=missing_signature_and_date, blob=mock.MagicMock()),
            expected_signature=None,
            expected_sign_date=None
        )

        # Some of the labels only have "'s Name" (missing Participant)
        partial_label = self._build_ce_pdf(pages=[
            CePdfPage(), CePdfPage(), CePdfPage(), CePdfPage(), CePdfPage(),
            CePdfPage(
                [
                    CePdfText(string='Partial Label', starting_at=Rect.from_edges(52, 60, 757, 766)),
                    CePdfText(string="'s Name (printed)", starting_at=Rect.from_edges(52, 60, 747, 756)),
                    CePdfText(string='10/31/2021', starting_at=Rect.from_edges(392, 400, 757, 766)),
                    CePdfText(string='Date', starting_at=Rect.from_edges(392, 400, 747, 756))
                ]
            )
        ])
        partial_label_expected_data = PrimaryConsentTestData(
            file=files.CePrimaryConsentFile(pdf=partial_label, blob=mock.MagicMock()),
            expected_signature='Partial Label',
            expected_sign_date=date(2021, 10, 31)
        )

        # Some of the signatures and dates are offset more than usual
        offset_label = self._build_ce_pdf(pages=[
            CePdfPage(), CePdfPage(), CePdfPage(), CePdfPage(), CePdfPage(),
            CePdfPage(
                [
                    CePdfText(string='Offset label', starting_at=Rect.from_edges(52, 60, 825, 835)),
                    CePdfText(string="Name (printed)", starting_at=Rect.from_edges(52, 60, 747, 756)),
                    CePdfText(string='10/31/2021', starting_at=Rect.from_edges(392, 400, 825, 835)),
                    CePdfText(string='Date', starting_at=Rect.from_edges(392, 400, 747, 756))
                ]
            )
        ])
        offset_label_expected_data = PrimaryConsentTestData(
            file=files.CePrimaryConsentFile(pdf=offset_label, blob=mock.MagicMock()),
            expected_signature='Offset label',
            expected_sign_date=date(2021, 10, 31)
        )

        return [basic_expected_data, missing_expected_data, partial_label_expected_data, offset_label_expected_data]

    def _get_vibrent_cabor_test_data(self) -> List['ConsentTestData']:
        """Builds a list of PDFs that represent the different layouts of Vibrent's CaBOR consent"""

        basic_cabor_pdf = self._build_pdf(pages=[
            [
                self._build_form_element(text='Test cabor', bbox=(116, 100, 517, 140)),
                self._build_form_element(text='April 27, 2020', bbox=(500, 100, 600, 140))
            ]
        ])
        basic_cabor_case = ConsentTestData(
            file=files.VibrentCaborConsentFile(pdf=basic_cabor_pdf, blob=mock.MagicMock()),
            expected_signature='Test cabor',
            expected_sign_date=date(2020, 4, 27)
        )

        older_cabor_pdf = self._build_pdf(pages=[
            [
                self._build_form_element(text='2017 Cabor', bbox=(150, 150, 350, 188)),
                self._build_form_element(text='Sep 8, 2017', bbox=(434, 153, 527, 182))
            ]
        ])
        older_cabor_case = ConsentTestData(
            file=files.VibrentCaborConsentFile(pdf=older_cabor_pdf, blob=mock.MagicMock()),
            expected_signature='2017 Cabor',
            expected_sign_date=date(2017, 9, 8)
        )

        return [basic_cabor_case, older_cabor_case]

    def _get_ce_cabor_test_data(self):
        basic_pdf = self._build_ce_pdf(pages=[
            CePdfPage(),
            CePdfPage(
                [
                    CePdfText(string='Test Cabor', starting_at=Rect.from_edges(52, 60, 789, 798)),
                    CePdfText(string="Participant's Name (printed)", starting_at=Rect.from_edges(52, 60, 779, 788)),
                    CePdfText(string='12/09/2020', starting_at=Rect.from_edges(392, 400, 789, 798)),
                    CePdfText(string='Date', starting_at=Rect.from_edges(392, 400, 779, 788))
                ]
            )
        ])
        basic_expected_data = ConsentTestData(
            file=files.CeCaborConsentFile(pdf=basic_pdf, blob=mock.MagicMock()),
            expected_signature='Test Cabor',
            expected_sign_date=date(2020, 12, 9)
        )

        missing_signature_and_date = self._build_ce_pdf(pages=[
            CePdfPage(), CePdfPage()
        ])
        missing_expected_data = ConsentTestData(
            file=files.CeCaborConsentFile(pdf=missing_signature_and_date, blob=mock.MagicMock()),
            expected_signature=None,
            expected_sign_date=None
        )

        return [basic_expected_data, missing_expected_data]

    def _get_vibrent_ehr_test_data(self) -> List['EhrConsentTestData']:
        six_empty_pages = [[], [], [], [], [], []]  # The EHR signature is expected to be on the 7th page
        basic_ehr_pdf = self._build_pdf(pages=[
            *six_empty_pages,
            [
                self._build_pdf_element(
                    cls=LTTextLineHorizontal,
                    text='You will have access to a signed copy of this form',
                ),
                self._build_form_element(text='Test ehr', bbox=(125, 150, 450, 180)),
                self._build_form_element(text='Dec 21, 2019', bbox=(125, 100, 450, 130))
            ]
        ])
        basic_ehr_case = EhrConsentTestData(
            file=files.VibrentEhrConsentFile(pdf=basic_ehr_pdf, blob=mock.MagicMock()),
            expected_signature='Test ehr',
            expected_sign_date=date(2019, 12, 21)
        )

        va_ehr_pdf = self._build_pdf(pages=[
            *six_empty_pages,
            [
                self._build_pdf_element(
                    cls=LTTextLineHorizontal,
                    text='You will have access to a signed copy of this form',
                ),
                self._build_pdf_element(
                    cls=LTTextLineHorizontal,
                    text='We may ask you to go to a local clinic to be measured'
                ),
                self._build_form_element(text='Test va ehr', bbox=(125, 150, 450, 180)),
                self._build_form_element(text='Oct 10, 2020', bbox=(125, 100, 450, 130))
            ]
        ])
        va_ehr_case = EhrConsentTestData(
            file=files.VibrentEhrConsentFile(pdf=va_ehr_pdf, blob=mock.MagicMock()),
            expected_signature='Test va ehr',
            expected_sign_date=date(2020, 10, 10),
            expected_to_be_va_file=True
        )

        return [basic_ehr_case, va_ehr_case]

    def _build_sensitive_ehr(self, with_initials: bool) -> files.EhrConsentFile:
        seven_empty_pages = [[], [], [], [], [], [], []]
        sensitive_agreement_page = [
            self._build_pdf_element(
                cls=LTTextLineHorizontal,
                text='I agree to release sensitive information from my EHRs'
            )
        ]
        if with_initials:
            sensitive_agreement_page.extend([
                self._build_form_element(text='Test', bbox=(80, 336, 135, 363)),
                self._build_form_element(text='Test', bbox=(80, 382, 135, 409)),
                self._build_form_element(text='Test', bbox=(80, 437, 135, 464)),
                self._build_form_element(text='Test', bbox=(80, 483, 135, 510)),
                self._build_form_element(text='Test', bbox=(80, 529, 135, 556))
            ])
        sensitive_pdf = self._build_pdf(pages=[
            *seven_empty_pages,
            sensitive_agreement_page
        ])

        return files.VibrentEhrConsentFile(pdf=sensitive_pdf, blob=mock.MagicMock())

    def _get_ce_ehr_test_data(self):
        basic_pdf = self._build_ce_pdf(pages=[
            CePdfPage(
                [
                    CePdfText(string='Test EHR', starting_at=Rect.from_edges(52, 60, 736, 745)),
                    CePdfText(string="Participant's Name (printed)", starting_at=Rect.from_edges(52, 60, 726, 735)),
                    CePdfText(string='2/3/2020', starting_at=Rect.from_edges(392, 400, 736, 745)),
                    CePdfText(string='Date', starting_at=Rect.from_edges(392, 400, 726, 735))
                ]
            )
        ])
        basic_expected_data = EhrConsentTestData(
            file=files.CeEhrConsentFile(pdf=basic_pdf, blob=mock.MagicMock()),
            expected_signature='Test EHR',
            expected_sign_date=date(2020, 2, 3)
        )

        missing_signature_and_date = self._build_ce_pdf(pages=[
            CePdfPage(), CePdfPage(),
            CePdfPage()
        ])
        missing_expected_data = EhrConsentTestData(
            file=files.CeEhrConsentFile(pdf=missing_signature_and_date, blob=mock.MagicMock()),
            expected_signature=None,
            expected_sign_date=None
        )

        return [basic_expected_data, missing_expected_data]

    def _get_vibrent_gror_test_data(self) -> List['GrorConsentTestData']:
        # The GROR signature is expected to be on the 10th page
        nine_empty_pages = [
            [], [], [], [], [], [], [], [], []
        ]
        basic_gror_pdf = self._build_pdf(pages=[
            *nine_empty_pages,
            [
                self._build_form_element(
                    children=[self._build_pdf_element(LTCurve)],
                    bbox=(65, 470, 75, 480)
                ),
                self._build_form_element(text='Test gror', bbox=(140, 150, 450, 180)),
                self._build_form_element(text='Jan 1st, 2021', bbox=(125, 100, 450, 130))
            ]
        ])
        basic_gror_case = GrorConsentTestData(
            file=files.VibrentGrorConsentFile(pdf=basic_gror_pdf, blob=mock.MagicMock()),
            expected_signature='Test gror',
            expected_sign_date=date(2021, 1, 1),
            has_yes_selected=True
        )

        gror_missing_check = self._build_pdf(pages=[
            *nine_empty_pages,
            [
                self._build_form_element(text='no confirmation', bbox=(140, 150, 450, 180)),
                self._build_form_element(text='Feb 1st, 2021', bbox=(125, 100, 450, 130))
            ]
        ])
        no_confirmation_case = GrorConsentTestData(
            file=files.VibrentGrorConsentFile(pdf=gror_missing_check, blob=mock.MagicMock()),
            expected_signature='no confirmation',
            expected_sign_date=date(2021, 2, 1),
            has_yes_selected=False
        )

        spanish_gror_pdf = self._build_pdf(pages=[
            *nine_empty_pages,
            [
                self._build_pdf_element(
                    cls=LTTextLineHorizontal,
                    text='¿Desea conocer alguno de sus resultados de ADN?'
                ),
                self._build_form_element(
                    children=[self._build_pdf_element(LTCurve)],
                    bbox=(30, 478, 40, 488)
                ),
                self._build_form_element(text='spanish gror', bbox=(140, 150, 450, 180)),
                self._build_form_element(text='May 1st, 2018', bbox=(125, 100, 450, 130))
            ]
        ])
        spanish_gror_case = GrorConsentTestData(
            file=files.VibrentGrorConsentFile(pdf=spanish_gror_pdf, blob=mock.MagicMock()),
            expected_signature='spanish gror',
            expected_sign_date=date(2018, 5, 1),
            has_yes_selected=True
        )

        return [basic_gror_case, no_confirmation_case, spanish_gror_case]

    def _get_ce_gror_test_data(self):
        basic_pdf = self._build_ce_pdf(pages=[
            CePdfPage(
                [
                    CePdfText(string='Test GROR', starting_at=Rect.from_edges(52, 60, 789, 798)),
                    CePdfText(string="Participant's Name (printed)", starting_at=Rect.from_edges(52, 60, 779, 788)),
                    CePdfText(string='Apr 1, 2018', starting_at=Rect.from_edges(392, 400, 789, 798)),
                    CePdfText(string='Date', starting_at=Rect.from_edges(392, 400, 779, 788))
                ]
            )
        ])
        basic_expected_data = GrorConsentTestData(
            file=files.CeGrorConsentFile(pdf=basic_pdf, blob=mock.MagicMock()),
            expected_signature='Test GROR',
            expected_sign_date=date(2018, 4, 1),
            has_yes_selected=True
        )

        missing_signature_and_date = self._build_ce_pdf(pages=[
            CePdfPage(), CePdfPage(), CePdfPage(), CePdfPage(),
            CePdfPage()
        ])
        missing_expected_data = GrorConsentTestData(
            file=files.CeGrorConsentFile(pdf=missing_signature_and_date, blob=mock.MagicMock()),
            expected_signature=None,
            expected_sign_date=None,
            has_yes_selected=True
        )

        return [basic_expected_data, missing_expected_data]

    def _get_vibrent_primary_update_test_data(self) -> List['PrimaryUpdateConsentTestData']:
        basic_update_pdf = self._build_pdf(pages=[
            [
                self._build_pdf_element(
                    cls=LTTextBoxHorizontal,
                    children=[
                        self._build_pdf_element(cls=LTTextLineHorizontal, text='Do you agree to this updated consent?')
                    ]
                ),
                self._build_form_element(
                    children=[self._build_pdf_element(LTChar, text='4')],
                    bbox=(34, 669, 45, 683)
                ),
                self._build_form_element(text='Test update', bbox=(116, 146, 521, 170)),
                self._build_form_element(text='Jan 1st, 2021', bbox=(116, 96, 521, 120))
            ]
        ])
        basic_update_case = PrimaryUpdateConsentTestData(
            file=files.VibrentPrimaryConsentUpdateFile(
                pdf=basic_update_pdf,
                blob=mock.MagicMock(),
                consent_date=datetime.now()
            ),
            expected_signature='Test update',
            expected_sign_date=date(2021, 1, 1),
            has_yes_selected=True,
            expected_to_be_va_file=False
        )

        va_update_pdf = self._build_pdf(pages=[
            [
                self._build_pdf_element(
                    cls=LTTextBoxHorizontal,
                    children=[
                        self._build_pdf_element(cls=LTTextLineHorizontal, text='Do you agree to this updated consent?')
                    ]
                ),
                self._build_pdf_element(
                    cls=LTTextBoxHorizontal,
                    children=[
                        self._build_pdf_element(cls=LTTextLineHorizontal, text='you will get care at a VA facility')
                    ]
                ),
                self._build_form_element(text='Test update', bbox=(116, 146, 521, 170)),
                self._build_form_element(text='Jan 1st, 2021', bbox=(116, 96, 521, 120))
            ]
        ])
        va_update_case = PrimaryUpdateConsentTestData(
            file=files.VibrentPrimaryConsentUpdateFile(
                pdf=va_update_pdf,
                blob=mock.MagicMock(),
                consent_date=datetime.now()
            ),
            expected_signature='Test update',
            expected_sign_date=date(2021, 1, 1),
            has_yes_selected=False,
            expected_to_be_va_file=True
        )

        # Build basic primary file for older version of PrimaryUpdate
        pdf = self._build_pdf(pages=[
            [
                *self._get_primary_consent_elements(),
                self._build_form_element(text='Test Name', bbox=(116, 147, 517, 169)),
                self._build_form_element(text='Aug 9, 2020', bbox=(116, 97, 266, 119))
            ]
        ])
        older_update_case = PrimaryUpdateConsentTestData(
            file=files.VibrentPrimaryConsentUpdateFile(
                pdf=pdf,
                blob=mock.MagicMock(),
                consent_date=datetime(2020, 8, 9)
            ),
            expected_signature='Test Name',
            expected_sign_date=date(2020, 8, 9),
            has_yes_selected=True
        )

        return [basic_update_case, va_update_case, older_update_case]

    @classmethod
    def _build_pdf(cls, pages) -> files.Pdf:
        """
        Builds a consent_files.Pdf object
        :param pages A list where each item represents a page,
            and each item is a list of pdf elements for what should be on that page
        """
        page_mocks = []
        for page_elements in pages:
            page_mock = mock.MagicMock(spec=LTPage)
            page_mock.__iter__.return_value = page_elements
            page_mock.testcontents = page_elements
            page_mocks.append(page_mock)

        return files.Pdf(pages=page_mocks, blob=mock.MagicMock())

    def _build_pdf_element(self, cls, text: str = None, children: list = None, bbox=None):
        """Create a generic pdf element to add to the page"""
        element = mock.MagicMock(spec=cls)
        self._set_bbox(bbox, element)

        if children:
            element.__iter__.return_value = children

        if hasattr(element, 'get_text'):
            if text is None:
                get_text_result = ''.join([child.get_text() for child in children])
            else:
                get_text_result = text
            element.get_text.return_value = get_text_result

        return element

    def _build_form_element(self, bbox, text: str = None, children: list = None):
        """
        Form elements don't have a get_text method, and (at least with the Vibrent PDFs) any text within them is
        laid out character by character
        """
        element = mock.MagicMock(spec=LTFigure)
        self._set_bbox(bbox, element)

        iterable_children = []
        if children:
            iterable_children = children
        else:
            for char_str in text:
                char_element = mock.MagicMock(spec=LTChar)
                char_element.get_text.return_value = char_str
                iterable_children.append(char_element)
            if text == '':
                char_element = mock.MagicMock(spec=LTChar)
                char_element.get_text.return_value = ''
                iterable_children.append(char_element)

        element.__iter__.return_value = iterable_children
        element.__len__.return_value = len(iterable_children)
        return element

    def _set_bbox(self, bbox, element_mock):
        """Set the data for a PDF element's bounding box on the Mock object"""
        if not bbox:
            left, bottom = self.fake.random_int(), self.fake.random_int()
            right, top = self.fake.random_int() + left, self.fake.random_int() + bottom
            bbox = (left, bottom, right, top)

        (x0, y0, x1, y1) = bbox
        element_mock.x0 = x0
        element_mock.y0 = y0
        element_mock.x1 = x1
        element_mock.y1 = y1
        element_mock.width = x1-x0
        element_mock.height = y1-y0
        element_mock.bbox = bbox

    def _build_ce_pdf(self, pages: Collection['CePdfPage']):
        return self._build_pdf([[self._build_ce_page_figure(page)] for page in pages])

    def _build_ce_page_figure(self, page: 'CePdfPage'):
        figure_mock = mock.MagicMock(spec=LTFigure)
        ce_text_chars = []
        for text in page.texts:
            ce_text_chars.extend(self._ce_chars_from_text(text))
        figure_mock.__iter__.return_value = ce_text_chars
        figure_mock.testfigcontents = ce_text_chars
        return figure_mock

    def _ce_chars_from_text(self, text: 'CePdfText'):
        current_rect = text.starting_at
        chars = []
        for char in text.string:
            chars.append(
                self._build_pdf_element(
                    cls=LTChar,
                    text=char,
                    bbox=(current_rect.left, current_rect.bottom, current_rect.right, current_rect.top)
                )
            )
            current_rect.left += 4
        return chars

    def _get_vibrent_etm_test_data(self):
        # The EtM signature is expected to be on the 8th page
        seven_empty_pages = [
            [], [], [], [], [], [], []
        ]
        basic_etm_pdf = self._build_pdf(pages=[
            *seven_empty_pages,
            [
                self._build_form_element(
                    children=[self._build_pdf_element(LTCurve)],
                    bbox=(65, 470, 75, 480)
                ),
                self._build_form_element(text='Test etm', bbox=(140, 150, 450, 180)),
                self._build_form_element(text='Jan 1st, 2021', bbox=(125, 100, 450, 130))
            ]
        ])
        basic_etm_case = ConsentTestData(
            file=files.VibrentEtmConsentFile(pdf=basic_etm_pdf, blob=mock.MagicMock()),
            expected_signature='Test etm',
            expected_sign_date=date(2021, 1, 1)
        )

        return [basic_etm_case]


@dataclass
class ConsentTestData:
    file: files.ConsentFile
    expected_signature: str or bool  # Text of the signature, or True if it's an image
    expected_sign_date: date or None


@dataclass
class PrimaryConsentTestData(ConsentTestData):
    file: files.PrimaryConsentFile
    expected_to_be_va_file: bool = False


@dataclass
class EhrConsentTestData(ConsentTestData):
    file: files.EhrConsentFile
    expected_to_be_va_file: bool = False


@dataclass
class GrorConsentTestData(ConsentTestData):
    file: files.GrorConsentFile
    has_yes_selected: bool = False


@dataclass
class PrimaryUpdateConsentTestData(ConsentTestData):
    file: files.PrimaryConsentUpdateFile
    has_yes_selected: bool = False
    expected_to_be_va_file: bool = False


@dataclass
class CePdfPage:
    texts: Collection['CePdfText'] = field(default_factory=list)


@dataclass
class CePdfText:
    starting_at: Rect
    string: str
