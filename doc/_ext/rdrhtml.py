from docutils.nodes import Element
from sphinx.application import Sphinx
from sphinx.builders.html import StandaloneHTMLBuilder
from sphinx.writers.html5 import HTML5Translator


class RdrHtml5Translator(HTML5Translator):
    def __init__(self, *args):
        super(RdrHtml5Translator, self).__init__(*args)

        self._is_visiting_complex_signature = False

    def visit_desc_signature(self, node: Element) -> None:
        # If we're visiting a signature that has more than two children, it's most likely a
        # class or function signature. Leaving them out to simplify the look and feel of the documentation.
        self._is_visiting_complex_signature = len(node.children) > 2

        if not self._is_visiting_complex_signature:
            super(RdrHtml5Translator, self).visit_desc_signature(node)
        else:
            node.children = []  # Remove the signature children so we don't print those out

    def depart_desc_signature(self, node: Element) -> None:
        if self._is_visiting_complex_signature:
            self._is_visiting_complex_signature = False
        else:
            super(RdrHtml5Translator, self).depart_desc_signature(node)


class RdrHtmlBuilder(StandaloneHTMLBuilder):
    name = 'html'  # Name must match the builder we're trying to override

    @property
    def default_translator_class(self):
        return RdrHtml5Translator


def setup(app: Sphinx):
    # Override the HTML builder so we can provide our own translator to get things looking just right
    app.add_builder(RdrHtmlBuilder, override=True)
