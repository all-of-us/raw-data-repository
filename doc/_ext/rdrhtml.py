from docutils.nodes import Element
from sphinx.application import Sphinx
from sphinx.builders.html import StandaloneHTMLBuilder
from sphinx.writers.html5 import HTML5Translator


class RdrHtml5Translator(HTML5Translator):
    def __init__(self, *args):
        super(RdrHtml5Translator, self).__init__(*args)

        self._is_visiting_class_desc_signature = False

    @staticmethod
    def _get_node_text(node):
        return node.astext().strip()

    def visit_desc_signature(self, node: Element) -> None:
        for child in node.children:
            child_class_name = child.__class__.__name__
            if child_class_name == 'desc_annotation':
                annotation_node = child.children[0]
                if self._get_node_text(annotation_node) == 'class':
                    self._is_visiting_class_desc_signature = True
                    node.children = []  # Remove the signature children so we don't print those out

        if not self._is_visiting_class_desc_signature:
            self.body.append(self.starttag(node, 'dt'))

    def depart_desc_signature(self, node: Element) -> None:
        if self._is_visiting_class_desc_signature:
            self._is_visiting_class_desc_signature = False
        else:
            super(RdrHtml5Translator, self).depart_desc_signature(node)


class RdrHtmlBuilder(StandaloneHTMLBuilder):
    name = 'readthedocs'  # Name must match the builder we're trying to override

    @property
    def default_translator_class(self):
        return RdrHtml5Translator


def setup(app: Sphinx):
    # Override the HTML builder so we can provide our own translator to get things looking just right
    app.add_builder(RdrHtmlBuilder, override=True)
