#!/usr/bin/env python
from __future__ import unicode_literals

import argparse
import logging
import os
import re
import sys

import pyprinttree


ID_FROM_FILENAME_PATTERN = re.compile(r'^([A-Za-z0-9]+)_')
ID_FROM_FILE_PATTERN = re.compile(r'''^revision\s*=\s*['"]([A-Za-z0-9]+)['"]''')
PARENT_ID_FROM_FILE_PATTERN = re.compile(r'''^down_revision\s*=\s*['"]([A-Za-z0-9]+)['"]''')


def get_alembic_path():
  return os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '../rest-api/alembic/versions'
  )


class AlembicProblem(Exception):
  pass


class AlembicParsingProblem(AlembicProblem):
  pass


class AlembicNode(pyprinttree.Node):

  @classmethod
  def create_from_filename(cls, filename):
    basename = os.path.basename(filename)
    match = re.search(ID_FROM_FILENAME_PATTERN, basename)
    if not match:
      return None
    filename_id = match.group(1)
    logging.debug('found filename_id: %s', filename_id)
    file_id = file_parent_id = None
    with open(filename, 'r') as file_handle:
      for line in file_handle:
        file_id_match = re.search(ID_FROM_FILE_PATTERN, line)
        if file_id_match:
          file_id = file_id_match.group(1)
        file_parent_id_match = re.search(PARENT_ID_FROM_FILE_PATTERN, line)
        if file_parent_id_match:
          file_parent_id = file_parent_id_match.group(1)
        if file_id and file_parent_id:
          break
    logging.debug('found file_id: %s', file_id)
    logging.debug('found file_parent_id: %s', file_parent_id)
    if not filename_id == file_id:
      raise AlembicParsingProblem(
        "Revision ID from filename does not match contents in {}".format(basename))
    return cls(basename, file_id, file_parent_id)

  def __init__(self, basename, id_, parent_id):
    pyprinttree.Node.__init__(self, id_)
    self.basename = basename
    self.parent_id = parent_id

  def __repr__(self):
    return str(self)

  def __str__(self):
    return self.basename


class AlembicTree(pyprinttree.Tree):

  @classmethod
  def create_from_versions_filepath(cls, alembic_versions_filepath):
    logging.debug('alembic_versions_filepath: %s', alembic_versions_filepath)
    tree = AlembicTree()
    # loading loop, parses and registers Node instances with tree
    for basename in os.listdir(alembic_versions_filepath):
      if basename.endswith('.pyc'):
        continue
      filename = os.path.join(alembic_versions_filepath, basename)
      logging.debug('found file: %s', filename)
      tree.add(AlembicNode.create_from_filename(filename))
    # linking loop, creates edges
    for node in tree.nodes.values():
      if node.parent_id:
        tree.add(node.parent_id, node)
    return tree

  def validate(self):
    root_nodes = self.get_roots()
    if len(root_nodes) > 1:
      raise AlembicProblem("Alembic tree has more than one ROOT: {}".format(root_nodes))
    head_nodes = self.get_leaves()
    if len(head_nodes) > 1:
      raise AlembicProblem("Alembic tree has more than one HEAD: {}".format(head_nodes))

  def render(self, stream=sys.stdout):
    pyprinttree.render_tree(self, stream)





parser = argparse.ArgumentParser()
parser.add_argument('--verbose', '-v', action='store_true', default=False)
parser.add_argument('--graph', '-g', action='store_true', default=False)


def main():
  args = parser.parse_args()
  logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING)
  sys.stdout.write("Checking integrity of Alembic tree... ")
  try:
    tree = AlembicTree.create_from_versions_filepath(get_alembic_path())
    tree.validate()
    sys.stdout.write("VALID\n")
    if args.graph:
      tree.render(sys.stdout)
  except AlembicParsingProblem, e:
    sys.stdout.write("ERROR\n")
    sys.stdout.write("{}\n".format(e))
    sys.exit(1)
  except AlembicProblem, e:
    sys.stdout.write("INVALID\n")
    if args.graph:
      tree.render(sys.stdout)
    sys.stdout.write('\n')
    sys.stdout.write("{}\n".format(e))
    sys.exit(1)


if __name__ == '__main__':
  main()
