#!/usr/bin/env python
"""Generates release notes for a given deployed AppEngine environment based on code at HEAD.

We expect deployed versions to be named with a git tag like v0-1-rc14. This script gets a project's
current version, then gets commit messages from that tag to HEAD, and formats the messages (with
JIRA's style of markup) to make release notes. HEAD is assumed (but not required) to also be tagged
for the release that's going out / the release for which we're generating notes.
"""

import httplib
import json
import logging
import re
import subprocess
import sys
import urllib2

_REPO_BASE_URL = 'https://github.com/vanderbilt/pmi-data'

# Git tags of this format denote releases.
_RELEASE_TAG_RE = re.compile(r'v[0-9]+(?:-[0-9]+)*-rc[0-9]+')

# Formatting for release notes in JIRA comments.
# Note that JIRA auto-linkifies JIRA IDs, so avoid using commit message text in a link.
_LOG_LINE_FORMAT = "--format=*   [%aN %ad|" + _REPO_BASE_URL + "/commit/%h] %s"
# Overall release notes template.
_RELEASE_NOTES_T = """h1. Release Notes for %(current)s
h2. since %(prev)s
%(history)s
"""


def _linkify_pull_request_ids(text):
  """Converts all substrings like "(#123)" to links to pull requests."""
  return re.sub(
      r'\(#([0-9]+)\)',
      r'([#\1|%s/pull/\1])' % _REPO_BASE_URL,
      text)


def _get_deployed_version(project_id):
  """Queries the app's version API (root path) for the currently serving version ID.

  This assumes app versions are of the form "<git tag>.<extra AppEngine identifier>", for example
  "v0-1-rc14.399070594381287524".

  Args:
    project_id: AppEngine project ID, for example all-of-us-rdr-staging. The appspot URL is
        constructed from this ID.

  Returns:
    A version ID / git tag of the currently serving code in the given environment.
  """
  version_url = 'https://%s.appspot.com/' % project_id
  response = urllib2.urlopen(version_url)
  if response.getcode() != httplib.OK:
    raise RuntimeError('HTTP %d for %r' % (response.getcode(), version_url))
  app_version = json.loads(response.read())['version_id']
  return app_version.split('.')[0]


def _get_release_notes_since_tag(deployed_tag):
  """Formats release notes for JIRA from commit messages, from the given tag to HEAD."""
  process = subprocess.Popen(
      ['git', 'log', deployed_tag + '..', _LOG_LINE_FORMAT],
      stdout=subprocess.PIPE)
  if process.wait() != 0:
    raise RuntimeError('Getting commit messages failed.')
  stdout, _ = process.communicate()
  commit_messages = stdout.decode('utf8')  # Keep further text processing from choking on non-ASCII.
  return _RELEASE_NOTES_T % {
    'current': _find_current_commit_tag(),
    'prev': deployed_tag,
    'history': _linkify_pull_request_ids(commit_messages),
  }


def _find_current_commit_tag():
  """Returns the current git tag (or tag + short commit hash) of the current commit."""
  process = subprocess.Popen(['git', 'describe', '--tags'], stdout=subprocess.PIPE)
  if process.wait() != 0:
    raise RuntimeError('Getting current tag.')
  stdout, _ = process.communicate()
  tag = stdout.strip()
  if not _RELEASE_TAG_RE.match(tag):
    logging.warning('Current tag %r does not look like a release tag.', tag)
  return tag


def main():
  """Looks up version tags, gets commit logs, and prints release notes."""
  if len(sys.argv) != 2:
    logging.critical('Usage: %s appengine_project_id', sys.argv[0])
    sys.exit(1)
  appengine_id = sys.argv[1]
  deployed_version = _get_deployed_version(appengine_id)
  if not _RELEASE_TAG_RE.match(deployed_version):
    logging.warning(
        'Tag %r from %r does not look like a release tag.', deployed_version, appengine_id)
  print _get_release_notes_since_tag(deployed_version)


if __name__ == '__main__':
  main()
