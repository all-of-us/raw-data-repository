#!/usr/bin/env python
"""Generates release notes for a given deployed AppEngine environment based on code at HEAD.

We expect deployed versions to be named with a git tag like v0-1-rc14. This script gets a project's
current version, then gets commit messages from that tag to HEAD, and formats the messages (with
JIRA's style of markup) to make release notes. HEAD is assumed (but not required) to also be tagged
for the release that's going out / the release for which we're generating notes.

This requires the
    JIRA_API_USER_PASSWORD and
    JIRA_API_USER_NAME
environment variables to be set. If it is also set, the comma-separated list of JIRA user names in
    JIRA_WATCHER_NAMES
will be set as watchers on newly created release trackers.
"""

import httplib
import json
import logging
import os
import re
import subprocess
import sys
import urllib2

import jira

_REPO_BASE_URL = 'https://github.com/all-of-us/raw-data-repository'

# Git tags of this format denote releases. Final letter suffixes denote cherry-picks.
# This should match old_circle.yml.
_RELEASE_TAG_RE = re.compile(r'v[0-9]+(?:-[0-9]+)*-rc[0-9]+[a-z]*$')
_CHERRY_PICK_RE = re.compile(r'(.*-rc[0-9]+)([a-z]*)')

# Formatting for release notes in JIRA comments.
# Note that JIRA auto-linkifies JIRA IDs, so avoid using commit message text in a link.
_LOG_LINE_FORMAT = "--format=*   [%aN %ad|" + _REPO_BASE_URL + "/commit/%h] %s"
# Overall release notes template.
_RELEASE_NOTES_T = """h1. Release Notes for %(current)s
h2. deployed to %(project)s, listing changes since %(prev)s
%(history)s
"""

_JIRA_INSTANCE_URL = 'https://precisionmedicineinitiative.atlassian.net/'
_JIRA_PROJECT_ID = 'PD'
_JIRA_NAME_VARNAME = 'JIRA_API_USER_NAME'
_JIRA_PASSWORD_VARNAME = 'JIRA_API_USER_PASSWORD'
_JIRA_WATCHERS_VARNAME = 'JIRA_WATCHER_NAMES'


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


def _get_release_notes_since_tag(deployed_tag, project_id, current_tag):
  """Formats release notes for JIRA from commit messages, from the given tag to HEAD."""
  process = subprocess.Popen(
      ['git', 'log', deployed_tag + '..', _LOG_LINE_FORMAT],
      stdout=subprocess.PIPE)
  if process.wait() != 0:
    raise RuntimeError('Getting commit messages failed.')
  stdout, _ = process.communicate()
  commit_messages = stdout.decode('utf8')  # Keep further text processing from choking on non-ASCII.
  return _RELEASE_NOTES_T % {
    'current': current_tag,
    'project': project_id,
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
  return tag


def _connect_to_jira():
  """Opens a JIRA API connection based on username/pw from env vars."""
  for varname in (_JIRA_PASSWORD_VARNAME, _JIRA_NAME_VARNAME):
    if varname not in os.environ:
      raise RuntimeError('No environment variable value for %r.' % varname)
  return jira.JIRA(
      _JIRA_INSTANCE_URL,
      basic_auth=(os.getenv(_JIRA_NAME_VARNAME), os.getenv(_JIRA_PASSWORD_VARNAME)))


def _strip_cherry_pick(version_id):
  """Returns a tuple of (version ID without cherry-pick suffix, boolean is_cherry_pick)."""
  match = _CHERRY_PICK_RE.search(version_id)
  if match is None:
    # Not a recognized format, don't try to parse it.
    return version_id, False
  else:
    return match.group(1), bool(match.group(2))


def _get_watchers():
  watchers = set()
  for name in [n.strip() for n in os.getenv(_JIRA_WATCHERS_VARNAME, '').split(',')]:
    if name:
      watchers.add(name)
  return watchers


def _update_or_create_release_tracker(jira_connection, full_version_id, release_notes):
  """Adds release notes to a new or existing JIRA issue."""
  version_id, is_cherry_pick = _strip_cherry_pick(full_version_id)
  summary = 'Release tracker for %s' % version_id
  issues = jira_connection.search_issues(
      'project = "%s" AND summary ~ "%s" ORDER BY created DESC' % (_JIRA_PROJECT_ID, summary))
  if issues:
    if len(issues) > 1:
      logging.warning(
          'Found multiple release tracker matches, using newest. %s',
          ', '.join('[%s] %s' % (issue.key, issue.fields().summary) for issue in issues))
    issue = issues[0]
    jira_connection.add_comment(issue, release_notes)
    what_happened = 'Updated'
  else:
    if is_cherry_pick:
      logging.warning(
          'Expected %r to exist since %s looks like a cherry-pick. Creating a new issue instead.',
          summary, full_version_id)
    issue = jira_connection.create_issue(
        project=_JIRA_PROJECT_ID,
        summary=summary,
        description=release_notes,
        issuetype={'name': 'Task'})
    for watcher_username in _get_watchers():
      try:
        jira_connection.add_watcher(issue, watcher_username)
      except jira.exceptions.JIRAError, e:
        logging.warning('Skipping invalid watcher %r (got %s).', watcher_username, e.status_code)
    what_happened = 'Created'
  logging.info('%s [%s] with release notes for %s.', what_happened, issue.key, full_version_id)


def main():
  """Looks up version tags, gets commit logs, and publishes release in JIRA."""
  logging.getLogger().setLevel(logging.INFO)
  if len(sys.argv) != 2:
    logging.critical('Usage: %s appengine_project_id', sys.argv[0])
    sys.exit(1)
  project_id = sys.argv[1]

  deployed_version = _get_deployed_version(project_id)
  if not _RELEASE_TAG_RE.match(deployed_version):
    logging.warning(
        'Tag %r from %r does not look like a release tag.', deployed_version, project_id)
  current_version = _find_current_commit_tag()
  if not _RELEASE_TAG_RE.match(current_version):
    logging.warning('Current tag %r does not look like a release tag.', current_version)

  jira_connection = _connect_to_jira()
  release_notes = _get_release_notes_since_tag(deployed_version, project_id, current_version)
  logging.info(release_notes)
  _update_or_create_release_tracker(jira_connection, current_version, release_notes)


if __name__ == '__main__':
  main()
