#!/usr/bin/env python
"""Generates release notes."""

import re
import subprocess

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


def _get_tag_output_lines():
  """Lists lines with git tags, ordered most recent first. May include non-release tags."""
  process = subprocess.Popen(
      ['git', 'log', '--tags', '--simplify-by-decoration', '--pretty=format:%ci %d'],
      stdout=subprocess.PIPE)
  if process.wait() != 0:
    raise RuntimeError('Getting tags failed.')
  stdout, _ = process.communicate()
  return stdout.split('\n')


def _get_current_and_prev_release_tags(tag_lines):
  """Extracts the current and previous (or None) releases' git tags from ordered git tag lines."""
  release_tags = []
  for line in tag_lines:
    line_tags = _RELEASE_TAG_RE.findall(line)
    if line_tags:
      if len(set(line_tags)) != 1:
        raise RuntimeError('Multiple releases %s on same commit %r.' % (line_tags, line))
      release_tags.append(line_tags[0])
  if len(release_tags) >= 2:
    return release_tags[:2]
  elif len(release_tags) == 0:
    raise RuntimeError('No valid release tags.')
  else:
    return release_tags[0], None


def _linkify_pull_request_ids(text):
  return re.sub(
      r'\(#([0-9]+)\)',
      r'([#\1|%s/pull/\1])' % _REPO_BASE_URL,
      text)


def _get_release_notes(current_tag, prev_tag):
  """Formats release notes from git commit messages between the given tags."""
  if prev_tag is not None:
    version_range = '%s..%s' % (prev_tag, current_tag)
  else:
    version_range = current_tag
  process = subprocess.Popen(
      ['git', 'log', version_range, _LOG_LINE_FORMAT],
      stdout=subprocess.PIPE)
  if process.wait() != 0:
    raise RuntimeError('Getting commit messages failed.')
  stdout, _ = process.communicate()
  return _RELEASE_NOTES_T % {
    'current': current_tag,
    'prev': prev_tag or 'beginning of history',
    'history': _linkify_pull_request_ids(stdout),
  }


if __name__ == '__main__':
  current, prev = _get_current_and_prev_release_tags(_get_tag_output_lines())
  print _get_release_notes(current, prev)
