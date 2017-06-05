#!/usr/bin/env python
"""Updates JIRA release notes when deploying to an environment.

This requires the
    JIRA_API_USER_PASSWORD and
    JIRA_API_USER_NAME
environment variables to be set, and flags for version and instance to be provided.
"""

import logging
import os
import jira
import sys

from tools.main_util import get_parser, configure_logging

_JIRA_INSTANCE_URL = 'https://precisionmedicineinitiative.atlassian.net/'
# Release tickets are moved from our usual project, DA, to the PD project
# for change approval, so for stable/prod releases look for tickets there.
_JIRA_PROJECT_ID = 'PD'

def _connect_to_jira(jira_username, jira_password):
  return jira.JIRA(_JIRA_INSTANCE_URL, basic_auth=(jira_username, jira_password))

def main(args):
  jira_username = os.getenv('JIRA_API_USER_NAME')
  jira_password = os.getenv('JIRA_API_USER_PASSWORD')
  if not jira_username or not jira_password:
    logging.error('JIRA_API_USER_NAME and JIRA_API_USER_PASSWORD variables must be set. Exiting.')
    sys.exit(-1)
  jira_connection = _connect_to_jira(jira_username, jira_password)
  summary = 'Release tracker for %s' % args.version
  issues = jira_connection.search_issues(
      'project = "%s" AND summary ~ "%s" ORDER BY created DESC' % (_JIRA_PROJECT_ID, summary))
  if issues:
    if len(issues) > 1:
      logging.warning(
          'Found multiple release tracker matches, using newest. %s',
          ', '.join('[%s] %s' % (issue.key, issue.fields().summary) for issue in issues))
    issue = issues[0]
    jira_connection.add_comment(issue, args.comment)
    logging.info('Updated issue %s', issue.key)
    sys.exit(0)
  else:
    logging.error(
        'No issue found with summary %r in project %r; exiting.', summary, _JIRA_PROJECT_ID)
    sys.exit(-1)

if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--version',
                      help='The version of the app being deployed (e.g. v0-1-rc21',
                      required=True)
  parser.add_argument('--comment',
                      type=str,
                      help='The comment to add to the issue',
                      required=True)
  main(parser.parse_args())
