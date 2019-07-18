

# all BQ schemas should be listed here
BQ_SCHEMAS = [
  # (python path, class)
  ('model.bq_participant_summary', 'BQParticipantSummary'),
]

BQ_VIEWS = [
  # (python path, var name, view name, view desc)
  ('model.bq_participant_summary', 'BQAnalyticsTeamParticipantSummary',
      'v_analytics_participant_summary', 'Analytics Team Participant Summary View'),
]
