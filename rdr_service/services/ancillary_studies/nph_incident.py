import logging
from rdr_service import clock, config
from rdr_service.services.slack_utils import SlackMessageHandler

from rdr_service.model.study_nph import Incident as NphIncident
from rdr_service.dao.study_nph_dao import NphIncidentDao


def get_slack_message_handler() -> SlackMessageHandler:
    slack_config = config.getSettingJson(config.NPH_SLACK_WEBHOOKS, {})
    if slack_config is None:
        logging.warning("'slack_config' for 'NPH_SLACK_WEBHOOKS' is empty")

    webbook_url = slack_config.get('nph_incident_alerts', None)
    if webbook_url is None:
        logging.warning("'nph_incident_alerts' is not available in slack config. 'webhook_url' is None")
    return SlackMessageHandler(webhook_url=webbook_url)


def create_nph_incident(**kwargs):
    """
    Creates an NphIncident and sends alert to Slack if default
    for slack arg is True and saves an incident record to NphIncident
    if save_incident arg is True.
    """
    nph_incident_dao = NphIncidentDao()
    nph_incident_alert_slack = get_slack_message_handler()
    num_days = 7
    incident = None
    message = kwargs.get('message', None)
    created_incident = nph_incident_dao.get_by_message(message) if message else None
    today = clock.CLOCK.now()

    if created_incident and (today.date() - created_incident.created.date()).days <= num_days:
        return

    if kwargs.get('save_incident', False):
        insert_obj = nph_incident_dao.get_model_obj_from_items(kwargs.items())
        incident: NphIncident = nph_incident_dao.insert(insert_obj)

    if kwargs.get('slack', False):
        message_data = {'text': message}
        slack_alert = nph_incident_alert_slack.send_message_to_webhook(
            message_data=message_data
        )
        if slack_alert and incident:
            incident.notification_sent_flag = 1
            incident.notification_date = today
            nph_incident_dao.update(incident)

    logging.warning(message)
