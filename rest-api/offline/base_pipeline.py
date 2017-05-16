import datetime
import logging
import os

from google.appengine.ext import db
from google.appengine.api import mail
import pipeline

import config


class BasePipeline(pipeline.Pipeline):

  def handle_pipeline_failure(self):
    """Invoked when a pipeline fails. Subclasses can override to implement custom behavior."""
    pass

  def finalized(self):
    """Finalizes this Pipeline after execution.

    Sends an e-mail to us if a pipeline fails; otherwise just logs an info message.
    """
    if self.pipeline_id == self.root_pipeline_id:
      app_id = os.environ['APPLICATION_ID']
      shard_index = app_id.find('~')
      if shard_index != -1:
        app_id = app_id[shard_index + 1:]
      pipeline_name = self.__class__.__name__
      base_path = '%s.appspot.com%s' % (app_id, self.base_path)
      status_link = 'http://%s/status?root=%s' % (base_path, self.root_pipeline_id)

      pipeline_record = db.get(self._root_pipeline_key)
      suffix = ''
      if pipeline_record and pipeline_record.start_time:
        duration = datetime.datetime.utcnow() - pipeline_record.start_time
        seconds = duration.total_seconds()
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        suffix = 'after %d:%02d:%02d' % (hours, minutes, seconds)
      if self.was_aborted:
        self.handle_pipeline_failure()
        message = "%s failed %s; results are at %s" % (pipeline_name, suffix, status_link)
        # This sender needs to be authorized per-environment in Email Authorized Senders,
        # see https://cloud.google.com/appengine/docs/standard/python/mail/.
        sender = config.getSetting(config.INTERNAL_STATUS_MAIL_SENDER)
        to_list = config.getSettingList(config.INTERNAL_STATUS_MAIL_RECIPIENTS)
        logging.error('%s (notification will be sent from %r to %r)', message, sender, to_list)
        try:
          mail.send_mail(sender, to_list, '%s failed' % pipeline_name, message)
        except (mail.InvalidSenderError, mail.InvalidEmailError):
          logging.error(
              'Could not send result email for root pipeline ID %r from sender %r.',
              self.root_pipeline_id, sender, exc_info=True)
      else:
        message = "%s succeeded %s; results are at %s" % (pipeline_name, suffix, status_link)
        logging.info(message)
