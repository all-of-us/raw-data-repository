class BaseMessageBroker:
    def __init__(self, message):
        self.dest_name = message.messageDest


class PTSCMessageBroker(BaseMessageBroker):
    def __init__(self, message):
        super(PTSCMessageBroker, self).__init__(message)
