
from rdr_service.message_broker.message_broker import BaseMessageBroker
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'ghost-api-check'
tool_desc = 'Check for ghost participants against PTSC API'


class GhostApiCheck(ToolBase):
    def run(self):
        super(GhostApiCheck, self).run()
        self.messageDest = 'vibrent'
        base = BaseMessageBroker(self)
        token = base.get_access_token()
        print(token)


def run():
    return cli_run(tool_cmd, tool_desc, GhostApiCheck)
