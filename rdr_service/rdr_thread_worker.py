
from gunicorn.workers.gthread import ThreadWorker


class RdrThreadWorker(ThreadWorker):
    def accept(self, server, listener):
        if not self.alive:
            return

        super().accept(server, listener)
