import threading
import queue

class WorkerThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self.queue = queue.Queue()
        self.alive = True

    def run(self):
        while self.alive:
            try:
                callback, args = self.queue.get(timeout=1)
            except queue.Empty:
                pass
            else:
                callback(*args)

    def kill(self):
        self.alive = False

    def put(self, callback, args):
        self.queue.put((callback, args))
