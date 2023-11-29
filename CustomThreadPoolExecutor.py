from threading import Thread
import queue
from time import sleep
from random import randint
import random
import string

def random_alphanumeric_hash(length=16):
    all_characters = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(all_characters) for _ in range(length))
    return random_string

class CustomThreadPool:

    def __init__(self, max_workers=10):
        self.max_workers = max_workers
        self.workers = []
        self.task_queue = queue.Queue()
        self.result_queue = queue.Queue()
        self.total_tasks = 0
        self.completed_tasks = 0
        
    def _worker(self):

        while True:
            task_id, task, args, kwargs = self.task_queue.get()
            if task is None:
                break

            try:
                result = task(*args, **kwargs)
                self.result_queue.put((task_id, result))
            except Exception as e:
                self.result_queue.put((task_id, e))
                
            self.completed_tasks += 1
            self.task_queue.task_done()

    def submit(self, task, *args, **kwargs):
        task_id = random_alphanumeric_hash()  # Unique ID for the task
        self.task_queue.put((task_id, task, args, kwargs))
        self.total_tasks += 1
        return task_id

            

    def get_results(self):
        if self.completed_tasks == self.total_tasks:
            while not self.result_queue.empty():
                try:
                    task_id, result = self.result_queue.get()
                    yield task_id, result
                except queue.Empty:
                    break
        else:        
            while self.completed_tasks < self.total_tasks:
                try:
                    task_id, result = self.result_queue.get()
                    yield task_id, result
                except queue.Empty:
                    break

    def start(self):
        for _ in range(self.max_workers):
            worker = Thread(target=self._worker)
            worker.start()
            self.workers.append(worker)

    def wait_completion(self):
        self.task_queue.join()

        # Signal threads to stop
        for _ in range(self.max_workers):
            self.task_queue.put((None, None, None, None))

        # Wait for all threads to complete
        for worker in self.workers:
            worker.join()
