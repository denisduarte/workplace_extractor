import logging


class Counter:
    def __init__(self, label):

        self.label = label
        self.total = 0
        self.count = 0

    def __str__(self):
        return f'progress: {(self.count * 100) // self.total}%'

    def increment(self):
        self.count += 1
        logging.debug(str(self))