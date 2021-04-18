class Node:
    def __init__(self, node_id):
        self.id = node_id

    def __str__(self):
        return f'{self._id}'

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value
