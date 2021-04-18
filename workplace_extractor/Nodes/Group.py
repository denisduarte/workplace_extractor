from workplace_extractor.Nodes import Node


class Group(Node):
    def __init__(self, data):
        Node.__init__(self, str(data.get('id')))
        self.name = data.get('name')

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name
        }
