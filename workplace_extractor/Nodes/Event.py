from workplace_extractor.Nodes.Node import Node
from workplace_extractor.Nodes.NodeCollection import PeopleCollection
from datetime import datetime


class Event(Node):
    def __init__(self, data):
        Node.__init__(self, str(data.get('id')))
        self.name = data.get('name')
        self.start_time = data.get('start_time')
        self.end_time = data.get('end_time')
        self.description = data.get('description')
        self.type = data.get('type')
        self.owner = None
        self.attendees = PeopleCollection()

    def to_dict(self, extractor):
        as_dict = {
            'id': self.node_id,
            'name': self.name,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'description': self.description,
            'type': self.type,
            'owner': self.owner.to_dict(extractor),
            'attendees': [person.to_dict(extractor) for person in self.attendees.nodes]
        }

        return as_dict
