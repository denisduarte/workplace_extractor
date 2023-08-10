from workplace_extractor.Nodes.Node import Node
from workplace_extractor.Nodes.NodeCollection import PeopleCollection
from datetime import datetime


class PollOption(Node):
    def __init__(self, data):
        Node.__init__(self, str(data.get('id')))
        self.name = data.get('name')
        self.vote_count = data.get('vote_count')
        self.voters = PeopleCollection()

    def to_dict(self, extractor):
        as_dict = {
            'id': self.node_id,
            'name': self.name,
            'vote_count': self.vote_count,
            'voters': [person.to_dict(extractor) for person in self.voters.nodes]
        }

        return as_dict
