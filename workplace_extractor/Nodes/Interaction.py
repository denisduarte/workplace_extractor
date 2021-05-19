from workplace_extractor.Nodes.Node import Node
from workplace_extractor.Nodes.NodeCollection import NodeCollection


class View:
    def __init__(self):
        self.person = None

    def to_dict(self, extractor):
        return {
            'person': self.person.to_dict(extractor) if self.person is not None else {}
        }


class Reaction:
    def __init__(self, data):
        self.person = None
        self.reaction_type = data.get('type')

    def to_dict(self, extractor):
        return {
            'person': self.person.to_dict(extractor) if self.person is not None else {},
            'type': self.reaction_type
        }


class Comment(Node):
    def __init__(self, data):
        Node.__init__(self, data.get('id'))
        self.person = None
        self.reactions = []
        self.comments = NodeCollection()

    def to_dict(self, extractor):
        return {
            'id': self.node_id,
            'person': self.person.to_dict(extractor) if self.person is not None else {},
            'reactions': [reaction.to_dict(extractor) for reaction in self.reactions],
            'comments': [comment.to_dict(extractor) for comment in self.comments.nodes]
        }
