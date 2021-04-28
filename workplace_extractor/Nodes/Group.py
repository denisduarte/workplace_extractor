from workplace_extractor.Nodes import Node
from workplace_extractor.Nodes.Feed import GroupFeed


class Group(Node):
    def __init__(self, data):
        Node.__init__(self, str(data.get('id')))
        self.name = data.get('name')
        self.privacy = data.get('privacy')
        self.feed = GroupFeed(data)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    def to_dict(self, export='posts'):
        as_dict = {
            'id': self.id,
            'name': self.name,
            'privacy': self.privacy
        }

        if export == 'posts':
            as_dict['feed'] = self.feed.to_dict() if self.feed is not None else {}

        return as_dict