from workplace_extractor.Nodes.Node import Node
from workplace_extractor.Nodes.NodeCollection import PostCollection


class Group(Node):
    def __init__(self, data):
        Node.__init__(self, str(data.get('id')))
        self.name = data.get('name')
        self.privacy = data.get('privacy')
        self.admins = []
        if data.get('admins', {}).get('data') is not None:
            for admin in data.get('admins', {}).get('data'):
                self.admins.append(admin['email'] if 'email' in admin else admin['id'])

        self.feed = PostCollection()

    def to_dict(self, extractor):
        as_dict = {
            'id': self.node_id,
            'name': self.name,
            'privacy': self.privacy,
            'admins': self.admins
        }

        if extractor.export == 'POSTS':
            as_dict['feed'] = [post.to_dict(extractor) for post in self.feed.nodes]

        return as_dict
