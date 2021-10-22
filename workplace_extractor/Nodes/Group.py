from workplace_extractor.Nodes.Node import Node
from workplace_extractor.Nodes.NodeCollection import PostCollection
from datetime import datetime


class Group(Node):
    def __init__(self, data, admins=None):
        Node.__init__(self, str(data.get('id')))
        self.name = data.get('name')
        self.privacy = data.get('privacy')
        self.purpose = data.get('purpose')
        self.qtd_members = data.get('members', {}).get('summary', {}).get('total_count')
        self.admins = admins
        self.members = []
        self.feed = PostCollection()

    def to_dict(self, extractor):
        as_dict = {
            'id': self.node_id,
            'name': self.name,
            'privacy': self.privacy,
            'purpose': self.purpose,
            'qtd_members': self.qtd_members,
            'admins': self.admins,
            'members': self.members
        }

        if extractor.export == 'POSTS':
            as_dict['feed'] = [post.to_dict(extractor) for post in self.feed.nodes]

        return as_dict


class Member(Node):
    def __init__(self, data):
        Node.__init__(self, str(data.get('id')))
        self.name = data.get('name')
        self.email = data.get('email')
        self.division = data.get('division')
        self.department = data.get('department')
        self.title = data.get('title')
        self.active = data.get('active')
        self.administrator = data.get('administrator')
        self.date_joined = datetime.utcfromtimestamp(data.get('joined')).strftime('%Y-%m-%d %H:%M:%S')
        self.added_by = data.get('added_by')

    def to_dict(self, extractor):
        as_dict = {
            'id': self.node_id,
            'name': self.name,
            'email': self.email,
            'division': self.division,
            'department': self.department,
            'title': self.title,
            'active': self.active,
            'administrator': self.administrator,
            'date_joined': self.date_joined,
            'added_by': self.added_by
        }

        return as_dict
