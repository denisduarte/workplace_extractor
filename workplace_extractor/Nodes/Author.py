from workplace_extractor.Nodes.Node import Node
from workplace_extractor.Nodes.NodeCollection import PostCollection

import numpy as np


class Author(Node):
    def __init__(self, node_id, name, author_type, title, active, division, department, email, emp_num,
                 invited, claimed, feed):
        Node.__init__(self, node_id)
        self.name = name
        self.author_type = author_type
        self.title = title
        self.active = active
        self.division = division
        self.department = department
        self.email = email
        self.emp_num = emp_num
        self.invited = invited
        self.claimed = claimed
        self.feed = feed

    def to_dict(self, extractor):
        as_dict = {
            'id': self.node_id,
            'name': self.name,
            'type': self.author_type,
            'title': self.title,
            'active': self.active,
            'division': self.division,
            'department': self.department,
            'email': self.email,
            'emp_num': self.emp_num,
            'invited': self.invited,
            'claimed': self.claimed
        }

        if extractor.export == 'POSTS' and self.feed is not None:
            as_dict['feed'] = [post.to_dict(extractor) for post in self.feed.nodes]

        return as_dict


class Person(Author):
    def __init__(self, data):
        node_id = str(data.get('id'))
        name = data.get('name', {}).get('formatted', np.nan)
        author_type = data.get('userType', np.nan)
        title = data.get('title', np.nan)
        active = data.get('active', np.nan)
        division = data.get('urn:scim:schemas:extension:enterprise:1.0', {}).get('division', np.nan)
        department = data.get('urn:scim:schemas:extension:enterprise:1.0', {}).get('department', np.nan)
        email = data.get('userName', np.nan)
        emp_num = data.get('urn:scim:schemas:extension:enterprise:1.0', {}).get('employeeNumber', np.nan)
        invited = data.get('urn:scim:schemas:extension:facebook:accountstatusdetails:1.0', {}).get('invited', np.nan)
        claimed = data.get('urn:scim:schemas:extension:facebook:accountstatusdetails:1.0', {}).get('claimed', np.nan)
        feed = PostCollection()

        Author.__init__(self, node_id, name, author_type, title, active, division,
                        department, email, emp_num, invited, claimed, feed)


class Bot(Author):
    def __init__(self, data):
        node_id = str(data.get('id'))
        name = data.get('name', {})
        author_type = 'Bot/Ext'
        title = ''
        active = ''
        division = ''
        department = ''
        email = ''
        emp_num = ''
        invited = ''
        claimed = ''
        feed = PostCollection()

        Author.__init__(self, node_id, name, author_type, title, active, division,
                        department, email, emp_num, invited, claimed, feed)
