from workplace_extractor.Nodes.Node import Node
from workplace_extractor.Nodes.NodeCollection import PostCollection
from datetime import datetime

import numpy as np


class Author(Node):
    def __init__(self, node_id, name, author_type, title, active, division, department, building, email, emp_num,
                 invited, invite_date, claimed, feed):
        Node.__init__(self, node_id)
        self.name = name
        self.author_type = author_type
        self.title = title
        self.active = active
        self.division = division
        self.department = department
        self.building = building
        self.email = email
        self.emp_num = emp_num
        self.invited = invited
        self.invite_date = invite_date
        self.claimed = claimed
        self.feed = feed

    def to_dict(self, extractor, origin='extractor'):
        as_dict = {
            'id': self.node_id,
            'name': self.name,
            'type': self.author_type,
            'title': self.title,
            'active': self.active,
            'division': self.division,
            'department': self.department,
            'building': self.building,
            'email': self.email,
            'emp_num': self.emp_num,
            'invited': self.invited,
            'invite_date': self.invite_date,
            'claimed': self.claimed
        }

        if extractor.export == 'POSTS' and self.feed is not None and origin == 'extractor':
            as_dict['feed'] = [post.to_dict(extractor) for post in self.feed.nodes]

        return as_dict


class Person(Author):
    def __init__(self, data):
        node_id = str(data.get('id'))
        name = data.get('name', {}).get('formatted', np.nan)
        author_type = data.get('userType', np.nan)
        email = data.get('userName', np.nan)
        title = data.get('title', np.nan)
        active = data.get('active', np.nan)
        division = data.get('urn:scim:schemas:extension:enterprise:1.0', {}).get('division', np.nan)
        department = data.get('urn:scim:schemas:extension:enterprise:1.0', {}).get('department', np.nan)
        building = None
        for address in data.get('addresses', []):
            if address.get('type') == 'work':
                building = address.get('formatted')
        emp_num = data.get('urn:scim:schemas:extension:enterprise:1.0', {}).get('employeeNumber', np.nan)
        invited = data.get('urn:scim:schemas:extension:facebook:accountstatusdetails:1.0', {}).get('invited', np.nan)
        date = data.get('urn:scim:schemas:extension:facebook:accountstatusdetails:1.0', {}).get('inviteDate', np.nan)
        invite_date = datetime.utcfromtimestamp(date).strftime('%Y-%m-%d %H:%M:%S')


        claimed = data.get('urn:scim:schemas:extension:facebook:accountstatusdetails:1.0', {}).get('claimed', np.nan)
        feed = PostCollection()

        Author.__init__(self, node_id, name, author_type, title, active, division,
                        department, building, email, emp_num, invited, invite_date, claimed, feed)


class Bot(Author):
    def __init__(self, data):
        node_id = str(data.get('id'))
        name = data.get('name', {})
        author_type = 'Bot/Ext'
        title = ''
        active = ''
        division = ''
        department = ''
        building = ''
        email = ''
        emp_num = ''
        invited = ''
        invite_date = ''
        claimed = ''
        feed = PostCollection()

        Author.__init__(self, node_id, name, author_type, title, active, division,
                        department, building, email, emp_num, invited, invite_date, claimed, feed)
