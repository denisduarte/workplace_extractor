import pandas as pd

from .Node import Node
from .NodeCollection import PostCollection
from datetime import datetime

import numpy as np


class Author(Node):
    def __init__(self, node_id, name, author_type, title, active, division, department, building, email,
                 emp_num, invited, invite_date, claimed, feed):
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
            'manager_level': self.manager_level,
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

    def __init__(self, extractor, data):
        node_id = str(data.get('id'))
        name = data.get('name', {}).get('formatted', np.nan)
        author_type = data.get('userType', np.nan)
        email = data.get('userName', np.nan)
        title = data.get('title', np.nan)
        active = data.get('active', np.nan)
        division = data.get('urn:ietf:params:scim:schemas:extension:enterprise:2.0:User', {}).get('division', np.nan)
        department = data.get('urn:ietf:params:scim:schemas:extension:enterprise:2.0:User', {}).get('department', np.nan)
        building = None
        for address in data.get('addresses', []):
            if address.get('type') == 'work':
                building = address.get('formatted')
        emp_num = data.get('urn:ietf:params:scim:schemas:extension:enterprise:2.0:User', {}).get('employeeNumber', np.nan)
        invited = data.get('urn:ietf:params:scim:schemas:extension:facebook:accountstatusdetails:2.0:User', {}).get('invited', np.nan)
        date = data.get('urn:ietf:params:scim:schemas:extension:facebook:accountstatusdetails:2.0:User', {}).get('inviteDate', '')
        #invite_date = datetime.utcfromtimestamp(date).strftime('%Y-%m-%d %H:%M:%S')

        try:
            invite_date = datetime.fromisoformat(date).strftime('%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            invite_date = ''

        claimed = data.get('urn:ietf:params:scim:schemas:extension:facebook:accountstatusdetails:2.0:User', {})\
                      .get('claimed', np.nan)
        feed = PostCollection()

        people_attributes = pd.read_csv(extractor.people_attributes_file, sep=';')

        join = extractor.people_attributes_join
        for column in people_attributes.columns:
            if column != join:
                join_value = data.get(join, '').lower()
                try:
                    setattr(self, column, people_attributes[people_attributes[join] == join_value][column].values[0])
                except IndexError:
                    setattr(self, column, '')

        Author.__init__(self, node_id, name, author_type, title, active, division,
                        department, building, email, emp_num, invited, invite_date, claimed, feed)


class Bot(Author):
    def __init__(self, data):
        node_id = str(data.get('id'))
        name = data.get('name', {})
        author_type = 'Bot/Ext'
        self.manager_level = ''
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
