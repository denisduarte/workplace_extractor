import pandas as pd

from .Node import Node
from .NodeCollection import PostCollection
from datetime import datetime


class Author(Node):
    def __init__(self, extractor, node_id, name='', author_type='', title='', active='', division='', department='',
                 building='', email='', emp_num='', invited=None, invite_date='', claimed=None, feed=None):
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

        self.set_additional_attributes(extractor)

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

        if hasattr(extractor, 'additional_people_attributes') and extractor.additional_people_attributes:
            files = extractor.additional_people_attributes.split(',')

            for file in files:
                # get the names of the additional attributes and remove first column (used as join)
                people_attributes = pd.read_csv(file, sep=';', nrows=0).columns.tolist()[1:]

                for column in people_attributes:
                    as_dict[column] = getattr(self, column)

        if extractor.export == 'POSTS' and self.feed is not None and origin == 'extractor':
            as_dict['feed'] = [post.to_dict(extractor) for post in self.feed.nodes]

        return as_dict

    def set_additional_attributes(self, extractor):

        if hasattr(extractor, 'additional_people_attributes') and extractor.additional_people_attributes:
            files = extractor.additional_people_attributes.split(',')

            for file in files:
                people_attributes = pd.read_csv(file, sep=';')
                join = people_attributes.columns[0]

                for column in people_attributes.columns:
                    if column != join:
                        join_value = getattr(self, join)
                        try:
                            value_index = people_attributes[join].str.lower() == join_value.lower()
                            setattr(self, column, people_attributes[value_index][column].values[0])
                        except IndexError:
                            setattr(self, column, '')


class Person(Author):
    def __init__(self, extractor, data):
        node_id = str(data.get('id'))
        name = data.get('name', {}).get('formatted', '')
        author_type = data.get('userType', '')
        email = data.get('userName', '')
        title = data.get('title', '')
        active = data.get('active', '')
        division = data.get('urn:ietf:params:scim:schemas:extension:enterprise:2.0:User', {}).get('division', '')
        department = data.get('urn:ietf:params:scim:schemas:extension:enterprise:2.0:User', {}).get('department', '')
        building = None
        for address in data.get('addresses', []):
            if address.get('type') == 'work':
                building = address.get('formatted')
        emp_num = data.get('urn:ietf:params:scim:schemas:extension:enterprise:2.0:User', {}).get('employeeNumber', '')
        invited = data.get('urn:ietf:params:scim:schemas:extension:facebook:accountstatusdetails:2.0:User', {}).get('invited', '')
        date = data.get('urn:ietf:params:scim:schemas:extension:facebook:accountstatusdetails:2.0:User', {}).get('inviteDate', '')
        #invite_date = datetime.utcfromtimestamp(date).strftime('%Y-%m-%d %H:%M:%S')

        try:
            invite_date = datetime.fromisoformat(date).strftime('%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            invite_date = ''

        claimed = data.get('urn:ietf:params:scim:schemas:extension:facebook:accountstatusdetails:2.0:User', {})\
                      .get('claimed', '')
        feed = PostCollection()

        Author.__init__(self, extractor, node_id, name, author_type, title, active, division,
                        department, building, email, emp_num, invited, invite_date, claimed, feed)

        self.set_additional_attributes(extractor)


class Bot(Author):
    def __init__(self, extractor, data):
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

        Author.__init__(self, extractor, node_id, name, author_type, title, active, division,
                        department, building, email, emp_num, invited, invite_date, claimed, feed)
