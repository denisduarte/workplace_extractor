from workplace_extractor.Nodes import Node
from workplace_extractor.Nodes.Feed import PersonFeed, GroupFeed, BotFeed
import numpy as np


class Author(Node):
    def __init__(self, id, name, type, title, active, division, department, email, emp_num, invited, claimed, feed):
        Node.__init__(self, id)
        self.name = name
        self.type = type
        self.title = title
        self.active = active
        self.division = division
        self.department = department
        self.email = email
        self.emp_num = emp_num
        self.invited = invited
        self.claimed = claimed
        self.feed = feed

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @property
    def title(self):
        return self._title

    @title.setter
    def title(self, value):
        self._title = value

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, value):
        self._active = value

    @property
    def division(self):
        return self._division

    @division.setter
    def division(self, value):
        self._division = value

    @property
    def department(self):
        return self._department

    @department.setter
    def department(self, value):
        self._department = value

    def to_dict(self, export='posts'):
        as_dict = {
            'id': self.id,
            'name': self.name,
            'type': self.type,
            'title': self.title,
            'active': self.active,
            'division': self.division,
            'department': self.department,
            'email': self.email,
            'emp_num': self.emp_num,
            'invited': self.invited,
            'claimed': self.claimed
        }

        if export == 'posts':
            as_dict['feed'] = self.feed.to_dict() if self.feed is not None else {}

        return as_dict


class Person(Author):
    def __init__(self, data):
        id = str(data.get('id'))
        if id == 'None':
            print(1)

        name = data.get('name', {}).get('formatted', np.nan)
        type = data.get('userType', np.nan)
        title = data.get('title', np.nan)
        active = data.get('active', np.nan)
        division = data.get('urn:scim:schemas:extension:enterprise:1.0', {}).get('division', np.nan)
        department = data.get('urn:scim:schemas:extension:enterprise:1.0', {}).get('department', np.nan)
        email = data.get('userName', np.nan)
        emp_num = data.get('urn:scim:schemas:extension:enterprise:1.0', {}).get('employeeNumber', np.nan)
        invited = data.get('urn:scim:schemas:extension:facebook:accountstatusdetails:1.0', {}).get('invited', np.nan)
        claimed = data.get('urn:scim:schemas:extension:facebook:accountstatusdetails:1.0', {}).get('claimed', np.nan)
        feed = PersonFeed(data)

        Author.__init__(self, id, name, type, title, active, division,
                        department, email, emp_num, invited, claimed, feed)


class Bot(Author):
    def __init__(self, data):
        id = str(data.get('id'))
        name = data.get('name', {})
        type = 'Bot/Ext'
        title = ''
        active = ''
        division = ''
        department = ''
        email = ''
        emp_num = ''
        invited = ''
        claimed = ''
        feed = BotFeed(data)

        Author.__init__(self, id, name, type, title, active, division,
                        department, email, emp_num, invited, claimed, feed)
