from workplace_extractor.Nodes import Node
import numpy as np


class Author(Node):
    def __init__(self, id, name, type, title, active, division, department):
        Node.__init__(self, id)
        self.name = name
        self.type = type
        self.title = title
        self.active = active
        self.division = division
        self.department = department

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

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'type': self.type,
            'title': self.title,
            'active': self.active,
            'division': self.division,
            'department': self.department,
        }


class Person(Author):
    def __init__(self, data):
        id = str(data.get('id'))
        name = data.get('name', {}).get('formatted', np.nan)
        type = data.get('userType', np.nan)
        title = data.get('title', np.nan)
        active = data.get('active', np.nan)
        division = data.get('urn:scim:schemas:extension:enterprise:1.0', {}).get('division', np.nan)
        department = data.get('urn:scim:schemas:extension:enterprise:1.0', {}).get('department', np.nan)

        Author.__init__(self, id, name, type, title, active, division, department)


class Bot(Author):
    def __init__(self, data):
        id = str(data.get('id'))
        name = data.get('name', {})
        type = 'Bot/Ext'
        title = ''
        active = ''
        division = ''
        department = ''

        Author.__init__(self, id, name, type, title, active, division, department)
