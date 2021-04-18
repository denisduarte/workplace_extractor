from workplace_extractor.Nodes import Node
from workplace_extractor.Nodes import Group
from workplace_extractor.Nodes.NodeCollection import PostCollection
from workplace_extractor.Nodes.Author import Person, Bot

import numpy as np


class Feed(Node):
    def __init__(self, id, name):
        Node.__init__(self, id)
        self.name = name
        self.posts = PostCollection()
        self.type = np.nan
        self.owner = np.nan

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
    def owner(self):
        return self._owner

    @owner.setter
    def owner(self, value):
        self._owner = value

    @property
    def posts(self):
        return self._posts

    @posts.setter
    def posts(self, value):
        self._posts = value

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'type': self.type,
            'owner': self.owner.to_dict(),
            'posts': [post.to_dict() for post in self.posts.nodes]
        }


class PersonFeed(Feed):
    def __init__(self, data):
        id = str(data.get('id'))
        name = data.get('name', {}).get('formatted', {})

        Feed.__init__(self, id, name)
        self.owner = Person(data)
        self.type = 'person'


class GroupFeed(Feed):
    def __init__(self, data):
        id = str(data.get('id'))
        name = data.get('name', {})

        Feed.__init__(self, id, name)
        self.owner = Group(data)
        self.type = 'group'


class BotFeed(Feed):
    def __init__(self, data):
        id = str(data.get('id'))
        name = data.get('name', {})

        Feed.__init__(self, id, name)
        self.owner = Bot(data)
        self.type = 'group'
