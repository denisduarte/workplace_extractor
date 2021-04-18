from workplace_extractor.Nodes import Node
import numpy as np


class Post(Node):
    def __init__(self, data):
        Node.__init__(self, str(data.get('id')))
        self.partial_id = np.nan
        self.type = data.get('type', np.nan)
        self.created_time = data.get('created_time', np.nan)
        self.status_type = data.get('status_type', np.nan)
        self.object_id = data.get('object_id', np.nan)
        self.link = data.get('link', np.nan)
        self.views = np.nan
        self.reactions = np.nan
        self.comments = np.nan
        self.author_id = data.get('from', {}).get('id', np.nan)
        self.author = None

    @property
    def partial_id(self):
        return self._partial_id

    @partial_id.setter
    def partial_id(self, value):
        self._partial_id = value

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @property
    def created_time(self):
        return self._created_time

    @created_time.setter
    def created_time(self, value):
        self._created_time = value

    @property
    def status_type(self):
        return self._status_type

    @status_type.setter
    def status_type(self, value):
        self._status_type = value

    @property
    def object_id(self):
        return self._object_id

    @object_id.setter
    def object_id(self, value):
        self._object_id = value

    @property
    def link(self):
        return self._link

    @link.setter
    def link(self, value):
        self._link = value

    @property
    def views(self):
        return self._views

    @views.setter
    def views(self, value):
        self._views = value

    @property
    def reactions(self):
        return self._reactions

    @reactions.setter
    def reactions(self, value):
        self._reactions = value

    @property
    def comments(self):
        return self._comments

    @comments.setter
    def comments(self, value):
        self._comments = value

    @property
    def author(self):
        return self._author

    @author.setter
    def author(self, value):
        self._author = value

    def to_dict(self):
        return {
            'id': self.id,
            'partial_id': self.partial_id,
            'type': self.type,
            'created_time': self.created_time,
            'status_type': self.status_type,
            'object_id': self.object_id,
            'link': self.link,
            'views': self.views,
            'reactions': self.reactions,
            'comments': self.comments,
            'author_id': self.author_id,
            'author': self.author.to_dict() if self.author else None
        }


class Summary:
    def __init__(self, data):
        self.summary = data.get('total_count', np.nan)
