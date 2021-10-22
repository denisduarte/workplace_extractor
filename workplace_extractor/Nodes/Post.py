from workplace_extractor.Nodes.Node import Node
from workplace_extractor.Nodes.NodeCollection import NodeCollection
import numpy as np
import re


class Post(Node):
    def __init__(self, data, extractor):
        Node.__init__(self, str(data.get('id')))
        self.partial_id = np.nan
        self.post_type = data.get('type', np.nan)
        self.created_time = data.get('created_time', np.nan)
        self.status_type = data.get('status_type', np.nan)
        self.object_id = data.get('object_id', np.nan)
        self.object_link = data.get('link', np.nan)
        self.author_id = data.get('from', {}).get('id', np.nan)
        self.author = None

        if extractor.export_content:
            self.message = data.get('message')
        else:
            self.message = True if data.get('message') else False

        self.story = True if data.get('story') else False
        self.seen = {'total': Summary(), 'data': []}
        self.reactions = {'total': Summary(), 'data': []}
        self.comments = {'total': Summary(), 'data': NodeCollection()}
        self.comments_reactions = {'total': Summary(), 'data': []}
        self.replies = {'total': Summary(), 'data': NodeCollection()}
        self.replies_reactions = {'total': Summary(), 'data': []}
        self.hashtags = self.extract_hashtags(data.get('message', ''))

    # return a list with the hashtags used in the post in lowercase
    @staticmethod
    def extract_hashtags(text):
        regex = '#(\\w+)'
        hashtag_list = re.findall(regex, text)

        return [hashtag.lower() for hashtag in hashtag_list]

    def to_dict(self, extractor):
        out_dict = {
            'id': self.node_id,
            'partial_id': self.partial_id,
            'type': self.post_type,
            'created_time': self.created_time,
            'status_type': self.status_type,
            'object_id': self.object_id,
            'object_link': self.object_link,
            'author_id': self.author_id,
            'author': self.author.to_dict(extractor, origin='post') if self.author else None,
            'message': self.message,
            'story': self.story,
            'hashtags': ','.join(hashtag for hashtag in self.hashtags)
        }

        if extractor.export == 'INTERACTIONS':
            out_dict['seen'] = [saw.to_dict(extractor) for saw in self.seen['data']]
            out_dict['reactions'] = [reaction.to_dict(extractor) for reaction in self.reactions['data']]
            out_dict['comments'] = [comment.to_dict(extractor) for comment in self.comments['data'].nodes]
        else:
            out_dict['seen'] = self.seen['total'].summary
            out_dict['reactions'] = self.reactions['total'].summary
            out_dict['comments'] = self.comments['total'].summary
            out_dict['comments_reactions'] = self.comments_reactions['total'].summary
            out_dict['replies'] = self.replies['total'].summary
            out_dict['replies_reactions'] = self.replies_reactions['total'].summary

        return out_dict


class Summary:
    def __init__(self, data=None):
        if data is None:
            data = {}

        self.summary = data.get('total_count', np.nan)
