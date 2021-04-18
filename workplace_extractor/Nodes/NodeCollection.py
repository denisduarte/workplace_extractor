from workplace_extractor.Nodes import Node

import pandas as pd
import logging


class NodeCollection:
    def __init__(self, items=None):
        if items is None:
            items = []

        self.nodes = items

    @property
    def nodes(self):
        return self._nodes

    @nodes.setter
    def nodes(self, value):
        self._nodes = value

    def unique_values(self, attribute):
        ids = [getattr(node, attribute) for node in self.nodes]
        return list(dict.fromkeys(ids))

    def drop_non_authors(self):
        self.nodes = [node for node in self.nodes if node.posts.nodes]

    def extend(self, items):
        if isinstance(items, NodeCollection):
            self.nodes.extend(items.nodes)

        elif isinstance(items, list):
            self.nodes.extend(items)

        elif isinstance(items, Node):
            self.nodes.append(items)

        else:
            raise TypeError('Got unknown type when extending NodeCollection')

    def to_pandas(self):
        data_dict = [feed.to_dict() for feed in self.nodes]
        rows = []
        i = 1
        for feed in data_dict:
            i += 1
            if feed['posts']:
                logging.debug(f'{i} of {len(data_dict)} feeds')
                row_feed = pd.DataFrame([{key: feed[key] for key in feed.keys() - ['posts', 'owner']}])
                row_feed = row_feed.add_prefix('feed_')

                rows_post = []
                for post in feed['posts']:
                    row_post = pd.DataFrame([{key: post[key] for key in post.keys() - ['author']}])
                    row_post = row_post.add_prefix('post_')

                    row_author = pd.DataFrame([post['author']])
                    row_author = row_author.add_prefix('author_')

                    rows_post.append(row_post.merge(row_author, how="left",
                                                    left_on='post_author_id', right_on='author_id'))

                new_rows = row_feed.merge(pd.concat(rows_post), how="cross")
                rows.append(new_rows)

        df = pd.concat(rows)

        df['post_created_date'] = pd.to_datetime(df['post_created_time']).dt.date
        df['post_created_time'] = pd.to_datetime(df['post_created_time']).dt.time

        column_order = ['feed_id', 'feed_type', 'feed_name', 'post_id', 'post_partial_id', 'post_created_date',
                        'post_created_time', 'post_type', 'post_status_type', 'post_link', 'post_object_id',
                        'post_views', 'post_reactions', 'post_comments', 'author_id', 'author_name', 'author_type',
                        'author_title', 'author_division', 'author_department', 'author_active']

        df = df[column_order]

        return df

    def setattr(self, attribute, value):
        for node in self.nodes:
            setattr(node, attribute, value)


class PostCollection(NodeCollection):

    def set_partial_id(self):
        for post in self.nodes:
            post.partial_id = post.id.split('_')[1]

    def drop_duplicates(self, filter_ids):
        for index, post in enumerate(self.nodes):
            if post.partial_id in filter_ids:
                self.nodes.pop(index)


