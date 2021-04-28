from workplace_extractor.Nodes import Node
#from workplace_extractor.Nodes.Author import Person

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
        #data_dict = [feed.to_dict() for feed in self.nodes]
        rows = []
        i = 1
        for feed in self.nodes:
            i += 1
            if feed.feed.posts.nodes:
                data_feed = feed.to_dict()
                logging.debug(f'{i} of {len(self.nodes)} feeds')
                row_feed = pd.DataFrame([{key: data_feed[key] for key in ['id', 'name']}])
                row_feed['type'] = 'group' if feed.__class__.__name__ == 'Group' else 'person'
                row_feed = row_feed.add_prefix('feed_')

                rows_post = []
                for post in feed.feed.posts.nodes:
                    data_post = post.to_dict()
                    row_post = pd.DataFrame([{key: data_post[key] for key in data_post.keys() - ['author']}])
                    row_post = row_post.add_prefix('post_')

                    data_author = data_post['author']
                    row_author = pd.DataFrame([{key: data_author[key] for key in data_author.keys() - ['feed']}])
                    row_author = row_author.add_prefix('author_')

                    rows_post.append(row_post.merge(row_author, how="left",
                                                    left_on='post_author_id', right_on='author_id'))

                new_rows = row_feed.merge(pd.concat(rows_post), how="cross")
                rows.append(new_rows)

        df = pd.concat(rows)

        df['post_created_date'] = pd.to_datetime(df['post_created_time']).dt.date
        df['post_created_time'] = pd.to_datetime(df['post_created_time']).dt.time
        base = 'https://petrobras.workplace.com'
        df['post_link'] = df.apply(lambda x: f'{base}/groups/{x.post_id.split("_")[0]}/permalink/{x.post_id.split("_")[1]}/'
                                        if x['feed_type'] == 'group'
                                        else f'{base}/permalink.php?story_fbid={x.post_id.split("_")[1]}&id={x.post_id.split("_")[0]}', axis=1)

        column_order = ['feed_id', 'feed_type', 'feed_name', 'post_id', 'post_partial_id', 'post_created_date',
                        'post_created_time', 'post_type', 'post_status_type', 'post_message', 'post_story',
                        'post_object_link', 'post_object_id', 'post_views', 'post_reactions', 'post_comments',
                        'post_link',  'author_id', 'author_name', 'author_type', 'author_title', 'author_division',
                        'author_department', 'author_active']

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
        self.nodes = [item for item in self.nodes if item.partial_id not in filter_ids]


class PeopleCollection(NodeCollection):

    def to_pandas(self):
        data_dict = [person.to_dict('people') for person in self.nodes]

        return pd.DataFrame(data_dict)


class GroupCollection(NodeCollection):

    def to_pandas(self):
        data_dict = [group.to_dict('groups') for group in self.nodes]

        return pd.DataFrame(data_dict)