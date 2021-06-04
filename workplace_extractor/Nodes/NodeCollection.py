from workplace_extractor.Nodes.Node import Node

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

    def setattr(self, attribute, value):
        for node in self.nodes:
            setattr(node, attribute, value)


class PostCollection(NodeCollection):

    def set_partial_id(self):
        for post in self.nodes:
            post.partial_id = post.node_id.split('_')[1]

    def drop_duplicates(self, filter_ids):
        self.nodes = [item for item in self.nodes if item.partial_id not in filter_ids]

    def filter_hashtags(self, hashtags):

        if hashtags and hashtags != ['']:
            self.nodes = [item for item in self.nodes if set(item.hashtags).intersection(set(hashtags))]

    def to_pandas(self, extractor):
        df = None
        if extractor.export == 'POSTS':
            rows = []
            for index, feed in enumerate(self.nodes):
                print(f'{index} of {len(self.nodes)}')
                if feed.feed.nodes:
                    data_feed = feed.to_dict(extractor)

                    row_feed = pd.DataFrame([{key: data_feed[key] for key in ['id', 'name']}])
                    row_feed['type'] = 'group' if feed.__class__.__name__ == 'Group' else 'person'
                    row_feed = row_feed.add_prefix('feed_')

                    rows_post = []
                    for post in feed.feed.nodes:
                        data_post = post.to_dict(extractor)
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

            base = extractor.config.get('URL', 'workplace')

            df['post_link'] = df.apply(
                lambda x: f'{base}/groups/{x.post_id.split("_")[0]}/permalink/{x.post_id.split("_")[1]}/'
                if x['feed_type'] == 'group'
                else f'{base}/permalink.php?story_fbid={x.post_id.split("_")[1]}&id={x.post_id.split("_")[0]}', axis=1)

            column_order = ['feed_id', 'feed_type', 'feed_name', 'post_id', 'post_partial_id', 'post_created_date',
                            'post_created_time', 'post_type', 'post_status_type', 'post_message', 'post_story',
                            'post_object_link', 'post_object_id', 'post_seen', 'post_reactions', 'post_comments',
                            'post_comments_reactions', 'post_replies', 'post_replies_reactions', 'post_link',
                            'post_hashtags', 'author_id', 'author_name', 'author_type', 'author_title',
                            'author_division', 'author_department', 'author_active']

            df = df[column_order]

            df['post_seen'] = df['post_seen'].astype('Int64')
            df['post_reactions'] = df['post_reactions'].astype('Int64')
            df['post_comments'] = df['post_comments'].astype('Int64')
            df['post_comments_reactions'] = df['post_comments_reactions'].astype('Int64')
            df['post_replies'] = df['post_replies'].astype('Int64')
            df['post_replies_reactions'] = df['post_replies_reactions'].astype('Int64')

        elif extractor.export == 'INTERACTIONS':
            df = pd.DataFrame(columns=['id', 'comment', 'reaction', 'view', 'comment_reply', 'comment_reaction'])
            for feed in self.nodes:
                if feed.feed.nodes:
                    for post in feed.feed.nodes:
                        if post.author.node_id not in df.id.tolist():
                            new_row = pd.DataFrame([{'id': post.author.node_id,
                                                     'comment': {},
                                                     'reaction': {},
                                                     'view': {},
                                                     'comment_reply': {},
                                                     'comment_reaction': {}}],
                                                   index=[post.author.node_id])
                            df = df.append(new_row)

                        for comment in post.comments['data'].nodes:
                            count = df.at[post.author.node_id, 'comment'].get(comment.person.node_id, 0)
                            df.at[post.author.node_id, 'comment'][comment.person.node_id] = count + 1

                            if comment.comments.nodes:
                                for reply in comment.comments.nodes:
                                    if comment.person.node_id not in df.id.tolist():
                                        new_row = pd.DataFrame([{'id': comment.person.node_id,
                                                                 'comment': {},
                                                                 'reaction': {},
                                                                 'view': {},
                                                                 'comment_reply': {},
                                                                 'comment_reaction': {}}],
                                                               index=[comment.person.node_id])
                                        df = df.append(new_row)

                                        count = df.at[comment.person.node_id, 'comment_reply']\
                                                  .get(reply.person.node_id, 0)
                                        df.at[comment.person.node_id, 'comment_reply'][
                                           reply.person.node_id] = count + 1

                                    for comment_reaction in post.reactions['data']:
                                        count = df.at[comment.person.node_id, 'comment_reaction']\
                                                  .get(comment_reaction.person.node_id, 0)
                                        df.at[comment.person.node_id, 'comment_reaction'][
                                           comment_reaction.person.node_id] = count + 1

                        for reaction in post.reactions['data']:
                            count = df.at[post.author.node_id, 'reaction'].get(reaction.person.node_id, 0)
                            df.at[post.author.node_id, 'reaction'][reaction.person.node_id] = count + 1

                        for view in post.seen['data']:
                            count = df.at[post.author.node_id, 'view'].get(view.person.node_id, 0)
                            df.at[post.author.node_id, 'view'][view.person.node_id] = count + 1
        return df


class PeopleCollection(NodeCollection):
    def to_pandas(self, extractor):
        data_dict = [person.to_dict(extractor) for person in self.nodes]

        return pd.DataFrame(data_dict)


class GroupCollection(NodeCollection):
    def to_pandas(self, extractor):
        data_dict = [group.to_dict(extractor) for group in self.nodes]

        return pd.DataFrame(data_dict)
