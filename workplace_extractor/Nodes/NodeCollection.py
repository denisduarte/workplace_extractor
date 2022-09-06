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

    def filter_author(self, author_id):
        self.nodes = [item for item in self.nodes if item.author_id == author_id]

    def filter_hashtags(self, hashtags):
        if hashtags != ['']:
            self.nodes = [item for item in self.nodes if set(item.hashtags).intersection(set(hashtags))]

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

    def to_pandas(self, extractor):

        try:
            df = None
            if extractor.export == 'Posts':
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
                                'post_hashtags', 'author_id', 'author_email', 'author_name', 'author_type', 'author_title',
                                'author_division', 'author_department', 'author_active']

                df = df[column_order]

                df['post_seen'] = df['post_seen'].astype('Int64')
                df['post_reactions'] = df['post_reactions'].astype('Int64')
                df['post_comments'] = df['post_comments'].astype('Int64')
                df['post_comments_reactions'] = df['post_comments_reactions'].astype('Int64')
                df['post_replies'] = df['post_replies'].astype('Int64')
                df['post_replies_reactions'] = df['post_replies_reactions'].astype('Int64')

            elif extractor.export == 'Interactions':
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
        except Exception as e:
            print(1)


class CommentCollection(NodeCollection):
    def to_pandas(self, extractor):

        comments = []
        for item in self.nodes:
            comment = self.comment_to_dict(item, extractor)
            comment['parent_id'] = None

            comments.append(comment)
            for subitem in item.comments.nodes:
                reply = self.comment_to_dict(subitem, extractor)
                reply['parent_id'] = comment.get('id')

                comments.append(reply)

        column_order = ['id', 'parent_id', 'replies', 'message', 'total_reactions', 'reactions_like', 'reactions_love',
                        'reactions_care', 'reactions_haha', 'reactions_wow', 'reactions_sad', 'reactions_angry',
                        'author_id', 'author_name', 'author_email', 'author_type', 'author_title', 'author_key',
                        'author_department']

        df = pd.DataFrame(comments)
        df = df[column_order]

        return df
        
    @staticmethod
    def comment_to_dict(comment, extractor):
        comment_dict = comment.to_dict(extractor)

        comment_dict['total_reactions'] = len(comment_dict.get('total_reactions', []))

        comment_dict['reactions_like'] = len(comment_dict.get('reactions_like', []))
        comment_dict['reactions_love'] = len(comment_dict.get('reactions_love', []))
        comment_dict['reactions_care'] = len(comment_dict.get('reactions_care', []))
        comment_dict['reactions_haha'] = len(comment_dict.get('reactions_haha', []))
        comment_dict['reactions_wow'] = len(comment_dict.get('reactions_wow', []))
        comment_dict['reactions_sad'] = len(comment_dict.get('reactions_sad', []))
        comment_dict['reactions_angry'] = len(comment_dict.get('reactions_angry', []))

        total_replies = len(comment.comments.nodes)
        comment_dict['replies'] = total_replies

        comment_dict['author_id'] = comment_dict.get('person', {}).get('id')
        comment_dict['author_name'] = comment_dict.get('person', {}).get('name')
        comment_dict['author_email'] = comment_dict.get('person', {}).get('email')
        comment_dict['author_type'] = comment_dict.get('person', {}).get('type')
        comment_dict['author_title'] = comment_dict.get('person', {}).get('title')
        comment_dict['author_key'] = comment_dict.get('person', {}).get('emp_num')
        comment_dict['author_department'] = comment_dict.get('person', {}).get('department')

        del comment_dict['person']
        
        return comment_dict
        

class PeopleCollection(NodeCollection):
    def to_pandas(self, extractor):
        data_dict = [person.to_dict(extractor) for person in self.nodes]

        return pd.DataFrame(data_dict)


class GroupCollection(NodeCollection):
    def to_pandas(self, extractor):
        data_dict = [group.to_dict(extractor) for group in self.nodes]

        return pd.DataFrame(data_dict)


class EventCollection(NodeCollection):
    def to_pandas(self, extractor):

        event = self.nodes[0]

        if event:
            evento_dict = event.to_dict(extractor)

            row_evento = pd.DataFrame([{key: evento_dict[key] for key in evento_dict.keys() - ['owner', 'attendees']}])
            row_evento = row_evento.add_prefix('event_')

            owner_dict = evento_dict.get('owner')
            row_owner = pd.DataFrame([{key: owner_dict[key] for key in owner_dict.keys()}])
            row_owner = row_owner.add_prefix('owner_')

            row_evento_owner = pd.concat([row_evento, row_owner], axis=1)

            rows_attendees = []
            for attendee in evento_dict.get('attendees'):
                row_attendee = pd.DataFrame([{key: attendee[key] for key in attendee.keys()}])
                row_attendee = row_attendee.add_prefix('attendee_')

                rows_attendees.append(row_attendee)

            df = row_evento_owner.merge(pd.concat(rows_attendees), how="cross")

            return df


class InteractionCollection(NodeCollection):
    def to_pandas(self, extractor):
        return self.nodes
