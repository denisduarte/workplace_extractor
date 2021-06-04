from workplace_extractor.Extractors.GroupExtractor import GroupExtractor
from workplace_extractor.Extractors.PersonExtractor import PersonExtractor
from workplace_extractor.Nodes.NodeCollection import PostCollection
from workplace_extractor.Nodes.Author import Bot
from workplace_extractor.Nodes.Post import Summary, Post
from workplace_extractor.Nodes.Interaction import View, Reaction, Comment

from copy import copy
import logging

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib
from random import randint


class PostExtractor:
    def __init__(self, extractor, since, until):
        self.extractor = extractor
        self.since = since
        self.until = until
        self.nodes = PostCollection()
        self.filter_ids = []

    async def extract(self):
        logging.info('Loading group feeds')

        group_extractor = GroupExtractor(self.extractor)
        await group_extractor.extract()
        self.nodes.extend(group_extractor.nodes)
        logging.info('Group feeds loaded')

        logging.info('Loading person feeds')
        people_extractor = PersonExtractor(self.extractor)
        await people_extractor.extract()
        self.nodes.extend(people_extractor.nodes)
        logging.info('Person feeds loaded')

        # fields that should be extracted from posts using the GRAPH API
        fields = 'id,from,type,created_time,status_type,object_id,link,message,story'

        logging.info(f'Extracting posts from {len(group_extractor.nodes.nodes)} groups.')
        http_calls = []
        for node in group_extractor.nodes.nodes:
            http_calls.append({'url': self.extractor.config.get('URL', 'GRAPH') + f'/{node.node_id}/feed?limit=100'
                                                                                  f'&fields={fields}'
                                                                                  f'&since={self.since}'
                                                                                  f'&until={self.until}',
                               'call': self.call,
                               'node': node,
                               'recursion': 1})

        await self.extractor.fetch(http_calls)

        logging.info(f'Extracting posts from {len(people_extractor.nodes.nodes)} people.')
        http_calls = []
        for node in people_extractor.nodes.nodes:
            http_calls.append({'url': self.extractor.config.get('URL', 'GRAPH') + f'/{node.node_id}/feed?limit=100'
                                                                                  f'&fields={fields}'
                                                                                  f'&since={self.since}'
                                                                                  f'&until={self.until}',
                               'call': self.call,
                               'node': node,
                               'recursion': 1})
        await self.extractor.fetch(http_calls)

        count = 0
        for node in self.nodes.nodes:
            count += len(node.feed.nodes)

        # Add view, reactions, comments and authors
        await self.fetch_posts_info()

        logging.info('Post extraction ended')

    async def fetch_posts_info(self):
        http_calls = []
        for node in self.nodes.nodes:
            for post in node.feed.nodes:
                http_calls.append({
                    'url': self.extractor.config.get('URL', 'GRAPH') + f'/{post.node_id}?fields='
                                                                       'reactions.limit(100).summary(1),'
                                                                       'seen.limit(100).summary(1),'
                                                                       'comments.limit(100).summary(1)'
                                                                       '{'
                                                                       'created_time,'
                                                                       'from,'
                                                                       'reactions.limit(100).summary(1),'
                                                                       'comments.limit(100).summary(1)'
                                                                       '{'
                                                                       'created_time,'
                                                                       'from,'
                                                                       'reactions.limit(100).summary(1)'
                                                                       '}'
                                                                       '}',
                    'call': self.call_info if self.extractor.export == 'INTERACTIONS' else self.call_count,
                    'post': post,
                    'recursion': 1})

                author = next((node for node in self.nodes.nodes if node.node_id == post.author_id), None)

                # including Bots
                if author is not None:
                    author = copy(author)
                    author.feed = None

                    post.author = author
                else:
                    http_calls.append({'url': self.extractor.config.get('URL', 'GRAPH') + f'/{post.author_id}',
                                       'call': self.call_bot,
                                       'post': post})

        await self.extractor.fetch(http_calls)

    async def call(self, url, session, **kwargs):
        data = await self.extractor.fetch_url(url, session, 'GRAPH', **kwargs)

        if 'data' in data and data['data']:
            collection = PostCollection([Post(post) for post in data['data']])

            # filter posts already extracted. Applies to posts in both group and author feed and for
            # GRAPH bug with infinite loop.
            collection.set_partial_id()
            collection.drop_duplicates(self.filter_ids)

            # if hashtags not empty, filter result by hashtag
            if self.extractor.hashtags:
                collection.filter_hashtags(self.extractor.hashtags)

            kwargs.get('node').feed.extend(collection)
            self.add_to_filter(collection.nodes)

            next_page = data.get('paging', {}).get('next')
            if next_page is not None:
                kwargs['recursion'] += 1
                await self.call(next_page, session, **kwargs)

    async def call_info(self, url, session, **kwargs):
        data = await self.extractor.fetch_url(url, session, 'GRAPH', **kwargs)

        await self.call_info_views(data.get('seen', {}), session, **kwargs)
        await self.call_info_reactions(data.get('reactions', {}), session, **kwargs)
        await self.call_info_comments(data.get('comments', {}), session, **kwargs)

    async def call_info_views(self, data, session, **kwargs):
        if 'data' in data:
            for item in data['data']:
                view = View()
                self.set_author(item, view, 'seen')

                kwargs.get('post').seen['data'].append(view)

        next_page = data.get('paging', {}).get('next')
        if next_page is not None:
            kwargs['recursion'] += 1

            new_data = await self.extractor.fetch_url(next_page, session, 'GRAPH', **kwargs)
            await self.call_info_views(new_data, session, **kwargs)

    async def call_info_reactions(self, data, session, **kwargs):
        if 'data' in data:
            for item in data['data']:
                reaction = Reaction(item)
                self.set_author(item, reaction, 'reactions')

                kwargs.get('post').reactions['data'].append(reaction)

        next_page = data.get('paging', {}).get('next')
        if next_page is not None:
            kwargs['recursion'] += 1

            new_data = await self.extractor.fetch_url(next_page, session, 'GRAPH', **kwargs)
            await self.call_info_reactions(new_data, session, **kwargs)

    async def call_info_comments(self, data, session, **kwargs):
        if 'data' in data:
            for item in data['data']:
                comment = Comment(item)
                self.set_author(item, comment, kwargs.get('type'))

                if item.get('reactions', {}).get('data'):
                    data_reactions = item.get('reactions', {})
                    while True:
                        for item_reaction in data_reactions.get('data'):
                            reaction = Reaction(item_reaction)
                            self.set_author(item_reaction, reaction, 'reactions')

                            comment.reactions.append(reaction)

                        next_page = data_reactions.get('paging', {}).get('next')
                        if next_page is not None:
                            kwargs['recursion'] += 1

                            data_reactions = await self.extractor.fetch_url(next_page, session, 'GRAPH', **kwargs)
                        else:
                            break

                if item.get('comments', {}).get('data'):
                    data_comments = item.get('comments', {})
                    while True:
                        for item_comment in data_comments.get('data'):
                            reply = Comment(item_comment)
                            self.set_author(item_comment, reply, 'comments')

                            if item_comment.get('reactions', {}).get('data'):
                                data_comment_reactions = item_comment.get('reactions', {})
                                while True:
                                    for item_comment_reaction in data_comment_reactions.get('data'):
                                        reaction = Reaction(item_comment_reaction)
                                        self.set_author(item_comment_reaction, reaction, 'reactions')

                                        reply.reactions.append(reaction)

                                    next_page = data_comment_reactions.get('paging', {}).get('next')
                                    if next_page is not None:
                                        kwargs['recursion'] += 1
                                        data_comment_reactions = await self.extractor.fetch_url(next_page,
                                                                                                session,
                                                                                                'GRAPH',
                                                                                                **kwargs)
                                    else:
                                        break

                            comment.comments.extend(reply)

                        next_page = data_comments.get('paging', {}).get('next')
                        if next_page is not None:
                            kwargs['recursion'] += 1
                            data_comments = await self.extractor.fetch_url(next_page, session, 'GRAPH', **kwargs)
                        else:
                            break

                kwargs.get('post').comments['data'].extend(comment)

        next_page = data.get('paging', {}).get('next')
        if next_page is not None:
            new_data = await self.extractor.fetch_url(next_page, session, 'GRAPH', **kwargs)
            await self.call_info_comments(new_data, session, **kwargs)

    async def call_bot(self, url, session, **kwargs):
        data = await self.extractor.fetch_url(url, session, 'GRAPH', **kwargs)

        if data:
            kwargs.get('post').author = Bot(data)

    async def call_count(self, url, session, **kwargs):
        data = await self.extractor.fetch_url(url, session, 'GRAPH', **kwargs)

        for summary_type in ('comments', 'reactions', 'seen'):
            if summary_type != 'comments':
                if summary_type in data:
                    summary = Summary(data.get(summary_type, {}).get('summary', {}))
                else:
                    summary = Summary({'total_count': 0})

                getattr(kwargs.get('post'), summary_type)['total'] = summary

            else:
                data_comments = data.get(summary_type, {})

                # set total of comments
                summary = Summary(data_comments.get('summary', {}))

                getattr(kwargs.get('post'), 'comments')['total'] = summary
                getattr(kwargs.get('post'), 'comments_reactions')['total'] = Summary({'total_count': 0})
                getattr(kwargs.get('post'), 'replies')['total'] = Summary({'total_count': 0})
                getattr(kwargs.get('post'), 'replies_reactions')['total'] = Summary({'total_count': 0})

                if 'data' in data_comments and data_comments['data']:
                    comments_reactions = 0
                    replies = 0
                    replies_reactions = 0
                    while True:
                        for comment in data_comments['data']:
                            comments_reactions += comment.get('reactions', {}) \
                                .get('summary', {}) \
                                .get('total_count', 0)
                            replies += comment.get('comments', {}) \
                                .get('summary', {}) \
                                .get('total_count', 0)

                            data_replies = comment.get('comments')
                            if 'data' in data_replies and data_replies['data']:
                                while True:
                                    for reply in data_replies['data']:
                                        replies_reactions += reply.get('reactions', {}) \
                                            .get('summary', {}) \
                                            .get('total_count', 0)

                                    next_page = data_replies.get('paging', {}).get('next')
                                    if next_page is not None:
                                        kwargs['recursion'] += 1
                                        data_replies = await self.extractor.fetch_url(next_page, session, 'GRAPH',
                                                                                      **kwargs)
                                    else:
                                        break

                        next_page = data_comments.get('paging', {}).get('next')
                        if next_page is not None:
                            kwargs['recursion'] += 1
                            data_comments = await self.extractor.fetch_url(next_page, session, 'GRAPH', **kwargs)
                        else:
                            break

                    summary = Summary({'total_count': comments_reactions})
                    getattr(kwargs.get('post'), 'comments_reactions')['total'] = summary

                    summary = Summary({'total_count': replies})
                    getattr(kwargs.get('post'), 'replies')['total'] = summary

                    summary = Summary({'total_count': replies_reactions})
                    getattr(kwargs.get('post'), 'replies_reactions')['total'] = summary

    def add_to_filter(self, posts):
        for post in posts:
            self.filter_ids.append(post.partial_id)

        self.filter_ids = list(dict.fromkeys(self.filter_ids))

    def set_author(self, data, interaction, interaction_type):
        if interaction_type == 'comments':
            author = next((node for node in self.nodes.nodes if node.node_id == data.get('from', {}).get('id')), None)
        else:
            author = next((node for node in self.nodes.nodes if node.node_id == data.get('id')), None)

        author = copy(author)
        if author is None:
            author = Bot(data.get('from', {}))

        author.feed = None

        interaction.person = author

    def create_network_plot(self):
        net = nx.Graph()

        for node in self.nodes.nodes:
            if node.feed.nodes:
                for post in node.feed.nodes:
                    if post.comments['data'] is not None and post.comments['data'].nodes:
                        for comment in post.comments['data'].nodes:
                            net.add_node(post.author.node_id,
                                         color=getattr(post.author, self.extractor.config.get('PLOT', 'color_field')),
                                         label=getattr(post.author, self.extractor.config.get('PLOT', 'label_field')))
                            net.add_node(comment.person.node_id,
                                         color=getattr(comment.person, self.extractor.config.get('PLOT', 'color_field')),
                                         label=getattr(comment.person, self.extractor.config.get('PLOT', 'label_field')))
                            net.add_edge(post.author.node_id, comment.person.node_id)

                    if post.reactions['data'] is not None and post.reactions['data']:
                        for reaction in post.reactions['data']:
                            net.add_node(post.author.node_id,
                                         color=getattr(post.author, self.extractor.config.get('PLOT', 'color_field')),
                                         label=getattr(post.author, self.extractor.config.get('PLOT', 'label_field')))
                            net.add_node(reaction.person.node_id,
                                         color=getattr(reaction.person, self.extractor.config.get('PLOT', 'color_field')),
                                         label=getattr(reaction.person, self.extractor.config.get('PLOT', 'label_field')))
                            net.add_edge(post.author.node_id, reaction.person.node_id)

        print(f'nodes = {net.number_of_nodes()}')
        print(f'edges = {net.number_of_edges()}')

        page_rank = nx.pagerank(net)

        colors = pd.DataFrame({node: net.nodes()[node]['color'] for node in net.nodes()}, index=[0]).transpose()
        colors = colors.rename(columns={0: 'color'})
        colors['color'] = pd.Categorical(colors['color'])
        color = ['#%06X' % randint(0, 0xFFFFFF) for i in range(len(colors['color'].unique().tolist()))]
        # noinspection PyUnresolvedReferences
        cmap = matplotlib.colors.ListedColormap(color)

        options = {
            'width': 0.1,
            'node_size': [page_rank[node] * 1000 for node in net.nodes()],
            'node_color': colors['color'].cat.codes,
            'cmap': cmap,
            'with_labels': False
        }

        plt.figure()
        pos_nodes = nx.kamada_kawai_layout(net)

        nx.draw(net, pos_nodes, **options)

        pos_attrs = {}
        for node, coords in pos_nodes.items():
            pos_attrs[node] = (coords[0], coords[1] + 0.04)

        options = {
            'labels': {node: net.nodes()[node]['label'] if page_rank[node] > 0.005 else '' for node in net.nodes()},
            'font_size': 7,
            'font_color': 'red'
        }

        nx.draw_networkx_labels(net, pos_attrs, **options)
        plt.savefig(self.extractor.config.get('MISC', 'output_dir') + 'interactions_plot.png')
        plt.show()
