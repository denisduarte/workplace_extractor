from workplace_extractor.Extractors.GroupExtractor import GroupExtractor
from workplace_extractor.Extractors.PersonExtractor import PersonExtractor

from workplace_extractor.Nodes.NodeCollection import PostCollection
from workplace_extractor.Nodes.Author import Bot
from workplace_extractor.Nodes.Post import Summary, Post
from workplace_extractor.Nodes.Interaction import View, Reaction, Comment

from workplace_extractor.Counter import Counter

import logging


class PostExtractor:
    def __init__(self, extractor):
        self.extractor = extractor
        # ensure that the extractor has all required attributes
        ensure_attribute = ['since', 'until', 'author_id', 'feed_id']
        for attribute in ensure_attribute:
            if not hasattr(self.extractor, attribute):
                setattr(self.extractor, attribute, '')

        self.nodes = PostCollection()
        self.filter_ids = []
        self.counter = Counter('Posts')

    async def extract(self):
        # fields that should be extracted from posts using the GRAPH API
        fields = 'id,from,type,created_time,status_type,object_id,link,message,story'
        per_page = 50

        # Extract all groups
        logging.info('Loading group feeds')
        group_extractor = GroupExtractor(self.extractor)
        await group_extractor.extract()
        self.nodes.extend(group_extractor.nodes)
        logging.info('Group feeds loaded')

        # Extract all workplace members
        logging.info('Loading person feeds')
        people_extractor = PersonExtractor(self.extractor)
        await people_extractor.extract()
        self.nodes.extend(people_extractor.nodes)
        logging.info('Person feeds loaded')

        if feed_id := self.extractor.feed_id:
            # if an feed_id was passed, extract only posts published in this feed

            logging.info(f'Extracting posts from feed {feed_id}')

            # check if its a group feed
            node = next((node for node in group_extractor.nodes.nodes if node.node_id == feed_id), None)

            # check if its a person feed
            if node is None:
                node = next((node for node in people_extractor.nodes.nodes if node.node_id == feed_id), None)

            if node is None:
                # feed not found. halting.
                exit(0)

            http_calls = [{'url': self.extractor.config.get('URL', 'GRAPH') + f'/{node.node_id}/feed'
                                                                              f'?limit={per_page}'
                                                                              f'&fields={fields}'
                                                                              f'&since={self.extractor.since}'
                                                                              f'&until={self.extractor.until}',
                           'call': self.call,
                           'node': node,
                           'recursion': 1}]

            self.counter.label = 'Feed'
            self.counter.total = len(http_calls)

            await self.extractor.fetch(http_calls)
        else:
            # if no feed_id was passed, extract posts from all feeds (groups and members)

            logging.info(f'Extracting posts from {len(group_extractor.nodes.nodes)} groups.')
            http_calls = []
            for node in group_extractor.nodes.nodes:
                http_calls.append({'url': self.extractor.config.get('URL', 'GRAPH') + f'/{node.node_id}/feed'
                                                                                      f'?limit={per_page}'
                                                                                      f'&fields={fields}'
                                                                                      f'&since={self.extractor.since}'
                                                                                      f'&until={self.extractor.until}',
                                   'call': self.call,
                                   'node': node,
                                   'recursion': 1})

            self.counter.label = 'Group feeds'
            self.counter.total = len(http_calls)

            await self.extractor.fetch(http_calls)

            """"""
            logging.info(f'Extracting posts from {len(people_extractor.nodes.nodes)} people.')
            http_calls = []
            for node in people_extractor.nodes.nodes:
                http_calls.append({'url': self.extractor.config.get('URL', 'GRAPH') + f'/{node.node_id}/feed'
                                                                                      f'?limit={per_page}'
                                                                                      f'&fields={fields}'
                                                                                      f'&since={self.extractor.since}'
                                                                                      f'&until={self.extractor.until}',
                                   'call': self.call,
                                   'node': node,
                                   'recursion': 1})

            self.counter.label = 'Person feeds'
            self.counter.total = len(http_calls)
            self.counter.count = 0

            await self.extractor.fetch(http_calls)

            """"""

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
                    'call': self.call_info if self.extractor.export == 'Interactions' else self.call_count,
                    'post': post,
                    'recursion': 1})

                author = next((node for node in self.nodes.nodes if node.node_id == post.author_id), None)

                # including Bots
                if author is not None:
                    post.author = author
                else:
                    http_calls.append({'url': self.extractor.config.get('URL', 'GRAPH') + f'/{post.author_id}',
                                       'call': self.call_bot,
                                       'post': post})

        self.counter.label = 'Post info'
        self.counter.total = len(http_calls)
        self.counter.count = 0

        await self.extractor.fetch(http_calls)

    async def call(self, url, session, **kwargs):
        recursion = kwargs.copy()['recursion']

        data = await self.extractor.fetch_url(url, session, 'GRAPH', **kwargs)

        if data is not None:
            if 'id' in data:
                data = {'data': [data]}

            if 'data' in data and data.get('data'):
                collection = PostCollection([Post(post, self.extractor) for post in data.get('data')])

                # filter posts already extracted. Applies to posts in both group and author feed and for
                # GRAPH bug with infinite loop.
                collection.set_partial_id()
                collection.drop_duplicates(self.filter_ids)

                # if hashtags not empty, filter result by hashtag
                if self.extractor.hashtags:
                    collection.filter_hashtags(self.extractor.hashtags)

                # if person_id not empty, filter result by author
                if self.extractor.author_id:
                    collection.filter_author(self.extractor.author_id)

                kwargs.get('node').feed.extend(collection)
                self.add_to_filter(collection.nodes)

                next_page = data.get('paging', {}).get('next')
                if next_page is not None:
                    kwargs['recursion'] += 1
                    await self.call(next_page, session, **kwargs)

            if recursion == 1:
                self.counter.increment()
                print(self.counter)

    async def call_info(self, url, session, **kwargs):
        data = await self.extractor.fetch_url(url, session, 'GRAPH', **kwargs)

        await self.call_info_views(data.get('seen', {}), session, **kwargs)
        await self.call_info_reactions(data.get('reactions', {}), session, **kwargs)
        await self.call_info_comments(data.get('comments', {}), session, **kwargs)

        self.counter.increment()
        print(self.counter)

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
        if isinstance(data, list):
            data = {'data': data}

        if 'data' in data:
            for item in data['data']:
                comment = Comment(item)
                self.set_author(item, comment, 'comments')

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

        self.counter.increment()
        print(self.counter)

    async def call_count(self, url, session, **kwargs):
        recursion = kwargs.copy()['recursion']

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

                try:
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
                except Exception as e:
                    print(1)
        if recursion == 1:
            self.counter.increment()
            print(self.counter)

    def add_to_filter(self, posts):
        for post in posts:
            self.filter_ids.append(post.partial_id)

        self.filter_ids = list(dict.fromkeys(self.filter_ids))

    def set_author(self, data, interaction, interaction_type):
        if interaction_type == 'comments':
            author = next((node for node in self.nodes.nodes if node.node_id == data.get('from', {}).get('id')), None)
        else:
            author = next((node for node in self.nodes.nodes if node.node_id == data.get('id')), None)

        # author = copy(author)
        if author is None:
            author = Bot(data.get('from', {}))
        author.feed = None

        interaction.person = author
