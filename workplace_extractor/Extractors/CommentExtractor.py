from workplace_extractor.Extractors import PostExtractor
from workplace_extractor.Extractors.PersonExtractor import PersonExtractor
from workplace_extractor.Nodes.NodeCollection import CommentCollection
from workplace_extractor.Nodes.Author import Bot

import logging


class CommentExtractor:
    def __init__(self, extractor):
        self.extractor = extractor
        self.nodes = CommentCollection()

    async def extract(self):
        logging.info('Loading comments extraction')

        post_extractor = PostExtractor(extractor=self.extractor)

        people_extractor = PersonExtractor(self.extractor)
        await people_extractor.extract()

        post_extractor.nodes.extend(people_extractor.nodes)

        # Dummy feed
        feed = Bot({'id': 1})

        # Get post
        fields = 'id,from,type,created_time,status_type,object_id,link,message,story'

        http_calls = [{'url': self.extractor.config.get('URL', 'GRAPH') + f'/{self.extractor.post_id}?'
                                                                          f'fields={fields}',
                       'call': post_extractor.call,
                       'node': feed,
                       'recursion': 1}]

        post_extractor.counter.total = len(http_calls)
        post_extractor.counter.count = 0

        await self.extractor.fetch(http_calls)

        # fields that should be extracted from posts using the GRAPH API
        fields = 'id,created_time,message,like_count,from,' \
                 'reactions.limit(100).summary(1),' \
                 'comments.limit(100).summary(1)' \
                 '{' \
                 'created_time,from,message,' \
                 'reactions.limit(100).summary(1)' \
                 '}'

        http_calls = [{'url': self.extractor.config.get('URL', 'GRAPH') + f'/{self.extractor.post_id}/comments'
                                                                          f'?limit=100&fields={fields}',
                       'call': self.call,
                       'node': feed,
                       'postExtractor': post_extractor,
                       'post': feed.feed.nodes[0],
                       'type': 'comments',
                       'recursion': 1}]

        post_extractor.counter.total = len(http_calls)
        post_extractor.counter.count = 0

        await self.extractor.fetch(http_calls)

        # if hashtags not empty, filter result by hashtag
        if self.extractor.hashtags:
            feed.feed.nodes[0].comments.get('data').filter_hashtags(self.extractor.hashtags)

        self.nodes.extend(feed.feed.nodes[0].comments.get('data'))

        logging.info('Comments extraction ended')

    async def call(self, url, session, **kwargs):
        data = await self.extractor.fetch_url(url, session, 'GRAPH', **kwargs)

        if 'data' in data and data.get('data'):
            await kwargs['postExtractor'].call_info_comments(data.get('data'), session, **kwargs)

            next_page = data.get('paging', {}).get('next')
            if next_page is not None:
                kwargs['recursion'] += 1
                await self.call(next_page, session, **kwargs)
