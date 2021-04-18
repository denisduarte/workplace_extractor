from workplace_extractor.Nodes import NodeCollection, PostCollection
from workplace_extractor.Nodes.Post import Summary
from workplace_extractor.Extractors.FeedExtractor import PersonFeedExtractor, GroupFeedExtractor

import logging


class PostExtractor:
    def __init__(self, extractor, since, until):
        self.extractor = extractor
        self.since = since
        self.until = until
        self.feeds = NodeCollection()
        self.filter_ids = []

    @property
    def extractor(self):
        return self._extractor

    @extractor.setter
    def extractor(self, value):
        self._extractor = value

    @property
    def since(self):
        return self._since

    @since.setter
    def since(self, value):
        self._since = value

    @property
    def until(self):
        return self._until

    @until.setter
    def until(self, value):
        self._until = value

    @property
    def filter_ids(self):
        return self._filter_ids

    @filter_ids.setter
    def filter_ids(self, value):
        self._filter_ids = value

    @property
    def feeds(self):
        return self._feeds

    @feeds.setter
    def feeds(self, value):
        self._feeds = value

    async def extract(self):
        logging.info('Loading group feeds')
        group_feeds = await GroupFeedExtractor(self.extractor).extract()
        self.feeds.extend(group_feeds)
        logging.info('Group feeds loaded')

        logging.info('Loading person feeds')
        person_feeds = await PersonFeedExtractor(self.extractor).extract()
        self.feeds.extend(person_feeds)
        logging.info('Person feeds loaded')

        fields = 'id,from,type,created_time,status_type,object_id,link'

        logging.info(f'Extracting posts from {len(group_feeds.nodes)} groups.')
        http_calls = {}
        for feed in group_feeds.nodes:
            http_calls[feed.id] = {
              'url': f'{self.extractor.base_url_GRAPH}/{feed.id}/feed?limit=100&fields={fields}&since={self.since}&until={self.until}',
              'callback': self.callback,
              'results': None,
              'params': {'feed': feed}}

        await self.extractor.fetch(http_calls)

        logging.info(f'Extracting posts from {len(group_feeds.nodes)} people.')
        http_calls = {}
        for feed in person_feeds.nodes:
            http_calls[feed.id] = {
              'url': f'{self.extractor.base_url_GRAPH}/{feed.id}/feed?limit=100&fields={fields}&since={self.since}&until={self.until}',
              'callback': self.callback,
              'results': None,
              'params': {'feed': feed}}

        await self.extractor.fetch(http_calls)

        await self.fetch_posts_info()

        logging.info('Post extraction ended')

    async def fetch_posts_info(self):
        http_calls = {}
        for feed in self.feeds.nodes:
            for post in feed.posts.nodes:
                http_calls[f'seen_{feed.id}_{post.partial_id}'] = {
                    'url': f'{self.extractor.base_url_GRAPH}/{post.id}/seen?summary=TRUE',
                    'callback': self.callback_count,
                    'results': None,
                    'params': {'post': post, 'type': 'views'}}

                http_calls[f'reactions_{feed.id}_{post.partial_id}'] = {
                    'url': f'{self.extractor.base_url_GRAPH}/{post.id}/reactions?summary=TRUE',
                    'callback': self.callback_count,
                    'results': None,
                    'params': {'post': post, 'type': 'reactions'}}

                http_calls[f'comments_{feed.id}_{post.partial_id}'] = {
                    'url': f'{self.extractor.base_url_GRAPH}/{post.id}/comments/stream?summary=TRUE',
                    'callback': self.callback_count,
                    'results': None,
                    'params': {'post': post, 'type': 'comments'}}

                post.author = next((node.owner for node in self.feeds.nodes if node.id == post.author_id), None)

                # including Bots
                if post.author is None:
                    bot = []
                    http_calls = {
                        0: {'url': f'{self.extractor.base_url_GRAPH}/{post.author_id}',
                            'callback': self.callback_bot,
                            'results': bot,
                            'params': {'post': post}}
                    }
                    await self.extractor.fetch(http_calls)

        await self.extractor.fetch(http_calls)

    async def callback(self, url, results, params, session):
        data = await self.extractor.fetch_url(url, session, 'Post')

        if isinstance(data['collection'], PostCollection) and data['collection'].nodes:
            data['collection'].set_partial_id()
            data['collection'].drop_duplicates(self.filter_ids)

            if data['collection'].nodes:
                params['feed'].posts.extend(data['collection'].nodes)
                self.add_to_filter(data['collection'].nodes)

                if data['next_page']:
                    await self.callback(data['next_page'], results, params, session)

    async def callback_bot(self, url, results, params, session):
        data = await self.extractor.fetch_url(url, session, 'Bot')

        params['post'].author = data.nodes[0].owner
        self.feeds.extend(data)

    async def callback_count(self, url, results, params, session):
        data = await self.extractor.fetch_url(url, session, 'Summary')

        if isinstance(data, Summary):
            setattr(params['post'], params['type'], data.summary)

    def add_to_filter(self, posts):
        for post in posts:
            self.filter_ids.append(post.partial_id)

        self.filter_ids = list(dict.fromkeys(self.filter_ids))
