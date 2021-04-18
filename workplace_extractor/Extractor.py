from workplace_extractor.Extractors import PostExtractor
from workplace_extractor.Nodes import Node
from workplace_extractor.Nodes import Post
from workplace_extractor.Nodes.NodeCollection import PostCollection, NodeCollection
from workplace_extractor.Nodes.Feed import PersonFeed, GroupFeed, BotFeed
from workplace_extractor.Nodes.Post import Summary

import sys
import logging
import asyncio
import aiohttp


class AuthTokenError(Exception):
    pass


class Extractor(object):

    semaphore = asyncio.Semaphore(400)

    def __init__(self, token, since, until, csv, loglevel):
        self.token = token
        self.since = since
        self.until = until
        self.csv = csv
        self.loglevel = loglevel
        self.base_url_GRAPH = 'https://graph.facebook.com'
        self.base_url_SCIM = 'https://www.workplace.com/scim/v1/Users'

    @property
    def token(self):
        return self._token

    @token.setter
    def token(self, value):
        self._token = value

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
    def csv(self):
        return self._csv

    @csv.setter
    def csv(self, value):
        self._csv = value

    @property
    def loglevel(self):
        return self._loglevel

    @loglevel.setter
    def loglevel(self, value):
        self._loglevel = value

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._extract())

    async def _extract(self):
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                            filename='workplace_extractor.log',
                            level=getattr(logging, self.loglevel))

        # set the access token to be used in the http calls
        try:
            await self.set_token()
        except AuthTokenError as e:
            sys.exit(e)

        extractor = PostExtractor(extractor=self, since=self.since, until=self.until)

        logging.info(f'Extracting posts from')
        await extractor.extract()
        logging.info(f'Extraction of posts finished')

        logging.info(f'Converting to Pandas')
        posts = extractor.feeds.to_pandas()
        logging.info(f'Done converting to Pandas')

        # if a name for the csv file was passed, save the posts in csv format
        if self.csv:
            posts.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=[" ", " "], regex=True) \
                 .to_csv(self.csv, index=False, sep=";")

        logging.info(f'Post extraction finished')

        return posts

    async def set_token(self):
        with open(self.token) as file:
            self.token = file.readline().rstrip()

        # check if access token is valid
        http_call = {0: {
            'url': f'{self.base_url_GRAPH}/community/members?fields=id&limit=1',
            'callback': self.check_access_token,
            'results': [],
            'params': {}}}

        await self.fetch(http_call)

    async def check_access_token(self, url, results, params, session):
        data = await self.fetch_url(url, session, 'Token')

        if not isinstance(data, Node):
            logging.error('The access token is invalid. Halting Execution.')
            raise AuthTokenError('Invalid access token')
        else:
            logging.info('Access token check passed.')

    async def fetch(self, http_calls):
        headers = {'Authorization': f'Bearer {self.token}',
                   'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_3) AppleWebKit/537.36 (KHTML, like Gecko)'
                                 ' Chrome/89.0.4389.114 Safari/537.36',
                   'Content-Type': 'application/json'}
        async with aiohttp.ClientSession(headers=headers) as session:
            tasks = []
            for key, value in http_calls.items():
                tasks.append(asyncio.ensure_future(self.bound_fetch(value['url'],
                                                   value['callback'],
                                                   value['results'],
                                                   value['params'],
                                                   session)))

            await asyncio.gather(*tasks)

    async def bound_fetch(self, url, callback, results, params, session):
        # Getter function with semaphore.
        async with self.semaphore:
            return await callback(url, results, params, session)

    async def fetch_url(self, url, session, type=''):
        logging.debug(f'GET {url}')

        tries = 0
        max_retries = 10
        while tries < max_retries:
            try:
                tries += 1
                async with session.get(url) as resp:

                    if resp.status in [400, 401, 404]:
                        logging.warning(f'Response returned {resp.status} for {url}.')
                        data = pd.DataFrame({'Errors': resp.status}, index=[0])
                        return data

                    if type == 'PersonFeed':
                        # for SCIM API
                        response = await resp.json(content_type='text/javascript')
                        if 'Resources' in response.keys():
                            collection = NodeCollection([PersonFeed(person) for person in response['Resources']])
                            return collection

                        # Remove after tests
                        collection = NodeCollection([PersonFeed(response)])
                        return collection
                        ###

                    elif type == 'GroupFeed':
                        response = await resp.json(content_type='application/json')
                        if 'data' in response.keys():
                            collection = NodeCollection([GroupFeed(group) for group in response['data']])
                            return dict(collection=collection, next_page=response.get('paging', {}).get('next', None))

                        # Remove after tests
                        collection = NodeCollection([GroupFeed(response)])
                        return dict(collection=collection, next_page=None)
                        ###

                    elif type == 'Bot':
                        response = await resp.json(content_type='application/json')
                        return NodeCollection([BotFeed(response)])

                    elif type == 'Post':
                        response = await resp.json(content_type='application/json')
                        collection = PostCollection([Post(post) for post in response['data']])
                        return dict(collection=collection, next_page=response.get('paging', {}).get('next', None))

                    elif type == 'Summary':
                        response = await resp.json(content_type='application/json')
                        if 'summary' not in response:
                            return Summary({})

                        return Summary(response['summary'])

                    elif type == 'Token':
                        response = await resp.json(content_type='application/json')
                        return Node(response['data'][0])

                    elif type == 'GRAPH':
                        response = await resp.json(content_type='application/json')
                        return response['data']

                    elif type == 'SCIM':
                        response = await resp.json(content_type='text/javascript')
                        return response

            except Exception as e:
                logging.error(f'Exception when trying to process {url}.')
                logging.error(e)
                logging.warning(f'Retrying {tries} of {max_retries}')

        if tries == max_retries:
            raise TimeoutError('Too many retries')