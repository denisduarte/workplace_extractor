from workplace_extractor.Extractors import PostExtractor, CommentExtractor, GroupExtractor, MembersExtractor
from workplace_extractor.Extractors import PersonExtractor, InteractionExtractor, EventExtractor

# import pickle
import sys
import os
import logging
import asyncio
import aiohttp
import pandas as pd
import configparser


class AuthTokenError(Exception):
    pass


class Extractor(object):

    def __init__(self, **kwargs):

        print(kwargs)

        # the colnfig.ini file should be in the same folder as the app
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')

        # required options
        self.token = self.config.get('MISC', 'access_token')
        self.loglevel = self.config.get('MISC', 'loglevel')

        self.export = kwargs.get('export')
        self.csv = kwargs.get('csv')
        self.export_content = kwargs.get('export_content', False)
        self.hashtags = [hashtag.lower() for hashtag in kwargs.get('hashtags', '').replace('#', '').split(',')]

        # optional options
        args = ['since', 'until', 'post_id', 'group_id', 'event_id', 'author_id', 'feed_id',
                'active_only', 'create_ranking', 'create_gexf']
        for key, value in kwargs.items():
            if key in args:
                setattr(self, key, value)

        # setting semaphore to control the number of concurrent calls
        self.semaphore = asyncio.Semaphore(int(self.config.get('MISC', 'concurrent_calls')))
        # setting recursion limit to prevent python from interrumpting large calls
        sys.setrecursionlimit(int(self.config.get('MISC', 'max_recursion')) * 2)

    async def init(self):
        # create folder to save output
        output_folder = os.path.dirname(self.config.get('MISC', 'output_dir'))

        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        if self.loglevel != 'NONE':
            logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                                filename=f'{self.config.get("MISC", "output_dir")}/workplace_extractor.log',
                                level=getattr(logging, self.loglevel))

        # set the access token to be used in the http calls
        try:
            await self.set_token()
        except AuthTokenError as e:
            sys.exit(e)

    def extract(self):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._extract())

    async def _extract(self):
        await self.init()

        extractor = None
        if self.export == 'Posts':
            extractor = PostExtractor(extractor=self)
        elif self.export == 'Comments':
            extractor = CommentExtractor(extractor=self)
        elif self.export == 'People':
            extractor = PersonExtractor(extractor=self)
        elif self.export == 'Groups':
            extractor = GroupExtractor(extractor=self)
        elif self.export == 'Members':
            extractor = MembersExtractor(extractor=self)
        elif self.export == 'Attendees':
            extractor = EventExtractor(extractor=self)
        elif self.export == 'Interactions':
            extractor = InteractionExtractor(extractor=self)

        print("Extracting data... ")
        await extractor.extract()
        print("DONE")

        # with open(f'{self.config.get("MISC", "output_dir")}/workplace_event.pickle', 'wb') as handle:
        #    pickle.dump(extractor.nodes, handle)

        # with open(f'{self.config.get("MISC", "output_dir")}/workplace_event.pickle', 'rb') as handle:
        #    self.feeds = pickle.load(handle)

        print("Converting results... ", end=" ")
        nodes_pd = extractor.nodes.to_pandas(self)
        print("DONE")

        print("Saving CSV file... ", end=' ')
        nodes_pd.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=[" ", " "], regex=True) \
                .to_csv(f'{self.config.get("MISC", "output_dir")}/{self.csv}', index=False, sep=";")
        print("DONE")

        return nodes_pd

    async def set_token(self):
        with open(self.token) as file:
            self.token = file.readline().rstrip()

        # check if access token is valid
        http_call = [{'url': self.config.get('URL', 'GRAPH') + '/community/members?fields=id&limit=1',
                      'call': self.check_access_token}]

        await self.fetch(http_call)

    async def check_access_token(self, url, session, **kwargs):
        print("Checking access token...", end=" ")

        data = await self.fetch_url(url, session, 'GRAPH', **kwargs)
        if not('data' in data and data['data']):
            logging.error('FAILED')
            raise AuthTokenError('Invalid access token')

        print('DONE')

    async def fetch(self, http_calls):
        headers = {'Authorization': f'Bearer {self.token}',
                   # 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_3) AppleWebKit/537.36'
                   #               ' (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36',
                   'Content-Type': 'application/json'}

        async with aiohttp.ClientSession(headers=headers) as session:
            tasks = []
            for call in http_calls:
                kwargs = {arg: value for arg, value in call.items() if arg not in ('url', 'call')}
                tasks.append(asyncio.ensure_future(self.bound_fetch(call['url'], session, call['call'], **kwargs)))

            await asyncio.gather(*tasks)

    async def bound_fetch(self, url, session, call, **kwargs):
        async with self.semaphore:
            return await call(url, session, **kwargs)

    async def fetch_url(self, url, session, api='', **kwargs):
        logging.debug(f'GET {url}')
        logging.debug('Recursion ' + str(kwargs.get('recursion', 0)))

        # to prevent GRAPH bug with infinite recursion
        if kwargs.get('recursion', 0) > int(self.config.get('MISC', 'max_recursion')):
            logging.error('TOO MUCH RECURSION - ignoring next pages')
            logging.error(url)
            return {}

        tries = 0
        max_retries = int(self.config.get('MISC', 'max_http_retries'))
        while tries < max_retries:
            try:
                tries += 1
                async with session.get(url) as resp:
                    if resp.status in [400, 401, 404]:
                        logging.warning(f'Response returned {resp.status} for {url}.')
                        data = pd.DataFrame({'Errors': resp.status}, index=[0])
                        return data
                    elif resp.status in [500]:
                        logging.error(f'Error 500 when calling {url}')
                        raise Exception
                    else:
                        if api == 'SCIM':
                            content_type = 'text/javascript'
                        elif api == 'GRAPH':
                            content_type = 'application/json'

                        return await resp.json(content_type=content_type)

            except Exception as e:
                logging.error(f'Exception when trying to process {url}. API: {api}')
                logging.error(e)
                logging.warning(f' {tries} of {max_retries}')

        if tries == max_retries:
            raise TimeoutError('Too many retries')

    @staticmethod
    def str_to_bool(str_arg):
        str_arg = str_arg.upper()
        if str_arg == 'TRUE':
            return True
        elif str_arg == 'FALSE':
            return False
