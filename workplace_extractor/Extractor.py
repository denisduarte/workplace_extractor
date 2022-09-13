from .Extractors import PostExtractor, CommentExtractor, GroupExtractor, MembersExtractor
from .Extractors import PersonExtractor, InteractionExtractor, EventExtractor

# import pickle
import sys
import os
import logging
import asyncio
import aiohttp
import pandas as pd
import random


class AuthTokenError(Exception):
    pass


class Extractor(object):

    def __init__(self, **kwargs):
        # required options
        self.token = kwargs.get('access_token')

        self.export = kwargs.get('export')
        self.export_file = f'{kwargs.get("export_file")}.xlsx'
        self.export_folder = kwargs.get('export_folder')

        self.export_content = kwargs.get('export_content', False)

        self.max_recursion = int(kwargs.get('max_recursion'))
        self.max_http_retries = int(kwargs.get('max_http_retries'))

        self.graph_url = kwargs.get('graph_url')
        self.scim_url = kwargs.get('scim_url')

        self.update_task_progress_func = kwargs.get('update_task_progress_func', None)

        if kwargs.get('hashtags', '') is not None:
            self.hashtags = [hashtag.lower() for hashtag in kwargs.get('hashtags', '').replace('#', '').split(',')]
        else:
            self.hashtags = []

            # optional options
        args = ['since', 'until', 'post_id', 'group_id', 'event_id', 'author_id', 'feed_id',
                'active_only', 'create_ranking', 'create_gexf', 'node_attributes', 'additional_node_attributes',
                'joining_column', 'people_attributes_file', 'people_attributes_join']
        for key, value in kwargs.items():
            if key in args:
                setattr(self, key, value)

        # setting semaphore to control the number of concurrent calls
        self.semaphore = asyncio.Semaphore(int(kwargs.get('concurrent_calls')))
        # setting recursion limit to prevent python from interrumpting large calls
        sys.setrecursionlimit(self.max_recursion * 2)

    async def init(self):
        if not os.path.exists(self.export_folder):
            os.makedirs(self.export_folder)

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

        await extractor.extract()

        nodes_pd = extractor.nodes.to_pandas(self)
        nodes_pd.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=[" ", " "], regex=True)

        return nodes_pd

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

        if self.update_task_progress_func and random.randint(1, 100) == 100:
            self.update_task_progress_func(url=url, message='randon log')

        # to prevent GRAPH bug with infinite recursion
        if kwargs.get('recursion', 0) > self.max_recursion:
            logging.error('TOO MUCH RECURSION - ignoring next pages')
            logging.error(url)
            return {}

        tries = 0
        max_retries = self.max_http_retries
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
                        #if api == 'SCIM':
                        #    content_type = 'text/javascript'
                        #elif api == 'GRAPH':
                        content_type = 'application/json'

                        return await resp.json(content_type=content_type)

            except Exception as e:
                logging.error(f'Exception when trying to process {url}. API: {api}')
                logging.error(e)
                logging.warning(f' {tries} of {max_retries}')

        if tries == max_retries:
            #raise TimeoutError('Too many retries')
            logging.critical(f'Response returned ERROR 500 for {url}.')

    @staticmethod
    def str_to_bool(str_arg):
        str_arg = str_arg.upper()
        if str_arg == 'TRUE':
            return True
        elif str_arg == 'FALSE':
            return False
