from workplace_extractor.Extractors import PostExtractor
from workplace_extractor.Extractors.GroupExtractor import GroupExtractor
from workplace_extractor.Extractors.PersonExtractor import PersonExtractor

import pickle
import sys
import os
import logging
import asyncio
import aiohttp
import pandas as pd


class AuthTokenError(Exception):
    pass


class Extractor(object):

    semaphore = asyncio.Semaphore(300)

    def __init__(self, token, export, since, until, csv, loglevel):
        self.token = token
        self.export = export
        self.since = since
        self.until = until
        self.csv = csv
        self.loglevel = loglevel
        self.url_GRAPH = 'https://graph.facebook.com'
        self.url_SCIM = 'https://www.workplace.com/scim/v1/Users'

    async def init(self):
        # create folder to save output
        output_folder = os.path.dirname('output/')
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                            filename='output/workplace_extractor.log',
                            level=getattr(logging, self.loglevel))

        # set the access token to be used in the http calls
        try:
            await self.set_token()
        except AuthTokenError as e:
            sys.exit(e)

    def extract(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._extract())

    async def _extract(self):
        await self.init()

        extractor = None
        if self.export == 'POSTS':
            extractor = PostExtractor(extractor=self, since=self.since, until=self.until)
        elif self.export == 'PEOPLE':
            extractor = PersonExtractor(self)
        elif self.export == 'GROUPS':
            extractor = GroupExtractor(self)
        elif self.export == 'INTERACTIONS':
            extractor = PostExtractor(extractor=self, since=self.since, until=self.until)

        logging.info(f'Extracting posts from')
        await extractor.extract()
        logging.info(f'Extraction of posts finished')

        if self.export == 'INTERACTIONS':
            with open('output/workplace_interactions.pickle', 'wb') as handle:
                pickle.dump(extractor.nodes, handle)

        logging.info(f'Converting to Pandas')
        nodes_pd = extractor.nodes.to_pandas(self)
        logging.info(f'Done converting to Pandas')

        # if a name for the csv file was passed, save the posts in csv format
        if self.csv:
            nodes_pd.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=[" ", " "], regex=True) \
                 .to_csv(f'output/{self.csv}', index=False, sep=";")

        logging.info(f'Post extraction finished')

        return nodes_pd

    async def set_token(self):
        with open(self.token) as file:
            self.token = file.readline().rstrip()

        # check if access token is valid
        http_call = [{'url': f'{self.url_GRAPH}/community/members?fields=id&limit=1',
                      'call': self.check_access_token}]

        await self.fetch(http_call)

    async def check_access_token(self, url, session, **kwargs):
        data = await self.fetch_url(url, session, 'GRAPH')

        if 'data' in data and data['data']:
            logging.info('Access token check passed.')
        else:
            logging.error('The access token is invalid. Halting Execution.')
            raise AuthTokenError('Invalid access token')

    async def fetch(self, http_calls):
        headers = {'Authorization': f'Bearer {self.token}',
                   'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_3) AppleWebKit/537.36 (KHTML, like Gecko)'
                                 ' Chrome/89.0.4389.114 Safari/537.36',
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

    @staticmethod
    async def fetch_url(url, session, api=''):
        logging.debug(f'GET {url}')

        tries = 0
        max_retries = 100
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
                logging.warning(f'Retrying {tries} of {max_retries}')

        if tries == max_retries:
            raise TimeoutError('Too many retries')
