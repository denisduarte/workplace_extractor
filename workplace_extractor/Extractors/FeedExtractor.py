from workplace_extractor.Nodes import NodeCollection

import numpy as np
import logging


class PersonFeedExtractor:
    def __init__(self, extractor):
        self.total = np.nan
        self.extractor = extractor

    @property
    def total(self):
        return self._total

    @total.setter
    def total(self, value):
        self._total = value

    @property
    def extractor(self):
        return self._extractor

    @extractor.setter
    def extractor(self, value):
        self._extractor = value

    async def extract(self, per_page=100):
        logging.info('Starting get_member_ids')

        await self.fetch_total()

        http_calls = {}
        starts = np.arange(1, self.total + 1, step=per_page)
        iterator = np.nditer(starts, flags=['f_index'])

        people = NodeCollection()
        for start in iterator:
            http_calls[iterator.index] = {'url': f'{self.extractor.base_url_SCIM}?count={per_page}&startIndex={start}',
                                          'callback': self.callback,
                                          'results': people,
                                          'params': None}

        await self.extractor.fetch(http_calls)

        logging.info(f'get_member_ids ended with {len(people.nodes)} members extracted')

        return people

    async def fetch_total(self):
        total = []
        http_calls = {0: {
            'url': self.extractor.base_url_SCIM,
            'callback': self.total_callback,
            'results': total,
            'params': None}}

        await self.extractor.fetch(http_calls)

        self.total = total[0]

        logging.info(f'Total number of members: {self.total}')

    async def callback(self, url, results, params, session):
        data = await self.extractor.fetch_url(url, session, 'PersonFeed')
        results.extend(data)

    async def total_callback(self, url, results, params, session):
        data = await self.extractor.fetch_url(url, session, 'SCIM')

        results.append(data['totalResults'])


class GroupFeedExtractor:

    def __init__(self, extractor):
        self.total = np.nan
        self.extractor = extractor

    @property
    def total(self):
        return self._total

    @total.setter
    def total(self, value):
        self._total = value

    async def extract(self, per_page=100):
        logging.info('Starting get_group_ids')

        groups = NodeCollection()
        http_calls = {0: {
            'url': f'{self.extractor.base_url_GRAPH}/community/groups?fields=id,name&limit={per_page}',
            'callback': self.callback,
            'results': groups,
            'params': None}}

        await self.extractor.fetch(http_calls)

        logging.info(f'get_group_ids ended with groups extracted')

        return groups

    async def callback(self, url, results, params, session):
        data = await self.extractor.fetch_url(url, session, 'GroupFeed')
        results.extend(data['collection'])

        if data['next_page']:
            await self.callback(data['next_page'], results, params, session)
