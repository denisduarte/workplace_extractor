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
        http_calls = {
            0: {'url': f'{self.extractor.base_url_SCIM}/100061671391770', 'callback': self.callback, 'results': people, 'params': None},
            1: {'url': f'{self.extractor.base_url_SCIM}/100041080666304', 'callback': self.callback, 'results': people, 'params': None},
            2: {'url': f'{self.extractor.base_url_SCIM}/100048187463378', 'callback': self.callback, 'results': people, 'params': None},
            3: {'url': f'{self.extractor.base_url_SCIM}/100041789363708', 'callback': self.callback, 'results': people, 'params': None},
            4: {'url': f'{self.extractor.base_url_SCIM}/100048629432452', 'callback': self.callback, 'results': people, 'params': None},
            5: {'url': f'{self.extractor.base_url_SCIM}/100048571625443', 'callback': self.callback, 'results': people, 'params': None},
            6: {'url': f'{self.extractor.base_url_SCIM}/100041774161937', 'callback': self.callback, 'results': people, 'params': None},
            7: {'url': f'{self.extractor.base_url_SCIM}/100041504117021', 'callback': self.callback, 'results': people, 'params': None},
            8: {'url': f'{self.extractor.base_url_SCIM}/100048497258001', 'callback': self.callback, 'results': people, 'params': None}
        }
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

        http_calls = {
                0: {'url': f'{self.extractor.base_url_GRAPH}/176968500657062', 'callback': self.callback, 'results': groups, 'params': None},
                1: {'url': f'{self.extractor.base_url_GRAPH}/3032149420166511', 'callback': self.callback, 'results': groups, 'params': None},
                2: {'url': f'{self.extractor.base_url_GRAPH}/1224091774442323', 'callback': self.callback, 'results': groups, 'params': None}
        }

        await self.extractor.fetch(http_calls)

        logging.info(f'get_group_ids ended with groups extracted')

        return groups

    async def callback(self, url, results, params, session):
        data = await self.extractor.fetch_url(url, session, 'GroupFeed')
        results.extend(data['collection'])

        if data['next_page']:
            await self.callback(data['next_page'], results, params, session)
