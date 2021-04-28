from workplace_extractor.Nodes.NodeCollection import PeopleCollection

import numpy as np
import logging

class PersonExtractor:
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

    async def extract(self, per_page=100, callback=None):
        logging.info('Starting people extraction')
        callback = self.callback if callback is None else callback

        await self.fetch_total()

        http_calls = {}
        starts = np.arange(1, self.total + 1, step=per_page)
        iterator = np.nditer(starts, flags=['f_index'])

        people = PeopleCollection()
        for start in iterator:
            http_calls[iterator.index] = {'url': f'{self.extractor.base_url_SCIM}?count={per_page}&startIndex={start}',
                                          'callback': callback,
                                          'results': people,
                                          'params': None}

        await self.extractor.fetch(http_calls)

        logging.info(f'People Extraction ended with {len(people.nodes)} members extracted')

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
        data = await self.extractor.fetch_url(url, session, 'Person')
        results.extend(data)

    async def total_callback(self, url, results, params, session):
        data = await self.extractor.fetch_url(url, session, 'SCIM')

        results.append(data['totalResults'])
