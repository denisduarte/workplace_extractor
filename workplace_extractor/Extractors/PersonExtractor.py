from workplace_extractor.Nodes.Author import Person
from workplace_extractor.Nodes.NodeCollection import PeopleCollection, NodeCollection

import numpy as np
import logging


class PersonExtractor:
    def __init__(self, extractor):
        self.total = np.nan
        self.nodes = PeopleCollection()
        self.extractor = extractor

    async def extract(self, per_page=100, call=None):
        logging.info('Starting people extraction')
        call = self.call if call is None else call

        await self.fetch_total()

        http_calls = []
        starts = np.arange(1, self.total + 1, step=per_page)
        iterator = np.nditer(starts, flags=['f_index'])

        for start in iterator:
            http_calls.append({'url': f'{self.extractor.url_SCIM}?count={per_page}&startIndex={start}',
                               'call': call,
                               'people': self.nodes})

        await self.extractor.fetch(http_calls)

        logging.info(f'People Extraction ended with {len(self.nodes.nodes)} members extracted')

    async def fetch_total(self):
        total = []
        http_calls = [{'url': self.extractor.url_SCIM,
                       'call': self.call_total,
                       'total': total}]

        await self.extractor.fetch(http_calls)

        self.total = total[0]
        logging.info(f'Total number of members: {self.total}')

    async def call(self, url, session, **kwargs):
        data = await self.extractor.fetch_url(url, session, 'SCIM')

        collection = NodeCollection()
        if 'Resources' in data:
            collection.extend([Person(person) for person in data['Resources']])

        kwargs.get('people').extend(collection)

    async def call_total(self, url, session, **kwargs):
        data = await self.extractor.fetch_url(url, session, 'SCIM')
        kwargs.get('total').append(data['totalResults'])
