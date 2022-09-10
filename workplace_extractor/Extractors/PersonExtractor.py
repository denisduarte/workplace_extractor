from ..Nodes.Author import Person
from ..Nodes.NodeCollection import PeopleCollection, NodeCollection
from ..Counter import Counter

import numpy as np
# import logging


class PersonExtractor:
    def __init__(self, extractor):
        self.extractor = extractor
        # ensure that the extractor has all required attributes
        ensure_attribute = ['active_only']
        for attribute in ensure_attribute:
            if not hasattr(self.extractor, attribute):
                setattr(self.extractor, attribute, '')

        self.total = np.nan
        self.nodes = PeopleCollection()
        self.counter = Counter('Person')

    async def extract(self, per_page=100, call=None):
        # logging.info('Starting people extraction')
        call = self.call if call is None else call

        await self.fetch_total()

        http_calls = []
        starts = np.arange(1, self.total + 1, step=per_page)
        iterator = np.nditer(starts, flags=['f_index'])

        for start in iterator:
            http_calls.append({'url': self.extractor.scim_url + f'?count={per_page}&startIndex={start}',
                               'call': call,
                               'people': self.nodes})

        self.counter.total = len(http_calls)

        await self.extractor.fetch(http_calls)

        # filter only people with 'active = True'
        if self.extractor.active_only:
            self.nodes.nodes = [person for person in self.nodes.nodes if person.active]

        # logging.info(f'People Extraction ended with {len(self.nodes.nodes)} members extracted')

    async def fetch_total(self):
        total = []
        http_calls = [{'url': self.extractor.scim_url,
                       'call': self.call_total,
                       'total': total}]

        await self.extractor.fetch(http_calls)

        self.total = total[0]
        # logging.info(f'Total number of members: {self.total}')

    async def call(self, url, session, **kwargs):
        data = await self.extractor.fetch_url(url, session, 'SCIM', **kwargs)

        collection = NodeCollection()
        if 'Resources' in data:
            collection.extend([Person(self.extractor, person) for person in data['Resources']])

        kwargs.get('people').extend(collection)

        self.counter.increment()
        print(self.counter)

    async def call_total(self, url, session, **kwargs):
        data = await self.extractor.fetch_url(url, session, 'SCIM', **kwargs)
        kwargs.get('total').append(data['totalResults'])
