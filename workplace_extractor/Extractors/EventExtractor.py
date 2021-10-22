from workplace_extractor.Nodes.NodeCollection import EventCollection
from workplace_extractor.Nodes.Event import Event
from workplace_extractor.Extractors.PersonExtractor import PersonExtractor
from workplace_extractor.Counter import Counter


import numpy as np
import logging


class EventExtractor:
    def __init__(self, extractor):
        self.extractor = extractor

        self.people_extractor = PersonExtractor(extractor)
        self.nodes = EventCollection()

        self.total = np.nan
        self.counter = Counter('Events')

    async def extract(self, per_page=100):
        logging.info('Starting groups extraction')

        await self.people_extractor.extract()

        fields = 'id, name,start_time,end_time,description,type,owner'

        http_calls = []
        for node in self.people_extractor.nodes.nodes:
            http_calls.append({'url': self.extractor.config.get('URL', 'GRAPH') + f'/{node.node_id}/events/attending?'
                                                                                  f'limit={per_page}'
                                                                                  f'&fields={fields}',
                               'call': self.call,
                               'node': node,
                               'recursion': 1})

        self.counter.label = 'Person'
        self.counter.total = len(http_calls)
        self.counter.count = 0

        await self.extractor.fetch(http_calls)

        logging.info(f'Groups Extraction ended with {len(self.nodes.nodes)} groups extracted')

    async def call(self, url, session, **kwargs):
        recursion = kwargs.copy()['recursion']

        data = await self.extractor.fetch_url(url, session, 'GRAPH', **kwargs)

        if data.get('data', []):
            for event_data in data.get('data', []):

                event_id = event_data.get('id')

                if event_id == self.extractor.event_id:
                    event = next((node for node in self.nodes.nodes if node.node_id == event_id), None)
                    if event is None:
                        event = Event(event_data)
                        self.nodes.extend(event)

                        owner_id = event_data.get('owner', {}).get('id')
                        owner = next((node for node in self.people_extractor.nodes.nodes if node.node_id == owner_id),
                                     None)
                        event.owner = owner

                    event.attendees.extend(kwargs.get('node'))

            next_page = data.get('paging', {}).get('next')
            if next_page is not None:
                kwargs['recursion'] += 1
                await self.call(next_page, session, **kwargs)

        if recursion == 1:
            self.counter.increment()
            print(self.counter)
