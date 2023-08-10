from .PersonExtractor import PersonExtractor
from ..Nodes.NodeCollection import PollOptionsCollection
from ..Nodes.Poll import PollOption
from ..Counter import Counter
from ..Nodes.Author import Bot

import numpy as np
import logging
import pickle


class PollExtractor:
    def __init__(self, extractor):
        self.extractor = extractor

        self.people_extractor = PersonExtractor(extractor)
        self.options_ids = []
        self.nodes = PollOptionsCollection()

        self.total = np.nan
        self.counter = Counter('Events')

    async def extract(self, items_per_page):
        logging.info('Starting groups extraction')

        await self.people_extractor.extract(items_per_page=items_per_page)

        # pega as opcoes da pesquisa
        http_calls = [{'url': f'{self.extractor.graph_url}/{self.extractor.post_id}'
                              f'?fields=poll.limit({items_per_page})' + '{options}',
                       'call': self.call_options,
                       'recursion': 1}]
        await self.extractor.fetch(http_calls)

        # pega as respostas
        http_calls = []
        for option in self.options_ids:
            http_calls.append(
                {'url': f'{self.extractor.graph_url}/{option}'
                        f'?fields=name,vote_count,votes.limit({items_per_page})',
                 'call': self.call_votes,
                 'node': option,
                 'recursion': 1})
        self.counter.label = 'Votes'
        self.counter.total = len(http_calls)
        self.counter.count = 0


        await self.extractor.fetch(http_calls)

        logging.info(f'Poll Extraction ended with {len(self.options_ids)} option extracted')

    async def call_options(self, url, session, **kwargs):
        data = await self.extractor.fetch_url(url, session, 'GRAPH', **kwargs)
        data = data.get('poll', {}).get('options', {}).get('data', [])

        if data:
            self.options_ids = [option.get('id') for option in data]

    async def call_votes(self, url, session, **kwargs):
        recursion = kwargs.copy()['recursion']

        data = await self.extractor.fetch_url(url, session, 'GRAPH', **kwargs)

        if data:
            option = PollOption(data)

            votes = data.get('votes', {})

            while votes and votes is not None:
                for voter_id in votes.get('data', {}):
                    voter = next(
                        (node for node in self.people_extractor.nodes.nodes if node.node_id == voter_id.get('id')),
                        None)

                    if voter is None:
                        voter = Bot(self.extractor, voter_id)

                    option.voters.extend(voter)

                next_page = votes.get('paging', {}).get('next')
                if next_page is not None:
                    kwargs['recursion'] += 1
                    votes = await self.extractor.fetch_url(next_page,  session, 'GRAPH', **kwargs)
                else:
                    break

            self.nodes.extend(option)
