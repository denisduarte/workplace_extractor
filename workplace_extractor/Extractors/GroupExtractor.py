from workplace_extractor.Nodes.NodeCollection import GroupCollection, NodeCollection
from workplace_extractor.Nodes.Group import Group

import numpy as np
import logging


class GroupExtractor:
    def __init__(self, extractor):
        self.total = np.nan
        self.nodes = GroupCollection()
        self.extractor = extractor

    async def extract(self, per_page=100, call=None):
        logging.info('Starting groups extraction')
        call = self.call if call is None else call

        http_calls = [{'url': f'{self.extractor.url_GRAPH}/community/groups?limit={per_page}' +
                              '&fields=id,name,privacy,admins{email}',
                       'call': call,
                       'groups': self.nodes}]

        await self.extractor.fetch(http_calls)

        logging.info(f'Groups Extraction ended with {len(self.nodes.nodes)} groups extracted')

    async def call(self, url, session, **kwargs):
        data = await self.extractor.fetch_url(url, session, 'GRAPH')

        if 'data' in data and data['data']:
            collection = NodeCollection([Group(group) for group in data['data']])
            kwargs.get('groups').extend(collection)

            next_page = data.get('paging', {}).get('next')

            if next_page is not None:
                await self.call(next_page, session, **kwargs)
