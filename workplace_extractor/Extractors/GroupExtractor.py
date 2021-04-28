from workplace_extractor.Nodes.NodeCollection import GroupCollection

import numpy as np
import logging


class GroupExtractor:
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
        logging.info('Starting groups extraction')
        callback = self.callback if callback is None else callback

        groups = GroupCollection()
        http_calls = {0: {
            'url': f'{self.extractor.base_url_GRAPH}/community/groups?fields=id,name,privacy&limit={per_page}',
            'callback': callback,
            'results': groups,
            'params': None}}

        await self.extractor.fetch(http_calls)

        logging.info(f'Groups Extraction ended with {len(groups.nodes)} groups extracted')

        return groups

    async def callback(self, url, results, params, session):
        data = await self.extractor.fetch_url(url, session, 'Group')

        if 'collection' in data and data['collection']:
            results.extend(data['collection'])

        if 'next_page' in data and data['next_page']:
            await self.callback(data['next_page'], results, params, session)
