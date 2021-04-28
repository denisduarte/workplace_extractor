from workplace_extractor.Extractors.GroupExtractor import GroupExtractor
from workplace_extractor.Extractors.PersonExtractor import PersonExtractor
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

        #call people extractor using a different callback function
        people_extractor = PersonExtractor(self.extractor)

        people = await people_extractor.extract(callback=self.callback)

        return people

    async def callback(self, url, results, params, session):
        data = await self.extractor.fetch_url(url, session, 'PersonFeed')
        results.extend(data)


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

        #call people extractor using a different callback function
        group_extractor = GroupExtractor(self.extractor)

        groups = await group_extractor.extract(callback=self.callback)

        logging.info(f'get_group_ids ended with groups extracted')

        return groups

    async def callback(self, url, results, params, session):
        data = await self.extractor.fetch_url(url, session, 'GroupFeed')
        results.extend(data['collection'])

        if data['next_page']:
            await self.callback(data['next_page'], results, params, session)
