from ..Nodes.NodeCollection import GroupCollection, NodeCollection, PeopleCollection
from ..Nodes.Group import Group, Member
from ..Counter import Counter

import numpy as np
import logging


class GroupExtractor:
    def __init__(self, extractor):
        self.total = np.nan
        self.nodes = GroupCollection()
        self.extractor = extractor
        self.counter = Counter('Groups')

    async def extract(self, items_per_page, call=None):
        logging.info('Starting groups extraction')
        call = self.call if call is None else call

        http_calls = [{'url': self.extractor.graph_url + f'/community/groups?limit={items_per_page}&fields=id,name,privacy,'
                                                         'purpose,admins.limit(100){email},members.summary(1)',
                       'call': call,
                       'groups': self.nodes,
                       'recursion': 1}]

        self.counter.total = len(http_calls)

        await self.extractor.fetch(http_calls)

        logging.info(f'Groups Extraction ended with {len(self.nodes.nodes)} groups extracted')

    async def call(self, url, session, **kwargs):
        recursion = kwargs.copy()['recursion']

        data = await self.extractor.fetch_url(url, session, 'GRAPH', **kwargs)

        if data is None:
            print(1)

        if data.get('data', []):
            for group in data.get('data', []):
                admins = []

                kwargs_sub = {'admins': admins}
                await self.get_admins(group.get('admins', {}), session, **kwargs_sub)

                current_group = Group(group, admins)
                kwargs.get('groups').extend([current_group])

            next_page = data.get('paging', {}).get('next')
            if next_page is not None:
                kwargs['recursion'] += 1
                await self.call(next_page, session, **kwargs)

        if recursion == 1:
            self.counter.increment()
            print(self.counter)

    async def get_members(self, data, session, **kwargs):
        for member in data.get('data', []):
            kwargs['members'].append(member.get('email'))

        next_page = data.get('paging', {}).get('next')
        if next_page is not None:
            await self.call_members(next_page, session, **kwargs)

        return kwargs['members']

    async def call_members(self, url, session, **kwargs):
        data = await self.extractor.fetch_url(url, session, 'GRAPH', **kwargs)

        await self.get_members(data, session, **kwargs)

    async def get_admins(self, data, session, **kwargs):
        for admin in data.get('data', []):
            kwargs['admins'].append(admin.get('email'))

        next_page = data.get('paging', {}).get('next')
        if next_page is not None:
            await self.call_admins(next_page, session, **kwargs)

        return kwargs['admins']

    async def call_admins(self, url, session, **kwargs):
        data = await self.extractor.fetch_url(url, session, 'GRAPH', **kwargs)

        await self.get_admins(data, session, **kwargs)


class MembersExtractor:
    def __init__(self, extractor):
        self.group_id = extractor.group_id
        self.nodes = PeopleCollection()
        self.extractor = extractor

    async def extract(self, items_per_page, call=None):
        call = self.call if call is None else call

        http_calls = [{'url': self.extractor.graph_url + f'/{self.group_id}/members?limit={items_per_page}' +
                                                         '&fields=id,administrator,joined,added_by,'
                                                         'name,email,division,department,title,active',
                       'call': call,
                       'members': self.nodes,
                       'recursion': 1}]

        await self.extractor.fetch(http_calls)

        logging.info(f'Member Extraction ended with {len(self.nodes.nodes)} members extracted')

    async def call(self, url, session, **kwargs):
        data = await self.extractor.fetch_url(url, session, 'GRAPH', **kwargs)

        if 'data' in data and data['data']:
            collection = NodeCollection([Member(self.extractor, member) for member in data['data']])
            kwargs.get('members').extend(collection)

            next_page = data.get('paging', {}).get('next')
            if next_page is not None:
                kwargs['recursion'] += 1
                await self.call(next_page, session, **kwargs)
