from workplace_extractor.miscelaneous import base_url_GRAPH, fetch, fetch_url
import numpy as np
import pandas as pd
import logging
import aiohttp


async def get_group_ids_callback(url, results, params, session):
    data = await fetch_url(url, session)
    if data is not None:
        posts = pd.json_normalize(data['data'][0])
        results.append(posts)

    if 'paging.next' in data.columns:
        await get_group_ids_callback(data['paging.next'].item(), results, params, session)


async def get_group_ids() -> np.ndarray:
    logging.info('Starting get_group_ids')

    # return np.array(['900752023414059', '176968500657062', '504877917019677'])

    group_ids = []
    http_calls = {0: {
        'url': f'{base_url_GRAPH}/community/groups?fields=id&limit=100',
        'callback': get_group_ids_callback,
        'results': group_ids,
        'params': None}}

    await fetch(http_calls)

    group_ids = pd.concat(group_ids)
    group_ids = np.array(group_ids['id'].tolist())

    logging.info(f'get_group_ids ended with {group_ids.shape[0]} groups extracted')

    return group_ids
