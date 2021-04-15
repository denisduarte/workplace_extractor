from workplace_extractor.miscelaneous import base_url_SCIM, fetch, fetch_url
import numpy as np
import pandas as pd
import logging


async def get_member_ids_callback(url, results, params, session):
    data = await fetch_url(url, session)
    if data is not None:
        users = pd.json_normalize(data['Resources'][0])

        logging.info(f'{users.shape[0]} users in for {url}')
        results.append(users)


async def get_member_total_callback(url, results, params, session):
    data = await fetch_url(url, session)

    results.append(data['totalResults'].item())


async def get_member_ids() -> np.ndarray:
    logging.info('Starting get_member_ids')
    # return np.array(['100041080666304', '100061671391770'])

    count_per_page = 100
    total = []
    http_calls = {0: {
        'url': base_url_SCIM,
        'callback': get_member_total_callback,
        'results': total,
        'params': None}}

    await fetch(http_calls)
    logging.info(f'Total number of members: {total[0]}')

    http_calls = {}
    starts = np.arange(1, total[0] + 1, step=count_per_page)
    iterator = np.nditer(starts, flags=['f_index'])
    member_ids = []
    for start in iterator:
        http_calls[iterator.index] = {'url': f'{base_url_SCIM}?count={count_per_page}&startIndex={start}',
                                      'callback': get_member_ids_callback,
                                      'results': member_ids,
                                      'params': None}
    await fetch(http_calls)
    member_ids = pd.concat(member_ids)
    member_ids = np.array(member_ids['id'].tolist())

    logging.info(f'get_member_ids ended with {member_ids.shape[0]} members extracted')

    return member_ids
