import aiohttp
import asyncio
import pandas as pd
import logging
import pickle

base_url_GRAPH = 'https://graph.facebook.com'
base_url_SCIM = 'https://www.workplace.com/scim/v1/Users'

access_token = ''
semaphore = asyncio.Semaphore(400)


class AuthTokenError(Exception):
    """Base class for other exceptions"""
    pass


async def check_access_token(url, results, params, session):
    data = await fetch_url(url, session)

    if data.empty:
        logging.error('The access token is invalid. Halting Execution.')
        raise AuthTokenError('Invalid access token')
    else:
        logging.info('Access token check passed.')


async def set_token(token_file):
    global access_token
    with open(token_file) as file:
        access_token = file.readline().rstrip()

    # check if access token is valid
    http_call = {0: {
        'url': f'{base_url_GRAPH}/community/members?fields=id&limit=1',
        'callback': check_access_token,
        'results': [],
        'params': {}}}

    await fetch(http_call)


async def fetch_url(url, session):
    logging.debug(f'GET {url}')

    # if url == 'https://graph.facebook.com/100048272327634/feed?limit=100&fields=id,from,type,created_time,' \
    #          'status_type,object_id,link&since=2021-03-01&until=2021-03-31':
    #    raise Exception('Chega!!!')

    data = pd.DataFrame()

    tries = 0
    max_retries = 10
    while tries < max_retries:
        try:
            tries += 1
            async with session.get(url) as resp:

                if resp.status in [400, 404]:
                    logging.warning(f'Response returned {resp.status} for {url}.')
                    data = pd.DataFrame({'Errors': resp.status}, index=[0])
                    return data

                try:
                    # for SCIM API
                    response = await resp.json(content_type='text/javascript')
                except aiohttp.ContentTypeError:
                    # for GRAPH API
                    response = await resp.json(content_type='application/json')
                except Exception as e:
                    logging.error(f'The content type for {url} was different from expected')
                    raise e

                data = pd.json_normalize(response)
                break

        except Exception as e:
            logging.error(f'Exception when trying to process {url}.')
            logging.error(e)
            logging.info(f'Retrying {tries} of {max_retries}')

    return data


async def bound_fetch(url, callback, results, params, session):
    # Getter function with semaphore.
    async with semaphore:
        return await callback(url, results, params, session)


async def fetch(http_calls):
    headers = {'Authorization': f'Bearer {access_token}',
               'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_3) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/89.0.4389.114 Safari/537.36',
               'Content-Type': 'application/json'}
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = []
        for key, value in http_calls.items():
            tasks.append(asyncio.ensure_future(bound_fetch(value['url'],
                                                           value['callback'],
                                                           value['results'],
                                                           value['params'],
                                                           session)))

        await asyncio.gather(*tasks)


def combine_data(data, group_first) -> pd.DataFrame:
    data['member_posts'] = pd.concat(data['member_posts']).drop_duplicates(subset='id')
    data['group_posts'] = pd.concat(data['group_posts']).drop_duplicates(subset='id')
    data['post_views'] = pd.concat(data['post_views']) \
        .drop_duplicates(subset='id') \
        .rename(columns={'count': 'views'})
    data['post_comments'] = pd.concat(data['post_comments']) \
        .drop_duplicates(subset='id') \
        .rename(columns={'count': 'comments'})
    data['post_reactions'] = pd.concat(data['post_reactions']) \
        .drop_duplicates(subset='id') \
        .rename(columns={'count': 'reactions'})

    data['nodes_info'] = pd.concat(data['nodes_info']).drop_duplicates(subset='from.id')

    posts = [data['group_posts'], data['member_posts']] if group_first else [data['member_posts'], data['group_posts']]
    posts = pd.concat(posts).drop_duplicates(subset='partial_id')

    if not posts.empty:
        posts = posts.merge(data['nodes_info'], on='from.id') \
                     .merge(data['post_views'], on='id') \
                     .merge(data['post_comments'], on='id') \
                     .merge(data['post_reactions'], on='id')

    return posts
