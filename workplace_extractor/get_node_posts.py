from workplace_extractor.miscelaneous import base_url_GRAPH, base_url_SCIM, fetch, fetch_url
import numpy as np
import pandas as pd
import logging
import pickle


async def get_node_info_callback(url, results, params, session):

    data = await fetch_url(url, session)

    if not data.empty:
        if 'Errors' not in data.columns:
            name = data.get('name.formatted', pd.Series(np.nan)).item()
            if pd.isnull(name):
                name = data.get('name', pd.Series(np.nan)).item()

            node_info = pd.DataFrame({'nodeType': params['node_type'],
                                      'from.id': data.get('id', pd.Series(np.nan)).astype('str'),
                                      'fullname': name,
                                      'userType': data.get('userType', np.nan),
                                      'title': data.get('title', np.nan),
                                      'active': data.get('active', np.nan),
                                      'division': data.get('urn:scim:schemas:extension:enterprise:1.0.'
                                                           'division', np.nan),
                                      'department': data.get('urn:scim:schemas:extension:enterprise:1.0.'
                                                             'department', np.nan)})

            results.append(node_info)
        else:
            # Trying to access node not present in SCIM. Trying GRAPH now.
            await get_node_info(f'{base_url_GRAPH}/{params["node_id"]}', results, params)

    logging.debug(f'get_node_info_callback returned for {url}')


async def get_node_info(url, results, params):
    http_call = {0: {
        'url': url,
        'callback': get_node_info_callback,
        'results': results,
        'params': params}}
    try:
        await fetch(http_call)
    except Exception as e:
        logging.error(f'Error when trying to retrieve info for {url}')
        logging.error(e)


async def get_post_count(url, results, params):
    http_call = {0: {
        'url': url,
        'callback': get_post_count_callback,
        'results': results,
        'params': params}}
    try:
        await fetch(http_call)
    except Exception as e:
        logging.error(f'Error when trying to retrieve views for {url}')
        logging.error(e)


async def get_post_count_callback(url, results, params, session):
    data = await fetch_url(url, session)

    if not data.empty:
        count = pd.DataFrame({'id': params['post_id'], 'count': data.get('summary.total_count', 0)}, index=[0])
        results.append(count)

    logging.debug(f'get_post_count_callback returned for {url}')


async def get_node_posts_callback(url, results, params, session):

    data = await fetch_url(url, session)

    try:
        if 'Errors' in data.columns:
            logging.warning(f'Bad response ({data["Errors"]}) for {url}')
            return

        if not data.empty and data['data'][0]:
            posts = pd.json_normalize(data['data'][0])

            posts = posts.assign(partial_id=posts['id'].str.split('_', expand=True)[1])
            posts = posts[~posts['partial_id'].isin(params['filter_ids'])]



            results.append(posts)

    except Exception as e:
        print(f'eeeerro que deu {e}')
        print(f'eeeerro que deu {url}')
        with open("data.pickle", "wb") as file:
            pickle.dump(data, file)

        raise e
    # Paginate
    if 'paging.next' in data.columns:
        await get_node_posts_callback(data['paging.next'].item(), results, params, session)
    else:
        node_id = url.split('/')[-2]
        #params['processed_ids'].append(node_id)


async def get_node_posts(node_ids, node_posts, node_type='', since='', until='', filter_ids=np.array([]), vault=None):
    logging.info('Starting getNodePosts')

    http_calls = {}
    fields = 'id,from,type,created_time,status_type,object_id,link'

    logging.info(f'Extraction posts from {len(node_ids)} nodes.')
    iterator = np.nditer(node_ids, flags=['f_index'])
    for node_id in iterator:
        http_calls[iterator.index] = {
            'url': f'{base_url_GRAPH}/{node_id}/feed?limit=100&fields={fields}&since={since}&until={until}',
            'callback': get_node_posts_callback,
            'results': node_posts,
            'params': {'filter_ids': filter_ids}}

    await fetch(http_calls)

    posts = pd.concat(node_posts).reset_index(drop=True)

    if not posts.empty:
        await fetch_posts_info(posts, node_type, vault)

    logging.info('getNodePosts ended')


async def fetch_posts_info(posts, node_type, vault):

    http_calls = {}
    iterator = np.nditer(posts['id'].to_numpy(), flags=['f_index', 'refs_ok'])
    for post_id in iterator:
        http_calls[f'seen_{iterator.index}'] = {
                            'url': f'{base_url_GRAPH}/{post_id}/seen?summary=TRUE',
                            'callback': get_post_count_callback,
                            'results': vault['post_views'],
                            'params': {'post_id': post_id}}

        http_calls[f'reactions_{iterator.index}'] = {
                            'url': f'{base_url_GRAPH}/{post_id}/reactions?summary=TRUE',
                            'callback': get_post_count_callback,
                            'results': vault['post_reactions'],
                            'params': {'post_id': post_id}}

        http_calls[f'comments_{iterator.index}'] = {
                            'url': f'{base_url_GRAPH}/{post_id}/comments/stream?summary=TRUE',
                            'callback': get_post_count_callback,
                            'results': vault['post_comments'],
                            'params': {'post_id': post_id}}

    iterator = np.nditer(posts['from.id'].drop_duplicates().to_numpy(), flags=['f_index', 'refs_ok'])
    for node_id in iterator:
        http_calls[f'info_{iterator.index}'] = {
                            'url': f'{base_url_SCIM}/{node_id}',
                            'callback': get_node_info_callback,
                            'results': vault['nodes_info'],
                            'params': {'node_id': node_id, 'node_type': node_type}}

    await fetch(http_calls)
