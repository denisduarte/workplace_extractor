import asyncio
import pandas as pd
import numpy as np
import argparse
import logging
import pickle

from workplace_extractor.miscelaneous import set_token, AuthTokenError, combine_data

from workplace_extractor.get_member_ids import get_member_ids
from workplace_extractor.get_node_posts import get_node_posts
from workplace_extractor.get_group_ids import get_group_ids
import sys


async def extract(args):

    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                        filename='extraction.log',
                        level=getattr(logging, args.loglevel))


    # set the access token to be used in the http calls
    try:
        await set_token(args.token)
    except AuthTokenError as e:
        sys.exit(e)

    vault = {'member_posts': [pd.DataFrame()], 'group_posts': [pd.DataFrame()],
             'processed_ids': [pd.DataFrame()], 'nodes_info': [pd.DataFrame()],
             'post_views': [pd.DataFrame()], 'post_comments': [pd.DataFrame()],
             'post_reactions': [pd.DataFrame()]}

    try:
        extraction = {'members': {'ids_function': get_member_ids,
                                  'extracted_posts': vault['member_posts'],
                                  'node_type': 'members'},
                      'groups': {'ids_function': get_group_ids,
                                 'extracted_posts': vault['group_posts'],
                                 'node_type': 'groups'}}

        filter_ids = np.array([])
        extraction_loop = args.extract_from_members + args.extract_from_groups
        for extraction_run in range(extraction_loop):
            if args.groups_first:
                if extraction_run == 0:
                    ids_function = extraction['groups']['ids_function']
                    extracted_posts = vault['group_posts']
                    node_type = 'groups'
                else:
                    ids_function = extraction['members']['ids_function']
                    extracted_posts = vault['member_posts']
                    node_type = 'members'
            else:
                if extraction_run == 0 and args.extract_from_members:
                    ids_function = extraction['members']['ids_function']
                    extracted_posts = vault['member_posts']
                    node_type = 'members'
                else:
                    ids_function = extraction['groups']['ids_function']
                    extracted_posts = vault['group_posts']
                    node_type = 'groups'

            logging.info(f'Extracting ids from {node_type}')
            ids = await ids_function()
            logging.info(f'Extraction of {node_type} ids finished')

            logging.info(f'Extracting posts from {node_type}')
            await get_node_posts(ids, extracted_posts, node_type=node_type, since=args.since, until=args.until,
                                 filter_ids=filter_ids, vault=vault)
            logging.info(f'Extraction of {node_type} posts finished')

            filter_ids = pd.concat(extracted_posts)['partial_id'].to_numpy()

    except Exception as e:
        with open("extraction.pickle", "wb") as file:
            pickle.dump(vault, file)
        raise e

    posts = combine_data(vault, args.groups_first)

    # if a name for the csv file was passed, save the posts in csv format
    if args.csv:
        posts.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=[" ", " "], regex=True) \
            .to_csv(args.csv, index=False, sep=";")

    return posts