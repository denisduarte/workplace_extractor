import asyncio
import pandas as pd
import numpy as np
import argparse
import logging
import pickle

from miscelaneous import set_token, AuthTokenError, combine_data

from get_member_ids import get_member_ids
from get_node_posts import get_node_posts
from get_group_ids import get_group_ids
import sys


async def run(args):
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
        extraction_loop = arguments.extract_from_members + arguments.extract_from_groups
        for extraction_run in range(extraction_loop):
            if arguments.groups_first:
                if extraction_run == 0:
                    ids_function = extraction['groups']['ids_function']
                    extracted_posts = vault['group_posts']
                    node_type = 'groups'
                else:
                    ids_function = extraction['members']['ids_function']
                    extracted_posts = vault['member_posts']
                    node_type = 'members'
            else:
                if extraction_run == 0 and arguments.extract_from_members:
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Params')
    parser.add_argument("token", type=str,
                        help='The file containing the access token')

    parser.add_argument('--extract_from_members', action=argparse.BooleanOptionalAction,
                        help='extract posts from members', default=False)
    parser.add_argument('--extract_from_groups', action=argparse.BooleanOptionalAction,
                        help='extract posts from groups', default=False)
    parser.add_argument('--groups_first', action=argparse.BooleanOptionalAction,
                        help='extract posts from members')
    parser.add_argument('--resume', action=argparse.BooleanOptionalAction,
                        help='resume an interrupted extraction')
    parser.add_argument('--since', type=str, default='',
                        help='start date for the extraction of posts (YYYY-MM-DD)')
    parser.add_argument('--until', type=str, default='',
                        help='end date for the extraction of posts (YYYY-MM-DD)')
    parser.add_argument('--csv', type=str, default=False,
                        help='Name of the CSV file.')
    parser.add_argument("--loglevel", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='WARNING',
                        help="Set the logging level")

    arguments = parser.parse_args()

    if not arguments.extract_from_groups and not arguments.extract_from_members:
        print('At least one node type (members or groups) must be selected for extraction')
        exit(0)

    if arguments.groups_first and not (arguments.extract_from_groups and arguments.extract_from_members):
        print('Argument groups_first can only be used if both member posts and group posts are being extracted')
        exit(0)

    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                        filename='extraction.log',
                        level=getattr(logging, arguments.loglevel))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(arguments))
