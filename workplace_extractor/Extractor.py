from workplace_extractor.Extractors import PostExtractor, CommentExtractor, GroupExtractor, MembersExtractor
from workplace_extractor.Extractors import PersonExtractor, InteractionExtractor, EventExtractor

# import pickle
import sys
import os
import logging
import asyncio
import aiohttp
import pandas as pd
import configparser
from gooey import Gooey, GooeyParser


class AuthTokenError(Exception):
    pass


class Extractor(object):

    def __init__(self, **kwargs):
        # the colnfig.ini file should be in the same folder as the app
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')

        # required options
        self.token = self.config.get('MISC', 'access_token')
        self.loglevel = self.config.get('MISC', 'loglevel')

        self.export = kwargs.get('export')
        self.export_file = kwargs.get('export_file')
        self.export_content = kwargs.get('export_content', False)

        if kwargs.get('hashtags', '') is not None:
            self.hashtags = [hashtag.lower() for hashtag in kwargs.get('hashtags', '').replace('#', '').split(',')]
        else:
            self.hashtags = []

            # optional options
        args = ['since', 'until', 'post_id', 'group_id', 'event_id', 'author_id', 'feed_id',
                'active_only', 'create_ranking', 'create_gexf', 'node_attributes', 'additional_node_attributes',
                'joining_column']
        for key, value in kwargs.items():
            if key in args:
                setattr(self, key, value)

        # setting semaphore to control the number of concurrent calls
        self.semaphore = asyncio.Semaphore(int(self.config.get('MISC', 'concurrent_calls')))
        # setting recursion limit to prevent python from interrumpting large calls
        sys.setrecursionlimit(int(self.config.get('MISC', 'max_recursion')) * 2)

    async def init(self):
        # create folder to save output
        output_folder = os.path.dirname(self.config.get('MISC', 'output_dir'))

        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        if self.loglevel != 'NONE':
            logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                                filename=f'{self.config.get("MISC", "output_dir")}/workplace_extractor.log',
                                level=getattr(logging, self.loglevel))

        # set the access token to be used in the http calls
        try:
            await self.set_token()
        except AuthTokenError as e:
            sys.exit(e)

    def extract(self):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._extract())

    async def _extract(self):
        await self.init()

        extractor = None
        if self.export == 'Posts':
            extractor = PostExtractor(extractor=self)
        elif self.export == 'Comments':
            extractor = CommentExtractor(extractor=self)
        elif self.export == 'People':
            extractor = PersonExtractor(extractor=self)
        elif self.export == 'Groups':
            extractor = GroupExtractor(extractor=self)
        elif self.export == 'Members':
            extractor = MembersExtractor(extractor=self)
        elif self.export == 'Attendees':
            extractor = EventExtractor(extractor=self)
        elif self.export == 'Interactions':
            extractor = InteractionExtractor(extractor=self)

        print("Extracting data... ", end=" ")
        await extractor.extract()
        print("DONE")

        print("Converting results... ", end=" ")
        print(extractor.nodes)
        print(f'file = {self.export_file}')
        nodes_pd = extractor.nodes.to_pandas(self)
        print("DONE")

        if self.config.get('MISC', 'save_file') == 'True':
            print("Saving CSV file... ", end=' ')
            # .to_csv(f'{self.config.get("MISC", "output_dir")}/{self.csv}', index=False, sep=";")
            nodes_pd = nodes_pd.applymap(lambda x: x.encode('unicode_escape')
                                                     .decode('utf-8') if isinstance(x, str) else x)
            nodes_pd.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=[" ", " "], regex=True) \
                    .to_excel(f'{self.config.get("MISC", "output_dir")}/{self.export_file}', sheet_name='Results', index=False)
            print("DONE")

        return nodes_pd

    async def set_token(self):
        with open(self.token) as file:
            self.token = file.readline().rstrip()

        # check if access token is valid
        http_call = [{'url': self.config.get('URL', 'GRAPH') + '/community/members?fields=id&limit=1',
                      'call': self.check_access_token}]

        await self.fetch(http_call)

    async def check_access_token(self, url, session, **kwargs):
        print("Checking access token...", end=" ")

        data = await self.fetch_url(url, session, 'GRAPH', **kwargs)
        if not('data' in data and data['data']):
            logging.error('FAILED')
            raise AuthTokenError('Invalid access token')

        print('DONE')

    async def fetch(self, http_calls):
        headers = {'Authorization': f'Bearer {self.token}',
                   # 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_3) AppleWebKit/537.36'
                   #               ' (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36',
                   'Content-Type': 'application/json'}

        async with aiohttp.ClientSession(headers=headers) as session:
            tasks = []
            for call in http_calls:
                kwargs = {arg: value for arg, value in call.items() if arg not in ('url', 'call')}
                tasks.append(asyncio.ensure_future(self.bound_fetch(call['url'], session, call['call'], **kwargs)))

            await asyncio.gather(*tasks)

    async def bound_fetch(self, url, session, call, **kwargs):
        async with self.semaphore:
            return await call(url, session, **kwargs)

    async def fetch_url(self, url, session, api='', **kwargs):
        logging.debug(f'GET {url}')
        logging.debug('Recursion ' + str(kwargs.get('recursion', 0)))

        # to prevent GRAPH bug with infinite recursion
        if kwargs.get('recursion', 0) > int(self.config.get('MISC', 'max_recursion')):
            logging.error('TOO MUCH RECURSION - ignoring next pages')
            logging.error(url)
            return {}

        tries = 0
        max_retries = int(self.config.get('MISC', 'max_http_retries'))
        while tries < max_retries:
            try:
                tries += 1
                async with session.get(url) as resp:
                    if resp.status in [400, 401, 404]:
                        logging.warning(f'Response returned {resp.status} for {url}.')
                        data = pd.DataFrame({'Errors': resp.status}, index=[0])
                        return data
                    elif resp.status in [500]:
                        logging.error(f'Error 500 when calling {url}')
                        raise Exception
                    else:
                        if api == 'SCIM':
                            content_type = 'text/javascript'
                        elif api == 'GRAPH':
                            content_type = 'application/json'

                        return await resp.json(content_type=content_type)

            except Exception as e:
                logging.error(f'Exception when trying to process {url}. API: {api}')
                logging.error(e)
                logging.warning(f' {tries} of {max_retries}')

        if tries == max_retries:
            #raise TimeoutError('Too many retries')
            logging.critical(f'Response returned ERROR 500 for {url}.')

    @staticmethod
    def str_to_bool(str_arg):
        str_arg = str_arg.upper()
        if str_arg == 'TRUE':
            return True
        elif str_arg == 'FALSE':
            return False


class run():
    def __init__(self):
        args = self.read_arguments()
        wp_extractor = Extractor(**vars(args))

        #args = {'export': 'Interactions', 'export_file': 'exported_data.xlsx', 'since': '2022-01-15', 'until': '2022-04-15', 'create_ranking': True, 'create_gexf': True, 'node_attributes': 'division,department,name,emp_num,email,title,manager_level,author_type', 'additional_node_attributes': '/Users/denisduarte/Petrobras/PythonProjects/output/diretorias.csv', 'joining_column': 'division', 'author_id': ''}
        #args = {'export': 'Posts', 'export_file': 'exported_data-dtdi-jan_fev.xlsx', 'since': '2022-01-01', 'until': '2022-03-01', 'export_content': True}
        #wp_extractor = Extractor(**args)

        wp_extractor.extract()

    """"""
    @Gooey(advanced=True,
           default_size=(800, 610),
           program_name='Workplace Extractor',
           program_description='Exportador de conteúdo do Workplace Petrobras',
           required_cols=1,
           optional_cols=2,
           progress_regex=r"^progress: (\d+)%$",
           hide_progress_msg=True)
    def read_arguments(self):
        parser = GooeyParser(description="Params")
        subparsers = parser.add_subparsers(help='Content to export', dest='export')

        # EXPORT POSTS
        post_parser = subparsers.add_parser("Posts")
        post_parser.add_argument('export_file', type=str, default='exported_data.xlsx', help='Name of the export file.')
        post_parser.add_argument('-since', type=str, default='',
                                 help='Start date for the extraction of posts (YYYY-MM-DD)',
                                 widget='DateChooser')
        post_parser.add_argument('-until', type=str, default='',
                                 help='End date for the extraction of posts (YYYY-MM-DD)',
                                 widget='DateChooser')
        post_parser.add_argument('-export_content', action='store_true',
                                 help="Either export the posts content or only a "
                                      "flag indicating that the post has a content")
        post_parser.add_argument('-hashtags', type=str, default='', help="Consider only posts with given hashtags "
                                                                         "(comma separated)")
        post_parser.add_argument('-author_id', type=str, default='', help="Fetch only posts made by this author.")
        post_parser.add_argument('-feed_id', type=str, default='', help="Fetch only posts made in this feed "
                                                                        "(group ou person).")

        # EXPORT COMMENTS
        comment_parser = subparsers.add_parser("Comments")
        comment_parser.add_argument('export_file', type=str, default='exported_data.xlsx', help='Name of the export file.')
        comment_parser.add_argument('post_id', type=str, default='', help="The ID of the post")

        # EXPORT PEOPLE
        people_parser = subparsers.add_parser("People")
        people_parser.add_argument('export_file', type=str, default='exported_data.xlsx', help='Name of the export file.')
        people_parser.add_argument('-active_only', action='store_true', help="Exports only currentcly active members")

        # EXPORT GROUPS
        groups_parser = subparsers.add_parser("Groups")
        groups_parser.add_argument('export_file', type=str, default='exported_data.xlsx', help='Name of the export file.')

        # EXPORT GROUP MEMBERS
        members_parser = subparsers.add_parser("Members")
        members_parser.add_argument('export_file', type=str, default='exported_data.xlsx', help='Name of the export file.')
        members_parser.add_argument('group_id', type=str, default='', help="The ID of the group")

        # EXPORT EVENT PARTICIPANTS
        members_parser = subparsers.add_parser("Attendees")
        members_parser.add_argument('export_file', type=str, default='exported_data.xlsx', help='Name of the export file.')
        members_parser.add_argument('event_id', type=str, default='', help="The ID of the event")

        # EXPORT INTERACTIONS
        interactions_parser = subparsers.add_parser("Interactions")
        interactions_parser.add_argument('export_file', type=str, default='exported_data.xlsx', help='Name of the export file.')
        interactions_parser.add_argument('-since', type=str, default='',
                                         help='Start date for the extraction of posts (YYYY-MM-DD)',
                                         widget='DateChooser')
        interactions_parser.add_argument('-until', type=str, default='',
                                         help='End date for the extraction of posts (YYYY-MM-DD)', widget='DateChooser')
        interactions_parser.add_argument('-create_ranking', action='store_true', help="Create user ranking")
        interactions_parser.add_argument('-create_gexf', action='store_true', help="Create GEXF file")
        interactions_parser.add_argument('-node_attributes', type=str, default='division,department,name,emp_num,email',
                                         help='Name of the export file.')
        interactions_parser.add_argument('-additional_node_attributes', type=str, default='',
                                         help='Path to a export containing columns to be merged')
        interactions_parser.add_argument('-joining_column', type=str, default='',
                                         help='Column to be used for joining')
        interactions_parser.add_argument('-author_id', type=str, default='',
                                         help="The ID of the user. Used to create ego "
                                              "networks")

        return parser.parse_args()
