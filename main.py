from workplace_extractor.extract import extract
import argparse
import asyncio


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

    loop = asyncio.get_event_loop()
    loop.run_until_complete(extract(arguments))
