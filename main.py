from workplace_extractor import Extractor
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Params')
    parser.add_argument("token", type=str,
                        help='The file containing the access token')
    parser.add_argument("config", type=str,
                        help='The file containing the config file')
    parser.add_argument('--export', choices=['POSTS', 'PEOPLE', 'GROUPS', 'INTERACTIONS'], default='POSTS',
                        help="what to export")
    parser.add_argument('--hashtags', type=str, default='',
                        help="Consider only posts with given hashtags (comma separated)")
    parser.add_argument('--since', type=str, default='',
                        help='Start date for the extraction of posts (YYYY-MM-DD)')
    parser.add_argument('--until', type=str, default='',
                        help='End date for the extraction of posts (YYYY-MM-DD)')
    parser.add_argument('--csv', type=str, default=False,
                        help='Name of the CSV file.')
    parser.add_argument("--loglevel", choices=['NONE', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='WARNING',
                        help="Set the logging level")
    args = parser.parse_args()

    wp_extractor = Extractor(args.token, args.config, args.export, args.hashtags,
                             args.since, args.until, args.csv, args.loglevel)
    wp_extractor.extract()
