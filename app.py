from workplace_extractor import Extractor
from gooey import Gooey, GooeyParser
from flask import Flask
app = Flask(__name__)

""""""
@Gooey(advanced=True,
       default_size=(800, 610),
       program_name='Workplace Extractor',
       program_description='Exportador de conteúdo do Workplace Petrobras',
       required_cols=1,
       optional_cols=2,
       progress_regex=r"^progress: (\d+)%$",
       hide_progress_msg=True)
def read_arguments():

    parser = GooeyParser(description="Params")
    subparsers = parser.add_subparsers(help='Content to export', dest='export')

    # EXPORT POSTS
    post_parser = subparsers.add_parser("Posts")
    post_parser.add_argument('csv', type=str, default='exported_data.csv', help='Name of the CSV file.')
    post_parser.add_argument('-since', type=str, default='', help='Start date for the extraction of posts (YYYY-MM-DD)',
                             widget='DateChooser')
    post_parser.add_argument('-until', type=str, default='', help='End date for the extraction of posts (YYYY-MM-DD)',
                             widget='DateChooser')
    post_parser.add_argument('-export_content', action='store_true', help="Either export the posts content or only a "
                                                                          "flag indicating that the post has a content")
    post_parser.add_argument('-hashtags', type=str, default='', help="Consider only posts with given hashtags "
                                                                     "(comma separated)")
    post_parser.add_argument('-author_id', type=str, default='', help="Fetch only posts made by this author.")
    post_parser.add_argument('-feed_id', type=str, default='', help="Fetch only posts made in this feed "
                                                                    "(group ou person).")



    # EXPORT COMMENTS
    comment_parser = subparsers.add_parser("Comments")
    comment_parser.add_argument('csv', type=str, default='exported_data.csv', help='Name of the CSV file.')
    comment_parser.add_argument('post_id', type=str, default='', help="The ID of the post")

    # EXPORT PEOPLE
    people_parser = subparsers.add_parser("People")
    people_parser.add_argument('csv', type=str, default='exported_data.csv', help='Name of the CSV file.')
    people_parser.add_argument('-active_only', action='store_true', help="Exports only currentcly active members")

    # EXPORT GROUPS
    groups_parser = subparsers.add_parser("Groups")
    groups_parser.add_argument('csv', type=str, default='exported_data.csv', help='Name of the CSV file.')

    # EXPORT GROUP MEMBERS
    members_parser = subparsers.add_parser("Members")
    members_parser.add_argument('csv', type=str, default='exported_data.csv', help='Name of the CSV file.')
    members_parser.add_argument('group_id', type=str, default='', help="The ID of the group")

    # EXPORT EVENT PARTICIPANTS
    members_parser = subparsers.add_parser("Attendees")
    members_parser.add_argument('csv', type=str, default='exported_data.csv', help='Name of the CSV file.')
    members_parser.add_argument('event_id', type=str, default='', help="The ID of the event")

    # EXPORT INTERACTIONS
    interactions_parser = subparsers.add_parser("Interactions")
    interactions_parser.add_argument('csv', type=str, default='exported_data.csv', help='Name of the CSV file.')
    interactions_parser.add_argument('-since', type=str, default='',
                                     help='Start date for the extraction of posts (YYYY-MM-DD)', widget='DateChooser')
    interactions_parser.add_argument('-until', type=str, default='',
                                     help='End date for the extraction of posts (YYYY-MM-DD)', widget='DateChooser')
    interactions_parser.add_argument('-create_ranking', action='store_true', help="Create user ranking")
    interactions_parser.add_argument('-create_gexf', action='store_true', help="Create GEXF file")
    interactions_parser.add_argument('-node_attributes', type=str, default='division,department,name',
                                     help='Name of the CSV file.')
    interactions_parser.add_argument('-additional_node_attributes', type=str, default='',
                                     help='Path to a CSV containing columns to be merged')
    interactions_parser.add_argument('-joining_column', type=str, default='',
                                     help='Column to be used for joining')
    interactions_parser.add_argument('-author_id', type=str, default='', help="The ID of the user. Used to create ego "
                                                                              "networks")

    return parser.parse_args()


@app.route("/")
def run():
    args = read_arguments()
    wp_extractor = Extractor(**vars(args))

    args = {'export': 'Interactions', 'csv': 'exported_data.csv', 'since': '', 'until': '', 'create_ranking': False,
            'create_gexf': True, 'node_attributes': 'division,department,name',
            'additional_node_attributes': '/Users/denisduarte/Petrobras/PythonProjects/workplace_extractor/output/'
                                          'diretorias.csv',
            'joining_column': 'division', 'author_id': ''}
    # wp_extractor = Extractor(**args)

    wp_extractor.extract()


if __name__ == "__main__":
    app.run(host="0.0.0.0")
