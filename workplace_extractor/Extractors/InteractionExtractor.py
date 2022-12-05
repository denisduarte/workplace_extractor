from . import PostExtractor
from ..Nodes.NodeCollection import PostCollection, NodeCollection, InteractionCollection

import pandas as pd
import networkx as nx


class InteractionExtractor:
    def __init__(self, extractor):
        self.extractor = extractor

        self.feeds = PostCollection()
        self.net = None
        self.net_undirected = None
        self.pagerank = None

        self.nodes = InteractionCollection()

        # Create list of node attributes
        self.node_attribute_list = []
        self.node_additional_attribute_list = []
        self.additional_attributes = None
        self.additional_attributes_join = extractor.additional_node_attributes_join

        # Attributes coming from the extraction
        if extractor.node_attributes:
            self.node_attribute_list = extractor.node_attributes.split(',')

        # Additional attributes coming from a external file

        if extractor.additional_node_attributes:
            self.additional_attributes = pd.read_csv(extractor.additional_node_attributes, sep=';')

            self.node_additional_attribute_list = self.additional_attributes.columns.values.tolist()
            self.node_additional_attribute_list.remove(extractor.additional_node_attributes_join)

    async def extract(self, items_per_page):
        post_extractor = PostExtractor(self.extractor)
        await post_extractor.extract(items_per_page)
        self.feeds = post_extractor.nodes

        # with open(f'{self.extractor.export_folder}/interactions-data.pickle', 'wb') as picke_file:
        #    pickle.dump(self.feeds , picke_file)
        # with open(f'{self.extractor.export_folder}/interactions-data.pickle', 'rb') as picke_file:
        #    self.feeds = pickle.load(picke_file)
        # with open(f'data.pickle', 'rb') as picke_file:
        #    self.feeds = pickle.load(picke_file)

        user_summary = self.build_user_summary()
        self.nodes.nodes = user_summary

        self.build_net()

        if self.extractor.create_ranking:
            self.build_ranking(net_type='directed')
            self.build_ranking(net_type='undirected')

        self.nodes.nodes = user_summary

    @staticmethod
    def convert_to_undirected(g_directed):

        # creating an undirected graph
        g_undirected = g_directed.to_undirected()

        # setting all weights to 0
        for node1, node2, edge_attributes in g_undirected.edges(data=True):
            g_undirected[node1][node2]['weight'] = 0

        # summing weights from directed graph and setting it to the undirected graph
        for node1, node2, edge_attributes in g_directed.edges(data=True):
            g_undirected[node1][node2]['weight'] += edge_attributes['weight']

        return g_undirected

    def build_net(self, include_inactive=False):
        # Build the net
        net = nx.DiGraph()
        for node in self.feeds.nodes:
            if node.feed is not None and node.feed.nodes:
                for post in node.feed.nodes:

                    if post.author is not None and post.author.node_id != 'None' and (post.author.active or include_inactive):

                        target = post.author.node_id

                        if post.comments['data'] is not None and post.comments['data'].nodes:
                            # add node for post author
                            net.add_node(target, **self.list_node_attributes(post.author))

                            for comment in post.comments['data'].nodes:
                                # remove external users
                                if comment.person.node_id != 'None' and (comment.person.active or include_inactive):

                                    source = comment.person.node_id

                                    # add node for comment author
                                    net.add_node(source,  **self.list_node_attributes(comment.person))

                                    if net.has_edge(source, target):
                                        # we added this one before, just increase the weight by one
                                        net.edges[source, target]['weight'] += float(self.extractor.comment_weight)
                                    else:
                                        # new edge. add with weight=1

                                        net.add_edge(source, target, weight=float(self.extractor.comment_weight),
                                                     source_division=net.nodes[source]['division'],
                                                     source_diretoria=net.nodes[source]['Diretoria'],
                                                     target_division=net.nodes[target]['division'],
                                                     target_diretoria=net.nodes[target]['Diretoria'])

                        if post.reactions['data'] is not None \
                                and post.reactions['data'] \
                                and post.author.node_id != 'None':
                            # add node for post author
                            net.add_node(target, **self.list_node_attributes(post.author))

                            for reaction in post.reactions['data']:
                                # remove external users
                                if reaction.person.node_id != 'None' and (reaction.person.active or include_inactive):

                                    source = reaction.person.node_id

                                    # add node for reaction author
                                    net.add_node(source, **self.list_node_attributes(reaction.person))

                                    if net.has_edge(source, target):
                                        # we added this one before, just increase the weight by one
                                        net.edges[source, target]['weight'] += float(self.extractor.reaction_weight)
                                    else:
                                        # new edge. add with weight=1
                                        net.add_edge(source, target, weight=float(self.extractor.reaction_weight),
                                                     source_division=net.nodes[source]['division'],
                                                     source_diretoria=net.nodes[source]['Diretoria'],
                                                     target_division=net.nodes[target]['division'],
                                                     target_diretoria=net.nodes[target]['Diretoria'])

        net_undirected = self.convert_to_undirected(net)

        # set pagerank for directed version
        pagerank = nx.pagerank(net, alpha=0.85, weight='weight')
        # betweenness = nx.betweenness_centrality(net, weight='weight')
        nx.set_node_attributes(net, pagerank, "pagerank")
        # nx.set_node_attributes(net, betweenness, "betweenness")

        # set pagerank for directed version
        pagerank = nx.pagerank(net_undirected, alpha=0.85, weight='weight')
        # betweenness = nx.betweenness_centrality(net_undirected, weight='weight')
        nx.set_node_attributes(net_undirected, pagerank, "pagerank")
        # nx.set_node_attributes(net_undirected, betweenness, "betweenness")

        if self.extractor.create_gexf:
            nx.write_gexf(net, f'{self.extractor.export_folder}/net.gexf')
            nx.write_gexf(net_undirected, f'{self.extractor.export_folder}/net_undirected.gexf')

        self.net = net
        self.net_undirected = net_undirected

    def build_ranking(self, net_type='directed'):

        if net_type == 'undirected':
            net = self.net_undirected
        else:
            net = self.net

        data = []
        for person_id, attributes in dict(net.nodes(data=True)).items():
            row = {'node_id': person_id,
                   **attributes}

            data.append(row)

        ranking = pd.DataFrame(data)

        # REMOVER
        # old  ['betweenness', 'pagerank']:
        for aggregation_field in ['pagerank']:
            ranking = ranking.sort_values([aggregation_field], ascending=False)
            ranking = ranking.reset_index(drop=True)
            ranking[f'global_position_{aggregation_field}'] = ranking.index + 1

            # ranking = ranking.sort_values(['division', aggregation_field], ascending=False).groupby('division')
            # ranking[f'division_position_{aggregation_field}'] = ranking.groupby(['division']).cumcount() + 1

            ranking[f'division_position_{aggregation_field}'] = ranking.sort_values(['division', aggregation_field],
                                                                                    ascending=False)\
                                                                       .groupby(['division'])\
                                                                       .cumcount() + 1

        ranking.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=[" ", " "], regex=True) \
               .to_csv(f'{self.extractor.export_folder}/rank_{net_type}.csv',
                       index=False,
                       sep=";",
                       float_format='%.15f')

    def build_user_summary(self):
        df = pd.DataFrame(columns=['posts', 'posts_reactions', 'posts_views', 'posts_comments',
                                   'posts_comment_reactions', 'posts_replies', 'posts_reply_reactions',
                                   'user_post_reactions', 'user_post_views',
                                   'user_comments', 'user_comment_reactions',
                                   'user_replies', 'user_reply_reactions',
                                   'user_id', 'user.name', 'user.department', 'user_email'])

        for feed in self.feeds.nodes:
            if feed.feed is not None:
                for post in feed.feed.nodes:
                    df = self.update_summary_row('posts', df, post.author)
                    df = self.update_summary_row('posts_reactions', df, post.author,
                                                 len(post.reactions.get('data')))
                    df = self.update_summary_row('posts_views', df, post.author,
                                                 len(post.seen.get('data')))
                    df = self.update_summary_row('posts_comments', df, post.author,
                                                 len(post.comments.get('data').nodes))

                    for reaction in post.reactions.get('data', []):
                        df = self.update_summary_row('user_post_reactions', df, reaction.person)

                    for view in post.seen.get('data', []):
                        df = self.update_summary_row('user_post_views', df, view.person)

                    for comment in post.comments.get('data', NodeCollection()).nodes:
                        df = self.update_summary_row('user_comments', df, comment.person)

                        for comment_reaction in comment.reactions:
                            df = self.update_summary_row('posts_comment_reactions', df, post.author)
                            df = self.update_summary_row('user_comment_reactions', df, comment_reaction.person)

                        for reply in comment.comments.nodes:
                            df = self.update_summary_row('posts_replies', df, post.author)
                            df = self.update_summary_row('user_replies', df, reply.person)

                            for reply_reaction in reply.reactions:
                                df = self.update_summary_row('posts_reply_reactions', df, post.author)
                                df = self.update_summary_row('user_reply_reactions', df, reply_reaction.person)

        return df

    @staticmethod
    def update_summary_row(action, data, user, total=1):

        # if user is external, ignore
        if user is None:
            return data
        if user.author_type == 'Bot/Ext':
            return data

        if user.node_id not in data.user_id.values:
            new_row = pd.DataFrame({'posts': 0, 'posts_reactions': 0, 'posts_views': 0, 'posts_comments': 0,
                                    'posts_comment_reactions': 0, 'posts_replies': 0, 'posts_reply_reactions': 0,
                                    'user_post_reactions': 0, 'user_post_views': 0,
                                    'user_comments': 0, 'user_comment_reactions': 0,
                                    'user_replies': 0, 'user_reply_reactions': 0,
                                    'user_id': user.node_id, 'user_email': user.email,
                                    'user.name': user.name, 'user.department': user.department}, index=[0])
            data = pd.concat([data, new_row], ignore_index=True)

        data.loc[data['user_id'] == user.node_id, action] += total

        return data

    def list_node_attributes(self, node):

        attributes = {}

        for attribute in self.node_attribute_list:
            attributes[attribute] = getattr(node, attribute)

        if self.additional_attributes is not None and self.node_additional_attribute_list:
            current_row_loc = self.additional_attributes[self.additional_attributes_join] == getattr(
                node, self.additional_attributes_join)

            for attribute in self.node_additional_attribute_list:
                if not self.additional_attributes.loc[current_row_loc, attribute].empty:
                    attributes[attribute] = self.additional_attributes.loc[current_row_loc, attribute].iloc[0]
                else:
                    attributes[attribute] = ''

        return attributes
