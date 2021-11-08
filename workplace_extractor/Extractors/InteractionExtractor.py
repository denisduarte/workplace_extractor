from workplace_extractor.Nodes.NodeCollection import PostCollection, NodeCollection, InteractionCollection
from workplace_extractor.Extractors import PostExtractor


import pandas as pd
import networkx as nx
import pickle
import operator


class InteractionExtractor:
    def __init__(self, extractor):
        self.extractor = extractor

        self.feeds = PostCollection()
        self.net = None
        self.pagerank = None

        self.nodes = InteractionCollection()

    async def extract(self):
        post_extractor = PostExtractor(self.extractor)
        #await post_extractor.extract()
        #self.feeds = post_extractor.nodes

        # with open(f'{self.extractor.config.get("MISC", "output_dir")}/workplace_interactions.pickle', 'wb') as handle:
        #    pickle.dump(self.feeds, handle)

        #with open(f'{self.extractor.config.get("MISC", "output_dir")}/workplace_interactions.pickle', 'rb') as handle:
        #     self.feeds = pickle.load(handle)

        user_summary = self.build_user_summary()
        self.net = self.build_net()
        self.pagerank = self.calculate_pagerank()

        if self.extractor.create_ranking:
            self.build_ranking('global')
            self.build_ranking('by_division')

        # self.create_network_plot()

        self.nodes.nodes = user_summary

    def build_net(self, include_inactive=False):

        ### Create list of node attributes

        # Attributes coming from the extraction
        node_attribute_list = []
        if self.extractor.node_attributes:
            node_attribute_list = self.extractor.node_attributes.split(',')

        # Additional attributes coming from a external file
        additional_attributes = pd.read_csv(self.extractor.additional_node_attributes, sep=';')

        node_additional_attribute_list = additional_attributes.columns.values.tolist()
        node_additional_attribute_list.remove(self.extractor.joining_column)

        ### Build the net

        net = nx.Graph()
        for node in self.feeds.nodes:
            if node.feed is not None and node.feed.nodes:
                for post in node.feed.nodes:
                    if post.author.node_id != 'None' and (post.author.active or include_inactive):

                        if post.comments['data'] is not None and post.comments['data'].nodes:
                            net.add_node(post.author.node_id,
                                         **self.list_node_attributes(post.author, self.extractor,
                                                                     node_attribute_list,
                                                                     node_additional_attribute_list,
                                                                     additional_attributes))

                            for comment in post.comments['data'].nodes:
                                # remove external users
                                if comment.person.node_id != 'None' and (comment.person.active or include_inactive):
                                    net.add_node(comment.person.node_id,
                                                 **self.list_node_attributes(comment.person, self.extractor,
                                                                             node_attribute_list,
                                                                             node_additional_attribute_list,
                                                                             additional_attributes))

                                    if net.has_edge(post.author.node_id, comment.person.node_id):
                                        # we added this one before, just increase the weight by one
                                        net[post.author.node_id][comment.person.node_id]['weight'] += 1
                                    else:
                                        # new edge. add with weight=1
                                        net.add_edge(post.author.node_id, comment.person.node_id, weight=1)

                        if post.reactions['data'] is not None and post.reactions['data'] and post.author.node_id != 'None':
                            net.add_node(post.author.node_id,
                                         **self.list_node_attributes(post.author, self.extractor,
                                                                     node_attribute_list,
                                                                     node_additional_attribute_list,
                                                                     additional_attributes))

                            for reaction in post.reactions['data']:
                                # remove external users
                                if reaction.person.node_id != 'None' and (reaction.person.active or include_inactive):
                                    net.add_node(reaction.person.node_id,
                                                 **self.list_node_attributes(reaction.person, self.extractor,
                                                                             node_attribute_list,
                                                                             node_additional_attribute_list,
                                                                             additional_attributes))

                                    if net.has_edge(post.author.node_id, reaction.person.node_id):
                                        # we added this one before, just increase the weight by one
                                        net[post.author.node_id][reaction.person.node_id]['weight'] += 1
                                    else:
                                        # new edge. add with weight=1
                                        net.add_edge(post.author.node_id, reaction.person.node_id, weight=1)

        if self.extractor.create_gexf:
            nx.write_gexf(net, f'{self.extractor.config.get("MISC", "output_dir")}/net.gexf')

        return net

    def calculate_pagerank(self):
        page_rank = nx.pagerank(self.net)
        return page_rank

    def build_ranking(self, ranking_type='global', top_k=5):

        centrality = sorted(self.pagerank.items(), key=operator.itemgetter(1), reverse=True)

        if ranking_type == 'by_division':

            data = []
            for person in self.net.nodes:
                row = {'node_id': person,
                       'department': self.net.nodes()[person]['department'],
                       'division': self.net.nodes()[person]['division'],
                       'name': self.net.nodes()[person]['name'],
                       'emp_num': self.net.nodes()[person]['emp_num'],
                       'email': self.net.nodes()[person]['email'],
                       'centrality': [node[1] for node in centrality if node[0] == person][0]}

                data.append(row)

            df = pd.DataFrame(data)
            df['centrality'] = pd.to_numeric(df['centrality'], errors='coerce')
            df = df.sort_values(['centrality'], ascending=False)
            df = df.reset_index(drop=True)
            df['global_position'] = df.index + 1

            ranking = df.sort_values(['division', 'centrality'], ascending=False).groupby('division').head(top_k)
            ranking['position'] = ranking.groupby(['division']).cumcount() + 1

        else:
            data = []
            for item in centrality:
                person = item[0]
                centrality_score = item[1]

                row = {'node_id': person,
                       'department': self.net.nodes()[person]['department'],
                       'division': self.net.nodes()[person]['division'],
                       'name': self.net.nodes()[person]['name'],
                       'emp_num': self.net.nodes()[person]['emp_num'],
                       'email': self.net.nodes()[person]['email'],
                       'centrality': centrality_score}

                data.append(row)

            df = pd.DataFrame(data)
            df['centrality'] = pd.to_numeric(df['centrality'], errors='coerce')

            ranking = df.sort_values(['centrality'], ascending=False)
            ranking = ranking.reset_index(drop=True)
            ranking['global_position'] = ranking.index + 1

        ranking.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=[" ", " "], regex=True) \
               .to_csv(f'{self.extractor.config.get("MISC", "output_dir")}/rank_{ranking_type}.csv', index=False, sep=";")

    def build_user_summary(self):
        df = pd.DataFrame(columns=['posts', 'post_reactions', 'post_views',
                                   'comments', 'comment_reactions',
                                   'replies', 'reply_reactions',
                                   'user_id', 'user_email'])

        for feed in self.feeds.nodes:
            if feed.feed is not None:
                for post in feed.feed.nodes:

                    df = self.update_summary_row('posts', df, post.author)
                    for reaction in post.reactions.get('data', []):
                        df = self.update_summary_row('post_reactions', df, reaction.person)

                    for view in post.seen.get('data', []):
                        df = self.update_summary_row('post_views', df, view.person)

                    for comment in post.comments.get('data', NodeCollection()).nodes:
                        df = self.update_summary_row('comments', df, comment.person)

                        for comment_reaction in comment.reactions:
                            df = self.update_summary_row('comment_reactions', df, comment_reaction.person)

                        for reply in comment.comments.nodes:
                            df = self.update_summary_row('replies', df, reply.person)

                            for reply_reaction in reply.reactions:
                                df = self.update_summary_row('reply_reactions', df, reply_reaction.person)

        return df

    @staticmethod
    def update_summary_row(action, data, user):

        # if user is external, ignore
        if user.author_type == 'Bot/Ext':
            return data

        if user.node_id not in data.user_id.values:
            new_row = pd.DataFrame({'posts': 0, 'post_reactions': 0, 'post_views': 0,
                                    'comments': 0, 'comment_reactions': 0,
                                    'replies': 0, 'reply_reactions': 0,
                                    'user_id': user.node_id, 'user_email': user.email}, index=[0])
            data = data.append(new_row, ignore_index=True)

        data.loc[data['user_id'] == user.node_id, action] += 1

        return data

    @staticmethod
    def list_node_attributes(node, extractor,
                             node_attribute_list, node_additional_attribute_list, additional_attributes):

        attributes = {}

        for attribute in node_attribute_list:
            attributes[attribute] = getattr(node, attribute)

        current_row_loc = additional_attributes[extractor.joining_column] == getattr(
            node, extractor.joining_column)

        for attribute in node_additional_attribute_list:
            if not additional_attributes.loc[current_row_loc, attribute].empty:
                attributes[attribute] = additional_attributes.loc[current_row_loc, attribute].iloc[0]

        return attributes
