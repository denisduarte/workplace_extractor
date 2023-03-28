from . import PostExtractor
from ..Nodes.NodeCollection import PostCollection, NodeCollection, InteractionCollection

import pandas as pd
import networkx as nx

import pickle
import datetime


class InteractionExtractor:
    def __init__(self, extractor):
        self.extractor = extractor

        self.feeds = PostCollection()
        self.nodes = InteractionCollection()
        self.net_directed = None
        self.net_undirected = None

        # Create list of node attributes
        self.node_attribute_list = []
        if extractor.node_attributes:
            self.node_attribute_list = extractor.node_attributes.split(',')

    async def extract(self, items_per_page):
        post_extractor = PostExtractor(self.extractor)
        await post_extractor.extract(items_per_page)
        self.feeds = post_extractor.nodes

        #with open(f'{self.extractor.export_folder}/pk_interactions_feeds.pickle', 'wb') as picke_file:
        #    pickle.dump(self.feeds, picke_file)
        #with open(f'{self.extractor.export_folder}/pk_interactions_feeds.pickle', 'rb') as picke_file:
        #    self.feeds = pickle.load(picke_file)

        print('starting build net')
        self.net_undirected, self.net_directed = self.build_net()

        # get a list of person fields from either net, ignoring pagerank
        nodes = []
        for person_id, attributes in dict(self.net_undirected.nodes(data=True)).items():
            row = {'node_id': person_id, **attributes}
            row.pop('pagerank', None)
            nodes.append(row)

        nodes = pd.DataFrame(nodes).set_index('node_id', drop=False)

        print('starting build rank')
        ranking_directed = self.build_ranking(self.net_directed)
        ranking_undirected = self.build_ranking(self.net_undirected)

        ranking = ranking_directed.merge(ranking_undirected,
                                         left_index=True, right_index=True,
                                         suffixes=('_directed', '_undirected'))
        nodes = nodes.merge(ranking, left_index=True, right_index=True)

        print('starting user summary')
        user_summary = self.build_user_summary(nodes)
        nodes = nodes.merge(user_summary, left_index=True, right_index=True)

        self.nodes.nodes = nodes
        print('all done')

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

                                        net.add_edge(source, target, weight=float(self.extractor.comment_weight))

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
                                        net.add_edge(source, target, weight=float(self.extractor.reaction_weight))

        net_undirected = self.convert_to_undirected(net)

        # set pagerank for directed version
        pagerank = nx.pagerank(net, alpha=0.85, weight='weight')
        nx.set_node_attributes(net, pagerank, "pagerank")

        # set pagerank for directed version
        pagerank = nx.pagerank(net_undirected, alpha=0.85, weight='weight')
        nx.set_node_attributes(net_undirected, pagerank, "pagerank")

        return net_undirected, net

    @staticmethod
    def build_ranking(net):
        data = []
        for person_id, attributes in dict(net.nodes(data=True)).items():
            row = {'node_id': person_id,
                   'division': attributes.get('division'),
                   'pagerank': attributes.get('pagerank')}

            data.append(row)

        ranking = pd.DataFrame(data)

        ranking = ranking.sort_values(['pagerank'], ascending=False)
        ranking = ranking.reset_index(drop=True)
        ranking[f'global_position'] = ranking.index + 1
        ranking[f'division_position'] = ranking.sort_values(['division', 'pagerank'], ascending=False)\
                                               .groupby(['division'])\
                                               .cumcount() + 1

        return ranking.drop(columns=['division']).set_index('node_id')

    def build_user_summary(self, nodes):

        df = pd.DataFrame(columns=['posts', 'posts_reactions', 'posts_views', 'posts_comments',
                                   'posts_comment_reactions', 'posts_replies', 'posts_reply_reactions',
                                   'user_post_reactions', 'user_post_views',
                                   'user_comments', 'user_comment_reactions',
                                   'user_replies', 'user_reply_reactions'])

        feed_generator = (f for f in self.feeds.nodes if f.feed is not None)
        for feed in feed_generator:
            post_generator = (p for p in feed.feed.nodes if p.author is not None)
            for post in post_generator:
                data = {'posts': 1,
                        'posts_reactions': len(post.reactions.get('data')),
                        'posts_views': len(post.seen.get('data')),
                        'posts_comments': len(post.comments.get('data').nodes)}
                ix = [post.author.node_id]
                df = self.update_summary_row(df, data=pd.DataFrame(data, index=ix))

                data = {'user_post_reactions': 1}
                ix = [r.person.node_id for r in post.reactions.get('data', []) if r.person.author_type != 'Bot/Ext']
                df = self.update_summary_row(df, data=pd.DataFrame(data, index=ix))

                data = {'user_post_views': 1}
                ix = [v.person.node_id for v in post.seen.get('data', []) if v.person.author_type != 'Bot/Ext']
                df = self.update_summary_row(df, data=pd.DataFrame(data, index=ix))

                comment_generator = (c for c in post.comments.get('data', NodeCollection()).nodes if c.person is not None)
                for comment in comment_generator:
                    data = {'user_comments': 1}
                    ix = [comment.person.node_id]
                    df = self.update_summary_row(df, data=pd.DataFrame(data, index=ix))

                    data = {'user_comment_reactions': 1}
                    ix = [cr.person.node_id for cr in comment.reactions if cr.person.author_type != 'Bot/Ext']
                    df = self.update_summary_row(df, data=pd.DataFrame(data, index=ix))

                    data = {'posts_comment_reactions': len(ix)}
                    ix = [post.author.node_id]
                    df = self.update_summary_row(df, data=pd.DataFrame(data, index=ix))

                    reply_generator = (r for r in comment.comments.nodes if r.person is not None)
                    for reply in reply_generator:
                        data = {'user_replies': 1}
                        ix = [reply.person.node_id]
                        df = self.update_summary_row(df, data=pd.DataFrame(data, index=ix))

                        data = {'user_reply_reactions': 1}
                        ix = [rr.person.node_id for rr in reply.reactions if rr.person.author_type != 'Bot/Ext']
                        df = self.update_summary_row(df, data=pd.DataFrame(data, index=ix))

                        data = {'posts_reply_reactions': len(ix),
                                'posts_replies': 1}
                        ix = [post.author.node_id]
                        df = self.update_summary_row(df, data=pd.DataFrame(data, index=ix))

        #list of node_ids in the net
        node_ids = nodes.node_id.to_list()

        return df[df.index.isin(node_ids)]
    
    @staticmethod
    def update_summary_row(df, data=None):
        df = df.add(data, fill_value=0, axis='columns').fillna(0)

        return df
    """
    def build_user_summary(self):
        df = pd.DataFrame(columns=['posts', 'posts_reactions', 'posts_views', 'posts_comments',
                                   'posts_comment_reactions', 'posts_replies', 'posts_reply_reactions',
                                   'user_post_reactions', 'user_post_views',
                                   'user_comments', 'user_comment_reactions',
                                   'user_replies', 'user_reply_reactions',
                                   'user_id', 'user.name', 'user.department', 'user_email'])

        for feed in self.feeds.nodes:
            if feed.feed is not None:
                now = datetime.datetime.now()
                print(f'{now.strftime("%Y-%m-%d %H:%M:%S")} - {feed.node_id}')

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
    """
    def list_node_attributes(self, node):

        attributes = {}

        for attribute in self.node_attribute_list:
            attributes[attribute] = getattr(node, attribute)

        return attributes
