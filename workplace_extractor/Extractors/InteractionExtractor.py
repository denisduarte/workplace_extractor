from copy import deepcopy

from workplace_extractor.Nodes.NodeCollection import PostCollection, NodeCollection, InteractionCollection
from workplace_extractor.Extractors import PostExtractor


import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
# import matplotlib
# from random import randint
import pickle
import operator
# import math


class InteractionExtractor:
    def __init__(self, extractor):
        self.extractor = extractor

        self.feeds = PostCollection()
        # self.net = since

        self.nodes = InteractionCollection()

    async def extract(self):
        post_extractor = PostExtractor(self.extractor)
        await post_extractor.extract()
        self.feeds = post_extractor.nodes

        with open(f'{self.extractor.config.get("MISC", "output_dir")}/workplace_interactions-newnew.pickle', 'wb') as handle:
            pickle.dump(self.feeds, handle)

        # with open('/Users/denisduarte/Petrobras/PythonProjects/workplace_extractor/output2/workplace_interactions-newnew.pickle', 'rb') as handle:
        #    self.feeds = pickle.load(handle)

        user_summary = self.build_user_summary()
        self.net = self.build_net()
        self.pagerank = self.calculate_pagerank()

        if self.extractor.create_ranking:
            self.build_ranking('global')
            self.build_ranking('by_division')

        # self.create_network_plot()

        self.nodes.nodes = user_summary

    def build_net(self, include_inactive=False):

        diretorias = pd.read_excel(f'{self.extractor.config.get("MISC", "output_dir")}/dDiretorias.xlsx')

        net = nx.Graph()
        for node in self.feeds.nodes:
            if node.feed is not None and node.feed.nodes:
                for post in node.feed.nodes:
                    if post.author.node_id != 'None' and (post.author.active or include_inactive):

                        author_diretoria = ''
                        try:
                            division = diretorias['Área'] == getattr(post.author, 'division')
                            author_diretoria = diretorias.loc[division, 'Diretoria'].iloc[0]
                        except:
                            pass

                        if post.comments['data'] is not None and post.comments['data'].nodes:
                            net.add_node(post.author.node_id,
                                         color=getattr(post.author, self.extractor.config.get('PLOT', 'color_field')),
                                         label=getattr(post.author, self.extractor.config.get('PLOT', 'label_field')),
                                         diretoria=author_diretoria,
                                         division=getattr(post.author, 'division'),
                                         department=getattr(post.author, 'department'),
                                         name=getattr(post.author, 'name'),
                                         emp_num=getattr(post.author, 'emp_num'),
                                         email=getattr(post.author, 'email'))

                            for comment in post.comments['data'].nodes:
                                # remove external users
                                if comment.person.node_id != 'None' and (comment.person.active or include_inactive):

                                    comment_diretoria = ''
                                    try:
                                        division = diretorias['Área'] == getattr(comment.person, 'division')
                                        comment_diretoria = diretorias.loc[division, 'Diretoria'].iloc[0]
                                    except:
                                        pass

                                    net.add_node(comment.person.node_id,
                                                 color=getattr(comment.person,
                                                               self.extractor.config.get('PLOT', 'color_field')),
                                                 label=getattr(comment.person,
                                                               self.extractor.config.get('PLOT', 'label_field')),
                                                 diretoria=comment_diretoria,
                                                 division=getattr(comment.person, 'division'),
                                                 department=getattr(comment.person, 'department'),
                                                 name=getattr(comment.person, 'name'),
                                                 emp_num=getattr(comment.person, 'emp_num'),
                                                 email=getattr(comment.person, 'email'))

                                    if net.has_edge(post.author.node_id, comment.person.node_id):
                                        # we added this one before, just increase the weight by one
                                        net[post.author.node_id][comment.person.node_id]['weight'] += 1
                                    else:
                                        # new edge. add with weight=1
                                        net.add_edge(post.author.node_id, comment.person.node_id, weight=1)

                        if post.reactions['data'] is not None and post.reactions['data'] and post.author.node_id != 'None':

                            net.add_node(post.author.node_id,
                                         color=getattr(post.author, self.extractor.config.get('PLOT', 'color_field')),
                                         label=getattr(post.author, self.extractor.config.get('PLOT', 'label_field')),
                                         diretoria=author_diretoria,
                                         division=getattr(post.author, 'division'),
                                         department=getattr(post.author, 'department'),
                                         name=getattr(post.author, 'name'),
                                         emp_num=getattr(post.author, 'emp_num'),
                                         email=getattr(post.author, 'email'))

                            for reaction in post.reactions['data']:
                                # remove external users
                                if reaction.person.node_id != 'None' and (reaction.person.active or include_inactive):

                                    reaction_diretoria = ''
                                    try:
                                        division = diretorias['Área'] == getattr(reaction.person, 'division')
                                        reaction_diretoria = diretorias.loc[division, 'Diretoria'].iloc[0]
                                    except:
                                        pass

                                    net.add_node(reaction.person.node_id,
                                                 color=getattr(reaction.person,
                                                               self.extractor.config.get('PLOT', 'color_field')),
                                                 label=getattr(reaction.person,
                                                               self.extractor.config.get('PLOT', 'label_field')),
                                                 diretoria=reaction_diretoria,
                                                 division=getattr(reaction.person, 'division'),
                                                 department=getattr(reaction.person, 'department'),
                                                 name=getattr(reaction.person, 'name'),
                                                 emp_num=getattr(reaction.person, 'emp_num'),
                                                 email=getattr(reaction.person, 'email'))

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

    def create_network_plot(self):

        net = deepcopy(self.net)

        max_rank = max(self.pagerank.items(), key=operator.itemgetter(1))[1]

        remove = [node for node, rank in self.pagerank.items() if rank < max_rank * 0.05]

        centrality = sorted(self.pagerank.items(), key=operator.itemgetter(1), reverse=True)

        max_nodes = 200
        max_index = min([max_nodes, net.number_of_nodes()-1])

        remove = [node for node, rank in self.pagerank.items() if rank < centrality[max_index][1]]
        net.remove_nodes_from(remove)

        node_ref = self.extractor.person_id

        # neighbours = [node for node in net.neighbors(node_ref)]
        # neighbours.append(node_ref)
        # remove = [node for node, rank in self.pagerank.items() if node not in neighbours]
        # net.remove_nodes_from(remove)

        # remove = [node for node, rank in self.pagerank.items() if rank < max_rank * 0.05]
        # net.remove_nodes_from(remove)

        colors = pd.DataFrame({node: net.nodes()[node]['diretoria'] for node in net.nodes()}, index=[0]).transpose()
        colors = colors.rename(columns={0: 'color'})
        colors['color'] = pd.Categorical(colors['color'])

        maxval = len(set(colors['color'].cat.codes))

        cmap = plt.cm.Spectral

        options = {
            'width': 0.1,
            'node_size': [min(self.pagerank[node] * 5000, 500) for node in net.nodes()],
            'node_color': [cmap(1.5*v/maxval) for v in colors['color'].cat.codes],
            'cmap': cmap,
            'with_labels': False,
            'edge_color': (0, 0, 0, 0.9),
            'edgecolors': (0, 0, 0, 0.9),
            'linewidths': 0.1
        }

        plt.figure()
        pos_nodes = nx.circular_layout(net)
        pos_nodes[node_ref] = np.array([0, 0])

        for node in pos_nodes:
            if node != node_ref:
                # print(net[node_ref][node]['weight'])
                wt = net[node_ref][node]['weight']
                wt = float(wt)
                pos_nodes[node] /= np.log10(wt**wt)+1

        nx.draw(net, pos_nodes, **options)

        pos_attrs = {}
        for node, coords in pos_nodes.items():
            pos_attrs[node] = (coords[0], coords[1] + 0.04)

        options = {
            'labels': {node: net.nodes()[node]['label'] if self.pagerank[node] >= max_rank * 0.5 else '' for node in net.nodes()},
            'font_size': 7,
            'font_color': 'red'
        }

        nx.draw_networkx_labels(net, pos_attrs, **options)

        for v in set(colors['color'].cat.codes):
            plt.scatter([], [], c=[cmap(1.5*v / maxval)], label=colors['color'].cat.categories[v])

        plt.legend()
        plt.show()

        plt.savefig(self.extractor.config.get('MISC', 'output_dir') + 'interactions_plot.png')

        plt.show()

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
