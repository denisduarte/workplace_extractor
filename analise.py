import pickle
import asyncio
from copy import copy

import pandas as pd

from workplace_extractor import Extractor
from workplace_extractor.Extractors.PersonExtractor import PersonExtractor

"""
async def run():
    extractor = Extractor('access.token', '', '', '', '', 'INFO')
    await extractor.init()
    people = await PersonExtractor(extractor).extract()
    return people


loop = asyncio.get_event_loop()
people = loop.run_until_complete(run())


df = pd.DataFrame(columns=['user_id', 'user_email', 'posts', 'comments', 'reactions', 'views'], index=[0])

for index, person in enumerate(people.nodes):
    row = pd.DataFrame({'user_id': person.node_id,
                        'user_email': person.email,
                        'posts': 0,
                        'comments': 0,
                        'reactions': 0,
                        'views': 0}, index=[index])
    df = df.append(row)

df = df.reset_index()
df = df.drop(0)
df = df.reset_index()

with open('df.pickle', 'wb') as handle:
    pickle.dump(df, handle)
with open('df.pickle', 'rb') as handle:
    df = pickle.load(handle)

with open('extract_interactions2.pickle', 'rb') as handle:
    data = pickle.load(handle)

try:
    for node in data.nodes:
        if node.feed.nodes:
            for post in node.feed.nodes:
                df.loc[df['user_id'] == post.author_id, 'posts'] += 1

                if post.comments is not None and post.comments.nodes:
                    for comment in post.comments.nodes:
                        if comment.person.node_id is not None and comment.person.node_id != 'None':
                            df.loc[df['user_id'] == comment.person.node_id, 'comments'] += 1

                if post.reactions is not None and post.reactions:
                    for reaction in post.reactions:
                        if reaction.person is not None and reaction.person.node_id is not None and reaction.person.node_id != 'None':
                            df.loc[df['user_id'] == reaction.person.node_id, 'reactions'] += 1

                if post.views is not None and post.views:
                    for view in post.views:
                        if view.person is not None and view.person.node_id is not None and view.person.node_id != 'None':
                                df.loc[df['user_id'] == view.person.node_id, 'views'] += 1

except Exception as e:
    print(1)

df2 = df
with open('df2.pickle', 'wb') as handle:
    pickle.dump(df2, handle)
with open('df2.pickle', 'rb') as handle:
    df2 = pickle.load(handle)

df2.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=[" ", " "], regex=True) \
   .to_csv('df2.csv', index=False, sep=";")
"""


with open('output/workplace_interactions-hash.pickle', 'rb') as handle:
    data = pickle.load(handle)


rh = pd.read_csv('/Users/denisduarte/Petrobras/COM/uso de workplace por regime de turno/rh_operacional.csv', sep=';')

emails_turno = rh.loc[rh.regime == 'operacional', 'Email'].tolist()
print(1)
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib

G = nx.Graph()
labels = {}
area = {}
try:
    for node in data.nodes:
        if node.feed.nodes:
            for post in node.feed.nodes:
                if post.comments['data'] is not None and post.comments['data'].nodes:
                    for comment in post.comments['data'].nodes:
                        G.add_node(post.author.node_id, area=post.author.division, chave=post.author.emp_num)
                        G.add_node(comment.person.node_id, area=comment.person.division, chave=comment.person.emp_num)
                        G.add_edge(post.author.node_id, comment.person.node_id)

                if post.reactions['data'] is not None and post.reactions['data']:
                    for reaction in post.reactions['data']:
                        G.add_node(post.author.node_id, area=post.author.division, chave=post.author.emp_num)
                        G.add_node(reaction.person.node_id, area=reaction.person.division, chave=reaction.person.emp_num)
                        G.add_edge(post.author.node_id, reaction.person.node_id)

except Exception as e:
    print(e)
print(2)
page_rank = nx.pagerank(G)

cores = pd.DataFrame({node:  G.nodes()[node]['area'] for node in G.nodes()}, index=[0]).transpose()
cores = cores.rename(columns={0: 'area'})
cores['cor'] = pd.Categorical(cores['area'])
print(3)
from random import randint
color = ['#%06X' % randint(0, 0xFFFFFF) for i in range(len(cores['cor'].unique().tolist()))]

cmap = matplotlib.colors.ListedColormap(color)

print(f'nodes = {G.number_of_nodes()}')
print(f'edges = {G.number_of_edges()}')
print(4)
options = {
    'width': 0.1,
    'node_size': [page_rank[node]*1000 for node in G.nodes()],
    'node_color': cores['cor'].cat.codes,
    'cmap': cmap,
    'with_labels': False
}

plt.figure()
#pos_nodes = nx.kamada_kawai_layout(G)
print(4.5)
#nx.draw(G, pos_nodes, **options)
nx.draw(G, pos_nodes, **options)
print(5)
pos_attrs = {}
for node, coords in pos_nodes.items():
    pos_attrs[node] = (coords[0], coords[1] + 0.04)

options = {
    'labels': {node: G.nodes()[node]['chave'] if page_rank[node] > 0.005 else '' for node in G.nodes()},
    'font_size': 7,
    'font_color': 'red'
}
print(6)
nx.draw_networkx_labels(G, pos_attrs, **options)
plt.show()
print(7)





"""

centrality = pd.DataFrame()

i = 0
for key, value in page_rank.items():
    centrality = centrality.append(pd.DataFrame({'user_id': key,
                                                 'centrality': value,
                                                 'chave': G.nodes()[key]['chave']}, index=[i]))
    i += 1

centrality.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=[" ", " "], regex=True) \
          .to_csv('centrality.csv', index=False, sep=";")

print(1)
"""