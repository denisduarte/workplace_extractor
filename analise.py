import pickle
import pandas as pd

from workplace_extractor.Nodes.NodeCollection import NodeCollection

with open('/Users/denisduarte/Petrobras/PythonProjects/workplace_extractor/output2/workplace_interactions-newnew.pickle', 'rb') as handle:
    feeds = pickle.load(handle)

