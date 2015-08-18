import networkx as nx
import matplotlib.pyplot as plt
import numpy as np

def normalized(array, x, y):
    # Normalize to [0, 1]:
    m = min(array)
    range = max(array) - m
    array = (array - m) / range

    # Then scale to [x,y]:
    range2 = y - x
    normalized = (array*range2) + x
    return normalized


def normalized_dict(dict, x, y):
    # Normalize to [0, 1]:
    m = min(dict.values())
    range = max(max(dict.values()) - m, m)
    array = {}
    for k in dict:
        array[k] = (dict[k] - m) / range

    # Then scale to [x,y]:
    range2 = y - x
    normalized = {}
    for k in array:
        normalized[k] = (array[k] * range2) + x
    return normalized


def draw_graph(dataframe, min_ds=5000, node_labels=None, graph_layout='shell',
               node_size=13000, node_color='grey', node_alpha=0.5,
               node_text_size=9,
               edge_color='grey', edge_alpha=0.5, edge_tickness=10,
               edge_text_pos=0.3,
               text_font='sans-serif'):

    # create networkx graph
    array = dataframe.copy().as_matrix()
    num_of_resources = np.diagonal(array).copy()
    np.fill_diagonal(array, 0)
    G = nx.from_numpy_matrix(array)

    # remove isolated ones
    G.remove_nodes_from(nx.isolates(G))
    # remove nodes with degree 1
    to_remove = []
    for n in G.nodes():
        rem = True
        for u, v, d in G.edges(n, data=True):
            if d['weight'] > min_ds:
                rem = False
                break
        if rem:
            to_remove.append(n)
    G.remove_nodes_from(to_remove)
    # normalize node size in range
    nodes = []
    for n in G.nodes():
        nodes.append(num_of_resources[n])

    node_sizes = normalized(nodes, 5000, node_size)

    # these are different layouts for the network you may try
    # shell seems to work best
    if graph_layout == 'spring':
        graph_pos=nx.spring_layout(G)
    elif graph_layout == 'spectral':
        graph_pos=nx.spectral_layout(G)
    elif graph_layout == 'random':
        graph_pos=nx.random_layout(G)
    else:
        graph_pos=nx.shell_layout(G)

    if node_labels is None:
        node_labels = {i: dataframe.index.values[i] for i in G.nodes()}
    else:
        node_labels = {i: node_labels[i] for i in G.nodes()}

    edge_labels = dict([((u, v,), int(d['weight'])) for u, v, d in G.edges(data=True)])
    #width_dict = edge_labels.copy()
    #width_dict = normalized_dict(width_dict, 0, edge_tickness)

    plt.figure(1,figsize=(14, 12))
    # draw graph
    nx.draw_networkx_nodes(G,graph_pos,node_size=node_sizes,
                           alpha=node_alpha, node_color=node_color)
    nx.draw_networkx_edges(G,graph_pos, #edge_width=width_dict,
                           alpha=edge_alpha,edge_color=edge_color)
    nx.draw_networkx_labels(G, graph_pos, node_labels, font_size=node_text_size,
                            font_family=text_font)

    nx.draw_networkx_edge_labels(G, graph_pos, edge_labels=edge_labels,
                                 label_pos=edge_text_pos)

    # show graph
    plt.show()