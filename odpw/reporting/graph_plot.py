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


def draw_graph(dataframe, filter_portals=None, min_node_label=5000, node_labels=None, graph_layout='spring', font_color='blue',
               node_max_size=6000, node_min_size=50, node_color='grey', node_alpha=0.5,
               node_text_size=12, edge_text_size=8, min_edge_label=100,
               edge_color='grey', edge_alpha=0.8,
               edge_text_pos=0.3,
               text_font='sans-serif'):

    # create networkx graph
    array = dataframe.copy().as_matrix()
    num_of_resources = np.diagonal(array).copy()
    np.fill_diagonal(array, 0)
    G = nx.from_numpy_matrix(array)
    # add labels
    if node_labels is None:
        node_labels = {i: dataframe.index.values[i] for i in G.nodes()}
    else:
        node_labels = {i: node_labels[dataframe.index.values[i]] for i in G.nodes()}

    if filter_portals:
        filter_nodes = [i for i in G.nodes() if dataframe.index.values[i] not in filter_portals]
        G.remove_nodes_from(filter_nodes)
    else:
        # remove isolated ones
        G.remove_nodes_from(nx.isolates(G))



    # normalize node size in range
    nodes_res = []
    for n in G.nodes():
        nodes_res.append(num_of_resources[n])

    node_sizes = normalized(nodes_res, node_min_size, node_max_size)

    # remove labels for small ones
    node_labels = {n: node_labels[n] + '\n' + str(num_of_resources[n]) if num_of_resources[n] > min_node_label else '' for n in G.nodes()}

    # these are different layouts for the network you may try
    # shell seems to work best
    if graph_layout == 'spring':
        graph_pos=nx.spring_layout(G, iterations=100, k=0.81, weight=None)
    elif graph_layout == 'spectral':
        graph_pos=nx.spectral_layout(G)
    elif graph_layout == 'random':
        graph_pos=nx.random_layout(G)
    else:
        graph_pos=nx.shell_layout(G)


    edge_labels = dict([((u, v,), int(d['weight']) if d['weight'] > min_edge_label else '') for u, v, d in G.edges(data=True)])
    #edge_opac = edge_labels.copy()
    #edge_opac = normalized_dict(edge_opac, 0.2, 1)

    fig = plt.figure(1, figsize=(14, 12))
    # draw graph
    nx.draw_networkx_nodes(G,graph_pos,node_size=node_sizes,
                           alpha=node_alpha, node_color=node_color)
    nx.draw_networkx_edges(G,graph_pos,
                           alpha=edge_alpha, edge_color=edge_color)
    nx.draw_networkx_labels(G, graph_pos, node_labels, font_size=node_text_size,
                            font_family=text_font, font_color=font_color)

    nx.draw_networkx_edge_labels(G, graph_pos, edge_labels=edge_labels,
                                 label_pos=edge_text_pos, font_size=edge_text_size)

    # show graph
    plt.axis('off')
    plt.show()