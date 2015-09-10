import pandas as pd
from odpw.db.dbm import PostgressDBM
from odpw.db.models import Portal
from odpw.reporting import graph_plot

__author__ = 'sebastian'


if __name__ == '__main__':

    # match portal names
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    sn = 1533
    portals = Portal.iter(dbm.getPortals())
    p_ids = []
    p_urls_dict = {}
    for portal in portals:
        if portal.iso3 == 'AUT':
            p_ids.append(portal.id)

        url = portal.url.strip()
        if url.startswith('http://'):
            url = url[7:]
        if url.startswith('https://'):
            url = url[8:]
        if url.endswith('/'):
            url = url[:-1]
        p_urls_dict[portal.id] = url

    # draw graph
    df = pd.DataFrame.from_csv(path='tmp/overlap/resourceoverlapreporter.csv')
    graph_plot.draw_graph(df, node_min_size=5000, node_max_size=70000, edge_text_size=14, node_text_size=16, filter_portals=p_ids, node_labels=p_urls_dict, min_node_label=0, min_edge_label=0)
