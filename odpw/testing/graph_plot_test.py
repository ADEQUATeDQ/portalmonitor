import pandas as pd
from odpw.db.dbm import PostgressDBM
from odpw.db.models import Portal
from odpw.reporting import graph_plot
import csv

__author__ = 'sebastian'


if __name__ == '__main__':

    # get header
    with open('tmp/overlap/resourceoverlapreporter.csv') as f:
        headers = f.readline().strip().split(',')

    # match portal names
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    sn = 1533
    portals = Portal.iter(dbm.getPortals())

    p_urls_dict = {}
    for p in portals:
        if p.id in headers:
            p_urls_dict[p.id] = p.url
    p_urls = []
    for h in headers:
        p_urls.append(p_urls_dict[h].strip().lstrip('http://').lstrip('https://').rstrip('/')[:24])

    # draw graph
    df = pd.DataFrame.from_csv(path='tmp/overlap/resourceoverlapreporter.csv', index_col=1)
    graph_plot.draw_graph(df, node_labels=p_urls)
