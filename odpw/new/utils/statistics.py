



import pandas as pd

from odpw.new.utils.plots import qa
from odpw.new.core.db import row2dict
from odpw.new.core.model import PortalSnapshotQuality


def portalSnapshotQualityDF(db, portalid, snapshot):
    data=None
    for r in db.Session.query(PortalSnapshotQuality)\
        .filter(PortalSnapshotQuality.portalid==portalid)\
        .filter(PortalSnapshotQuality.snapshot==snapshot):
        data=row2dict(r)
        break
    d=[]

    datasets= int(data['datasets'])
    for inD in qa:
        for k , v in inD['metrics'].items():
            k=k.lower()
            value=float(data[k])
            perc=int(data[k+'N'])/(datasets*1.0) if datasets>0 else 0
            c= { 'Metric':k, 'Dimension':inD['dimension'],
                 'dim_color':inD['color'], 'value':value, 'perc':perc}
            c.update(v)
            d.append(c)
    return pd.DataFrame(d)
