from collections import defaultdict

import pandas as pd
from sqlalchemy import inspect

from odpw.new.core.db import row2dict
from odpw.new.core.model import Dataset, DatasetData, DatasetQuality, PortalSnapshotQuality
from odpw.new.utils.timing import Timer


def aggregateByPortal1(db, portalid, snapshot):
    with Timer(key="qualityDF1", verbose=True):
        result = defaultdict(list)
        q=db.Session.query(Dataset).filter(Dataset.snapshot==snapshot).filter(Dataset.portalid==portalid)
        print str(q)
        print '-'*50
        for d in q:
            with Timer(key="inspect1"):
                instance = inspect(d.data.quality)
            for key, x in instance.attrs.items():
                result[key].append(x.value)
        return pd.DataFrame(result)


def aggregateByPortal2(db, portalid, snapshot):
    with Timer(key="qualityDF2"):
        result2 = defaultdict(list)
        q=db.Session.query(DatasetQuality).join(DatasetData).join(Dataset).filter(Dataset.snapshot==snapshot).filter(Dataset.portalid==portalid)
        print str(q)
        print '-'*50
        for d in q:
            instance = inspect(d)
            for key, x in instance.attrs.items():
                result2[key].append(x.value)
        return pd.DataFrame(result2)

def aggregateByPortal3(db, portalid, snapshot):
    with Timer(key="qualityDF3"):
        result = defaultdict(list)
        q=db.Session.query(Dataset).filter(Dataset.snapshot==snapshot).filter(Dataset.portalid==portalid).join(DatasetQuality, Dataset.md5==DatasetQuality.md5)
        #print str(q)
        #print '-'*50
        for d in q:
            with Timer(key="inspect2"):
                instance = inspect(d.data.quality)
            for key, x in instance.attrs.items():
                result[key].append(x.value)
        return pd.DataFrame(result)


#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*
# QUALITY AGGREGATES
#--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*
boolTypeCol=['cocu','coce','coli','coac','exac','exco','opli']

def aggregatePortalQuality(db, portalid, snapshot):

    with Timer(key=portalid+'-agg', verbose=True):
        df= aggregateByPortal3(db, portalid, snapshot)

    data={}
    with Timer(key=portalid+'-mean', verbose=True):
        if df.shape[0] != 0:
            for c in boolTypeCol:
                df[c]=df[c].apply(bool)

            data={ k:float(str(v[['mean']]['mean'].round(decimals=2))) for k,v  in dict(df.describe()).items()}
            data.update({ k+'N':int(v[['count']]['count']) for k,v  in dict(df.describe()).items()})

    data['datasets']=df.shape[0]
    PSQ= PortalSnapshotQuality(portalid=portalid, snapshot=snapshot, **data)
    db.add(PSQ)

def aggregate(db, snapshot):
    for portalid in  db.Session.query(Dataset.portalid).distinct():
        aggregatePortalQuality(db,portalid,snapshot)


def aggregatePortalInfo(db, portalid, snapshot):
    stats={}
    with Timer(key=portalid+'-agg', verbose=True):
        PS= db.portalSnapshot(snapshot, portalid).first()
        if PS is None:
            return stats
        ds=PS.datasetCount

        for key, cFunc, dFunc in [
                        ('organisation',db.organisationDist, db.distinctFormats)
                        ,('license',db.licenseDist,db.distinctLicenses)
                        ,('format',db.formatDist,db.distinctFormats)
                         ]:

            with Timer(key=portalid+'-'+key, verbose=True):

                s=[]
                q=cFunc(snapshot,portalid=portalid)
                print str(q)
                for i in q.limit(3):
                    d=row2dict(i)
                    print d
                    d['perc']=d['count']/(1.0*ds)
                    s.append(d)
                t=sum(item['count'] for item in s)
                if ds-t != 0:
                    s.append({key:'Others', 'count':ds-t,'perc':(ds-t)/(1.0*ds)})
                stats[key]={'distinct':cFunc(snapshot,portalid=portalid).count(),'top3Dist':s}

    return stats







#opfo
#opli
#opma
#cofo
