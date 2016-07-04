

from collections import defaultdict
import numpy as np
from sqlalchemy import inspect
from new.model import Dataset, DatasetData, DatasetQuality
import pandas as pd
from utils.timer import Timer

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
        print str(q)
        print '-'*50
        for d in q:
            with Timer(key="inspect2"):
                instance = inspect(d.data.quality)
            for key, x in instance.attrs.items():
                result[key].append(x.value)
        return pd.DataFrame(result)



def aggregate(db, snapshot):
    for portalid in  db.Session.query(Dataset.portalid).distinct():
        with Timer(key=portalid[0], verbose=True):
            with Timer(key=portalid[0]+'-agg', verbose=True):
                df= aggregateByPortal3(db, portalid[0], snapshot)
            with Timer(key=portalid[0]+'-mean', verbose=True):
                dfm=df.mean().round(decimals=2).copy()

                data={k:float(str(v)) for k,v  in dict(dfm).items()}
                data['datasets']=df.shape[0]
                print portalid[0],data




