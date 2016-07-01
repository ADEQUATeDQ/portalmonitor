from collections import defaultdict

from sqlalchemy import inspect

from new.db import DBClient
from new.model import Dataset, DatasetData, DatasetQuality
import pandas as pd

from utils.timer import Timer

if __name__ == '__main__':
    db= DBClient(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    snapshot=1429
    with Timer(key="qualityDF1", verbose=True):
        result = defaultdict(list)
        q=db.session.query(Dataset).filter(Dataset.snapshot==snapshot).filter(Dataset.portalid=='data_gv_at')
        print str(q)
        print '-'*50
        for d in q:
            with Timer(key="inspect1"):
                instance = inspect(d.data.quality)
            for key, x in instance.attrs.items():
                result[key].append(x.value)
        df = pd.DataFrame(result)

    with Timer(key="qualityDF2"):
        result2 = defaultdict(list)
        q=db.session.query(DatasetQuality).join(DatasetData).join(Dataset).filter(Dataset.snapshot==snapshot).filter(Dataset.portalid=='data_gv_at')
        print str(q)
        print '-'*50
        for d in q:
            instance = inspect(d)
            for key, x in instance.attrs.items():
                result2[key].append(x.value)
        df2 = pd.DataFrame(result2)

    with Timer(key="qualityDF3"):
        result = defaultdict(list)
        q=db.session.query(Dataset).filter(Dataset.snapshot==snapshot).filter(Dataset.portalid=='data_gv_at').join(DatasetQuality,Dataset.md5==DatasetQuality.md5)
        print str(q)
        print '-'*50
        for d in q:
            with Timer(key="inspect2"):
                instance = inspect(d.data.quality)
            for key, x in instance.attrs.items():
                result[key].append(x.value)
        df = pd.DataFrame(result)

    print df.describe()
    print df2.describe()

    Timer.printStats()