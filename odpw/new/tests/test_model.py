import datetime

from analysers.quality.analysers import dcat_analyser
from new.db import DBClient
from new.model import Portal, Dataset, DatasetData, Base, DatasetQuality, PortalSnapshot, MetaResource, ResourceInfo, \
    PortalSnapshotFetch

if __name__ == '__main__':


    db= DBClient(user='opwu', password='0pwu', host='portalwatch.ai.wu.ac.at', port=5432, db='portalwatch')
    db= DBClient(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')
    db.init(Base)

    P = Portal(id='data_wu_ac_at', uri='http://data.wu.ac.at/', apiuri='http://data.wu.ac.at/', software="CKAN", iso='AT')

    PS = PortalSnapshot(portalid='data_wu_ac_at',snapshot=1620)
    PSF = PortalSnapshotFetch(portalid='data_wu_ac_at',snapshot=1620)


    DD= DatasetData(md5='asx', raw={'x':0, 'y':12})
    D= Dataset(id="test", snapshot=1620,portalid=P.id, md5=DD.md5)
    DQ= DatasetQuality(md5=DD.md5)

    R = MetaResource(uri="http://", md5=DD.md5)

    db.add(P)
    db.add(PS)
    db.add(PSF)
    db.add(DD)
    db.add(DQ)
    db.add(D)
    db.add(R)

    print P.id
    for s in P.snapshots:
        print ' ',s
        for d in s.datasets:
            print '  ',d
            print '    ->',d.data
            print '     ->',d.data.quality
            print '     <-',d.data.quality.data
            print '    <-',d.data.dataset
            for r in d.data.resources:
                print '    >',r
                print '   <',r.dataset

    PSF.end=    datetime.datetime.now()
    db.commit()
    print PSF.fetchtime