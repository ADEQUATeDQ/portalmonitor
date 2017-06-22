import datetime
from odpw.core.api import DBClient
from odpw.core.db import DBManager
from odpw.core.model import PortalSnapshot, Dataset, Portal, PortalSnapshotDynamicity

import structlog
log =structlog.get_logger()


def dynPortal(db, portal, cursn, prevsn):
    ds_cur = {r.id: r.md5 for r in
              db.Session.query(Dataset).filter(Dataset.portalid == portal.id).filter(Dataset.snapshot == cursn)}
    ds_prev = {r.id: r.md5 for r in
              db.Session.query(Dataset).filter(Dataset.portalid == portal.id).filter(Dataset.snapshot == prevsn)}

    inter = 1.0 * len(list(
        set(list(ds_cur.keys())) | set(list(ds_prev.keys()))
    ))

    c = {'sn': cursn, 'pid': p.id,
        "size": len(ds_cur),
        'adds': 0, 'dels': 0,
        'ups': 0 , 'static': 0}

    for id,md5 in ds_cur.items():
        if id not in ds_prev:
            c['adds'] += 1
        else:
            if md5 != ds_prev[id]:
                c['ups'] += 1
            else:
                c['static'] += 1
            #del key from prev snapshot
            del ds_prev[id]

    for id, md5 in ds_prev.items():
        if id not in ds_cur:
            c['dels'] += 1


    pdss=[ psd for psd in db.Session.query(PortalSnapshotDynamicity).filter(PortalSnapshotDynamicity.snapshot<cursn).filter(PortalSnapshotDynamicity.portalid == portal.id) ]
    #TODO compute hindex
    snChanged = 1 if (c['adds']+c['dels']+c['ups']) >0 else 0
    dynRatioList = []
    for pds in pdss:
        if (pds.updated + pds.added + pds.deleted) > 0:
            snChanged += 1
        dynRatioList.append(pds.dyratio)

    dynRatioList.append((c['adds']+c['dels']+c['ups'])/ (1.0 * inter) ) if inter >0 else 0
    dynRatioList = sorted(dynRatioList, reverse=True)

    h = 0
    for i, e in enumerate(dynRatioList):
        bin = int(e * 100)
        ratio = 100 * ((i + 1) / (1.0 * len(dynRatioList)))
        if ratio >= bin:
            h = bin
            break
    if cursn==1701:
        print ""

    psd = PortalSnapshotDynamicity(
        portalid=portal.id,
        snapshot=cursn,

        dindex=h,
        changefrequ= snChanged / (1.0 * (len(pdss) +1)),
        updated=c['ups'],
        added=c['adds'],
        deleted=c['dels'],
        static=c['static'],
        intersected=inter,
        size=c['size']
    )

    db.add(psd)
    log.debug("Dynamicity computation", portal=portal.id, dIndex=str(psd.dindex), changeFrequ=str(psd.changefrequ))


if __name__ == '__main__':

    dbm=DBManager(user='opwu', host='localhost', port=2112, db='portalwatch')
    db = DBClient(dbm)

    portals=[p for p in db.Session.query(Portal).all()]
    pdata = []
    a=0
    for p in portals:
        print '*-'*10,p,'-*'*10
        #if p.id != "www_opendataportal_at":
        #    continue

        sn=[ ps.snapshot for ps in db.Session.query(PortalSnapshot).filter(PortalSnapshot.portalid == p.id)]
        sn=sorted(sn)

        target_cursn = None#getSnapshotfromTime(datetime.datetime.now())

        changes = {}
        for i in range(1,len(sn)):
            # from 0 to len-1

            cursn=sn[i]
            if target_cursn and cursn!=target_cursn:
                continue

            prevsn = sn[i - 1] if i != 0 else None
            psd = db.getPortalSnapshotDynamics(cursn, p.id).all()
            if not psd:
                dynPortal(db, p, cursn, prevsn)
