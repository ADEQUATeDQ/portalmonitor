import structlog

from odpw.core.model import ResourceInfo
from odpw.utils.timer import Timer

log =structlog.get_logger()




def resourceMigrate(snapshot, db, dbm):

    from odpw.db.models import Resource
    iter=Resource.iter(dbm.getResources(snapshot=snapshot))

    batch=[]
    for R in iter:
        uri=R.url
        uri=uri.replace("http:// \thttp:","http:")
        uri=uri.replace("http:// http://","http://")

        r={
            'snapshot':R.snapshot
            ,'uri':uri
            ,'timestamp':R.timestamp
            ,'status':R.status
            ,'exc':R.exception
            ,'header':R.header
            ,'mime':R.mime
            ,'size':R.size
        }
        RI=ResourceInfo(**r)

        if not db.exist_resourceinfo(RI.uri, RI.snapshot):
            if db.exist_metaresource(RI.uri):
                batch.append(RI)
                print len(batch)
            else:
                log.warn("URI missing", uri=RI.uri)
                print R.url, uri


        if len(batch)==1000:
            log.info("BatchInsert", size=len(batch))
            with Timer(key="BatchInsert", verbose=True):
                db.bulkadd(batch)
            batch=[]

    log.info("BatchInsert", size=len(batch))
    db.bulkadd(batch)
