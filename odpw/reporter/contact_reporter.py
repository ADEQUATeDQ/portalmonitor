from collections import defaultdict

from odpw.core import portal_fetch_processors
from odpw.core.db import row2dict
from odpw.core.dcat_access import getContactPoint, getContactEmail, getContactPointValues
from odpw.core.dataset_converter import dict_to_dcat
from odpw.core.model import Dataset, DatasetData, DatasetQuality, ResourceInfo, MetaResource
from odpw.utils.timing import Timer
from odpw.reporter import dataset_reporter

def contactPerOrga(Session, portal, snapshot, orga):
    q = Session.query(Dataset) \
        .filter(Dataset.portalid == portal.id) \
        .filter(Dataset.snapshot == snapshot) \
        .filter(Dataset.organisation == orga) \
        .join(DatasetData, DatasetData.md5 == Dataset.md5) \
        .join(DatasetQuality, DatasetQuality.md5 == Dataset.md5) \
        .add_entity(DatasetData).add_entity(DatasetQuality)
    pereMail = set([])
    for res in q:  # Dataset, DatasetData, DatasetQuality
        print(res)
        ds = row2dict(res)

        d = portal_fetch_processors.Dataset(snapshot=snapshot, portalID=portal.id, did=ds['id'], data=ds['raw'],
                                            status=200, software=portal.software)
        d.dcat = dict_to_dcat(ds['raw'], portal)
        contact = getContactPointValues(d)
        if len(contact) > 1:
            pereMail.add(contact[1])
    return pereMail

def orgaReport(Session, portal, snapshot, orga, contact=None):
    with Timer(key=orga, verbose=True):
        q = Session.query(Dataset) \
            .filter(Dataset.portalid == portal.id) \
            .filter(Dataset.snapshot == snapshot) \
            .filter(Dataset.organisation == orga) \
            .join(DatasetData, DatasetData.md5 == Dataset.md5) \
            .join(DatasetQuality, DatasetQuality.md5 == Dataset.md5) \
            .add_entity(DatasetData).add_entity(DatasetQuality)
        pereMail = {}
        uris=set([])
        summary={'status':defaultdict(int)}
        summary['status'][200]=0
        summary['status'][404]=0
        summary['status']['total'] = 0

        for res in q: #Dataset, DatasetData, DatasetQuality
            ds={}
            ds['dataset'] = row2dict(res[0])
            #ds['dataset']['external_uri']=portal.apiuri+"/katalog/dataset"
            ds['data'] = row2dict(res[1])
            ds['quality']=row2dict(res[2])


            d = portal_fetch_processors.Dataset(snapshot=snapshot, portalID=portal.id, did=ds['dataset']['id'], data=ds['data']['raw'],
                                                status=200, software=portal.software)
            d.dcat = dict_to_dcat(ds['data']['raw'], portal)
            contactInfo = getContactPointValues(d)
            if len(contactInfo) > 1:
                if contact is not None and contact!=contactInfo[1]:
                    continue

                ds['report'] = dataset_reporter.report(res[1], res[2], portal.software)

                orgas = pereMail.setdefault(contactInfo[1], {})
                ds_list = orgas.setdefault(orga, [])
                ds_list.append(ds)
                ds['resourcesStatus']=defaultdict(int)
                ds['resourcesStatus']['total']=0
                ds['resources']=[row2dict(r) for r in res[1].resources]
                for resou in ds['resources']:
                    resri=Session.query(ResourceInfo).filter(ResourceInfo.uri == resou['uri']).filter(ResourceInfo.snapshot==snapshot).first()
                    if resri is not None:
                        resou['info']=row2dict(resri)
                        ds['resourcesStatus'][resou['info']['status']]+=1
                        ds['resourcesStatus']['total'] += 1
                        if resou['uri'] not in uris:
                            summary['status'][resou['info']['status']]+=1
                            summary['status']['total'] += 1
                    if resou['uri'] not in uris:
                        uris.add(resou['uri'])

                ds['resourcesStatus']=dict(ds['resourcesStatus'])
        ContactCount = 0
        #print "  Organisation:", orga
        for k, v in pereMail.items():
            print "  contact:", k
            for orga, ds_list in v.items():
                print "   ", orga, len(ds_list)
                ContactCount += len(ds_list)
                for ds in ds_list:
                    print "    >",ds['report']

        summary['status']=dict(summary['status'])
        pereMail['summary']=summary
        pereMail['summary']['totaluris']=len(uris)
        return pereMail


def report(Session, portal, snapshot):
    q = Session.query(Dataset.organisation) \
        .filter(Dataset.portalid == portal.id) \
        .filter(Dataset.snapshot == snapshot).distinct(Dataset.organisation)

    for res in q:
        orgaReport(Session, portal, snapshot, res[0])
        print res

