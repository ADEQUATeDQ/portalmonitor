from new.core.model import Portal
from odpw.db.dbm import PostgressDBM
from odpw.db.models import Dataset
from odpw.new.services.fetch_insert import dict_to_dcat
from odpw.utils.dcat_access import getOrganization

if __name__ == '__main__':
    from pprint import pprint

    dbm=PostgressDBM(user='opwu', password='0pwu', host='portalwatch.ai.wu.ac.at', port=5432, db='portalwatch')
    for D in Dataset.iter(dbm.getDatasets(software="CKAN", snapshot=1620, limit=1)):
        print D.software ,D.data.keys()
        P = Portal(id=D.portal_id, software=D.software, apiuri='http')

        D.dcat = dict_to_dcat(D.data,P)
        pprint(D.dcat)
        print getOrganization(D)


    for D in Dataset.iter(dbm.getDatasets(software="Socrata", snapshot=1620, limit=1)):
        print D.software,D.data['view']
        P = Portal(id=D.portal_id, software=D.software, apiuri='http')

        D.dcat = dict_to_dcat(D.data,P)
        pprint(D.dcat)
        print getOrganization(D)



    for D in Dataset.iter(dbm.getDatasets(software=None, snapshot=1620, limit=1)):
        print D.software
        pprint(D.data)
        P = Portal(id=D.portal_id, software='OpenDataSoft', apiuri='http')

        D.dcat = dict_to_dcat(D.data,P)
        pprint(D.dcat)
        print getOrganization(D)
