'''
Created on Aug 14, 2015

@author: jumbrich
'''
from odpw.analysers import process_all, AnalyserSet
from odpw.analysers.core import DBAnalyser
from odpw.reporting.reporters import SoftWareDistReporter, ISO3DistReporter,\
    PortalListReporter, SnapshotsPerPortalReporter, DatasetSumReporter,\
    ResourceCountReporter, ResourceSizeReporter, TagReporter,\
    OrganisationReporter, FormatCountReporter, Report, UIReporter, CLIReporter,\
    CSVReporter, Reporter, DBReporter, DFtoListDict, addPercentageCol

from odpw.analysers.count_analysers import DCATTagsCount, DCATOrganizationsCount,\
    DCATFormatCount, PMDResourceStatsCount, DatasetCount
from odpw.analysers.resource_analysers import ResourceSize
from odpw.analysers.process_period_analysers import HeadPeriod, FetchPeriod
from odpw.reporting.time_period_reporting import FetchTimePeriodReporter,\
    HeadTimePeriodReporter


import pandas as pd
import numpy as np


def systeminfoall(dbm):
    """
        country and software distribution of portals in the system
        full list of portals
    """
    
    a = process_all(DBAnalyser(),dbm.getSoftwareDist())
    ab = process_all(DBAnalyser(),dbm.getCountryDist())
    pa = process_all(DBAnalyser(),dbm.getPortals())
    return  Report([    SoftWareDistReporter(a),
                        ISO3DistReporter(ab),
                        PortalListReporter(pa)
                 ])


def portalinfo(dbm, sn, portal_id):
    a= process_all( DBAnalyser(), dbm.getSnapshots( portalID=portal_id,apiurl=None))
    r=SnapshotsPerPortalReporter(a,portal_id)

        
    aset = AnalyserSet()
    #lc=aset.add(CKANLicenseCount())# how many licenses
    #lcc=aset.add(CKANLicenseConformance())

    tc= aset.add(DCATTagsCount())   # how many tags
    oc= aset.add(DCATOrganizationsCount())# how many organisations
    fc= aset.add(DCATFormatCount())# how many formats

    resC= aset.add(PMDResourceStatsCount())   # how many resources
    dsC=dc= aset.add(DatasetCount())    # how many datasets
    rsize=aset.add(ResourceSize())
    fa= aset.add(FetchPeriod())
    ha= aset.add(HeadPeriod())

    #use the latest portal meta data object
    if not sn:
        pmd = dbm.getLatestPortalMetaData(portalID=portal_id)
    else:
        pmd = dbm.getPortalMetaData(portalID=portal_id, snapshot=sn)
            
    aset = process_all(aset, [pmd])

    rep = Report([r,
                    DatasetSumReporter(dsC),
    ResourceCountReporter(resC),
    ResourceSizeReporter(rsize),
    #LicensesReporter(lc,lcc,topK=3),
    TagReporter(tc,dc, topK=3),
    OrganisationReporter(oc, topK=3),
    FormatCountReporter(fc, topK=3),
    FetchTimePeriodReporter(fa),
    HeadTimePeriodReporter(ha)
    ]
                 )
    
    return rep

class SystemPortalInfoReporter(DBReporter, UIReporter, CLIReporter, CSVReporter):
    
    
    def uireport(self):
        
        df = self.getDataFrame()
        
        
        
        dfsoftware=df.groupby("software",as_index=False)
        dfsoftwareSum= dfsoftware.aggregate(np.sum)
        t= dfsoftwareSum['count'].sum()
        print t
        res={ 
             'softwaredist':DFtoListDict(addPercentageCol(dfsoftwareSum)),
             'portals':t
        }
        for k in dfsoftware.groups:
            print k
            kISO= dfsoftware.get_group(k).groupby("iso3",as_index=False).aggregate(np.sum)
            res[k+"ISOdist"]=DFtoListDict(addPercentageCol(kISO))
        
        dfd= df.groupby("iso3",as_index=False).aggregate(np.sum)
        res["allISOdist"]=DFtoListDict(addPercentageCol(dfd))
        
       
        
            
        #sd= pd.DataFrame(dfsoftware.to_dict().items())
        #print sd
        
        
        
        return res
        
        
