from odpw.reporting.quality_reports import portalquality, portalsquality
from odpw.db.models import Portal
__author__ = 'jumbrich'

import os
from odpw.reporting.activity_reports import systemactivity
from odpw.reporting.info_reports import systeminfoall, portalinfo
from odpw.reporting.evolution_reports import systemevolution, portalEvolution_report

from odpw.utils.util import getSnapshot

import structlog
log =structlog.get_logger()


def name():
    return 'Report'
def help():
    return "Generate various reports"

def setupCLI(pa):
    
    pa.add_argument("-sn","--snapshot",  type=int, help='what snapshot is it', dest='snapshot')
    pa.add_argument("-ns","--nosnap",  help='no snapshot', dest='snapshotignore', action='store_true')
    
    tasks = pa.add_argument_group("Views")
    tasks.add_argument("-i",  help='generate the info report', dest='info', action='store_true')
    tasks.add_argument("-d",  help='generate the detailed report', dest='details', action='store_true')
    tasks.add_argument("-a",  help='generate the activity report', dest='activity', action='store_true')
    tasks.add_argument("-e",  help='generate the evolution report', dest='evolution', action='store_true')
    tasks.add_argument("-q",  help='generate the quality report', dest='quality', action='store_true')
    tasks.add_argument("-f", '--full',  help='generate the quality report', dest='full', action='store_true')
    
    focus = pa.add_argument_group("Views")
    focus.add_argument("-p",  help='Portal id ', dest='portal')
    focus.add_argument("-u",'--url'  help='Portal apiurl ', dest='url')
    focus.add_argument("-s",  help='System', dest='system', action='store_true')
    focus.add_argument("--portals",  help='Specify a set of portals', dest='portals', nargs='+')
    focus.add_argument("--software",  help='Specify a set of portals', dest='software', nargs='+')
    focus.add_argument("--iso3",  help='Specify a set of portals', dest='iso3', nargs='+')
    
    
    out = pa.add_argument_group("Output")
    out.add_argument("-o",  help='outputfolder to write the reports', dest='out')
    out.add_argument("--json",  help='json output ', dest='csv', action='store_true')

    
def cli(args,dbm):
    
    outdir= args.out
    if not outdir and any([args.csv]):
        print "No output dir "
        return
    
    reports=[]
    sn=None
    if args.info:
        if args.system:
            reports.append( systeminfoall(dbm) ) 
            
        if args.portal:
            reports.append( portalinfo(dbm, getSnapshot(args) , args.portal) )
    
    if args.details:
         if args.portal:
            reports.append( portaldetails(dbm, getSnapshot(args) , args.portal) )   
    
    if args.activity:
        if args.system:
            reports.append( systemactivity(dbm, sn) )
            
    
    if args.evolution:
        if args.system:
            reports.append( systemevolution(dbm) ) 
            
            
        if args.portal:
            sn = getSnapshot(args)
            reports.append( portalevolution(dbm,sn , args.portal) )
            
    if args.quality:
        sn = getSnapshot(args)
        if args.portal:
            reports.append( portalquality(dbm,sn , args.portal) )
        if args.portals:
            pid= args.portals
            reports.append( portalsquality(dbm,sn , pid) )
        elif args.iso3:
            pid=[]
            for iso3 in args.iso3: 
                for P in Portal.iter( dbm.getPortals(iso3=iso3)):
                    if P.id not in pid:
                        pid.append(P.id)
            reports.append( portalsquality(dbm,sn , pid) )
    
    for r in reports:
        output( r , args, snapshot=sn)
        

#===============================================================================
#     if args.pdetail:
#         pmds = dbm.getLatestPortalMetaDatas()
# 
#         a = AnalyserSet()
#         da = PMDDatasetCountAnalyser(bins=[0,50,100,500,1000,5000,10000,50000,100000])
#         #ra = PMDResourceCountAnalyser()
# 
#         a.add(da)
#         #a.add(ra)
#         process_all(a, PortalMetaData.iter(pmds))
# 
#         print da.getResult()
# 
#         ds = dbm.getDatasets(portalID='opendata_socrata_com', snapshot=args.snapshot)
# 
#         a = AnalyserSet()
#         #a.add(CKANFormatCount())
#         #a.add(CKANLicenseCount())
#         #a.add(CKANOrganizationsCount())
#         #a.add(CKANTagsCount())
# 
#         a.add(SocrataKeyAnalyser())
# 
#         process_all(a, Dataset.iter(ds))
# 
#         for res in a.getAnalysers():
#             print res.getResult()
#===============================================================================

    
             
    
    #===========================================================================
    # if args.sysactivity:
    #     it =PortalMetaData.iter(dbm.getPortalMetaDatas(snapshot=sn, portalID=args.portal_id))
    #     a = process_all(PMDActivityAnalyser(),it)
    #     totalDS = dbm.countDatasets(snapshot=sn)
    #     totalRes= dbm.countResources(snapshot=sn)
    #     
    #     report = Report([SystemActivityReporter( a, snapshot=sn, portalID=args.portal_id, dbds=totalDS, dbres= totalRes)])
    #     
    #     output(report,args)
    # 
    # if any([args.pevolution,args.pgen,args.pdetail,args.pquality, args.pactivity ]) and not any([  args.portal_id]):
    #     print "Portal URL or ID is missing"
    #     return 
    # 
    # if args.pgen:    
    #     #get all available snapshots
    #     
    #     
    #     
    #     
    # if args.pdetail:
    #     pass
    #===========================================================================
#===============================================================================
#         aset = AnalyserSet()
#         
#         #lc=aset.add(CKANLicenseCount())
#         #lcc=aset.add(CKANLicenseConformance())
#         
#         tc= aset.add(DCATTagsCount())   # how many tags
#         oc= aset.add(DCATOrganizationsCount())# how many organisations
#         fc= aset.add(DCATFormatCount())# how many formats
# 
#         resC= aset.add(PMDResourceStatsCount())   # how many resources
#         dsC=dc= aset.add(DatasetCount())    # how many datasets
#         rsize=aset.add(ResourceSize())
# 
# 
#         it = dbm.getLatestPortalMetaData(portalID=args.portal_id)
#         aset = process_all(aset, PortalMetaData.iter(it))
#     
#         
#         rep = Report([LicensesReporter(lc,lcc),
#                       TagReporter(tc,dc),
#                       OrganisationReporter(oc),
#                       FormatCountReporter(fc)])
#         
#         output(rep,args)
#===============================================================================
    
    
  #=============================================================================
  #   if args.pquality:
  #       pass
  #   if args.pevolution:
  #       aset = AnalyserSet()
  #       
  #       de=aset.add(DatasetEvolution())
  #       re= aset.add(ResourceEvolution())
  #       
  #       
  #       it = dbm.getPortalMetaDatas(portalID=args.portal_id)
  #       aset = process_all(aset, PortalMetaData.iter(it))
  #       
  #       rep = Report([EvolutionReporter(de)])
  # 
  #       output(rep,args)
  #       
  #   if args.pactivity:
  #       pass
  #   
  #=============================================================================
def output(rE, args,  snapshot=None):
    
    #rE.clireport()
    import pprint
    if args.csv:
        outdir= os.path.join(args.out, snapshot ) if snapshot else args.out 
        print rE.csvreport(outdir )
    if args.json:
        pprint.pprint( rE.json() )
