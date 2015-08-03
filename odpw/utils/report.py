from odpw.reporting.reporters import SystemActivityReporter, ReporterEngine, SoftWareDistReporter,\
    ISO3DistReporter, SnapshotsPerPortalReporter
from odpw.analysers.core import DBAnalyser

__author__ = 'jumbrich'

from odpw.utils.util import getSnapshot

import structlog
log =structlog.get_logger()


def name():
    return 'Report'
def help():
    return "Generate various reports"

def setupCLI(pa):
    
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    pa.add_argument("-i","--ignore",  help='Force to use current date as snapshot', dest='ignore', action='store_true')
    
    
    
    tasks = pa.add_argument_group("system reports")
    tasks.add_argument("--overview",  help='generate the system overview report', dest='sysover', action='store_true')
    tasks.add_argument("--activity",  help='generate the system activity report', dest='sysactivity', action='store_true')
    
    tasks = pa.add_argument_group("portal reports")
    #tasks.add_argument("--apiurl",  help='Portal apiurl ', dest='apiurl')
    tasks.add_argument("--pid",  help='Portal id ', dest='portal_id')
    
    tasks.add_argument("--pgen",  help='Portal overview report', dest='pgen', action='store_true')
    tasks.add_argument("--pdetail",  help='Portal detail report', dest='pdetail', action='store_true')
    tasks.add_argument("--pquality",  help='Portal quality report', dest='pquality', action='store_true')
    tasks.add_argument("--pactivity",  help='Portal activity report', dest='pactivity', action='store_true')
    tasks.add_argument("--pevolution",  help='Portal evolution report', dest='pevolution', action='store_true')
    
    
    out = pa.add_argument_group("Output")
    out.add_argument("--out",  help='outputfolder to write the reports', dest='outdir')
    out.add_argument("--csv",  help='generate the system activity report', dest='csv', action='store_true')
    out.add_argument("--ui",  help='generate the system activity report', dest='ui', action='store_true')
    
def cli(args,dbm):
    
    
    #for pmd in dbm.getLatestPortalMetaDatas():
    #    print pmd
    outdir= args.outdir
    if not outdir and any([args.csv]):
        print "No output dir "
        return
        
    
    if args.sysover:
        
        (DBAnalyser, dbm.getSoftwareDist)
        (DBAnalyser, dbm.getCountryDist)
        
        sys_or = ReporterEngine([SoftWareDistReporter(dbm),
                                 ISO3DistReporter(dbm)])
        sys_or.run()
    
        output(sys_or,args)
        
    if any([args.pevolution,
            args.pgen,
            args.pdetail,
            args.pquality, 
            args.pactivity,
            args.sysactivity ]):
        sn = getSnapshot(args)
        if not sn:
            print "No snapshot specified"
            return
             
    
    if args.sysactivity:
        
        
        sys_act_rep = ReporterEngine([SystemActivityReporter(dbm,sn)])
        sys_act_rep.run()
        
        output(sys_act_rep,args)
    
    if any([args.pevolution,args.pgen,args.pdetail,args.pquality, args.pactivity ]) and not any([  args.portal_id]):
        print "Portal URL or ID is missing"
        return 
    
    if args.pgen:    
        rep = ReporterEngine([SnapshotsPerPortalReporter(dbm,portalID=args.portal_id)])
        
        rep.run()
        
        output(rep,args)
        
    if args.pdetail:
        pass
    if args.pquality:
        pass
    if args.pevolution:
        pass
    if args.pactivity:
        pass
    
def output(rE, args):
    
    rE.clireport()
    
    if args.csv:
        rE.csvreport(args.outdir)
    if args.ui:
        print rE.uireport()
