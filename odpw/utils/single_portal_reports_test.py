'''
Created on Dec 10, 2015

@author: jumbrich
'''
# -*- coding: utf-8 -*-
import errno

from analysers.process_period_analysers import FetchPeriod, HeadPeriod
from odpw.db.dbm import PostgressDBM
from odpw.reporting.info_reports import portalinfo
from odpw.reporting.portal_reports import report_portalbasics, report_portalAll, analyse_portalAll, aggregate_perSoftwareISO, \
    analyse_perOrganisation, analyse_organisations, aggregate_perISO, aggregate_allportals
import json
from pprint import pprint
from datetime import date
from datetime import datetime

from odpw.utils.timer import Timer
import os

from odpw.utils.util import getNextWeek, tofirstdayinisoweek, progressIterator
from odpw.reporting.reporters.portal_reporter import OrgaReport, PerOrgaReporter, PerOrgaReporter, PortalReporter, \
    PerSoftIsoReporter, PerSoftwareReporter, PerISOReporter, AllPortalReporter

import structlog
log =structlog.get_logger()

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    if isinstance(obj, date):
        serial = str(obj)
        return serial

    raise TypeError ("Type not serializable")

def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.
    """
    import unicodedata
    import re
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
    value = unicode(re.sub('[^\w\s-]', '', value).strip().lower())
    value= re.sub('[-\s]+', '-', value)
    value=value.replace(" ","_")
    return value

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def storeJSON(fName, reporter):
    with Timer(key="json.dump"):
        with open(fName+'.json', 'w') as outfile:
            json.dump(reporter.jsonreport(), outfile, default=json_serial)

    log.info("Write to file", file=fName+'.json', size=sizeof_fmt(os.stat(fName+'.json').st_size))
    with Timer(key="json.load"):
        json.load( open( fName+'.json', "r" ) )


def updateEvolution(fName, reporter):
    evolv={'snapshots':{}}
    fname = fName+'_evolution.json'

    with Timer(key="evolution_json.load"):
        if os.path.isfile(fname):
            evolv = json.load( open( fname, "r" ) )

    evolv['snapshots'].update(reporter.snapshotreport())

    if 'first' in evolv:
        del evolv['first']
    if 'last' in evolv:
        del evolv['last']

    min_sn= min(evolv['snapshots'].keys())
    max_sn= max(evolv['snapshots'].keys())
    evolv['first']={'sn':min_sn,'date':str(tofirstdayinisoweek(min_sn).date())}
    evolv['last']={'sn':max_sn,'date':str(tofirstdayinisoweek(max_sn).date())}

    with Timer(key="evolution_json.dump"):
        with open(fname, 'w') as outfile:
            json.dump(evolv, outfile, default=json_serial)
    log.info("Evolution to file", file=fname, size=sizeof_fmt(os.stat(fname).st_size))






def name():
    return 'FileStats'
def help():
    return "Generate various json reports"


def setupCLI(pa):
    pa.add_argument("-sn","--snapshot",  help='what snapshot is it', dest='snapshot')
    #pa.add_argument('-s','--software',choices=['CKAN', 'Socrata', 'OpenDataSoft'], dest='software')
    pa.add_argument('-b','--base',type=str, dest='base' , help="base directory")
    pa.add_argument('-u','--url',type=str, dest='url' , help="the CKAN API url")
    pa.add_argument('-f','--filter',type=str, dest='filter' , help="Filter by format (csv)", default='csv')
    pa.add_argument('--store',  action='store_true', default=False, help="store the files in the out directory")

def cli(args, dbm):

    if args.base is None:
        raise IOError("--out is not set")
    base=args.base
    log.info("Preparing base", base=base)
    base_dirs={
        'portal':os.path.join(base,'portal'),
        'software':os.path.join(base,'software'),
        'iso':os.path.join(base,'iso'),
    }
    for d in base_dirs.values():
        log.info("Creating base dir", basedir=d)
        mkdir_p(d)
        mkdir_p(os.path.join(d, 'evol'))

    if args.snapshot is None:
        raise IOError("--snapshot not set")

    portalIDs=[]
    if args.url:
        p = dbm.getPortal(url=args.url)
        if not p:
            raise IOError(args.url + ' not found in DB')
        portals = [p.id]

    snapshots=[]
    if args.snapshot:
        snapshots.append(args.snapshot)
    else:
        for sn in dbm.getSnapshotsFromPMD():#portalID='data_wu_ac_at'
            snapshots.append(sn[1])

    log.info("Snapshots", snapshots=snapshots)

    for sn in progressIterator(sorted(snapshots),len(snapshots), 1, label='PerSnapshot'):
        sn_dirs={ k:os.path.join(v,str(sn)) for k,v in base_dirs.items() }
        for d in sn_dirs: mkdir_p(d)

        portal_analysers=[]

        sn_portals=[]
        for portalID in dbm.getPortalIDs(snapshot=sn):
            sn_portals.append(portalID)

        for portalID in progressIterator(sn_portals,len(sn_portals), 5, label=sn):
            pID = portalID[0]

            if len(portalIDs)==0 or pID in portalIDs:
                P= dbm.getPortal(portalID=pID)

                pmd= dbm.getPortalMetaData(portalID=P.id, snapshot=sn)
                fp= FetchPeriod()
                fp.analyse_PortalMetaData(pmd)

                log.info("Analysing organisations", snapshot=sn, portal=pID)

                p_dir=os.path.join(sn_dirs['portal'], P.id)
                p_dir_evol=os.path.join( *[base_dirs['portal'],'evol', P.id])

                mkdir_p(p_dir)
                mkdir_p(p_dir_evol)

                #### ANALYSE per Organisation
                perOrga_analyser = analyse_perOrganisation(dbm, sn, P)#PerOrganisationAnalyser

                reporter= PerOrgaReporter(P, sn, perOrga_analyser)
                for org, reporter in reporter.orga_reporters().items():
                    org_slug= slugify(unicode(org))
                    o_file=os.path.join(p_dir, org_slug)
                    o_file_evol=os.path.join(p_dir_evol, org_slug)

                    reporter.addResults(fp)
                    storeJSON(o_file, reporter)

                    updateEvolution(o_file_evol, reporter)

                log.info("Analysing portal", snapshot=sn, portal=pID)
                portal_anaylser = analyse_organisations(P, sn, perOrga_analyser)
                portal_anaylser.addAnalyser(fp)

                reporter= PortalReporter(P, sn, portal_anaylser)


                storeJSON(p_dir, reporter)
                updateEvolution(p_dir_evol, reporter)

                portal_analysers.append(portal_anaylser)

                ##Evaluation analysis

        #end for
        log.info("Analysing software iso", snapshot=sn)
        result=aggregate_perSoftwareISO(portal_analysers)


        reporter= PerSoftIsoReporter( sn, result)
        for soft, iso, reporter in reporter.soft_iso_reporters():
            o_file=os.path.join(sn_dirs['software'], soft)
            mkdir_p(o_file)
            iso_file=os.path.join(o_file, iso)
            storeJSON(iso_file, reporter)

            o_file_evol=os.path.join( *[base_dirs['software'],'evol', soft])
            mkdir_p(o_file_evol)
            iso_file_evol=os.path.join(o_file_evol, iso)
            updateEvolution(iso_file_evol, reporter)

        reporter= PerSoftwareReporter( sn, result)
        for soft,  reporter in reporter.software_reporters():
            o_file=os.path.join(sn_dirs['software'], soft)
            mkdir_p(o_file)
            storeJSON(o_file, reporter)

            o_file_evol=os.path.join( *[base_dirs['software'],'evol', soft])
            mkdir_p(o_file_evol)

            updateEvolution(o_file_evol, reporter)

        log.info("Analysing iso", snapshot=sn)
        result=aggregate_perISO(portal_analysers)
        reporter= PerISOReporter( sn, result)
        for iso,  reporter in reporter.iso_reporters():
            o_file=os.path.join(sn_dirs['iso'], iso)
            mkdir_p(o_file)

            storeJSON(o_file, reporter)

            o_file_evol=os.path.join( *[base_dirs['iso'],'evol', iso])
            mkdir_p(o_file_evol)

            updateEvolution(o_file_evol, reporter)

        log.info("Analysing portals", snapshot=sn)
        result=aggregate_allportals(portal_analysers)
        reporter= AllPortalReporter( sn, result)

        o_file=os.path.join(sn_dirs['portal'], 'all')
        mkdir_p(o_file)

        storeJSON(o_file, reporter)

        o_file_evol=os.path.join( *[base_dirs['portal'],'evol', 'all'])
        mkdir_p(o_file_evol)

        updateEvolution(o_file_evol, reporter)

        Timer.printStats()

    Timer.printStats()
    import sys
    sys.exit(0)


