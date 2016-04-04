from collections import OrderedDict
import json
import datetime
import traceback
import numpy as np
from odpw.util import defaultdict
import os
from freshness.estimators import NaiveLastModified, ImprovedLastModified, AgeSamplingEmpiricalDistribution, \
    IntuitiveFrequency, ImprovedFrequency, MarkovChain, ComparisonSamplingEmpiricalDistribution
import dateutil.parser
from isoweek import Week
from freshness.obd16 import delta_to_days
from odpw.analysers.core import HistogramAnalyser
from odpw.reporting.reporters import Report
from odpw.reporting.reporters.plot_reporter import MultiHistogramReporter

__author__ = 'sebastian'

def comp_freshness(data):
    c1 = IntuitiveFrequency()
    c2 = ImprovedFrequency()
    mar = MarkovChain(history=1)
    emp = ComparisonSamplingEmpiricalDistribution()
    estimators = [c1, c2, mar, emp]

    prev_sn = None
    prev_t = 0

    y = int('20'+str(data['snapshots'][0])[:2])
    w = int(str(data['snapshots'][0])[2:])
    ACC = Week(y, w).monday()

    interval = datetime.timedelta(days=7)
    I = delta_to_days(interval)
    for e in estimators:
        e.setInterval(I)

    for sn, ct in zip(data['snapshots'], data['value']):
        try:
            t = dateutil.parser.parse(ct).date()
        except Exception as e:
            t = datetime.datetime.fromtimestamp(ct).date()
        change = 1 if t != prev_t else 0


        # Ti is the time to the previous change in the ith access
        Ti = delta_to_days(ACC - t)

        for e in estimators:
            e.update(change)
        prev_t = t
        # set access time to next interval
        ACC += interval

    current = delta_to_days(Week(2016, 9).monday() - t)
    return {'c_cho_naive': c1.cdf_poisson(current), 'c_cho_impr': c2.cdf_poisson(current), 'c_emp_dist': emp.cdf(current)}


def age_freshness(data):

    a1 = NaiveLastModified()
    a2 = ImprovedLastModified()
    emp = AgeSamplingEmpiricalDistribution()
    estimators = [a1, a2, emp]

    prev_sn = None

    y = int('20'+str(data['snapshots'][0])[:2])
    w = int(str(data['snapshots'][0])[2:])
    ACC = Week(y, w).monday()

    for sn, ct in zip(data['snapshots'], data['value']):
        if prev_sn and not (str(sn).startswith('16') and str(prev_sn).startswith('15')):
            interval = datetime.timedelta(days=(sn - prev_sn)*7)
        else:
            interval = datetime.timedelta(days=7)
        try:
            t = dateutil.parser.parse(ct).date()
        except Exception as e:
            t = datetime.datetime.fromtimestamp(ct).date()
        I = delta_to_days(interval)

        # Ti is the time to the previous change in the ith access
        Ti = delta_to_days(ACC - t)

        for e in estimators:
            e.update(Ti, I, t)
        prev_sn = sn
        # set access time to next interval
        ACC += interval

    current = delta_to_days(Week(2016, 14).monday() - t)
    return {'a_cho_naive': 1-a1.cdf_poisson(current), 'a_cho_impr': 1-a2.cdf_poisson(current), 'a_emp_dist': 1-emp.cdf(current), 'snapshots': len(data['snapshots']), 'changes': len(set(data['value']))}


def portal_freshness(urls):
    count = 0.0

    a_cho_naive = 0.0
    a_cho_impr = 0.0
    a_emp_dist = 0.0

    c_cho_naive = 0.0
    c_cho_impr = 0.0
    c_emp_dist = 0.0
    c_markov = 0.0

    avg_changes = 0.0
    snapshots = 0.0

    for url in urls:
        try:
            res = age_freshness(urls[url])
            if res['a_cho_naive'] >= 0 and res['a_cho_impr'] >= 0 and res['a_emp_dist'] >= 0:
                a_cho_naive += res['a_cho_naive']
                a_cho_impr += res['a_cho_impr']
                a_emp_dist += res['a_emp_dist']
                avg_changes += res['changes']
                snapshots += res['snapshots']
                count += 1
        except Exception as e:
            if not isinstance(e, ZeroDivisionError):
                traceback.print_exc()
    print 'snapshots: ', str(snapshots/count)

    return {'a_cho_naive': a_cho_naive/count,
            'a_cho_impr': a_cho_impr/count,
            'a_emp_dist': a_emp_dist/count,
            'datasets': count/len(urls),
            'changes': avg_changes/count,
            'snapshots': snapshots/count}


def comp_avg_acc(data, train=10, eval=5, percent=0.9):
    labels = ['c_cho_naive', 'c_cho_impr', 'c_emp_dist', 'c_umb_markov']
    c1 = IntuitiveFrequency()
    c2 = ImprovedFrequency()
    mar = MarkovChain(history=1)
    emp = ComparisonSamplingEmpiricalDistribution()
    estimators = {'c_cho_naive': c1, 'c_cho_impr': c2, 'c_emp_dist': emp, 'c_umb_markov': mar}

    results = defaultdict(list)
    diff = defaultdict(list)
    errors = defaultdict(int)

    prev_sn = None
    prev_t = 0
    count = 0

    y = int('20'+str(data['snapshots'][0])[:2])
    w = int(str(data['snapshots'][0])[2:])
    ACC = Week(y, w).monday()

    interval = datetime.timedelta(days=7)
    I = delta_to_days(interval)
    for e in estimators:
        estimators[e].setInterval(I)

    for sn, ct in zip(data['snapshots'], data['value']):
        try:
            t = dateutil.parser.parse(ct).date()
        except Exception as e:
            t = datetime.datetime.fromtimestamp(ct).date()
        change = 1 if t != prev_t else 0

        # Ti is the time to the previous change in the ith access
        Ti = delta_to_days(ACC - t)

        if prev_t != t:
            count += 1
            if train < count <= train + eval:
                for e in labels:
                    try:
                        next = estimators[e].ppf(percent)
                        d = datetime.timedelta(days=next)
                        res = prev_t + d

                        # tf values
                        results[e].append(int(res >= t))
                        diff[e].append(delta_to_days(res - t))
                    except Exception as e:
                        errors[str(e)] += 1

        # update new values
        for e in estimators:
            estimators[e].update(change)

        prev_sn = sn
        prev_t = t
        # set access time to next interval
        ACC += interval

    return results, diff


def parse_date(ct):
    try:
        t = dateutil.parser.parse(ct).date()
    except Exception as e:
        t = datetime.datetime.fromtimestamp(ct).date()
    return t

def age_avg_acc(data, train=10, eval=5, percent=0.9):

    labels = ['a_cho_naive', 'a_cho_impr', 'a_emp_dist']
    a1 = NaiveLastModified()
    a2 = ImprovedLastModified()
    emp = AgeSamplingEmpiricalDistribution()
    estimators = {'a_cho_naive': a1, 'a_cho_impr': a2, 'a_emp_dist': emp}

    prev_sn = None
    prev_t = None
    results = defaultdict(list)
    diff = defaultdict(list)

    y = int('20'+str(data['snapshots'][0])[:2])
    w = int(str(data['snapshots'][0])[2:])
    if len(set(data['value'])) < train + eval:
        raise Exception('not enough values')

    # set to wednesday due to some delayed fetch starts
    ACC = Week(y, w).thursday()

    count = 0
    for sn, ct in zip(data['snapshots'], data['value']):
        if prev_sn and not (str(sn).startswith('16') and str(prev_sn).startswith('15')):
            interval = datetime.timedelta(days=(sn - prev_sn)*7)
        else:
            interval = datetime.timedelta(days=7)
        t = parse_date(ct)
        I = delta_to_days(interval)


        # Ti is the time to the previous change in the ith access
        Ti = delta_to_days(ACC - t)
        if Ti <= 0:
            #ACC += interval
            #prev_sn = sn
            #prev_t = t
            #continue
            raise Exception('modification date is in future')

        if prev_t != t:
            count += 1
            if train < count <= train + eval:
                for e in labels:
                    try:
                        next = estimators[e].ppf(percent)
                        d = datetime.timedelta(days=next)
                        res = prev_t + d

                        # tf values
                        results[e].append(int(res >= t))
                        diff[e].append(delta_to_days(res - t))
                    except Exception as e:
                        pass

        # update new values
        for e in estimators:
            estimators[e].update(Ti, I, t)

        prev_sn = sn
        prev_t = t
        # set access time to next interval
        ACC += interval

    return results, diff

def portals_freshness_score():
    col = ['#7fc97f', '#fdc086', '#386cb0', '#beaed4', '#ffff99']
    estimators = ['a_cho_naive', 'a_cho_impr', 'a_emp_dist']
    key_labels = {'a_cho_naive': '$A_{ChoNaive}$', 'a_cho_impr': '$A_{ChoImpr}$', 'a_emp_dist': '$A_{EmpDist}$'}

    portals_count = 0
    avg_ds = 0.0
    avg_changes = 0.0
    avg_snapshots = 0.0
    values = {}
    for e in estimators:
        values[e] = HistogramAnalyser()
    path = '/home/sebastian/Repositories/ODPortalWatch_2/freshness/socrata/'
    for i in os.listdir(path):
        if i.endswith(".json"):
            with open(os.path.join(path,i)) as f:
                print i
                urls = json.load(f)
                if len(urls) > 0:
                    try:
                        res = portal_freshness(urls)
                        for e in estimators:
                            values[e].analyse_generic(res[e])
                        portals_count += 1
                        avg_ds += res['datasets']
                        avg_changes += res['changes']
                        avg_snapshots += res['snapshots']

                    except Exception as e:
                        print e

    bins = np.arange(0.0, 1.1, 0.1)
    xlabel = "Freshness"
    ylabel = "Portals"
    filename = "hist.pdf"

    data = OrderedDict()
    for e in estimators:
        data[e] = values[e].getResult()
    rep = MultiHistogramReporter(data, labels=key_labels, xlabel=xlabel, ylabel=ylabel, filename=filename, colors=col)
    re = Report([rep])
    re.plotreport('/home/sebastian/Repositories/ODPortalWatch_2/freshness/')
    print 'portals:', portals_count
    print 'avg_ds:', avg_ds/portals_count
    print 'avg_changes:', avg_changes/portals_count
    print 'avg_snapshots:', avg_snapshots/portals_count

#    for url in urls:
#        try:
#            res = comp_freshness(urls[url])
#            print res
#            if res['c_cho_naive'] >= 0 and res['c_cho_impr'] >= 0 and res['c_emp_dist'] >= 0 and res['c_markov'] >= 0:
#                c_cho_naive += res['c_cho_naive']
#                c_cho_impr += res['c_cho_impr']
#                c_emp_dist += res['c_emp_dist']
#                c_markov += res['c_markov']
#
#                count += 1
#        except Exception as e:
#            print e
#
#    print 'c_cho_naive', c_cho_naive/count
#    print 'c_cho_impr', c_cho_impr/count
#    print 'c_emp_dist', c_emp_dist/count
#    print 'c_markov', c_markov/count

if __name__ == '__main__':
    avg_res = defaultdict(list)
    avg_diff = defaultdict(list)
    datasets = 0
    total = 0
    errors = defaultdict(int)
    counts = defaultdict(int)

    path = '/home/sebastian/Repositories/ODPortalWatch_2/freshness/socrata/'
    for i in os.listdir(path):
        if i.endswith(".json"):
            with open(os.path.join(path, i)) as f:
                print i
                urls = json.load(f)
                if len(urls) > 0:
                    for url in urls:
                        total += 1
                        try:
                            res, diff = comp_avg_acc(urls[url])
                            for e in res:
                                avg_res[e].append(sum(res[e])/float(len(res[e])))
                                avg_diff[e].append(sum(diff[e])/float(len(diff[e])))
                                counts[e] += 1
                                datasets += 1
                        except Exception as e:
                            errors[str(e)] += 1

    print 'datasets', str(datasets) + '/' + str(total)
    for e in avg_res:
        print e
        print sum(avg_res[e])/float(len(avg_res[e]))
        print 'diff', sum(avg_diff[e])/float(len(avg_diff[e]))
    print 'counts',  str(dict(counts))
    print 'errors', str(dict(errors))
