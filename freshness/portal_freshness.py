from collections import OrderedDict
import json
import datetime
import numpy as np
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
        if prev_sn:
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

    current = delta_to_days(Week(2016, 9).monday() - t)
    return {'a_cho_naive': 1-a1.cdf_poisson(current), 'a_cho_impr': 1-a2.cdf_poisson(current), 'a_emp_dist': 1-emp.cdf(current)}


def portal_freshness(urls):
    count = 0.0

    a_cho_naive = 0.0
    a_cho_impr = 0.0
    a_emp_dist = 0.0

    c_cho_naive = 0.0
    c_cho_impr = 0.0
    c_emp_dist = 0.0
    c_markov = 0.0
    for url in urls:
        try:
            res = age_freshness(urls[url])
            if res['a_cho_naive'] >= 0 and res['a_cho_impr'] >= 0 and res['a_emp_dist'] >= 0:
                a_cho_naive += res['a_cho_naive']
                a_cho_impr += res['a_cho_impr']
                a_emp_dist += res['a_emp_dist']

                count += 1
        except Exception as e:
            pass

    return {'a_cho_naive': a_cho_naive/count,
            'a_cho_impr': a_cho_impr/count,
            'a_emp_dist': a_emp_dist/count,
            'datasets': count/len(urls)}


if __name__ == '__main__':
    col = ['#7fc97f', '#fdc086', '#386cb0', '#beaed4', '#ffff99']
    estimators = ['a_cho_naive', 'a_cho_impr', 'a_emp_dist']
    key_labels = {'a_cho_naive': '$A_{ChoNaive}$', 'a_cho_impr': '$A_{ChoImpr}$', 'a_emp_dist': '$A_{EmpDist}$'}

    portals_count = 0
    avg_ds = 0.0
    values = {}
    for e in estimators:
        values[e] = HistogramAnalyser()
    path = '/home/sebastian/Repositories/ODPortalWatch_2/freshness/socrata/'
    for i in os.listdir(path):
        if i.endswith(".json"):
            with open(os.path.join(path,i)) as f:
                urls = json.load(f)
                if len(urls) > 0:
                    try:
                        res = portal_freshness(urls)
                        for e in estimators:
                            values[e].analyse_generic(res[e])
                        portals_count += 1
                        avg_ds += res['datasets']
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



