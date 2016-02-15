import datetime
from freshness.page import *

__author__ = 'sebastian'


def comparison_eval(page, interval, duration, percent):

    c1 = IntuitiveFrequency()
    c2 = ImprovedFrequency()
    mar = MarkovChain(history=3)
    emp = ComparisonSamplingEmpiricalDistribution()
    estimators = [c1, c2, mar, emp]

    # initiate estimators
    I = interval.total_seconds()
    for e in estimators:
        e.setInterval(I)

    # perform first steps
    tmp = datetime.timedelta()
    for Xi, t in page.iterComparisonSampling(interval):
        tmp += interval
        if tmp >= duration:
            break
        for e in estimators:
            e.update(Xi)
        prev_t = t

    # evaluation
    for Xi, t in page.iterComparisonSampling(interval):
        if t != prev_t:
            # estimations
            next1 = c1.ppf_poisson(percent)
            next2 = c2.ppf_poisson(percent)
            mar_next, mar_p = mar.estimated_next_change(percent)
            emp_next = emp.ppf(percent)

            d1 = datetime.timedelta(seconds=next1)
            d2 = datetime.timedelta(seconds=next2)
            emp_next = datetime.timedelta(seconds=emp_next)
            print 'next_orig', t
            print 'delta1', d1
            res1 = prev_t + d1
            print 'next1', res1
            print 'diff1', res1 - t
            print 'delta2', d2
            res2 = prev_t + d2
            print 'next2', res2
            print 'diff2', res2 - t
            mar_d = mar_next * interval
            print 'mar_delta', mar_d
            mar_res = prev_t + mar_d
            print 'mar', mar_res
            print 'mar_diff', mar_res - t
            #
            print 'emp', emp_next
            emp_res = prev_t + emp_next
            print emp_res
            print
        prev_t = t

        # update with new interval
        for e in estimators:
            e.update(Xi)




if __name__ == '__main__':
    # filter wiki articles
    # number of revisions
    # min, max -> delta
    # max interval, min interval
    # delta/num revisions
    p = Page("revs/sportleagues/China_League_One")
    i = datetime.timedelta(days=10)
    d = datetime.timedelta(days=1000)
    comparison_eval(p, i, d, 0.8)
