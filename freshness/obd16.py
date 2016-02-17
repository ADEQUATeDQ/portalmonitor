import datetime
from freshness.page import *
import matplotlib.pyplot as plt


__author__ = 'sebastian'


def plot_p_values(samples, p_values):

    plt.plot(samples, p_values[0], 'r')
    plt.plot(samples, p_values[1], 'b')

def delta_to_days(delta):
    return delta.total_seconds()/86400


def comparison_eval(page, interval, inital_skip, percent):

    c1 = IntuitiveFrequency()
    c2 = ImprovedFrequency()
    mar = MarkovChain(history=3)
    emp = ComparisonSamplingEmpiricalDistribution()
    estimators = [c1, c2, mar, emp]

    # initiate estimators
    I = delta_to_days(interval)
    for e in estimators:
        e.setInterval(I)

    samples = []
    p1_values = []
    p2_values = []
    prev_change_p = 0
    prev_t = 0

    for Xi, exact_t, sampling_p in page.iterComparisonSampling(interval):

        if (sampling_p - page.startTime()) >= inital_skip:
            # enter here only after initial skip
            samples.append(sampling_p)
            real_delta = delta_to_days(sampling_p - prev_change_p)
            p1_values.append(c1.cdf_poisson(real_delta))
            # delta to intervals
            real_i = math.ceil(real_delta/I)
            p2_values.append(mar.cumm_percent(real_i))

            if exact_t != prev_t:
                # estimations
                next1 = c1.ppf_poisson(percent)
                next2 = c2.ppf_poisson(percent)
                mar_next, mar_p = mar.estimated_next_change(percent)
                emp_next = emp.ppf(percent)

                d1 = datetime.timedelta(days=next1)
                d2 = datetime.timedelta(days=next2)
                emp_next = datetime.timedelta(days=emp_next)
                print 'next_orig', exact_t
                print 'delta1', d1
                res1 = prev_t + d1
                print 'next1', res1
                print 'diff1', res1 - exact_t
                print 'delta2', d2
                res2 = prev_t + d2
                print 'next2', res2
                print 'diff2', res2 - exact_t
                mar_d = mar_next * interval
                print 'mar_delta', mar_d
                mar_res = prev_t + mar_d
                print 'mar', mar_res
                print 'mar_diff', mar_res - exact_t
                #
                print 'emp', emp_next
                emp_res = prev_t + emp_next
                print emp_res
                print

        # standard update of ustimators
        for e in estimators:
            e.update(Xi)
        if Xi:
            prev_change_p = sampling_p
        prev_t = exact_t


    plot_p_values(samples, [p1_values, p2_values])

def age_eval(page, interval, duration, percent):
    I = interval.total_seconds()

    a1 = NaiveLastModified()
    a2 = ImprovedLastModified()
    exp = ExponentialSmoothing(0.3)
    estimators = [a1, a2, exp]

    # perform first steps
    # the access time
    ACC = page.startTime()
    tmp = datetime.timedelta()
    for t, sample_p in page.iterAgeSampling(interval):
        tmp += interval
        if tmp >= duration:
            break
        # Ti is the time to the previous change in the ith access
        Ti = (ACC - t).total_seconds()
        for e in estimators:
            e.update(Ti, I, t)
        prev_t = t
        # set access time to next interval
        ACC += interval


def load_pages(dir):
    pages = []
    for (dirpath, dirnames, filenames) in walk(dir):
        for fname in filenames:
            pages.append(Page(join(dirpath,fname)))
    return pages

def monthly_updated(pages):
    # update deltas all between range
    #min_delta = datetime.timedelta(days=3).total_seconds()
    max_delta = datetime.timedelta(days=80).total_seconds()

    avg_delta_max = datetime.timedelta(days=20).total_seconds()
    avg_delta_min = datetime.timedelta(days=40).total_seconds()
    # min available snapshots
    min_length = 10
    res_set = []

    for p in pages:
        if p.length > min_length and p.max_delta < max_delta and avg_delta_min < p.frequency < avg_delta_max:
            res_set.append(p)
    return res_set

if __name__ == '__main__':
    # filter wiki articles
    # number of revisions
    # min, max -> delta
    # max interval, min interval
    # delta/num revisions
    pages = load_pages('revs')

    reg = weekly_updated(pages)
    print len(reg)


    i = datetime.timedelta(days=10)
    d = datetime.timedelta(days=1000)
    comparison_eval(reg[0], i, d, 0.8)

    #age_eval(p, i, d, 0.8)


