import datetime
from freshness.page import *
import matplotlib.pyplot as plt


__author__ = 'sebastian'


def plot_tf_values(samples, values):
    avg_vals = []
    avg_v = 0
    i = 0.0
    for v in values:
        i += 1
        avg_v += v
        avg_vals.append(avg_v/i)

    plt.plot(samples, values, 'ro')
    plt.plot(samples, avg_vals, 'b')
    plt.show()


def plot_p_values(samples, values):
    plt.plot(samples, values['c_emp_dist'], 'r')
    plt.plot(samples, values['c_cho_naive'], 'b')
    plt.plot(samples, values['c_cho_impr'], 'g')
    plt.plot(samples, values['c_umb_markov'], 'y')
    plt.show()

def delta_to_days(delta):
    return delta.total_seconds()/86400


def comparison_eval(page, interval, inital_skip, percent):

    c1 = IntuitiveFrequency()
    c2 = ImprovedFrequency()
    mar = MarkovChain(history=1)
    emp = ComparisonSamplingEmpiricalDistribution()
    estimators = [c1, c2, mar, emp]

    # initiate estimators
    I = delta_to_days(interval)
    for e in estimators:
        e.setInterval(I)

    samples = []
    p_values = defaultdict(list)
    prev_change_p = 0
    prev_t = 0

    tf_samples = []
    tf_values = defaultdict(list)

    for Xi, exact_t, sampling_p in page.iterComparisonSampling(interval):

        if (sampling_p - page.startTime()) >= inital_skip:
            # enter here only after initial skip
            samples.append(sampling_p)
            real_delta = delta_to_days(sampling_p - prev_change_p)
            p_values['c_cho_naive'].append(c1.cdf_poisson(real_delta))
            p_values['c_cho_impr'].append(c2.cdf_poisson(real_delta))
            # delta to intervals
            real_i = math.ceil(real_delta/I)
            p_values['c_emp_dist'].append(emp.cdf(real_i))
            p_values['c_umb_markov'].append(mar.cumm_percent(real_i))

            if exact_t != prev_t:
                tf_samples.append(sampling_p)
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
                tf_values['c_cho_naive'].append(int(res1 > exact_t))
                print 'delta2', d2
                res2 = prev_t + d2
                print 'next2', res2
                print 'diff2', res2 - exact_t
                tf_values['c_cho_impr'].append(int(res2 > exact_t))
                mar_d = mar_next * interval
                print 'mar_delta', mar_d
                mar_res = prev_t + mar_d
                print 'mar', mar_res
                print 'mar_diff', mar_res - exact_t
                tf_values['c_umb_markov'].append(int(mar_res > exact_t))
                #
                print 'emp', emp_next
                emp_res = prev_t + emp_next
                print emp_res
                print 'emp_diff', emp_res - exact_t
                tf_values['c_emp_dist'].append(int(emp_res > exact_t))
                print

        # standard update of ustimators
        for e in estimators:
            e.update(Xi)
        if Xi:
            prev_change_p = sampling_p
        prev_t = exact_t


    plot_p_values(samples, p_values)
    # avg tf values
    vals = tf_values['c_umb_markov']
    plot_tf_values(tf_samples, vals)

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

def weekly_updated(pages):
    # update deltas all between range
    #min_delta = datetime.timedelta(days=3).total_seconds()
    max_delta = datetime.timedelta(days=40).total_seconds()

    avg_delta_max = datetime.timedelta(days=8).total_seconds()
    avg_delta_min = datetime.timedelta(days=4).total_seconds()
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
    #pages = load_pages('revs')

    #reg = weekly_updated(pages)
    #print len(reg)

    fname = 'revs/politicians/Luiz_In%C3%A1cio_Lula_da_Silva'
    p = Page(fname)

    i = datetime.timedelta(days=1)
    d = datetime.timedelta(days=900)
    comparison_eval(p, i, d, 0.9)

    print len(p.rev_hist)
    #age_eval(p, i, d, 0.8)


