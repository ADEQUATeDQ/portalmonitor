import datetime
from freshness.page import *
import matplotlib.pyplot as plt


__author__ = 'sebastian'


def delta_to_days(delta):
    return delta.total_seconds()/86400


def comparison_eval(page, interval, initial_skip, percent):

    c1 = IntuitiveFrequency()
    c2 = ImprovedFrequency()
    mar = MarkovChain(history=1)
    emp = ComparisonSamplingEmpiricalDistribution()
    estimators = [c1, c2, mar, emp]

    # initiate estimators
    I = delta_to_days(interval)
    for e in estimators:
        e.setInterval(I)

    p_samples = []
    p_values = defaultdict(list)
    prev_change_p = 0
    prev_t = 0

    tf_samples = []
    tf_values = defaultdict(list)
    initial_skip_counter = 0
    for Xi, exact_t, sampling_p in page.iterComparisonSampling(interval):

        if initial_skip <= initial_skip_counter:
            # enter here only after initial skip
            # delta to intervals
            real_delta = delta_to_days(sampling_p - prev_change_p)
            real_i = math.ceil(real_delta/I)

            # comparison sampling
            mar_cdf = mar.cumm_percent(real_i)
            mar_next, mar_p = mar.estimated_next_change(percent)
            emp_cdf = emp.cdf(real_i)
            c1_cdf = c1.cdf_poisson(real_delta)
            c2_cdf = c2.cdf_poisson(real_delta)

            if exact_t != prev_t:
                # estiamtions
                next1 = c1.ppf_poisson(percent)
                next2 = c2.ppf_poisson(percent)
                d1 = datetime.timedelta(days=next1)
                d2 = datetime.timedelta(days=next2)
                res1 = prev_t + d1
                res2 = prev_t + d2

                mar_d = mar_next * interval
                mar_res = prev_t + mar_d

                emp_next = emp.ppf(percent)
                emp_next = datetime.timedelta(days=emp_next)
                emp_res = prev_t + emp_next

                # tf values
                tf_values['c_emp_dist'].append(int(emp_res > exact_t))
                tf_values['c_cho_naive'].append(int(res1 > exact_t))
                tf_values['c_cho_impr'].append(int(res2 > exact_t))
                tf_values['c_umb_markov'].append(int(mar_res > exact_t))
                tf_samples.append(sampling_p)

            # store values
            p_values['c_emp_dist'].append(emp_cdf)
            p_values['c_cho_naive'].append(c1_cdf)
            p_values['c_cho_impr'].append(c2_cdf)
            p_values['c_umb_markov'].append(mar_cdf)
            p_samples.append(sampling_p)

        # standard update of ustimators
        for e in estimators:
            e.update(Xi)
        if Xi:
            prev_change_p = sampling_p
        prev_t = exact_t
        initial_skip_counter += 1

    return {'p': p_values, 'tf': tf_values}
    #plot_p_values(samples, p_values)
    # avg tf values
    #vals = tf_values['c_umb_markov']
    #plot_tf_values(tf_samples, vals)

def age_eval(page, interval, initial_skip, percent):
    I = interval.total_seconds()

    a1 = NaiveLastModified()
    a2 = ImprovedLastModified()
    emp = AgeSamplingEmpiricalDistribution()
    exp = ExponentialSmoothing(0.3)
    estimators = [a1, a2, exp, emp]

    tf_samples = []
    tf_values = defaultdict(list)
    initial_skip_counter = 0

    # the access time
    ACC = page.startTime()
    tmp = datetime.timedelta()
    for exact_t, sample_p in page.iterAgeSampling(interval):

        if initial_skip <= initial_skip_counter:
            # enter here only after initial skip
            # delta to intervals
            #real_delta = delta_to_days(exact_t - prev_t)

            # comparison sampling
            #c1_cdf = a1.cdf_poisson(real_delta)
            #c2_cdf = a2.cdf_poisson(real_delta)

            if exact_t != prev_t:
                # estiamtions
                next1 = a1.ppf_poisson(percent)
                next2 = a2.ppf_poisson(percent)
                d1 = datetime.timedelta(seconds=next1)
                d2 = datetime.timedelta(seconds=next2)
                res1 = prev_t + d1
                res2 = prev_t + d2

                emp_next = emp.ppf(percent)
                emp_next = datetime.timedelta(seconds=emp_next)
                emp_res = prev_t + emp_next

                # tf values
                tf_values['a_emp_dist'].append(int(emp_res > exact_t))
                tf_values['a_cho_naive'].append(int(res1 > exact_t))
                tf_values['a_cho_impr'].append(int(res2 > exact_t))
                tf_samples.append(sample_p)

        # Ti is the time to the previous change in the ith access
        Ti = (ACC - exact_t).total_seconds()
        for e in estimators:
            e.update(Ti, I, exact_t)
        # set access time to next interval
        ACC += interval
        prev_t = exact_t
        initial_skip_counter += 1

    return {'tf': tf_values}


def load_pages(dir):
    pages = []
    for (dirpath, dirnames, filenames) in walk(dir):
        for fname in filenames:
            pages.append(Page(join(dirpath,fname)))
    return pages

def filter_updated(pages):
    # update deltas all between range
    min_delta = datetime.timedelta(days=3).total_seconds()
    max_delta = datetime.timedelta(days=250).total_seconds()

    avg_delta_max = datetime.timedelta(days=60).total_seconds()
    avg_delta_min = datetime.timedelta(days=20).total_seconds()
    # min available snapshots
    min_length = 30
    res_set = []

    for p in pages:
        if p.length > min_length and avg_delta_min < p.frequency < avg_delta_max and p.max_delta < max_delta:
        #if p.length > min_length and p.max_delta - p.min_delta > max_delta:
            res_set.append(p)
    return res_set

if __name__ == '__main__':
    # filter wiki articles
    # number of revisions
    # min, max -> delta
    # max interval, min interval
    # delta/num revisions
    pages = load_pages('revs')

    reg = filter_updated(pages)
    print 'total articles', len(reg)

    res = {}
    i = datetime.timedelta(days=30)
    d = 10
    for p in reg:
        #try:
        res[p.filename] = age_eval(p, i, d, 0.8)
        #except Exception as e:
        #    print e

    with open('age_regular.json', 'w') as f:
        json.dump(res, f)

    #fname = 'revs/politicians/Luiz_In%C3%A1cio_Lula_da_Silva'
    #p = Page(fname)



    #age_eval(p, i, d, 0.8)


