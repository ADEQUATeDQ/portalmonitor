import datetime
from freshness.page import *
import matplotlib.pyplot as plt


__author__ = 'sebastian'


def delta_to_days(delta):
    return delta.total_seconds()/86400


def comparison_eval(page, interval, initial_skip, percent, markov_hist=1):

    c1 = IntuitiveFrequency()
    c2 = ImprovedFrequency()
    mar = MarkovChain(history=markov_hist)
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
    tf_diff = defaultdict(list)
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
                tf_values['c_emp_dist'].append(int(emp_res >= exact_t))
                tf_values['c_cho_naive'].append(int(res1 >= exact_t))
                tf_values['c_cho_impr'].append(int(res2 >= exact_t))
                tf_values['c_umb_markov'].append(int(mar_res >= exact_t))
                tf_samples.append(sampling_p)

                tf_diff['c_emp_dist'].append(delta_to_days(emp_res - exact_t))
                tf_diff['c_cho_naive'].append(delta_to_days(res1 - exact_t))
                tf_diff['c_cho_impr'].append(delta_to_days(res2 - exact_t))
                tf_diff['c_umb_markov'].append(delta_to_days(mar_res - exact_t))

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

    return {'p': p_values, 'tf': tf_values, 'tf_diff': tf_diff}
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
    tf_diff = defaultdict(list)
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
                tf_values['a_emp_dist'].append(int(emp_res >= exact_t))
                tf_values['a_cho_naive'].append(int(res1 >= exact_t))
                tf_values['a_cho_impr'].append(int(res2 >= exact_t))
                tf_samples.append(sample_p)

                tf_diff['a_emp_dist'].append(delta_to_days(emp_res - exact_t))
                tf_diff['a_cho_naive'].append(delta_to_days(res1 - exact_t))
                tf_diff['a_cho_impr'].append(delta_to_days(res2 - exact_t))

        # Ti is the time to the previous change in the ith access
        Ti = (ACC - exact_t).total_seconds()
        for e in estimators:
            e.update(Ti, I, exact_t)
        # set access time to next interval
        ACC += interval
        prev_t = exact_t
        initial_skip_counter += 1

    return {'tf': tf_values, 'tf_diff': tf_diff}


def push_eval(page, initial_skip, percent):
    a1 = NaiveLastModified()
    #a2 = ImprovedLastModified()
    emp = AgeSamplingEmpiricalDistribution()
    estimators = [a1, emp]

    tf_values = defaultdict(list)
    tf_diff = defaultdict(list)
    initial_skip_counter = 0

    # the access time
    prev_t = None
    for exact_t in page.iterExact():

        if initial_skip <= initial_skip_counter:
            if exact_t != prev_t:
                # estiamtions
                next1 = a1.ppf_poisson(percent)
                #next2 = a2.ppf_poisson(percent)
                d1 = datetime.timedelta(seconds=next1)
                #d2 = datetime.timedelta(seconds=next2)
                res1 = prev_t + d1
                #res2 = prev_t + d2

                emp_next = emp.ppf(percent)
                emp_next = datetime.timedelta(seconds=emp_next)
                emp_res = prev_t + emp_next

                # tf values
                tf_values['p_emp_dist'].append(int(emp_res >= exact_t))
                tf_values['p_cho_naive'].append(int(res1 >= exact_t))
                #tf_values['a_cho_impr'].append(int(res2 > exact_t))

                tf_diff['p_emp_dist'].append(delta_to_days(emp_res - exact_t))
                tf_diff['p_cho_naive'].append(delta_to_days(res1 - exact_t))
                #tf_diff['a_cho_impr'].append(delta_to_days(res2 - exact_t))

        if prev_t:
            # Ti is the time to the previous change in the ith access
            Ti = (exact_t - prev_t).total_seconds()
            for e in estimators:
                # set interval to Ti+1 to indicate a change in every interval
                e.update(Ti, Ti+1, exact_t)
            # set access time to next interval
            if exact_t != prev_t:
                initial_skip_counter += 1
        prev_t = exact_t

    return {'tf': tf_values, 'tf_diff': tf_diff}


def load_pages(dir):
    pages = []
    for (dirpath, dirnames, filenames) in walk(dir):
        for fname in filenames:
            pages.append(Page(join(dirpath,fname)))
    return pages

MIN_LENGTH = 30
def filter_regular(pages):
    # update deltas all between range
    delta = datetime.timedelta(days=250).total_seconds()

    avg_delta_max = datetime.timedelta(days=60).total_seconds()
    avg_delta_min = datetime.timedelta(days=20).total_seconds()
    res_set = []
    avg_d = []

    for p in pages:
        if p.length > MIN_LENGTH and avg_delta_min < p.frequency < avg_delta_max and (p.max_delta - p.min_delta) < delta:
            res_set.append(p)
            avg_d.append(p.frequency)

    print 'filtered articles', len(res_set)
    d = datetime.timedelta(seconds=sum(avg_d)/float(len(avg_d)))
    print 'avg delta', d
    return res_set


def filter_irregular(pages):
    delta = datetime.timedelta(days=500).total_seconds()

    # min available snapshots
    res_set = []
    avg_d = []

    for p in pages:
        if p.length > MIN_LENGTH and (p.max_delta - p.min_delta) > delta:
            res_set.append(p)
            avg_d.append(p.frequency)

    print 'filtered articles', len(res_set)
    d = datetime.timedelta(seconds=sum(avg_d)/float(len(avg_d)))
    print 'avg delta', d
    return res_set

def no_filter(pages):
    res_set = []
    avg_d = []

    for p in pages:
        if p.length > MIN_LENGTH:
            res_set.append(p)
            avg_d.append(p.frequency)

    print 'filtered articles', len(res_set)
    d = datetime.timedelta(seconds=sum(avg_d)/float(len(avg_d)))
    print 'avg delta', d
    return res_set



def push_table(pages, samples=20):
    estimators = ['p_emp_dist', 'p_cho_naive']
    res = [['p', 'documents'] + estimators + [e + '_delta' for e in estimators]]

    for p in [0.7, 0.8, 0.9]:
        tf_values = {}
        print p
        for page in pages:
            try:
                tmp = push_eval(page, 20, p)
                if len(tmp['tf']['p_emp_dist']) >= samples:
                    tf_values[page.name] = tmp
            except Exception as e:
                print e

        if len(tf_values) <= 0:
            res.append([str(p), '0'] + ['' for e in estimators] + ['' for e in estimators])
            continue

        avg_p = defaultdict(list)
        avg_delta = defaultdict(list)
        for x in range(samples):
            for e in estimators:
                avg_p[e].append(sum([tf_values[k]['tf'][e][x] for k in tf_values])/float(len(tf_values)))
                avg_delta[e].append(sum([abs(tf_values[k]['tf_diff'][e][x]) for k in tf_values])/float(len(tf_values)))

        line = [str(p), str(len(tf_values))]
        for e in estimators:
            line.append(str(sum(avg_p[e])/len(avg_p[e])))
        for e in estimators:
            line.append(str(sum(avg_delta[e])/len(avg_delta[e])))
        res.append(line)
    return res


def age_table(pages, initial_skip, samples=20):
    estimators = ['a_emp_dist', 'a_cho_naive', 'a_cho_impr']
    res = [['p', 'i', 'documents'] + estimators + [e + '_delta' for e in estimators]]

    for p in [0.7, 0.8, 0.9]:
        for i in [10, 20, 50]:
            interval = datetime.timedelta(days=int(i))
            tf_values = {}
            print p, i, int(initial_skip/i)
            for page in pages:
                try:
                    tmp = age_eval(page, interval, int(initial_skip/i), p)
                    if len(tmp['tf']['a_emp_dist']) >= samples:
                        tf_values[page.name] = tmp
                except Exception as e:
                    print e

            if len(tf_values) <= 0:
                res.append([str(p), str(i), '0'] + ['' for e in estimators] + ['' for e in estimators])
                continue

            avg_p = defaultdict(list)
            avg_delta = defaultdict(list)
            for x in range(samples):
                for e in estimators:
                    avg_p[e].append(sum([tf_values[k]['tf'][e][x] for k in tf_values])/float(len(tf_values)))
                    avg_delta[e].append(sum([abs(tf_values[k]['tf_diff'][e][x]) for k in tf_values])/float(len(tf_values)))

            line = [str(p), str(i), str(len(tf_values))]
            for e in estimators:
                line.append(str(sum(avg_p[e])/len(avg_p[e])))
            for e in estimators:
                line.append(str(sum(avg_delta[e])/len(avg_delta[e])))
            res.append(line)
    return res



def comp_table(pages, initial_skip, samples=20):
    estimators = ['c_emp_dist', 'c_umb_markov', 'c_cho_naive', 'c_cho_impr']
    res = [['p', 'i', 'documents'] + estimators + [e + '_delta' for e in estimators]]

    for p in [0.7, 0.8, 0.9]:
        for i in [10, 20, 50]:
            interval = datetime.timedelta(days=int(i))
            tf_values = {}
            print p, i, int(initial_skip/i)
            for page in pages:
                try:
                    tmp = comparison_eval(page, interval, int(initial_skip/i), p)
                    if len(tmp['tf']['c_emp_dist']) >= samples:
                        tf_values[page.name] = tmp
                except Exception as e:
                    print e

            if len(tf_values) <= 0:
                res.append([str(p), str(i), '0'] + ['' for e in estimators] + ['' for e in estimators])
                continue

            avg_p = defaultdict(list)
            avg_delta = defaultdict(list)
            for x in range(samples):
                for e in estimators:
                    avg_p[e].append(sum([tf_values[k]['tf'][e][x] for k in tf_values])/float(len(tf_values)))
                    avg_delta[e].append(sum([abs(tf_values[k]['tf_diff'][e][x]) for k in tf_values])/float(len(tf_values)))

            line = [str(p), str(i), str(len(tf_values))]
            for e in estimators:
                line.append(str(sum(avg_p[e])/len(avg_p[e])))
            for e in estimators:
                line.append(str(sum(avg_delta[e])/len(avg_delta[e])))
            res.append(line)
    return res


def markov_table(pages, interval, initial_skip, samples=20):
    res = []
    res.append(['p','markov hist', 'avg value', 'avg delta', 'documents'])
    for p in np.arange(0.5, 1, 0.1):
        for hist in range(1, 5, 1):
            tf_values = {}
            tf_diff = {}
            for page in pages:
                try:
                    tmp = comparison_eval(page, interval, initial_skip, p, hist)
                except Exception as e:
                    continue
                if len(tmp['tf']['c_umb_markov']) >= samples:
                    tf_values[page.name] = tmp['tf']['c_umb_markov']
                    tf_diff[page.name] = tmp['tf_diff']['c_umb_markov']

            avg_p = []
            for i in range(samples):
                avg_p.append(sum(tf_values[k][i] for k in tf_values)/float(len(tf_values)))

            avg_diff = []
            for i in range(samples):
                avg_diff.append(sum(abs(tf_diff[k][i]) for k in tf_diff)/float(len(tf_diff)))

            res.append([str(p), str(hist), str(sum(avg_p)/len(avg_p)), str(sum(avg_diff)/len(avg_diff)), len(tf_values)])

    return res

def create_files():
    pages = load_pages('revs')
    print 'total articles', len(pages)

    #reg = no_filter(pages)
    reg = filter_regular(pages)
    #reg = filter_irregular(pages)

    age = {}
    comp = {}
    push = {}
    i = datetime.timedelta(days=20)
    d = 10
    for p in reg:
        try:
            age[p.filename] = age_eval(p, i, d, 0.8)
            push[p.filename] = push_eval(p, d, 0.8)
            comp[p.filename] = comparison_eval(p, i, d, 0.8)
        except Exception as e:
            print e

    with open('tmp/age_regular.json', 'w') as f:
        json.dump(age, f)
    with open('tmp/push_regular.json', 'w') as f:
        json.dump(push, f)
    with open('tmp/comparison_regular.json', 'w') as f:
        json.dump(comp, f)



def create_csv_markov_sets():
    pages = load_pages('revs')
    print 'total articles', len(pages)

    i = datetime.timedelta(days=20)
    d = 10

    import csv
    pages = load_pages('revs')
    fil = filter_irregular(pages)
    res = markov_table(fil, i, d)
    with open('markov_table_irreg.csv', 'w') as f:
        csvw = csv.writer(f)
        csvw.writerows(res)

    fil = filter_regular(pages)
    res = markov_table(fil, i, d)
    with open('markov_table_reg.csv', 'w') as f:
        csvw = csv.writer(f)
        csvw.writerows(res)

    fil = no_filter(pages)
    res = markov_table(fil, i, d)
    with open('markov_table_all.csv', 'w') as f:
        csvw = csv.writer(f)
        csvw.writerows(res)


def create_csv_comp():
    pages = load_pages('revs')
    print 'total articles', len(pages)

    d = 20*20

    import csv
    pages = load_pages('revs')

    fil = filter_regular(pages)
    res = comp_table(fil, d)
    with open('comp_table_reg.csv', 'w') as f:
        csvw = csv.writer(f)
        csvw.writerows(res)

    fil = filter_irregular(pages)
    res = comp_table(fil, d)
    with open('comp_table_irreg.csv', 'w') as f:
        csvw = csv.writer(f)
        csvw.writerows(res)

    fil = no_filter(pages)
    res = comp_table(fil, d)
    with open('comp_table_all.csv', 'w') as f:
        csvw = csv.writer(f)
        csvw.writerows(res)



def create_csv_age():
    pages = load_pages('revs')
    print 'total articles', len(pages)

    d = 20*20

    import csv
    pages = load_pages('revs')

    fil = filter_regular(pages)
    res = age_table(fil, d)
    with open('age_table_reg.csv', 'w') as f:
        csvw = csv.writer(f)
        csvw.writerows(res)

    fil = filter_irregular(pages)
    res = age_table(fil, d)
    with open('age_table_irreg.csv', 'w') as f:
        csvw = csv.writer(f)
        csvw.writerows(res)

    fil = no_filter(pages)
    res = age_table(fil, d)
    with open('age_table_all.csv', 'w') as f:
        csvw = csv.writer(f)
        csvw.writerows(res)

def create_csv_push():
    pages = load_pages('revs')
    print 'total articles', len(pages)

    import csv
    pages = load_pages('revs')

    fil = filter_regular(pages)
    res = push_table(fil)
    with open('push_table_reg.csv', 'w') as f:
        csvw = csv.writer(f)
        csvw.writerows(res)

    fil = filter_irregular(pages)
    res = push_table(fil)
    with open('push_table_irreg.csv', 'w') as f:
        csvw = csv.writer(f)
        csvw.writerows(res)

    fil = no_filter(pages)
    res = push_table(fil)
    with open('push_table_all.csv', 'w') as f:
        csvw = csv.writer(f)
        csvw.writerows(res)


if __name__ == '__main__':
    create_csv_comp()
    create_csv_age()
    create_csv_push()
    #create_csv_markov_sets()

    #fname = 'revs/politicians/Luiz_In%C3%A1cio_Lula_da_Silva'
    #p = Page(fname)
    #age_eval(p, i, d, 0.8)


