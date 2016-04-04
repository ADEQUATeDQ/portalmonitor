from collections import defaultdict
import csv
import json

__author__ = 'sebastian'


import matplotlib.pyplot as plt



def plot_tf_values(samples, values, labels, colors, estimators, filename, p_value=-1):
    avg_vals = defaultdict(list)
    for m in estimators:
        avg_v = 0
        i = 0.0
        for v in values[m]:
            i += 1
            avg_v += v
            avg_vals[m].append(avg_v/i)

    plt.figure(figsize=(8, 5))
    ax = plt.subplot(111)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()
    #ax.set_ylim([-0.05, 1.05])
    plt.xticks(samples, fontsize=14)
    #Labels
    plt.xlabel('Update points', fontsize=16)
    plt.ylabel('Average Prediction Rate', fontsize=16)

    plt.rc('text', usetex=True)
    plt.rc('font', family='serif')

    plt.plot(samples, [p_value for _ in range(len(values[estimators[0]]))], 'grey')

    for m in estimators:
        plt.plot(samples, values[m], colors[m]+'o')
        plt.plot(samples, avg_vals[m], colors[m], label=labels[m])

    plt.legend(scatterpoints=1,
               loc='lower right',
               ncol=1,
               fontsize=10)

    print "Saving figure to ", filename
    plt.savefig(filename, bbox_inches="tight")


def plot_p_values(samples, values):
    plt.plot(samples, values['c_emp_dist'], 'r')
    plt.plot(samples, values['c_cho_naive'], 'b')
    plt.plot(samples, values['c_cho_impr'], 'g')
    plt.plot(samples, values['c_umb_markov'], 'y')
    plt.show()


def comparison(datafile, filename, p_value):
    with open(datafile) as f:
        res = json.load(f)
    print 'total articles', len(res)
    #min_p = min(len(res[k]['p']['c_emp_dist']) for k in res)
    #min_tf = min(len(res[k]['tf']['c_emp_dist']) for k in res)

    samples = 20
    #print min_p, min_tf
    tf_values = {}
    for k in res:
        if 'c_emp_dist' in res[k]['tf']:
            if len(res[k]['tf']['c_emp_dist']) >= samples:
                tf_values[k] = res[k]['tf']
        else:
            print res[k]

    #tf_values = {k: res[k]['tf'] for k in res if len(res[k]['tf']['c_emp_dist']) >= samples}
    print 'filtered tf values', len(tf_values)
    avg_p = defaultdict(list)
    xlabels = []
    ylabels = {'c_emp_dist': '$C_{EmpDist}$', 'c_umb_markov': '$C_{UmbMarkov}$', 'c_cho_impr': '$C_{ChoImpr}$', 'c_cho_naive': '$C_{ChoNaive}$'}
    estimators = ['c_emp_dist', 'c_umb_markov', 'c_cho_naive', 'c_cho_impr']
    colors = {'c_umb_markov': 'b', 'c_emp_dist': 'r', 'c_cho_naive': 'y', 'c_cho_impr': 'g'}

    for i in range(samples):
        xlabels.append(i)
        for e in estimators:
            avg_p[e].append(sum(tf_values[k][e][i] for k in tf_values)/float(len(tf_values)))

    plot_tf_values(samples=xlabels, values=avg_p, estimators=estimators, labels=ylabels, colors=colors, filename=filename, p_value=p_value)


def age(datafile, filename, p_value):
    with open(datafile) as f:
        res = json.load(f)
    print 'total articles', len(res)

    samples = 20
    tf_values = {}
    for k in res:
        if 'a_cho_naive' in res[k]['tf']:
            if len(res[k]['tf']['a_cho_naive']) >= samples:
                tf_values[k] = res[k]['tf']
        else:
            #pass
            print res[k]

    #tf_values = {k: res[k]['tf'] for k in res if len(res[k]['tf']['c_emp_dist']) >= samples}
    print 'filtered tf values', len(tf_values)
    avg_p = defaultdict(list)
    xlabels = []
    ylabels = {'a_emp_dist': '$A_{EmpDist}$', 'a_cho_naive': '$A_{ChoNaive}$', 'a_cho_impr': '$A_{ChoImpr}$'}
    estimators = ['a_emp_dist', 'a_cho_naive', 'a_cho_impr']
    colors = {'a_emp_dist': 'r', 'a_cho_naive': 'b', 'a_cho_impr': 'g'}

    for i in range(samples):
        xlabels.append(i)
        for e in estimators:
            avg_p[e].append(sum(tf_values[k][e][i] for k in tf_values)/float(len(tf_values)))

    plot_tf_values(samples=xlabels, values=avg_p, estimators=estimators, labels=ylabels, colors=colors, filename=filename, p_value=p_value)


def push(datafile, filename, p_value):
    with open(datafile) as f:
        res = json.load(f)
    print 'total articles', len(res)

    samples = 20
    tf_values = {}
    for k in res:
        if 'a_cho_naive' in res[k]['tf']:
            if len(res[k]['tf']['a_cho_naive']) >= samples:
                tf_values[k] = res[k]['tf']
        else:
            print res[k]

    print 'filtered tf values', len(tf_values)
    avg_p = defaultdict(list)
    xlabels = []
    ylabels = {'a_emp_dist': '$P_{EmpDist}$', 'a_cho_naive': '$P_{ChoNaive}$'}
    estimators = ['a_emp_dist', 'a_cho_naive']
    colors = {'a_emp_dist': 'r', 'a_cho_naive': 'b'}

    for i in range(samples):
        xlabels.append(i)
        for e in estimators:
            avg_p[e].append(sum(tf_values[k][e][i] for k in tf_values)/float(len(tf_values)))

    plot_tf_values(samples=xlabels, values=avg_p, estimators=estimators, labels=ylabels, colors=colors, filename=filename, p_value=p_value)


def p_vs_accr(data, y, labels, colors, filename='p_accr.pdf'):

    plt.figure(figsize=(8, 5))
    ax = plt.subplot(111)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()
    #ax.set_ylim([0.05, 0.95])
    #ax.set_xlim([0.05, 0.95])

    #Labels
    plt.xlabel('p', fontsize=16)
    plt.ylabel('Average Prediction Rate', fontsize=16)

    plt.rc('text', usetex=True)
    plt.rc('font', family='serif')

    plt.plot(y, y, 'grey')
    for e in data:
        x = data[e]
        plt.plot(y, x, colors[e], label=labels[e])

        plt.legend(scatterpoints=1,
                   loc='lower right',
                   ncol=1,
                   fontsize=10)

    print "Saving figure to ", filename
    plt.savefig(filename, bbox_inches="tight")


def running_avg_plts():
    push('tmp/push_all.json', 'tmp/push_all.pdf', 0.8)
    push('tmp/push_irregular.json', 'tmp/push_irregular.pdf', 0.8)
    push('tmp/push_regular.json', 'tmp/push_regular.pdf', 0.8)

    age('tmp/age_all.json', 'tmp/age_all.pdf', 0.8)
    age('tmp/age_irregular.json', 'tmp/age_irregular.pdf', 0.8)
    age('tmp/age_regular.json', 'tmp/age_regular.pdf', 0.8)

    comparison('tmp/comparison_all.json', 'tmp/comparison_all.pdf', 0.8)
    comparison('tmp/comparison_irregular.json', 'tmp/comparison_irregular.pdf', 0.8)
    comparison('tmp/comparison_regular.json', 'tmp/comparison_regular.pdf', 0.8)


if __name__ == '__main__':
    labels = {'c_emp_dist': '$C_{EmpDist}$', 'c_umb_markov': '$C_{UmbMarkov}$', 'c_cho_impr': '$C_{ChoImpr}$', 'c_cho_naive': '$C_{ChoNaive}$'}
    colors = {'c_umb_markov': 'b', 'c_emp_dist': 'r', 'c_cho_naive': 'y', 'c_cho_impr': 'g'}

    with open('comp_table_reg.csv') as f:
        csvr = csv.reader(f)
        headers = csvr.next()
        data = defaultdict(list)
        y = []
        for row in csvr:
            y.append(float(row[0]))
            data['c_emp_dist'].append(float(row[3]))
            data['c_umb_markov'].append(float(row[4]))
            data['c_cho_naive'].append(float(row[5]))
            data['c_cho_impr'].append(float(row[6]))

    p_vs_accr(data, y, labels=labels, colors=colors)