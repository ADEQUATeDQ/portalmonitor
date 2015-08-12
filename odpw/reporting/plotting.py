__author__ = 'jumbrich'

#scipy, numpy, matplotlib
#import scipy as sp
import numpy as np
import matplotlib as mpl
from matplotlib.ticker import FuncFormatter
from matplotlib import pyplot as plt

import collections
import itertools
import os

def createDir(dName):
    if not os.path.exists(dName):
        os.makedirs(dName)

def format_percent(x, at_least=2, tex = False):
    s = '{0:.2f}'.format(x*100, at_least)
    if s == "0.00":
        s = str(0);
    if s == "100.00":
        s = str(100);
    # The percent symbol needs escaping in latex
    if plt.rcParams['text.usetex'] == True:
        return s + r'$\%$'
    elif tex:
        return s + '$\\%$'
    else:
        return s + '%'


def to_percent(y, position):
    # Ignore the passed in position. This has the effect of scaling the default
    # tick locations.
    s = str(100 * y)

    # The percent symbol needs escaping in latex
    if plt.rcParams['text.usetex'] == True:
        return s + r'$\%$'
    else:
        return s + '%'




def histplot( data, xlabel, ylabel, title, dir, filename, bins=np.linspace(0,1,11)):
    createDir(dir)
    hist, bins = np.histogram(data, bins = bins)
    # You typically want your plot to be ~1.33x wider than tall.
    # Common sizes: (10, 7.5) and (12, 9)
    plt.figure(figsize=(8, 6))

    # Remove the plot frame lines. They are unnecessary chartjunk.
    ax = plt.subplot(111)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Ensure that the axis ticks only show up on the bottom and left of the plot.
    # Ticks on the right and top of the plot are generally unnecessary chartjunk.
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()

    plt.ylim([0,1.05])
    plt.xticks(fontsize=14)
    ax.set_xticks(bins)

    #Labels
    plt.xlabel(xlabel, fontsize=16)
    plt.ylabel(ylabel, fontsize=16)

    plt.title(title)


    plt.rc('text', usetex=True)
    plt.rc('font', family='serif')

    width = 1 * (bins[1] - bins[0])
    center = (bins[:-1] + bins[1:]) / 2
    formatter = FuncFormatter(to_percent)

    # Set the formatter
    plt.gca().yaxis.set_major_formatter(formatter)

    # plt.bar(center, hist, align='center', width=width)
    # plt.hist(data, bins= bins, normed=1, cumulative=0)
    bars = plt.bar(bins[:-1],hist.astype(np.float32)/hist.sum(),width = width)

    def autolabel(rects):
        # attach some text labels
        for rect in rects:
            height = rect.get_height()
            #print format_percent(height)
            ax.text(rect.get_x()+rect.get_width()/2., 1.02*height, format_percent(height),
                ha='center', va='bottom')

    autolabel(bars)
    print "Saving figure to ", os.path.join(dir, filename)

    plt.savefig(os.path.join(dir, filename), bbox_inches="tight");



def histplotComb(data, labels, xlabel, ylabel, dir, filename,bins=np.linspace(0,1,11)):
    createDir(dir)

    # You typically want your plot to be ~1.33x wider than tall.
    # Common sizes: (10, 7.5) and (12, 9)
    plt.figure(figsize=(8, 4.5))

    # Remove the plot frame lines. They are unnecessary chartjunk.
    ax = plt.subplot(111)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Ensure that the axis ticks only show up on the bottom and left of the plot.
    # Ticks on the right and top of the plot are generally unnecessary chartjunk.
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()


    plt.xticks(fontsize=14)


    hist, bins = np.histogram(data[0], bins = bins)

    #print "bins", bins
    xtics = []
    tics = []
    for i in range(len(bins)-1):
        if i == len(bins)-2:
            tics.append("["+str(bins[i])+" - "+str(bins[i+1])+" ]")
        else:
            tics.append("["+str(bins[i])+" - "+str(bins[i+1])+" [")
        xtics.append(i/10.0+0.05)

    #print xtics
    plt.xticks(xtics, tics, rotation=45)

    #Labels
    plt.xlabel(xlabel, fontsize=16)
    plt.ylabel(ylabel, fontsize=16)

    plt.rc('text', usetex=True)
    plt.rc('font', family='serif')

    width = 1 * (bins[1] - bins[0])/(len(data)+1)
    center = (bins[:-1] + bins[1:]) / 2
    formatter = FuncFormatter(to_percent)

    # Set the formatter
    plt.gca().yaxis.set_major_formatter(formatter)

    m  = 0
    for i in range(len(data)):
        hist, bins = np.histogram(data[i], bins = bins)
        m =  max(hist.max()*1.0 / hist.sum(), m)
        col = (i*.3, i*.3, i*.3)
        #col = ['black', 'white', 'grey', 'blue']
        bars = plt.bar( bins[:-1]+(width/2)+(width*i),hist.astype(np.float32)/hist.sum(),width = width, color=col, label=labels[i])

    plt.ylim([0,m+0.05])
    plt.legend(loc='upper right')

    createDir(dir)

    print "Saving figure to ", os.path.join(dir, filename)
    plt.savefig(os.path.join(dir, filename), bbox_inches="tight");


def freqPlot( counter, xlabel, ylabel, title, dir, filename):
    labels, values = zip(*counter)

    #labels, values = zip(*counter.items())

    #print "labels: ",labels
    #print "values: ",values

    # hist, bins = np.histogram(data, bins = bins)
    # You typically want your plot to be ~1.33x wider than tall.
    # Common sizes: (10, 7.5) and (12, 9)
    plt.figure(figsize=(8, 6))
    #
     # Remove the plot frame lines. They are unnecessary chartjunk.
    ax = plt.subplot(111)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    #
    # Ensure that the axis ticks only show up on the bottom and left of the plot.
    # Ticks on the right and top of the plot are generally unnecessary chartjunk.
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()


    plt.xticks(fontsize=14)


    #Labels
    plt.xlabel(xlabel, fontsize=16)
    plt.ylabel(ylabel, fontsize=16)

    plt.title(title)


    plt.rc('text', usetex=True)
    plt.rc('font', family='serif')


    plt.plot(labels, values, 'ro')
    print "Saving figure to ", os.path.join(dir, filename)
    plt.savefig(os.path.join(dir, filename), bbox_inches="tight");

def freqPlot1( freqs, xlabel, ylabel, title, dir, filename):



    # hist, bins = np.histogram(data, bins = bins)
    # You typically want your plot to be ~1.33x wider than tall.
    # Common sizes: (10, 7.5) and (12, 9)
    plt.figure(figsize=(8, 6))
    #
     # Remove the plot frame lines. They are unnecessary chartjunk.
    ax = plt.subplot(111)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    #
    # Ensure that the axis ticks only show up on the bottom and left of the plot.
    # Ticks on the right and top of the plot are generally unnecessary chartjunk.
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()

    plt.ylim([0.0001,1.05])
    plt.xlim([0,1.05])

    plt.xticks(fontsize=14)
    ax.set_yscale('log')

    #Labels
    plt.xlabel(xlabel, fontsize=16)
    plt.ylabel(ylabel, fontsize=16)


    formatter = FuncFormatter(to_percent)

    # Set the formatter
    plt.gca().yaxis.set_major_formatter(formatter)
    #plt.gca().xaxis.set_major_formatter(formatter)
    plt.title(title)



    plt.rc('text', usetex=True)
    plt.rc('font', family='serif')

    colors = itertools.cycle(["r", "b", "g"])
    plots = []
    for k in freqs:
        labels, values = zip(*freqs[k])
        l = np.array(labels)
        v = np.array(values)

        # for a in freqs[k]:
        #     if a[0] == 87:
        #         ax.text( a[1]/v.sum(), 1.02, a[1],
        #         ha='center', va='bottom')

        p = plt.plot( l.astype(np.float32)/87, v.astype(np.float32)/v.sum(),"ro", label=k, color=next(colors))
        plots.append(p)


    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    #plt.legend( plots,
           #list(freqs.keys()),
           #scatterpoints=1,
           #loc='upper left',
           #ncol=3,
           #fontsize=8)

    print "Saving figure to ", os.path.join(dir, filename)
    plt.savefig(os.path.join(dir, filename), bbox_inches="tight");




def scatterplot( data, xlabel, ylabel, title, dir,filename):


    # You typically want your plot to be ~1.33x wider than tall.
    # Common sizes: (10, 7.5) and (12, 9)
    plt.figure(figsize=(8, 6))

    # Remove the plot frame lines. They are unnecessary chartjunk.
    ax = plt.subplot(111)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Ensure that the axis ticks only show up on the bottom and left of the plot.
    # Ticks on the right and top of the plot are generally unnecessary chartjunk.
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()


    plt.xticks(fontsize=14)


    #Labels
    plt.xlabel(xlabel, fontsize=16)
    plt.ylabel(ylabel, fontsize=16)

    plt.title(title)


    plt.rc('text', usetex=True)
    plt.rc('font', family='serif')

    formatter = FuncFormatter(to_percent)
    # Set the formatter
    #plt.gca().yaxis.set_major_formatter(formatter)
    #plt.gca().xaxis.set_major_formatter(formatter)

    plt.scatter(data["c"],data['u'],color='red')

    print "Saving figure to ", os.path.join(dir, filename)
    plt.savefig(os.path.join(dir, filename), bbox_inches="tight");

def scatterplotComb( datad,datae,datar, xlabel, ylabel, title, dir,filename):


    # You typically want your plot to be ~1.33x wider than tall.
    # Common sizes: (10, 7.5) and (12, 9)
    plt.figure(figsize=(8, 6))

    # Remove the plot frame lines. They are unnecessary chartjunk.
    ax = plt.subplot(111)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Ensure that the axis ticks only show up on the bottom and left of the plot.
    # Ticks on the right and top of the plot are generally unnecessary chartjunk.
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()
    ax.set_xlim([-0.05, 1.05])
    ax.set_ylim([-0.05, 1.05])


    plt.xticks(fontsize=14)


    #Labels
    plt.xlabel(xlabel, fontsize=16)
    plt.ylabel(ylabel, fontsize=16)

    plt.title(title)


    plt.rc('text', usetex=True)
    plt.rc('font', family='serif')

    formatter = FuncFormatter(to_percent)
    # Set the formatter
    #plt.gca().yaxis.set_major_formatter(formatter)
    #plt.gca().xaxis.set_major_formatter(formatter)

    core = plt.scatter(datad["c"],datad['u'],color='red', s=10)
    extra = plt.scatter(datae["c"],datae['u'],color='blue', s=10)
    res = plt.scatter(datar["c"],datar['u'],color='green', s=10)

    plt.legend((core, extra, res),
           ('$\mathcal{K^C}$', '$\mathcal{K^E}$', '$\mathcal{K^R}$'),
           scatterpoints=1,
           loc='upper left',
           ncol=3,
           fontsize=8)

    print "Saving figure to ", os.path.join(dir, filename)
    plt.savefig(os.path.join(dir, filename), bbox_inches="tight");

