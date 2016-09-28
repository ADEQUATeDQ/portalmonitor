from collections import defaultdict
import math
from statsmodels.distributions.empirical_distribution import ECDF
import matplotlib.pyplot as plt
from scipy.stats import poisson
import numpy as np

__author__ = 'sebastian'


def delta_to_days(delta):
    return delta.total_seconds()/86400


class Estimator(object):
    def __init__(self):
        self.N = 0.0 # total number of accesses
        self.X = 0.0 # number of detected changes
        self.T = 0.0 # sum of the times from changes

    def estimate(self):
        raise NotImplementedError()


class ComparisonSampling(Estimator):
    def setInterval(self, I):
        self.I = I

    def update(self, Xi):
        raise NotImplementedError()



class ComparisonSamplingEmpiricalDistribution(ComparisonSampling):
    def __init__(self):
        self.history = []

    def _comupteIntervals(self):
        intervals = []
        i = 0
        for x in self.history:
            i += 1
            if x:
                intervals.append(i)
                i = 0
        return intervals


    def update(self, Xi):
        self.history.append(Xi)

    def ppf(self, q):
        intervals = self._comupteIntervals()
        perc = np.percentile(intervals, q=q*100)
        return perc * self.I

    def cdf(self, real_interval):
        intervals = self._comupteIntervals()
        cdf = ECDF(intervals)
        return cdf(real_interval)


class ChoGarciaFrequencyEstimator(ComparisonSampling):
    """
    X/T
    X: number of detected changes
    T: monitoring period
    N: number of accesses
    I: interval
    f: frequency = 1/I
    T = nI = n/f

    Estimating frequency of change,
    Cho, Junghoo
    Garcia-Molina, Hector
    """
    def update(self, Xi):
        self.N += 1
        # Has the element changed? (X is 0 or 1)
        self.X += Xi

    def ppf(self, q):
        # Percent point function (inverse of cdf)
        l = self.estimate()
        t = poisson.ppf(q, 1/l)
        return t

    def cdf(self, t):
        mu = 1/self.estimate()
        p = poisson.cdf(t, mu)
        return p


class IntuitiveFrequency(ChoGarciaFrequencyEstimator):
    """
    Poisson process:
    X(t): number of occurences of a change in the interval (0,t]
    lambda: average frequency that a change occurs
    goal: estimate lambda, given X and T
    estimate ratio r=lamda/f

    Estimator:
    r = X/n
    lambda = rf
    """
    def estimate(self):
        self.f = 1/float(self.I)

        self.r = self.X/float(self.N)
        return self.r * self.f



class ImprovedFrequency(ChoGarciaFrequencyEstimator):
    """
    r = -log((n - X + 0.5)/(n + 0.5))
    """
    def estimate(self):
        self.f = 1/float(self.I)

        self.r = -math.log((self.N - self.X + 0.5)/(self.N + 0.5))
        return self.r * self.f


class AgeSampling(Estimator):
    def __init__(self):
        Estimator.__init__(self)
        self.dates = set()

    def update(self, Ti, Ii, timestamp):
        self.dates.add(timestamp)

    def computeIntervals(self):
        deltas = []
        prev = None
        for d in sorted(self.dates):
            if prev:
                deltas.append(delta_to_days(d - prev))
            prev = d
        return deltas


class ChoGarciaLastModifiedEstimator(AgeSampling):
    """
    Estimating frequency of change,
    Cho, Junghoo
    Garcia-Molina, Hector
    """
    def update(self, Ti, Ii, timestamp):
        super(ChoGarciaLastModifiedEstimator, self).update(Ti, Ii, timestamp)
        # Ti is the time to the previous change in the ith access
        self.N += 1
        # Has the element changed?
        if Ti < Ii:
            self.X += 1
            self.T += Ti
        else:
            # element has not changed
            self.T += Ii

    def ppf(self, q):
        # Percent point function (inverse of cdf)
        l = self.estimate()
        t = poisson.ppf(q, 1/l)
        return t

    def cdf(self, t):
        mu = 1/self.estimate()
        p = poisson.cdf(t, mu)
        return p


class NaiveLastModified(ChoGarciaLastModifiedEstimator):
    def estimate(self):
        return self.X/self.T if self.T > 0 else -1


class ImprovedLastModified(ChoGarciaLastModifiedEstimator):
    def estimate(self):
        if self.X/self.N == 1:
            return -1
        x = (self.X - 1) - self.X/(self.N * math.log(1 - self.X/self.N))
        return x/self.T



class AgeSamplingEmpiricalDistribution(AgeSampling):

    def ppf(self, q):
        perc = np.percentile(self.computeIntervals(), q=q*100)
        return perc

    def cdf(self, t):
        intervals = self.computeIntervals()
        cdf = ECDF(intervals)
        return cdf(t)

    def plotDistribution(self):
        days = [t/86400 for t in self.computeIntervals()]
        cdf = ECDF(days)
        days.sort()
        F = cdf(days)
        plt.step(days, F)
        plt.show()


class ExponentialSmoothing(AgeSampling):
    def __init__(self, alpha):
        AgeSampling.__init__(self)
        self.alpha = alpha

    def estimate(self):
        deltas = self.computeIntervals()

        y = None
        for d in deltas:
            if not y:
                y = d
            else:
                y = self.alpha * y + (1 - self.alpha) * d

        return y


class MarkovChain(ComparisonSampling):
    def __init__(self, history=2):
        ComparisonSampling.__init__(self)
        self.history = history + 1
        self.data = []
        self.frequencies = defaultdict(int)

    def update(self, Xi):
        self.data.append(Xi)

        if len(self.data) >= self.history:
            key = ''.join(str(k) for k in self.data[-self.history:])
            self.frequencies[key] += 1

    def _estimate(self):
        prob = {}
        bin_len = self.history - 1

        # build probabilities
        for i in range(int(math.pow(2, bin_len))):
            k = bin(i).lstrip('0b').zfill(bin_len)
            key_0 = k + '0'
            key_1 = k + '1'

            t = float(self.frequencies[key_0] + self.frequencies[key_1])

            prob[key_0] = self.frequencies[key_0]/t if t > 0 else 0
            prob[key_1] = self.frequencies[key_1]/t if t > 0 else 0

        return prob

    def estimate(self):
        prob = self._estimate()
        bin_len = self.history - 1
        # calculate next step
        if len(self.data) >= self.history:
            key = ''.join(str(k) for k in self.data[-bin_len:])
            key_0 = key + '0'
            key_1 = key + '1'
            return ('0', prob[key_0]) if prob[key_0] > prob[key_1] else ('1', prob[key_1])
        return None, None

    def ppf(self, percent):
        prob = self._estimate()
        # access key
        key = ''.join(str(k) for k in self.data[-(self.history - 1):])
        key_0 = key + '0'

        current_perc = 0.0
        zeros = 1.0
        intervals = 0
        i = 0
        while current_perc <= percent:
            intervals += 1
            zeros *= prob[key_0]
            current_perc = 1.0 - zeros
            key_0 = key_0[1:] + '0'
            i += 1
            if i > 10000:
                raise Exception('no state change')

        return intervals

    def cdf(self, delta):
        prob = self._estimate()
        # access key
        key = ''.join(str(k) for k in self.data[-(self.history - 1):])
        key_0 = key + '0'

        zeros = 1.0
        for i in range(int(delta)):
            zeros *= prob[key_0]
            key_0 = key_0[1:] + '0'

        return 1.0 - zeros

