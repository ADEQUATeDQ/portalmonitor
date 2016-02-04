import datetime

__author__ = 'sebastian'

class IntuitiveFrequencyEstimator:
    """
    X/T
    X: number of detected changes
    T: monitoring period
    n: number of accesses
    I: interval
    f: frequency = 1/I
    T = nI = n/f

    Poisson process
    X(t): number of occurences of a change in the interval (0,t]
    lambda: average frequency that a change occurs
    goal: estimate lambda, given X and T
    first estimate ratio r=lamda/f

    Estimator:
    r = X/n
    lambda = rf

    Estimating frequency of change,
    Cho, Junghoo
    Garcia-Molina, Hector
    """

    def __init__(self, p, interval):
        revisions = [r for r in p.iterContentSampling(interval)]
        self.n = len(revisions)
        self.X = revisions.count(1)
        self.I = interval.total_seconds()

        self.f = 1/float(self.I)
        self.T = self.n * self.I

        self.r = self.X/float(self.n)
        self.est = datetime.timedelta(seconds=self.r * self.I)


class ImprovedEstimator:
    """
    r = -log((X + 0.5)/(n + 0.5))
    

    Estimating frequency of change,
    Cho, Junghoo
    Garcia-Molina, Hector
    """