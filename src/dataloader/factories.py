import uuid
import random
import fastrand
import numpy as np
import numpy.random
import factory.fuzzy

rng = np.random.default_rng()


def randint(start, end):
    """ Faster version of random.randint """
    v = fastrand.pcg32bounded(end)
    if v < start:
        v += start
    if v > end:
        return (v + start) / 2
    return v


class FuzzyUuid(factory.fuzzy.BaseFuzzyAttribute):
    def __init__(self, as_str=True):
        super(FuzzyUuid, self).__init__()
        self._as_str = as_str

    def fuzz(self):
        u = uuid.uuid4()
        if self._as_str:
            u = str(u)
        return u


class FuzzyText(factory.fuzzy.BaseFuzzyAttribute):
    def __init__(self, sz=16):
        super(FuzzyText, self).__init__()
        self.sz = 16 if sz <= 0 else sz

    def fuzz(self):
        s, e = (97, 123)  # a ~ z
        seed = rng.integers(s, e, size=(1, min(16, self.sz)), dtype=np.int8)
        return ''.join([chr(x) for x in seed[0]])


class FuzzyBoolean(factory.fuzzy.BaseFuzzyAttribute):
    def __init__(self, as_str=False):
        super(FuzzyBoolean, self).__init__()
        self._as_str = as_str

    def fuzz(self):
        return random.choice([True, False])
