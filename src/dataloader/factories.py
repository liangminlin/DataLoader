import uuid
import random
import numpy as np
import numpy.random
import factory.fuzzy

rng = np.random.default_rng()


class FuzzyUuid(factory.fuzzy.BaseFuzzyAttribute):
    def __init__(self, as_str=False):
        super(FuzzyUuid, self).__init__()
        self._as_str = as_str

    def fuzz(self):
        u = uuid.uuid4()
        if self._as_str:
            u = str(u)
        return u


class FuzzyText(factory.fuzzy.BaseFuzzyAttribute):
    def __init__(self, as_str=False):
        super(FuzzyText, self).__init__()
        self._as_str = as_str
        
    def fuzz(self):
        s, e = (97, 123)  # a ~ z
        seed = rng.integers(s, e, size=(1, 16), dtype=np.int8)
        return ''.join([chr(x) for x in seed[0]])
    

class FuzzyBoolean(factory.fuzzy.BaseFuzzyAttribute):
    def __init__(self, as_str=False):
        super(FuzzyBoolean, self).__init__()
        self._as_str = as_str

    def fuzz(self):
        return random.choice([True, False])


class FuzzyMultiChoices(factory.fuzzy.BaseFuzzyAttribute):
    def __init__(self, sample, number=None, **kwargs):
        super(FuzzyMultiChoices, self).__init__()
        self._sample = sample
        self._k = number
        self._sample_len = len(sample)

    def fuzz(self):
        return set(random.sample(
            self._sample,
            k=self._k or random_int(self._sample_len)
        ))


class FuzzyFasterChoice(FuzzyMultiChoices):
    def __init__(self, sample, number=1, **kwargs):
        super(FuzzyMultiChoices, self).__init__()
        self._sample = sample
        self._k = number
        self._sample_len = len(sample)