import random
import fastrand
from dataloader import factories


def choice(list_item):
    """ Faster version of random.choice """
    return random.choice(list_item)


def randint(start, end):
    """ Faster version of random.randint """
    # return random.randint(start, end)

    while True:
        v = fastrand.pcg32bounded(end)
        if v >= start:
            return v



def randuuid():
    """ Faster version of rand UUID """
    return factories.FuzzyUuid()
