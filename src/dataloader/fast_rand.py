import random
from dataloader import factories


def choice(list_item):
    """ Faster version of random.choice """
    return random.choice(list_item)


def randint(start, end):
    """ Faster version of random.randint """
    return random.randint(start, end)


def randuuid():
    """ Faster version of rand UUID """
    return factories.FuzzyUuid()
