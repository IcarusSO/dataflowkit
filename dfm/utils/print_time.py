from __future__ import division
from __future__ import print_function
import datetime

__time_dict = {}


a = datetime.datetime.now()

b = datetime.datetime.now()
delta = (b - a).total_seconds()


def print_time(name):
    time = datetime.datetime.now()
    __time_dict[name] = time


def print_end(name):
    time_b = datetime.datetime.now()
    if name in __time_dict:
        time_a = __time_dict[name]
        delta = (time_b - time_a).total_seconds()
        print(name, delta)
    else:
        print(name, 'undefined')
