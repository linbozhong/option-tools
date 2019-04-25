# coding:utf-8

from datetime import datetime


def str_to_dt(date_str):
    if '-' in date_str:
        year, month, day = date_str.split('-')
    else:
        year, month, day = date_str[0:4], date_str[4:6], date_str[6:]
    return datetime(int(year), int(month), int(day))


def dt_to_str(dt):
    return '{:0>4d}-{:>02d}-{:>02d}'.format(dt.year, dt.month, dt.day)


def date_to_dt(date):
    return datetime(date.year, date.month, date.day)
