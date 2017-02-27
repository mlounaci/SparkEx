# -*- coding: utf-8 -*-
"""
Created on Mon Jun 06 11:20:01 2016

@author: t817682
"""
import datetime as dt
import json
import os
import re
import time


def getstring(s, stripchars=' '):
    result = None
    try:
        if s is not None and str(s).strip(stripchars).encode() != '':
            result = str(s).strip(stripchars).encode()
    except Exception:
        pass
    return result


def getfloat(f):
    result = None
    try:
        if f is not None:
            result = float(getstring(f))
    except Exception:
        pass
    return result


def getint(i):
    result = None
    try:
        if i is not None:
            result = int(getstring(i))
    except Exception:
        pass
    return result


def getboolean(b):
    result = None
    try:
        result = (True
                  if (getstring(b) == '1' or getstring(b).upper() == 'Y' or
                      getstring(b).upper() == 'YES' or getstring(b).upper() == 'OUI' or
                      getstring(b).upper() == 'TRUE')
                  else
                  False
                  if (getstring(b) == '0' or getstring(b).upper() == 'N' or
                      getstring(b).upper() == 'NO' or getstring(b).upper() == 'NON' or
                      getstring(b).upper() == 'FALSE')
                  else None)
    except Exception:
        pass
    return result


def getdate(d_string, d_format):
    result = None
    try:
        result = dt.datetime.strptime(getstring(d_string), getstring(d_format)).date()
    except Exception:
        pass
    return result


def getunixtime(d_string):
    result = None
    try:
        result = time.mktime(getdate(d_string).timetuple())
    except Exception:
        pass
    return result


def regexmatches(string, regex):
    result = False
    try:
        result = (True if re.compile(regex).search(getstring(string)) is not None else False)
    except Exception:
        pass
    return result


def getjsonparams(paramfilepath):
    data = None
    with open(paramfilepath) as data_file:
        data = json.load(data_file)
    return data


def getparamvalue(mydict, key):
    res = None
    try:
        res = (os.path.expandvars(mydict[key]) if mydict[key].lower() != "none" else None)
    except Exception:
        pass
    return res


def tabtostringlist(tab):
    res = None
    try:
        res = "("
        for val in tab:
            res += "'" + str(val) + "',"
        res = res[:len(res) - 1] + ")"
    except Exception:
        pass
    return res


def exists(variable):
    try:
        eval(variable)
    except NameError:
        return None
    return eval(variable)
