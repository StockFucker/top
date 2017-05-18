#  -*- coding: utf-8 -*-

import datetime
import numpy as np
import pandas as pd
import time
import os  
import multiprocessing
import warnings
from scipy.stats import zscore
from itertools import combinations

data_df = pd.read_csv("top2.csv",index_col=0, parse_dates=True)
data_df = data_df[data_df["change"] < 1.22]
data_df = data_df[~data_df["minute_low"].isnull()]

def low(day):
    return "minute_low" if day == 2 else "low" + str(day)
def yz(day):
    return (data_df[low(day)] == data_df["high_limit" + str(day)])
def notyz(day):
    return (data_df[low(day)] < data_df["high_limit" + str(day)])
def top(day):
    return (data_df["close" + str(day)] == data_df["high_limit" + str(day)])
def nottop(day):
    return (data_df["close" + str(day)] < data_df["high_limit" + str(day)])
def opentop(day):
    return (data_df["open" + str(day)] == data_df["high_limit" + str(day)])
def jump(day):
    return (data_df[low(day)] > data_df["high" + str(day - 1)]) 
def foot(day,degree = 1.0):
    return (data_df[low(day)] >= data_df["open" + str(day)] * degree)
def speedup(day):
    return (data_df["high" + str(day)] - data_df[low(day)] < data_df["high" + str(day - 1)] - data_df[low(day - 1)])
def recent(day,valid_data_df):
    return valid_data_df[valid_data_df["recent"] == day]


isnew = data_df["isnew"] == 1
small_capq = data_df["capq"] < 0.5
small_cap = data_df["circap"] < 200
minute = (data_df["minute"] < "11:00:00")
small_volume = (data_df["minute_volume"] < data_df["volume1"] * 0.5)

#前天/昨天/今天开盘涨停
opentop0 = opentop(0)
opentop1 = opentop(1)
opentop2 = opentop(2)

# 前天/昨天/今天是/不是一字板
yz0 = yz(0)
yz1 = yz(1)
yz2 = yz(2)

# 前天/昨天是/否涨停
top0 = top(0)
top1 = top(1)

# 前天/昨天/今天 光脚
foot0 = foot(0,0.995)
foot1 = foot(1,0.995)
foot2 = foot(2,0.995)

# 昨天/今天 跳高
jump1 = jump(1)
jump2 = jump(2)

#昨天/今天 加速
speedup1 = speedup(1)
speedup2 = speedup(2)


filterTss = [["yz"],["~top"],["~yz","top","opentop"],["~opentop","top"]]

filterOs = ["opentop2","speedup1","jump1","speedup2","jump2","foot0","foot1","foot2","isnew","small_capq","small_cap","minute","small_volume"]


# filterTss = [["yz"],["~top"]]
# filterOs = ["speedup1","jump1","opentop2"]

all_filter_types = ["0","1"] + filterOs

def __eval(filter_name):
    if filter_name[0] == "~":
        return ~eval(filter_name[1:])
    else:
        return eval(filter_name)

def combineFilters(filters):
    return reduce(lambda x, y: x & y, [__eval(filter_name) for filter_name in filters] + [~yz2])

def reverseType(filter_type,filters):
    return [single_filter.replace(filter_type,"~"+filter_type) for single_filter in filters]

def replacefilter(afrom,tos,total_filterss):
    filterss = []
    for filters in total_filterss:
        afilters = filters[:]
        afilters.remove(afrom)
        afilters = afilters + [to+afrom for to in tos]
        filterss.append(afilters)
    return filterss

def extractFilters(filter_types):
    total_filterss = [filter_types]
    for filter_type in filter_types:
        if filter_type in ["0","1"]:
            eachT_filterss = [replacefilter(filter_type,filterTs,total_filterss) for filterTs in filterTss]
            total_filterss = eachT_filterss[0] + eachT_filterss[1] + eachT_filterss[2] + eachT_filterss[3]
        else:
            total_filterss = total_filterss + [reverseType(filter_type,filters) for filters in total_filterss]
    return total_filterss


win_ratio_se = pd.Series()
mean_se = pd.Series()
count_se = pd.Series()

for i in range(3,len(all_filter_types)):
    filter_typess = list(combinations(all_filter_types, i))
    for filter_types in filter_typess:
        filterss = extractFilters(list(filter_types))
        for each_filters in filterss:
            combined_filter = combineFilters(each_filters)
            valid_data_df = data_df[combined_filter]
            if len(valid_data_df) < 30:
                continue
            for j in range(1,8):
                valid2_data_df = recent(j,valid_data_df)
                valid2_data_df = valid2_data_df.sort_values("minute")
                valid2_data_df = valid2_data_df.groupby("date").first()
                count = len(valid2_data_df)
                if count < 10:
                    continue
                mean = valid2_data_df.change.mean()
                if mean > 1.02:
                    win_df = valid2_data_df[valid2_data_df["change"] > 1.0]
                    win_ratio = float(len(win_df))/len(valid2_data_df)
                    if win_ratio > 0.6:
                        name = "-".join(each_filters) + "-" + str(j)
                        win_ratio_se[name] = win_ratio
                        mean_se[name] = mean
                        count_se[name] = count

df = pd.DataFrame()
df["win_ratio"] = win_ratio_se
df["mean"] = mean_se
df["count"] = count_se
df.to_csv("output3.csv")
