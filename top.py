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

data_df = pd.read_csv("top.csv",index_col=0, parse_dates=True)

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
#前天/昨天/今天开盘涨停
opentop0 = opentop(0)
opentop1 = opentop(1)
opentop2 = opentop(2)
# 前天/昨天/今天是/不是一字板
yz0 = yz(0)
yz1 = yz(1)
notyz0 = notyz(0)
notyz1 = notyz(1)
notyz2 = notyz(2)
# 前天/昨天是/否涨停
top0 = top(0)
top1 = top(1)
nottop0 = nottop(0)
nottop1 = nottop(1)
def jump(day):
    return (data_df[low(day)] > data_df["high" + str(day - 1)]) 
def foot(day,degree = 1.0):
    return (data_df[low(day)] >= data_df["open" + str(day)] * degree)
# 昨天/今天 跳高
jump1 = jump(1)
jump2 = jump(2)
# 前天/昨天/今天 光脚
foot0 = foot(0,0.995)
foot1 = foot(1,0.995)
foot2 = foot(2,0.995)
def speedup(day):
    return (data_df["high" + str(day)] - data_df[low(day)] < data_df["high" + str(day - 1)] - data_df[low(day - 1)])

#昨天/今天 加速
speedup1 = speedup(1)
speedup2 = speedup(2)

minute = (data_df["minute"] < "11:00:00")
volume = (data_df["minute_volume"] < data_df["volume1"] * 0.5)

def recent(day,valid_data_df):
    return valid_data_df[valid_data_df["recent"] == day]

all_filter = ["opentop0","opentop1","opentop2","notyz0","notyz1","yz0","yz1","top0","top1","nottop0","nottop1",\
            "jump1","jump2","foot0","foot1","foot2","speedup1","speedup2","minute","volume"]
# all_filter = ["yz0" , "top1" , "opentop1", "notyz1","jump1" ,"minute","volume"]

win_ratio_se = pd.Series()
mean_se = pd.Series()
count_se = pd.Series()

for i in range(2,len(all_filter)):
    print i
    combines = list(combinations(all_filter, i))
    for combine in combines:
        combine_filter = reduce(lambda x, y: x & y, [eval(filter_name) for filter_name in combine] + [notyz2])
        valid_data_df = data_df[combine_filter]
        if len(valid_data_df) < 30:
            continue
        for i in range(1,8):
            valid2_data_df = recent(i,valid_data_df)
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
                    name = "-".join(combine)
                    win_ratio_se[name] = win_ratio
                    mean_se[name] = mean
                    count_se[name] = count

df = pd.DataFrame()
df["win_ratio"] = win_ratio_se
df["mean"] = mean_se
df["count"] = count_se
df.to_csv("output2.csv")
