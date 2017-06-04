#  -*- coding: utf-8 -*-

import datetime
import numpy as np
import pandas as pd
import time
import os  
import multiprocessing
import warnings
from scipy.stats import zscore
from scipy.stats import mstats
from itertools import combinations
from utils import *

filterTss = [["yz"],["~top"],["~yz","top","opentop"],["~opentop","top"]]

filterOs = ["opentop2","speedup1","jump1","speedup2","jump2","foot0","foot1","foot2","isnew","small_volume"]


# filterTss = [["yz"],["~top"]]
# filterOs = ["speedup1","jump1","opentop2"]

all_filter_types = ["0","1"] + filterOs

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
    print i
    filter_typess = list(combinations(all_filter_types, i))
    for filter_types in filter_typess:
        filterss = extractFilters(list(filter_types))
        for each_filters in filterss:
            combined_filter = combineFilters(each_filters)
            valid_data_df = data_df[combined_filter]
            if len(valid_data_df) < 30:
                continue
            # for j in range(1,8):
            # sub_valid_data_df = valid_data_df[recent(j,valid_data_df)]
            sub_valid_data_df = valid_data_df
            sub_valid_data_df = sub_valid_data_df.sort_values("minute")
            sub_valid_data_df = sub_valid_data_df.groupby("date").first()
            count = len(sub_valid_data_df)
            if count < 30:
                continue
            change_se = mstats.winsorize(sub_valid_data_df.change,limits=[0.05, 0.05])
            mean = change_se.mean()
            if mean > 1.02:
                win_df = sub_valid_data_df[sub_valid_data_df["change"] > 1.0]
                win_ratio = float(len(win_df))/len(sub_valid_data_df)
                if win_ratio > 0.6:
                    name = "-".join(each_filters)
                    win_ratio_se[name] = win_ratio
                    mean_se[name] = mean
                    count_se[name] = count

df = pd.DataFrame()
df["win_ratio"] = win_ratio_se
df["mean"] = mean_se
df["count"] = count_se
df.to_csv("output6.csv")
