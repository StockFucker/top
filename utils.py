#  -*- coding: utf-8 -*-
import pandas as pd

data_df = pd.read_csv("top99.csv",index_col=0, parse_dates=True)
data_df = data_df[data_df["change"] < 1.22]
data_df = data_df[~data_df["minute_low"].isnull()]
data_df = data_df[~data_df.index.duplicated()] 
data_df = data_df[data_df["st"] == False]
# data_df = data_df[data_df["date"] < "2015-06-01"]

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
    return valid_data_df["recent"] == day
def volumeup(degree = 1.0):
    return data_df["volume0"] * degree < data_df["volume1"]

# isnew = data_df["isnew"] == 1
# small_capq = data_df["capq"] < 0.5
# small_cap = data_df["circap"] < 200
minute = (data_df["minute"] < "10:30:00")
minute2 = (data_df["minute"] > "09:35:00")
small_volume = (data_df["minute_volume"] < data_df["volume1"] * 0.5)
volumeup1 = volumeup()
volumeup15 = volumeup(1.5)
volumeup2 = volumeup(2)
# break_top = data_df["top_count"] > 3

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
top2 = top(2)

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

data_df = data_df[~yz2]

def __eval(filter_name):
    if filter_name[0] == "~":
        return ~eval(filter_name[1:])
    else:
        return eval(filter_name)

def combineFilters(filters):
    return reduce(lambda x, y: x & y, [__eval(filter_name) for filter_name in filters] + [~yz2])

def getFilter(filter_key,with_recent = False):
    filter_names = None
    if with_recent:
        filter_names = filter_key.split("-")[:-1]
    else:
        filter_names = filter_key.split("-")
    the_filter = combineFilters(filter_names)
    if with_recent:
        recent_filter = recent(int(filter_key[-1]),data_df)
        return the_filter & recent_filter
    else:
        return the_filter

def result(valid_data_df):
    result_df = pd.DataFrame()
    result_df["mean"] = valid_data_df.groupby(["recent"]).change.mean()
    result_df["count"] = valid_data_df.groupby(["recent"]).change.count()
    result_df = result_df[result_df["count"] > 1]
    print result_df
    win_df = valid_data_df[valid_data_df["change"] > 1.0]
    win_ratio = float(len(win_df))/len(valid_data_df)
    print win_ratio,len(valid_data_df),valid_data_df.change.mean()
#     valid_data_df = valid_data_df.sort_index()
#     value_se = valid_data_df.change.cumprod()
#     print value_se.plot(figsize=(7,4))