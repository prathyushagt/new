from datetime import datetime
import numpy as np
import collections as cln
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
import pandas as pd
import json
import collections
daterange = pd.date_range('2017-01-01', '2017-12-31')
new_array = list(daterange)
len_array = len(new_array)
i = 0
cal_days_list = []
while i < 365:
    a = str(new_array[i])[0:10]
    order_status = ['Accepted','Cancelled','Fabricated','Shipped']
    order_type = ['Production','Rework','Color Match','Remake']
    var = 0
    while var < 4:
      for s in order_type:
          d=collections.OrderedDict()
          d['eventDate'] = a
          d['orderStatus'] = order_status[var]
          d['orderType'] = s
          cal_days_list.append(d)
      var = var + 1
    i = i + 1
j =json.dumps(cal_days_list)
df = pd.read_json(j)
print(df)
#for row in cal_days:
#      print row
#for single_date in daterange:
#    print(single_date.strftime("%Y-%m-%d"))
