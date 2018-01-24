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
sc=SparkContext(appName="testsql")
sqlContext = HiveContext(sc)
#sqlContext.sql("delete from default.cTop_orders")
#result=sqlContext.sql("create table if not exists cTop_orders_new4(eventDate string, customerName string,customerCode string, orderNumber int,itemQuantity string, material string,materialcode string,materialDesc string,thickness string,orderStatus string,orderType string,installationDate string,receivedDate string,plannedCompletionDate Date,regionName string) row format delimited fields terminated by '|' stored as textfile location 'hdfs://cy1-hdoop-master:9000/ctop_mo_event'")
#result1=sqlContext.sql("load data inpath 'hdfs://cy1-hdoop-master:9000/cTop_new' into table cTop_orders_new4").collect()
results=sqlContext.sql("select * from cTop_orders_new4").collect()
######calculate the number of orders for each Day
daterange = pd.date_range('2017-01-01','2018-01-20')
new_array = list(daterange)
len_array = len(new_array)
i = 0
#######################################################
#### Region                                       #####
#######################################################
cal_days_region = []
i = 0
while i < 385:
    a = str(new_array[i])[0:10]
    order_status = ['Accepted','Cancelled','Fabricated','Shipped']
    order_region = ['Austin','Dallas','Houston','San Antonio']
    var = 0
    while var < 4:
      for s in order_region:
          d=collections.OrderedDict()
          d['eventDay'] = a
          d['status'] = order_status[var]
          d['region'] = s
          cal_days_region.append(d)
      var = var + 1
    i = i + 1
j4 = json.dumps(cal_days_region)
df_cal_days_region = pd.read_json(j4)
df_cal_days_region = df_cal_days_region[['eventDay','status','region']]
#print(df_cal_days_region)
rowarray_list1=[]
for row in results:
    chk_orderType = str(row.orderType)
    chk_var = str(row.installationDate)
    if chk_orderType.lower() == 'sample':
       pass
#      print("ignored recs with sample")
    else:
      if row.orderNumber == '' or row.orderNumber == ' ' or row.orderNumber is None:
         orderNo = 0
      else:
         orderNo = row.orderNumber
      if row.itemQuantity == '' or row.orderNumber == ' ':
         Sqft = 0
      else:
         Sqft = float(row.itemQuantity.encode('ascii','ignore'))
      Sqrft = int(Sqft)
      d=collections.OrderedDict()
      d['eventDay']=row.eventDate[0:10]
      d['orderNumber']=int(orderNo)
      d['Sqrft']=Sqrft
      d['orderStatus']=row.orderStatus
      d['regionName']=row.regionName
      rowarray_list1.append(d)
j1=json.dumps(rowarray_list1)
df1 = pd.read_json(j1)
print(j1)
No_of_orders_region = df1.groupby(['eventDay','orderNumber','orderStatus','regionName']).agg({'Sqrft':'sum'}).reset_index()
No_of_orders11_region = pd.DataFrame(No_of_orders_region)
num_of_orders_region = pd.DataFrame(No_of_orders11_region.to_records())
num_of_orders_region_with_zero = num_of_orders_region.loc[num_of_orders_region['orderNumber']==0]
num_of_orders_region_with_out_zero = num_of_orders_region.loc[num_of_orders_region['orderNumber']<>0]
#######################################################
Regions_orders_itemQuantity = num_of_orders_region_with_out_zero.groupby(['eventDay','orderStatus','regionName']).agg({'orderNumber':'count','Sqrft':'sum'}).reset_index()
rgn_orders_itemQuantity = pd.DataFrame(Regions_orders_itemQuantity)
rgn_orders_Sqrft = pd.DataFrame(rgn_orders_itemQuantity.to_records())
rgn_orders_Sqrft_new = pd.concat([num_of_orders_region_with_zero,rgn_orders_Sqrft])
final_rgn_orders_Sqrft = rgn_orders_Sqrft_new.rename(index=str,columns={"orderStatus":"status","regionName":"region","orderNumber":"orders","Sqrft":"sqFt"})
final_rgn_orders_Sqrft = final_rgn_orders_Sqrft[['eventDay','status','region','orders','sqFt']]
new_frame_r = pd.merge(df_cal_days_region, final_rgn_orders_Sqrft,on=['eventDay','status','region'],how='left')
fill_zero = int(0)
new_frame_r = new_frame_r.fillna(fill_zero)
new_frame_r[['orders','sqFt']] = new_frame_r[['orders','sqFt']].astype(int)
json_orders_region = new_frame_r.to_json(orient='records')
file_orders_region = '/home/prathyushag/Downloads/orders_region'
f = open(file_orders_region,'w')
print>>f,json_orders_region



