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
cal_days_list = []
while i < 385:
    a = str(new_array[i])[0:10]
    order_status = ['Accepted','Cancelled','Fabricated','Shipped']
    order_type = ['Production','Rework','Color Match','Remake']
    var = 0
    while var < 4:
      for s in order_type:
          d=collections.OrderedDict()
          d['eventDay'] = a
          d['status'] = order_status[var]
          d['type'] = s
          cal_days_list.append(d)
      var = var + 1
    i = i + 1
j_c = json.dumps(cal_days_list)
df_cal_days_list = pd.read_json(j_c)
rowarray_list=[]
for row in results:
     chk_orderType = str(row.orderType)
     chk_var = str(row.installationDate)
     if chk_orderType.lower() == 'model' or chk_orderType.lower() == 'pull off shelf':
         orderType = 'Production'
     else:
         orderType = chk_orderType
     if chk_orderType.lower() == 'sample':
         pass
#        print("ignored recs with Sample order type")
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
        d['orderType']=orderType
#    eventDay = [row.eventDate[0:10],row.orderNumber,Sqrft,row.orderStatus,row.orderType]
        rowarray_list.append(d)
j=json.dumps(rowarray_list)
df = pd.read_json(j)
######################################################
#### 1st visualization                           #####
######################################################
#######################################################
##Group by event_day,orderNumber,orderStatus, ordertype
#######################################################
No_of_orders = df.groupby(['eventDay','orderNumber','orderStatus','orderType']).agg({'Sqrft':'sum'}).reset_index()
No_of_orders11 = pd.DataFrame(No_of_orders)
num_of_orders = pd.DataFrame(No_of_orders11.to_records())
#######################################################
num_of_orders_with_zero = num_of_orders.loc[num_of_orders['orderNumber']==0]
num_of_orders_with_out_zero = num_of_orders.loc[num_of_orders['orderNumber']<>0]
orders_itemQuantity = num_of_orders_with_out_zero.groupby(['eventDay','orderStatus','orderType']).agg({'orderNumber':'count','Sqrft':'sum'}).reset_index()
orders_Sqft = pd.DataFrame(orders_itemQuantity)
orders_Sqrft = pd.DataFrame(orders_Sqft.to_records())
orders_Sqrft_new = pd.concat([num_of_orders_with_zero,orders_Sqrft])
final_orders_Sqrft = orders_Sqrft_new.rename(index=str,columns={"orderStatus":"status","orderType":"type","orderNumber":"orders","Sqrft":"sqFt"})
final_orders_Sqrft = final_orders_Sqrft[['eventDay','status','type','orders','sqFt']]
############################################################
######## new code is being added here    ###################
############################################################
new_frame = pd.merge(df_cal_days_list, final_orders_Sqrft,on=['eventDay','status','type'],how='left')
fill_zero = int(0)
new_frame = new_frame.fillna(fill_zero)
new_frame[['orders','sqFt']] = new_frame[['orders','sqFt']].astype(int)
json_orders_daily = new_frame.to_json(orient='records')
file_orders_daily = '/home/prathyushag/Downloads/orders_daily'
f = open(file_orders_daily,'w')
print>>f,json_orders_daily

