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
result=sqlContext.sql("create table if not exists cTop_orders_test1(eventDate string, customerName string,customerCode string, orderNumber int,itemQuantity string, material string,materialcode string,materialDesc string,thickness string,orderStatus string,orderType string,installationDate string,receivedDate string,plannedCompletionDate Date,regionName string) row format delimited fields terminated by '|' stored as textfile location 'hdfs://cy1-hdoop-master:9000/test_file1'")
results=sqlContext.sql("select * from cTop_orders_test1").collect()
######calculate the number of orders for each Day
rowarray_list=[]
for row in results:
    chk_orderType = str(row.orderType)
    chk_var = str(row.installationDate)
    if chk_orderType.lower() == 'sample' or row.material == '' or chk_var == '1900-01-01':
      print("ignored recs with Sample order type")
    else:
      print("/////////////////////")
      print(row.orderNumber)
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
      d['orderType']=row.orderType
#    eventDay = [row.eventDate[0:10],row.orderNumber,Sqrft,row.orderStatus,row.orderType]
      rowarray_list.append(d)
j=json.dumps(rowarray_list)
print(j)
df = pd.read_json(j)
No_of_orders = df.groupby(['eventDay','orderNumber','orderStatus','orderType']).agg({'Sqrft':'sum'}).reset_index()
No_of_orders11 = pd.DataFrame(No_of_orders)
num_of_orders = pd.DataFrame(No_of_orders11.to_records())
#######################################################
print("num of orders/////////////////////")
num_of_orders_with_zero = num_of_orders.loc[num_of_orders['orderNumber']==0]
num_of_orders_with_out_zero = num_of_orders.loc[num_of_orders['orderNumber']<>0]
orders_itemQuantity = num_of_orders_with_out_zero.groupby(['eventDay','orderStatus','orderType']).agg({'orderNumber':'count','Sqrft':'sum'}).reset_index()
orders_Sqft = pd.DataFrame(orders_itemQuantity)
orders_Sqrft = pd.DataFrame(orders_Sqft.to_records())
orders_Sqrft_new = pd.concat([num_of_orders_with_zero, orders_Sqrft])
final_orders_Sqrft = orders_Sqrft_new.rename(index=str,columns={"orderStatus":"status","orderType":"type","orderNumber":"orders","Sqrft":"sqFt"})
final_orders_Sqrft = final_orders_Sqrft[['eventDay','status','type','orders','sqFt']]
json_orders_daily = final_orders_Sqrft.to_json(orient='records')
file_orders_daily = '/home/prathyushag/Downloads/test_file'
f = open(file_orders_daily,'w')
print>>f,json_orders_daily

