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
#############################################################
##### Material Type                             #############
#############################################################
rowarray_list3=[]
for row in results:
    chk_orderType = str(row.orderType)
    chk_var = str(row.installationDate)
    if chk_orderType.lower() == 'sample' or row.material == '':
       pass
#      print("do nothing")
    else:
      Sqft = float(row.itemQuantity.encode('ascii','ignore'))
      Sqrft = int(Sqft)
      d=collections.OrderedDict()
      d['orderNumber']=row.orderNumber
      d['eventDay']=row.eventDate[0:10]
      d['Sqrft']=Sqrft
      d['orderStatus']=row.orderStatus
      d['material']=row.material.lower()
      rowarray_list3.append(d)
j3=json.dumps(rowarray_list3)
df3=pd.read_json(j3)
No_of_orders_material = df3.groupby(['eventDay','orderNumber','orderStatus','material']).agg({'Sqrft':'sum'}).reset_index()
No_of_orders11_material = pd.DataFrame(No_of_orders_material)
num_of_orders_material = pd.DataFrame(No_of_orders11_material.to_records())
###############################################################
orders_material = num_of_orders_material.groupby(['eventDay','orderStatus','material']).agg({'orderNumber':'count','Sqrft':'sum'}).reset_index()
orders11_material = pd.DataFrame(orders_material)
orders22_material = pd.DataFrame(orders11_material.to_records())
final_orders22_material = orders22_material.rename(index=str,columns={"orderStatus":"status","orderNumber":"orders","Sqrft":"sqFt"})
final_orders22_material = final_orders22_material[['eventDay','status','material','orders','sqFt']]
json_orders_material = final_orders22_material.to_json(orient='records')
file_orders_material = '/home/prathyushag/Downloads/orders_material'
f = open(file_orders_material,'w')
print>>f,json_orders_material

