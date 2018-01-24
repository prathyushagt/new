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
###########################################################
## Lead Time calculation                           ########
###########################################################
rowarray_list2=[]
for row in results:
    chk_var = str(row.installationDate)
    chk_orderType = str(row.orderType)
    if chk_orderType.lower() == 'sample' or chk_var == '1900-01-01':
       pass
#      print("ignored rec with 1900-01-01")
    else:
      Sqft = float(row.itemQuantity.encode('ascii','ignore'))
      Sqrft = int(Sqft)
      d=collections.OrderedDict()
      d['eventDay']=row.eventDate[0:10]
      d['orderNumber']=row.orderNumber
      d['Sqrft']=Sqrft
      d['orderStatus']=row.orderStatus
      var1=str(row.installationDate)
      var2=row.receivedDate[0:10].encode('ascii','ignore')
      x = datetime.strptime(var1,'%Y-%m-%d')
      y = datetime.strptime(var2,'%Y-%m-%d')
###########################################################
### ignoring records with 1900-01-01
###########################################################
      holiday_list = ['2017-01-02','2017-05-29','2017-07-03','2017-07-04','2017-09-04','2017-11-23','2017-11-24','2017-12-25']
      bus_days = np.busday_count(y,x,holidays=holiday_list)
      business_days = bus_days + 1
      d['leadTime']=business_days
      rowarray_list2.append(d)
###########################################################
j2=json.dumps(rowarray_list2)
df2=pd.read_json(j2)
No_of_orders_lead = df2.groupby(['eventDay','orderNumber','orderStatus','leadTime']).agg({'Sqrft':'sum'}).reset_index()
No_of_orders11_lead = pd.DataFrame(No_of_orders_lead)
num_of_orders_lead = pd.DataFrame(No_of_orders11_lead.to_records())
###########################################################
lead_orders_itemQuantity = num_of_orders_lead.groupby(['eventDay','orderStatus','leadTime']).agg({'orderNumber':'count','Sqrft':'sum'}).reset_index()
ld_orders_itemQuantity = pd.DataFrame(lead_orders_itemQuantity)
lead_orders_Sqrft = pd.DataFrame(ld_orders_itemQuantity.to_records())
final_lead_orders_Sqrft = lead_orders_Sqrft.rename(index=str,columns={"orderStatus":"status","orderNumber":"orders","Sqrft":"sqFt"})
final_lead_orders_Sqrft = final_lead_orders_Sqrft[['eventDay','status','leadTime','orders','sqFt']]
json_orders_lead = final_lead_orders_Sqrft.to_json(orient='records')
file_orders_lead = '/home/prathyushag/Downloads/orders_lead'
f = open(file_orders_lead,'w')
print>>f,json_orders_lead

