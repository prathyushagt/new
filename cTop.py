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
#sparkConf = SparkConf().setAppName("test-app").setMaster("local")
#sc = SparkContext(sparkConf)
sqlContext = HiveContext(sc)
#sqlContext.refreshTable("veo.veo_echelon_linked_addresses")
print("////////////////////////results///////////////")
#result=sqlContext.sql("create table if not exists veo_echelon_linked_addresses(session_id string,session_create_date date,session_first_complete_date date,builder_id varchar(15),address_id int,job_type varchar(25),ech_row_num int,veo_row_num int,period int) row format delimited fields terminated by '\x01' stored as textfile location '/user/hive/warehouse/veo.db/veo_echelon_linked_addresses'")
#results= sqlContext.sql("select * from veo_echelon_linked_addresses").collect()
#textFile=sc.textFile("hdfs://cy1-hdoop-master:9000/cTop/").collect()
print("\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\")
#print(textFile[1][0])
#result=sqlContext.sql("create table if not exists cTop_orders(eventDate string, customerName string,customerCode string, orderNumber int,itemQuantity string, material string,materialcode string,materialDesc string,thickness string,orderStatus string,orderType string,installationDate Date,receivedDate string,plannedCompletionDate Date,regionName string) row format delimited fields terminated by '|' stored as textfile location 'hdfs://cy1-hdoop-master:9000/cTop'")
results=sqlContext.sql("select * from cTop_orders").collect()
print("the newwwwwwww results")
######calculate the number of orders for each Day
rowarray_list=[]
for row in results:
    Sqft = float(row.itemQuantity.encode('ascii','ignore'))
    Sqrft = int(Sqft)
    d=collections.OrderedDict()
    d['eventDay']=row.eventDate[0:10]
    d['orderNumber']=row.orderNumber
    d['Sqrft']=Sqrft
    d['orderStatus']=row.orderStatus
    d['orderType']=row.orderType
#    eventDay = [row.eventDate[0:10],row.orderNumber,Sqrft,row.orderStatus,row.orderType]
    rowarray_list.append(d)
j=json.dumps(rowarray_list)
df = pd.read_json(j)
#print(j)
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

#######################################################
#### Region                                       #####
#######################################################
rowarray_list1=[]
for row in results:
    Sqft = float(row.itemQuantity.encode('ascii','ignore'))
    Sqrft = int(Sqft)
    d=collections.OrderedDict()
    d['eventDay']=row.eventDate[0:10]
    d['orderNumber']=row.orderNumber
    d['Sqrft']=Sqrft
    d['orderStatus']=row.orderStatus
    d['regionName']=row.regionName
    rowarray_list1.append(d)
j=json.dumps(rowarray_list1)
df1 = pd.read_json(j)
print("till here")
No_of_orders_region = df1.groupby(['eventDay','orderNumber','orderStatus','regionName']).agg({'Sqrft':'sum'}).reset_index()
No_of_orders11_region = pd.DataFrame(No_of_orders_region)
num_of_orders_region = pd.DataFrame(No_of_orders11_region.to_records())
#########################################################
##### Lead time calculation                      ########
#########################################################
holiday_list = ['2017-01-02','2017-05-29','2017-07-03','2017-07-04','2017-09-04','2017-11-23','2017-11-24','2017-12-25']
rowarray_list2=[]
for row in results:    
    Sqft = float(row.itemQuantity.encode('ascii','ignore'))
    Sqrft = int(Sqft)
    d=collections.OrderedDict()
    d['eventDay']=row.eventDate[0:10]
    d['orderNumber']=row.orderNumber
    d['Sqrft']=Sqrft
    d['installationDate']=row.installationDate
    d['receivedDate']=row.receivedDate
    var1=str(row.installationDate)
    var2=row.receivedDate[0:10].encode('ascii','ignore')
    x = datetime.strptime(var1,'%Y-%m-%d')
    y = datetime.strptime(var2,'%Y-%m-%d')
    diff = x - y
    d['leadTime']=diff
    d['orderStatus']=row.orderStatus
    rowarray_list2.append(d)
   
