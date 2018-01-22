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
results=sqlContext.sql("select * from cTop_orders_new4").collect()
######calculate the number of orders for each Day
daterange = pd.date_range('2017-01-01','2017-12-31')
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
        d['orderType']=row.orderType
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
#######################################################
#### Region                                       #####
#######################################################
cal_days_region = []
i = 0
while i < 365:
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
