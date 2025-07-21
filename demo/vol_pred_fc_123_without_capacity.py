#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error,r2_score,mean_absolute_error,explained_variance_score
from sklearn.model_selection import train_test_split
import xgboost as xgb
import matplotlib.pyplot as plt
import connectorx as cx
import seaborn as sns
import mlflow
from mlflow.models import infer_signature
import shap


# In[3]:


pd.set_option('display.max_columns',None)


# In[4]:


sql_command="""
select * from(SELECT DISTINCT isg_osm_id,next_isg_osm_id AS conn_isg_osm_id,
cur_fc,next_fc AS conn_fc,
highway_id ,next_highway_id AS conn_highway_id,
countyid,next_countyid AS conn_countyid,
region ,next_region AS conn_region,
roadwaytype_id, next_roadwaytype_id as conn_roadwaytype_id,
next_connected_count AS conn_count,next_pcc AS conn_ncc_pcc,
cur_lanes,next_lanes AS conn_lanes,
cur_maxspeed,next_maxspeed AS conn_maxspeed,
cur_final_place ,next_final_place AS conn_final_place,
volume,next_volume AS conn_volume,
'next' AS conn_tag
FROM tm_new_data.nys_hour_8_volume_240321
WHERE next_isg_osm_id>0 AND volume>0 AND cur_fc IN (1,2,3) AND next_fc IN (1,2,3) AND next_connected_count > 1 AND next_pcc=1
UNION 
SELECT DISTINCT isg_osm_id, prev_isg_osm_id, 
cur_fc, prev_fc, 
highway_id, prev_highway_id, 
countyid, prev_countyid,
region, prev_region, 
roadwaytype_id, prev_roadwaytype_id,
prev_connected_count, prev_ncc,  
cur_lanes, prev_lanes, 
cur_maxspeed, prev_maxspeed,
cur_final_place, prev_final_place,
volume, prev_volume,
'prev' as conn_tag
FROM tm_new_data.nys_hour_8_volume_240321 
WHERE prev_isg_osm_id >0 AND volume>0 AND cur_fc IN (1,2,3) AND prev_fc IN (1,2,3) AND prev_connected_count = 1 AND prev_ncc > 1)foo
"""

data=cx.read_sql("postgres://postgres:123@NjMpTs@20.62.75.39:9030/isgpostgres",sql_command)


# In[5]:


data.columns


# In[6]:


data.info()


# In[11]:


f_places=set(data['cur_final_place'].unique().tolist()+data['conn_final_place'].unique().tolist())
places={}

for cnt , i in enumerate(f_places):
    places.update({i:cnt})

data['cur_final_place_enc']=list(map(lambda x:places[x],data['cur_final_place']))
data['conn_final_place_enc']=list(map(lambda x:places[x],data['conn_final_place']))
data


# In[18]:


seen_data=data[data['conn_volume']>0]
unseen_data=data[data['conn_volume']<0]
seen_data


# In[25]:


total_feature_lst=['isg_osm_id', 'conn_isg_osm_id', 'cur_fc', 'conn_fc', 'highway_id',
       'conn_highway_id', 'countyid', 'conn_countyid', 'region', 'conn_region',
       'roadwaytype_id', 'conn_roadwaytype_id', 'conn_count', 'conn_ncc_pcc',
       'cur_lanes', 'conn_lanes', 'cur_maxspeed', 'conn_maxspeed',
       'cur_final_place', 'conn_final_place', 'volume', 'conn_volume',
       'conn_tag','cur_final_place_enc','conn_final_place_enc']

input_feature_lst=['cur_fc', 'conn_fc', 'highway_id',
       'conn_highway_id', 'countyid', 'conn_countyid', 'region', 'conn_region',
       'roadwaytype_id', 'conn_roadwaytype_id', 'conn_count', 'conn_ncc_pcc',
       'cur_lanes', 'conn_lanes', 'cur_maxspeed', 'conn_maxspeed',
        'cur_final_place_enc','conn_final_place_enc','volume']

target_feature=['conn_volume']


# In[26]:


unique_seen_data=seen_data[input_feature_lst+target_feature].drop_duplicates()
unique_seen_data.shape


# In[27]:


X,y=unique_seen_data[input_feature_lst],unique_seen_data[target_feature]
X_train,X_temp,y_train,y_temp=train_test_split(X,y,train_size=0.7,random_state=42)             #Test
X_val,X_test,y_val,y_test=train_test_split(X_temp,y_temp,train_size=0.5,random_state=42)       #Validation


# In[28]:


params= {'subsample': 0.8, 'n_estimators': 10000, 'max_depth': 8,'learning_rate': 0.02, 
            'colsample_bytree': 0.8, 'gamma' : 5 , 'tree_method': 'exact', 'alpha': 5, 'lambda': 5,
            'min_child_weight':5,
           'objective': 'count:poisson','base_score': np.log(np.mean(y_train)),
            'early_stopping_rounds': 20, 'eval_metric': ['rmse', 'mae']}
   
xgb_reg=xgb.XGBRegressor(**params)

eval_set=[(X_train,y_train),(X_val,y_val)]

xgb_reg.fit(X_train,y_train,eval_set=eval_set)
y_pred=xgb_reg.predict(X_test)


# In[31]:


X_train['conn_volume']=y_train['conn_volume']


# In[32]:


X_train=pd.merge(seen_data[['isg_osm_id','conn_isg_osm_id']],X_train,left_index=True,right_index=True)
X_train


# In[34]:


unseen_pred=xgb_reg.predict(unseen_data[input_feature_lst])
unseen_pred


# In[35]:


unseen_data['conn_volume']=unseen_pred
unseen_data['conn_volume']=unseen_data['conn_volume'].round().astype(int)


# In[36]:


unseen_data


# In[37]:


unseen_data.to_csv('unseen_prediction.csv')

