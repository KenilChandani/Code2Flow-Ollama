#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score, explained_variance_score, mean_absolute_error
import xgboost as xgb
import matplotlib.pyplot as plt
import connectorx as cx
import math
import time
import shap
import seaborn as sns
import statistics
import mlflow
from mlflow.models import infer_signature
import gc
import os


# In[3]:


pd.set_option('display.max_columns',None)


# In[4]:


sql_command = """select distinct foo.*,areatype,r_type,capacity from
(select 
distinct isg_osm_id, next_isg_osm_id as conn_isg_osm_id, 
cur_fc, next_fc as conn_fc, 
highway_id, next_highway_id as conn_highway_id, 
countyid, next_countyid as conn_countyid,
region, next_region as conn_region, 
roadwaytype_id, next_roadwaytype_id as conn_roadwaytype_id,
next_connected_count as conn_count,  next_pcc as conn_ncc_pcc, 
cur_lanes, next_lanes as conn_lanes, 
cur_maxspeed, next_maxspeed as conn_maxspeed,
cur_final_place, next_final_place as conn_final_place,
volume, next_volume as conn_volume,
'next' as conn_tag
from 
tm_new_data.nys_hour_8_volume_240321 where next_isg_osm_id >0 and volume>0 and(next_connected_count > 1 or next_pcc > 1)
and 
(highway_id in(3,6,10,13,16) and next_highway_id in(3,6,10,13,16) and next_pcc = 2)
or (highway_id in(3,6,10,13,16) and next_highway_id not in(3,6,10,13,16) and next_pcc = 1)
or highway_id not in(3,6,10,13,16)
union
select 
distinct isg_osm_id, prev_isg_osm_id, 
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
from 
tm_new_data.nys_hour_8_volume_240321 where prev_isg_osm_id >0 and volume>0 and(prev_connected_count > 1 or prev_ncc > 1)
and 
(highway_id in(3,6,10,13,16) and prev_highway_id in(3,6,10,13,16) and prev_ncc = 2)
or (highway_id in(3,6,10,13,16) and prev_highway_id not in(3,6,10,13,16) and prev_ncc = 1)
or highway_id not in(3,6,10,13,16)
)foo,
gis_tables.nys_osm_hvc_data_240430 cap
where cap.isg_osm_id = foo.conn_isg_osm_id
 """

data = cx.read_sql("postgres://postgres:123@NjMpTs@20.62.75.39:9030/isgpostgres", sql_command)
# data = pd.read_csv('model_input_data.csv')


# In[5]:


data.columns


# In[6]:


data.info()


# In[7]:


data.head()


# In[8]:


data


# In[9]:


final_places=set(data['cur_final_place'].unique().tolist()+data['conn_final_place'].unique().tolist())
places ={}
for cnt,i in enumerate(final_places):
    places.update({i:cnt})

data['cur_final_place_encoded'] = list(map(lambda x:places[x],data['cur_final_place']))
data['conn_final_place_encoded'] = list(map(lambda x:places[x],data['conn_final_place']))


# In[10]:


areaTypes = {}
for cnt,i in enumerate(set(data['areatype'].unique().tolist())):
    areaTypes.update({i:cnt})

data['areatype_encoded'] = list(map(lambda x:areaTypes[x],data['areatype']))


# In[11]:


data.head()


# In[12]:


seen_df = data[(data['conn_fc'] < 4)&(data['conn_volume']>0)]
unseen_df = data[(data['conn_fc'] < 4)&(data['conn_volume']<0)]


# In[13]:


seen_df


# In[15]:


# input_feature_lst = [
#                      'cur_fc', 
#                      'conn_fc',
#                      'highway_id', 
#                      'conn_highway_id', 
#                      'conn_countyid',
#                      'conn_region', 
#                      'roadwaytype_id', 
#                      'conn_roadwaytype_id',
#                      'conn_count', 'conn_ncc_pcc',
#                      'cur_lanes', 
#                      'conn_lanes',
#                      'cur_maxspeed',
#                      'conn_maxspeed',
#                      'cur_final_place', 
#                      'conn_final_place_encoded',
#                      'volume']
input_feature_lst = [
                     'highway_id', 
                     'conn_highway_id', 
                     'conn_countyid',
                     'roadwaytype_id', 
                     'conn_roadwaytype_id',
                     'conn_count', 'conn_ncc_pcc',
                     'cur_lanes',
                     'conn_lanes',
                     'conn_maxspeed',
                     'conn_final_place_encoded',
                     'capacity',
                     'areatype_encoded',
                     'volume']

target_feature_lst = ['conn_volume']


# In[ ]:


# # Correlation Analysis

# correlation_matrix = seen_df[input_feature_lst+target_feature_lst].corr(method = 'spearman')

# # Create a heatmap
# plt.figure(figsize=(8, 6))
# sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', linewidths=.5)
# plt.title('Correlation Heatmap')
# plt.show()


# In[16]:


uni_seen_df = seen_df[input_feature_lst+target_feature_lst].drop_duplicates()
uni_seen_df.shape


# In[17]:


X,y = uni_seen_df[input_feature_lst],uni_seen_df[target_feature_lst]
# Split the data into training and testing sets
X_train, X_temp, y_train, y_temp = train_test_split(X, y, test_size=0.3, random_state=42)        #Test Split
X_val, X_test, y_val, y_test = train_test_split(X_temp, y_temp, test_size=0.5, random_state=42)  #Validation Split
X_train.shape,X_test.shape,X_val.shape


# In[18]:


# def geh_check(current_vol,next_vol):
#     return round(math.sqrt(2*((current_vol-next_vol)*(current_vol-next_vol))/(current_vol+next_vol)),2)

def geh_check(y_true, y_pred):
    current_vol = y_true
    next_vol = y_pred
    metric_value = float(np.sqrt(2 * np.sum((current_vol - next_vol) ** 2) / np.sum(current_vol + next_vol)))
    return metric_value


# In[19]:


import statistics
def mean_geh(y_test, y_pred):
    geh_lst = []
    for i,j in zip(y_test, y_pred):
        geh_lst.append(geh_check(i,j))
    return statistics.mean(geh_lst)


# In[20]:


def log_model_summary(xg_reg,X_train,X_val,X_test,y_train,y_val,y_test):
    
    # Log Parameter
    xgb_params = xg_reg.get_xgb_params()
    mlflow.log_params(xgb_params)
    
    Test_Data_Df = X_test.copy()
    Test_Data_Df['conn_volume'] = y_test['conn_volume']
    Test_Data_Df['predicted_conn_volume']=y_pred
    Test_Data_Df['predicted_conn_volume']=Test_Data_Df['predicted_conn_volume'].round().astype(int)
    Test_Data_Df['diff']=Test_Data_Df['conn_volume']-Test_Data_Df['predicted_conn_volume']
    Test_Data_Df['geh'] = Test_Data_Df.apply(lambda row: geh_check(row['conn_volume'], row['predicted_conn_volume']), axis=1)
    Test_Data_Df['geh'] = Test_Data_Df['geh'].round(2)
    
    # Log Metrics
    mse = round(mean_squared_error(y_test, y_pred),3)
    rmse = round(np.sqrt(mse),3)
    r2 = round(r2_score(y_test, y_pred),3)
    n = X_test.shape[0]
    p = X_test.shape[1]
    adjusted_r2score =  round((1 - (1 - r2) * (n - 1) / (n - p - 1)),3)
    mae = round(mean_absolute_error(y_test, y_pred),3)
    
    m_geh = round(mean_geh(y_test.values, y_pred),3)
    geh_grt_5 = Test_Data_Df[Test_Data_Df['geh']>5].shape[0]
    geh_error_per = round(((geh_grt_5/X_test.shape[0])*100),3)
    
    metrics = {'mse':mse,'rmse':rmse,'r2score':r2,'mae':mae,'adjusted_r2score' : adjusted_r2score,
               'mean_geh':m_geh,'geh_grt_5':geh_grt_5,'geh_error':geh_error_per,'geh_error_per':geh_error_per}
    
    mlflow.log_metrics(metrics)
    
    
    # log tags
    tags = {'Model_Name' : 'XGBoost',
            
            'Train Size' : X_train.shape[0],
            'Valid Size' : X_val.shape[0],
            'Test Size' : X_test.shape[0],
            'No. Of Input Feature' : len(input_feature_lst),
            'No. Of Target Feature ': len(target_feature_lst),
            'input_feature_lst':input_feature_lst,
            'target_feature_lst': target_feature_lst}
    
    mlflow.set_tags(tags)

    
    return  Test_Data_Df
   


# In[21]:


os.environ['MLFLOW_TRACKING_USERNAME'] = 'isgadmin'
os.environ['MLFLOW_TRACKING_PASSWORD'] = 'infosense12@'
tracking_server_uri = "https://ptt2-mlflow.isgsuite.com/"
mlflow.set_tracking_uri(tracking_server_uri)
tracking_uri = mlflow.get_tracking_uri()
print("Tracking URI:", tracking_uri)


# In[22]:


# experiment_name = "TurningMovement"
# experiment = mlflow.set_experiment(experiment_name)
# experiment_id = experiment.experiment_id
run_name = 'xgb_with_fc_1_2_3_run with capacity'
description = 'added capacity feature to predict only fc 1,2,3'
# Get Experiment Details
# print(f"Experiment_id: {experiment.experiment_id}")
print(f"Run Name: {run_name}")
print(f"Description: {description}")
# print(f"Artifact Location: {experiment.artifact_location}")


# In[23]:


with mlflow.start_run(experiment_id="45",run_name = run_name,description=description) as run:

    # Define the XGBoost regressor with the specified parameters
    params= {'subsample': 0.8, 'n_estimators': 10000, 'max_depth': 8,'learning_rate': 0.02, 
             'colsample_bytree': 0.8, 'gamma' : 5 , 'tree_method': 'exact', 'alpha': 5, 'lambda': 5,
             'min_child_weight':5,
            'objective': 'count:poisson','base_score': np.log(np.mean(y_train)),
             'early_stopping_rounds': 20, 'eval_metric': ['rmse', 'mae']}
    
    

    xg_reg = xgb.XGBRegressor(**params)

    # Train the model with a validation dataset and early stopping
    eval_set = [(X_train, y_train), (X_val, y_val)]

    xg_reg.fit(X_train, y_train, eval_set=eval_set)
    # Make predictions on the test set
    y_pred = xg_reg.predict(X_test)
    
    
#     mlflow.shap.log_explanation(xg_reg.predict,X_test)

    # Log Model Summary
    Test_Data_Df = log_model_summary(xg_reg,X_train,X_val,X_test,y_train,y_val,y_test)
    
    # Log Feature Importance

    explainer = shap.TreeExplainer(xg_reg)
    shap_values = explainer.shap_values(X_train[:500])
    fig = shap.summary_plot(shap_values, X_train[:500], plot_type="bar",show=False)
    # Save the Image
    image_path = "Feature_Importance.png"
    plt.savefig(image_path)
    mlflow.log_artifact(image_path, "images")

    #Log Learning Curve

    results = xg_reg.evals_result()
    training_rounds = range(len(results['validation_0']['rmse']))
    plt.figure(figsize=(12, 6))
    plt.plot(training_rounds, results['validation_0']['rmse'], label='Training Loss')
    plt.plot(training_rounds, results['validation_1']['rmse'], label='Validation Loss')
    plt.xlabel('Boosting Round')
    plt.ylabel('RMSE') 
    plt.title('XGBoost Training and Validation Loss')
    plt.legend()

    image_path = "Learning_Curve.png"
    plt.savefig(image_path)
    mlflow.log_artifact(image_path, "images")
    
#     Actual Vs Prediction Plot

    plt.figure(figsize=(8, 6))
    sns.scatterplot(data=Test_Data_Df, x='conn_volume', y='predicted_conn_volume', color='blue', label='Data Points')
    sns.lineplot(x=Test_Data_Df['conn_volume'], y=Test_Data_Df['conn_volume'], color='red', label='Identity Line')
    plt.title('Actual vs. Predicted')
    plt.xlabel('Actual Values')
    plt.ylabel('Predicted Values')
    plt.legend()
    plt.tight_layout()

    image_path = "Actual_Vs_Prediction_Plot.png"
    plt.savefig(image_path)
    mlflow.log_artifact(image_path, "images")
    
#     Actual And Prediction Distribution
    
    fig, axs = plt.subplots(1, 2, figsize=(14, 6))
    sns.histplot(Test_Data_Df['conn_volume'], color='blue', kde=True, ax=axs[0])
    axs[0].set_title('Actual Distribution')
    axs[0].set_xlabel('Values')
    axs[0].set_ylabel('Frequency')
    sns.histplot(Test_Data_Df['predicted_conn_volume'], color='orange', kde=True, ax=axs[1])
    axs[1].set_title('Predicted Distribution')
    axs[1].set_xlabel('Values')
    axs[1].set_ylabel('Frequency')
    plt.tight_layout()

    image_path = "Actual_And_Prediction_Distribution.png"
    plt.savefig(image_path)
    mlflow.log_artifact(image_path, "images")
    
    
#     Residuals Plot
    plt.figure(figsize=(8, 6))
    sns.histplot(Test_Data_Df['diff'], bins=100, kde=False, color='skyblue')
    plt.xlabel('Values')
    plt.ylabel('Frequency')
    plt.title('Histogram with KDE')
    plt.xlim(-500, 500)
    plt.grid(True)
    
    image_path = "Residuals_Plot.png"
    plt.savefig(image_path)
    mlflow.log_artifact(image_path, "images")
    
#     Log Model
    signature = infer_signature(X_test, xg_reg.predict(X_test))
    mlflow.sklearn.log_model(xg_reg, "model", signature=signature)
    
    mlflow.end_run()  


# In[ ]:


Test_Data_Df


# In[ ]:


X_train['conn_volume'] = y_train['conn_volume']


# In[ ]:


X_train = pd.merge(seen_df[['isg_osm_id','conn_isg_osm_id']], X_train,  left_index=True, right_index=True)


# In[ ]:


Test_Data_Df = pd.merge(seen_df[['isg_osm_id','conn_isg_osm_id']], Test_Data_Df,  left_index=True, right_index=True)


# In[ ]:


unseen_pred = xg_reg.predict(unseen_df[input_feature_lst])


# In[ ]:


unseen_df['conn_volume'] = unseen_pred
unseen_df['conn_volume'] = unseen_df['conn_volume'].round().astype(int)


# In[ ]:


unseen_df


# In[ ]:


unseen_df[unseen_df['conn_isg_osm_id'].isin([46858893001,38099934001,122625735001])]


# In[ ]:


2881*(1983.0/(1983.0+1799.0)),2881*(1799.0/(1983.0+1799.0))


# In[ ]:


import urllib.parse
from sqlalchemy import create_engine
import psycopg2
import psycopg2.extras as extras

# Insert Data Into DB

database = 'isgpostgres'
user = 'postgres'
password = '123@NjMpTs'
host = '20.62.75.39'
port = '9030'

encoded_password = urllib.parse.quote(password)

schema_name = 'tm_new_data'
train_table_name = 'nys_hour_8_volume_240416_xgb_train_EV'
test_table_name = 'nys_hour_8_volume_240416_xgb_test_EV'
unseen_table_name = 'nys_hour_8_volume_240416_xgb_unseen_withut_EV'

connection_string = "postgresql://" + user + ":" + encoded_password + "@" + host + ":" + port + "/" + database
engine = create_engine(connection_string)


# In[ ]:


X_train.to_sql(train_table_name,engine,if_exists='append',schema=schema_name, chunksize=10000,index=False,method='multi')


# In[ ]:


Test_Data_Df.to_sql(test_table_name,engine,if_exists='append',schema=schema_name, chunksize=10000,index=False,method='multi')


# In[ ]:


unseen_df.to_sql(unseen_table_name,engine,if_exists='append',schema=schema_name, chunksize=10000,index=False,method='multi')

