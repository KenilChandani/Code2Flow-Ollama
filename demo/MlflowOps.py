# The code snippet you provided is importing several Python libraries/modules. Here is a brief explanation of each import statement:
import mlflow
import os
import psycopg2
import pandas as pd
import traceback
import shutil
import argparse
import ast
from pathlib import Path

class del_experiments:
    
    def __init__(self, database='mlflow', user='postgres', password='123@NjMpTs', host='20.62.75.39', port='9030'):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cursor = None
        
        # This code snippet is setting up the configuration for connecting to an MLflow tracking server. Here's what each line is doing:
        os.environ['MLFLOW_TRACKING_USERNAME']='isgadmin'
        os.environ['MLFLOW_TRACKING_PASSWORD']='infosense12@'
        tracking_server_uri = "https://ptt2-mlflow.isgsuite.com/"
        mlflow.set_tracking_uri(tracking_server_uri)
        #tracking_uri=mlflow.get_tracking_uri()
        #print("Tracking URI:",tracking_uri)
    
    # This code snippet defines a function named `connect_database` that establishes a connection to a PostgreSQL database using the 
    # psycopg2 library. The function sets up the database parameters such as database name, username, password, host, and port. 
    # It then connects to the database using these parameters and returns the connection object `conn` and
    # the cursor object `cursor` for executing SQL queries.
    #Database Connection
    def connect_database(self):
        
        #Database Connection
        self.conn=psycopg2.connect(database=self.database,user=self.user,password=self.password,host=self.host,port=self.port)
        self.cursor=self.conn.cursor()

    def commit_database(self):
        if self.conn:
            self.conn.commit()
            
    def close_database_connection(self):
        if self.conn:
            self.cursor.close()
            self.conn.close()
    
    #This function 'del_params' deletes all the records of 'params' table with condition where run_uuid is in run_id list 
    def del_params(self,run_id):
        sql_command="""
                        delete from params where run_uuid in %s
                    """
        self.cursor.execute(sql_command,vars=(tuple(run_id),))
        print("Params Deleted")
        
    #This function 'del_tag' deletes all the records of 'tags' table with condition where run_uuid is in run_id list 
    def del_tag(self,run_id):
        sql_command="""
                    delete from tags where run_uuid in %s
        """

        self.cursor.execute(sql_command,vars=(tuple(run_id),))
        print("Tags Deleted")
        
    #This function 'del_metrics' deletes all the records of 'metrics' table with condition where run_uuid is in run_id list 
    def del_metrics(self,run_id):
        sql_command="""
                    delete from metrics where run_uuid in %s
        """
        self.cursor.execute(sql_command,vars=(tuple(run_id),))
        print("Metrics Deleted")
        

    #This function 'del_runs' deletes all the records of 'runs' table with condition where run_uuid is in run_id list 
    def del_runs(self,run_id):
        sql_command="""
                        delete from runs where run_uuid in %s
                    """
        self.cursor.execute(sql_command,vars=(tuple(run_id),))
        print("Runs Deleted")
        
    #This function 'del_latest_metrics' deletes all the records of 'latest_metrics' table with condition where run_uuid is in run_id list 
    def del_latest_metrics(self,run_id):
        sql_command="""
                    delete from latest_metrics where run_uuid in %s
        """
        self.cursor.execute(sql_command,vars=(tuple(run_id),))
        print("Latest Metrics Deleted")
        
    #This function 'del_experiment' deletes all the records of 'experiments' table with condition where lifecycle_stage is 'deleted'
    def del_experiment(self,exp_id):
        sql_command=f"""
                                delete  from  experiments
                                where  lifecycle_stage='{str('deleted')}'
                            """
        self.cursor.execute(sql_command)
        
    # This python function 'delete_experiment' selects all the experiments based on the given list and lifecycle_stage='deleted' condition and 
    # it selects all the run_uuid obtained from the above experiments list returned.
    def delete_experiment(self,experiment_list):
        del_experiment_id=[]
        run_id=[]
        e=() 
        sql_command1=f"""
                                select *  from  experiments
                                where lifecycle_stage='{str('deleted')}'
                            """
        try:
            for experiment_name in experiment_list:

                #Getting Experiment
                experiment=mlflow.get_experiment_by_name(experiment_name)

                #Getting experiment_id
                
                experiment_id=experiment.experiment_id
                #print(experiment_id)
                del_experiment_id.append(experiment_id)
                #print(del_experiment_id)
                e=tuple(map(int,del_experiment_id))
                #print(e)
                                
                #Delete experiment
                mlflow.delete_experiment(experiment_id)
                
            #Getting Runs
            if len(e)==0:
                sql_command=f"""
                                select * from runs 
                                where  lifecycle_stage='{str('deleted')}' or status ='{str('FAILED')}'"""
            else:
                sql_command=f"""
                                select * from runs 
                                where  experiment_id in %s or lifecycle_stage='{str('deleted')}' or status ='{str('FAILED')}'  
                            """    


            #Get Experiments to be Deleted
            run_df=pd.read_sql_query(sql_command, self.conn, params=(e,))
            exp_df=pd.read_sql_query(sql_command1, self.conn)

            #Experiment ID list
            exp_id=exp_df['experiment_id']

            #Run ID list
            run_id=run_df['run_uuid']
            run_exp_id=run_df['experiment_id'].unique()

            if len(run_id)!=0:
                  #Delete Tags from Database
                  self.del_tag(run_id)
        
            #     #Delete Latest metrics from Database
                  self.del_latest_metrics(run_id)
        
            #     #Delete Metrics from Database
                  self.del_metrics(run_id)
        
            #     #Delete Params from Database
                  self.del_params(run_id) 
        
            #     #Delete Runs from Database
                  self.del_runs(run_id)

            

            #Delete Experiment from Database
            self.del_experiment(exp_id)


            if len(exp_id)==0:
                print("No Experiments Deleted")
            else:
                print("Experiments Deleted")

                #Print Experiments to be Deleted
                print(exp_df.loc[:,['experiment_id','name']] )

            #Remove directory from fileshare    
            self.delete_content_fileshare(exp_id,run_exp_id,run_id)


        except:
            print("No such Experiment Found")
            print(traceback.format_exc())
    
    # This Python function `delete_content_fileshare` is designed to delete content from a file share directory based on the provided 
    # `experiment_id` and `run_id`. Here is a breakdown of what the function does:
    def delete_content_fileshare(self,experiment_id,run_exp_id,run_id=None):
        try:
            
            for exp_id in experiment_id:
                fileshare_path=f"/shared_data/mlflow_artifact/mlartifacts/{exp_id}/{run_id}"
                path_to_check="/" + os.path.join(*fileshare_path.split("/")[:-1]) + "/"

                if os.path.isdir(path_to_check) is True:
                    shutil.rmtree(path_to_check)
                    #print(f"Deleted Experiment {exp_id}")
                else:
                    pass
                    
            for exp_id in run_exp_id:   
                fileshare_path=f"/shared_data/mlflow_artifact/mlartifacts/{exp_id}/{run_id}"
                path_to_check = "/" + os.path.join(*fileshare_path.split("/")[:-1]) + "/"
                
                #Other method to get the path
                #path_to_check=str(fileshare_path.split("/")[:-1]).replace(",","/").replace("'","").replace(" ","").replace("[","").replace("]","")
                    
                if os.path.isdir(path_to_check) is True:
                    for run in run_id:
                        check_run=f"/shared_data/mlflow_artifact/mlartifacts/{exp_id}/{run}"
                        if os.path.isdir(check_run) is True:
                            shutil.rmtree(path_to_check + run)
                            #print(f"Deleted Experiment {exp_id} and Run {run}")
                        else: 
                             pass
                            
                        # if len(os.listdir(path_to_check))==0:
                        #     print(f"Deleted Experiment {exp_id}")
                        #     mlflow.delete_experiment(exp_id)
                        #     del_experiment(exp_id)
                            #shutil.rmtree(path_to_check)
                else:
                     print("Check Path")

        except Exception as e:
               print(e)

obj=del_experiments()
experiment_list=[]
obj.connect_database()
obj.delete_experiment(experiment_list)
obj.commit_database()
obj.close_database_connection()
                
# obj=del_experiments()
# parse=argparse.ArgumentParser()
# parse.add_argument('exp_list',type=str)
# args=parse.parse_args()
# exp_list_arg = ast.literal_eval(args.exp_list)
# experiment_list=[str(item) for item in exp_list_arg]
# obj.connect_database()
# obj.delete_experiment(experiment_list)
# obj.close_database_connection()