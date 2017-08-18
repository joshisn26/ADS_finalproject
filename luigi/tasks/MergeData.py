
# coding: utf-8

# In[ ]:

import pandas as pd
import math
import os
from pathlib import Path
import glob
import luigi
import time

def merge(allFiles,fileName):
    filepath = Path(fileName)
    filelist = glob.glob(allFiles)  
    heading = 0
    if(filelist) != 0:
        print("Total",len(filelist), allFiles ,"exists")
        with open(fileName, 'w',encoding='utf-8', newline="") as file:
            for f in filelist:                                                                                                                    
                print("Processing",f)
                df = pd.read_csv(f, skipinitialspace=True)
                if heading == 0:
                    df.columns = ['State_Code','County_Code','Site_Num','Parameter_Code','POC','Latitude','Longitude','Datum','Parameter_Name','Sample_Duration','Pollutant_Standard','Date_Local','Units_of_Measure','Event_Type' ,'Observation_Count','Observation_Percent','Arithmetic_Mean','Max_Value','1st_Max_Hour','AQI','Method_Code','Method_Name','Local_Site_Name','Address','State_Name','County_Name','City_Name','CBSA_Name','Date_of_Last_Change']
                    df.to_csv(file, header=True,index=False)
                    heading = 1 
                else:
                    df.to_csv(file, header=False, index=False, mode='a')
                    print(fileName,"generated!")
                    print("----------------------------------------")
    else:
        print("file list is empty!!")

class MergeDataDownloaded1(luigi.Task):
    did_run = False
    def run(self):
        ftype = ['daily_44201_*.csv','daily_42401_*.csv',"daily_42602_*.csv"]
        finalname = ['Summarized_daily_ozone.csv','Summarized_daily_SO2.csv', 'Summarized_daily_NO2.csv']
        for i in range(len(ftype)):
                    ftype[i]= os.getcwd() + '/RawData/'+ ftype[i]
                    merge(ftype[i],finalname[i])
        self.did_run = True
        time.sleep(1)
        
    def complete(self):
        return self.did_run
                
class MergeDataDownloaded2(luigi.Task):
    did_run = False
    def requires(self):
        return MergeDataDownloaded1()
        
    def run(self):
        
        ftype = ["daily_42101_*.csv",'daily_88101_*.csv','daily_81102_*.csv']
        finalname = ['Summarized_daily_CO.csv','Summarized_daily_PM2.5.csv','Summarized_daily_PM10.csv']
        for i in range(len(ftype)):
                    ftype[i]= os.getcwd() + '/RawData/'+ ftype[i]
                    merge(ftype[i],finalname[i])
        self.did_run = True
        time.sleep(1)
        
    def complete(self):
        return self.did_run       
       

                


