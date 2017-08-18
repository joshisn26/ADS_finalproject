import pandas as pd
import math
import os
from pathlib import Path
import glob
import luigi
import MergeData
#import ExtractData
import zipfile

def merge_3_col(df1):
    df1['Site_Num'] = df1['Site_Num'].astype('str').str.zfill(4)
    df1['County_Code'] = df1['County_Code'].astype('str').str.zfill(3)
    df1['State_Code'] = df1.State_Code.astype('str').str.cat(df1.County_Code).str.cat(df1.Site_Num)
    df1.drop(df1.columns[[1, 2]], axis=1,inplace=True)
    df1[['Date_Local']] = df1[['Date_Local']].astype('str')
    df1['Date_Local'] = df1['Date_Local'].str.replace('-','')
    df1[['Date_Local']] = df1[['Date_Local']].astype('int64')
    return df1
    
def drange(x,start, stop):
    if (x >= start) & (x <= stop):
        r = 1
    else:
        r = 0
    return r  

def rewrite_file(df, filename):
    with open(filename, 'w',encoding='utf-8', newline="") as file:
        df.to_csv(file, header=True,index=False)
		
def cleanCOAQI(df):
    df['AQI'] = df['AQI'].fillna(5000) 
    count = 0 
    for i, row in df.iterrows():
        if row['Max_Value'] <= 0:
            df = df.set_value(i, 'Max_Value' ,1)
        elif row['Max_Value'] >=50.4:
            df = df.set_value(i, 'Max_Value' ,50.3)
        if row['AQI'] == 5000:
            count =  count + 1
            c = row['Max_Value']
            if  drange(c,0,4.49) == 1:
                #print("in 1st range", c)
                aqi = (50/4.4*c)
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
            elif drange(c,4.50,9.49) == 1:
                #print("in 2nd range", c)
                aqi = ((49/4.9)*(c-4.5)) + 51
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
                #print("in 2nd range aqi", df4['AQI'])
            elif drange(c,9.50,12.49)== 1:
                aqi = ((49/2.9)*(c-9.5)) + 101
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
                #print("in 3rd range aqi", row['AQI'])
            elif drange(c,12.50,15.49)== 1:
                aqi = ((49/2.9)*(c-12.5)) + 151
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
            elif drange(c,15.50,30.49)== 1:
                aqi = ((99/14.9)*(c-15.5)) + 201
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
            elif drange(c,30.50,40.49)== 1:
                aqi = ((99/9.9)*(c-30.5)) + 301
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
            elif drange(c,40.50,50.49)== 1:
                aqi = ((99/9.9)*(c-40.5)) + 401
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
    print('Cleaned',count, "rows")
    return df

def cleanPM10AQI(df):
    df['AQI'] = df['AQI'].fillna(5000) 
    count = 0 
    for i, row in df.iterrows():
        if row['Max_Value'] <= 0:
            df = df.set_value(i, 'Max_Value' ,1)
            df = df.set_value(i, 'AQI' ,5000)
        elif row['Max_Value'] >= 604:
            df = df.set_value(i, 'Max_Value' ,603)
            df = df.set_value(i, 'AQI' ,5000)
        if row['AQI'] == 5000:
            count =  count + 1
            c = row['Max_Value']
            if  drange(c,0,54.99) == 1:
                #print("in 1st range", c)
                aqi = (50/54)*c
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
            elif drange(c,55,154.99) == 1:
                #print("in 2nd range", c)
                aqi = ((49/99)*(c-55)) + 51
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
                #print("in 2nd range aqi", df4['AQI'])
            elif drange(c,155,254.99)== 1:
                aqi = ((49/99)*(c-155)) + 101
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
                #print("in 3rd range aqi", row['AQI'])
            elif drange(c,255,354.99)== 1:
                aqi = ((49/99)*(c-255)) + 151
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
            elif drange(c,355,424.99)== 1:
                aqi = ((99/69)*(c-355)) + 201
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
            elif drange(c,425,504.99)== 1:
                aqi = ((99/79)*(c-425)) + 301
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
            elif drange(c,505,604)== 1:
                aqi = ((99/99)*(c-505)) + 401
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
    print('Cleaned',count, "rows")
    return df	
	
def cleanPM25AQI(df):
    df['AQI'] = df['AQI'].fillna(5000) 
    count = 0
    for i,row in df.iterrows():
        if row['Max_Value'] <= 0:
            df = df.set_value(i, 'Max_Value' ,1)
        elif row['Max_Value'] >= 500.4:
            df = df.set_value(i, 'Max_Value' ,500.3)
        if row['AQI'] == 5000: 
            count =  count +1 
            c = row['Max_Value']
            #print(index, row['AQI'] , c)
            if  drange(c,0.0,12) == 1:
                #print("in 1st range", c)
                aqi = (50/12*c)
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
            elif drange(c,12.1,35.4) == 1:
                #print("in 2nd range", c)
                aqi = ((49/23.3)*(c-12.1)) + 51
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
                #print("in 2nd range aqi", df4['AQI'])
            elif drange(c,35.5,55.4)== 1:
                aqi = ((49/19.9)*(c-35.5)) + 101
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
                #print("in 3rd range aqi", row['AQI'])
            elif drange(c,55.5,150.4)== 1:
                aqi = ((49/94.9)*(c-55.5)) + 151
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
            elif drange(c,150.5,250.4)== 1:
                aqi = ((99/99.9)*(c-150.5)) + 201
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
            elif drange(c,250.5,350.4)== 1:
                aqi = ((99/99.9)*(c-250.5)) + 301
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
            elif drange(c,350.5,500.4)== 1:
                aqi = ((99/149.9)*(c-350.5)) + 401
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
    print('Cleaned',count, "rows")
    return df

def cleanSO2AQI(df):
    df['AQI'] = df['AQI'].fillna(5000) 
    count = 0 
    for i, row in df.iterrows():
        if row['Max_Value'] <= 0:
            df = df.set_value(i, 'Max_Value' ,1)
        elif row['Max_Value'] >= 1004:
            df = df.set_value(i, 'Max_Value' ,1003)
        if row['AQI'] == 5000:  
            count =  count + 1
            c = row['Max_Value']
            if  drange(c,0,35.99) == 1:
                #print("in 1st range", c)
                aqi = (50/35)*c
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
            elif drange(c,36,75.99) == 1:
                #print("in 2nd range", c)
                aqi = ((49/39)*(c-36)) + 51
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
                #print("in 2nd range aqi", df4['AQI'])
            elif drange(c,76,185.99)== 1:
                aqi = ((49/109)*(c-76)) + 101
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
                #print("in 3rd range aqi", row['AQI'])
            elif drange(c,186,304.99)== 1:
                aqi = ((49/118)*(c-186)) + 151
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
            elif drange(c,305,604.99)== 1:
                aqi = ((99/299)*(c-305)) + 201
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
            elif drange(c,605,804.99)== 1:
                aqi = ((99/199)*(c-605)) + 301
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
            elif drange(c,805,1004)== 1:
                aqi = ((99/199)*(c-805)) + 401
                df = df.set_value(i, 'AQI' ,math.floor(aqi))
    print('Cleaned',count, "rows")
    return df
	
class cleanozone(luigi.Task):
	did_run = False
	def requires(self):
		return MergeData.MergeDataDownloaded1()
		
	def run(self):
		print("Cleaning ozone")
		
		df = pd.read_csv('Summarized_daily_ozone.csv',low_memory=False)
		#print(df.isnull().sum())
		df.drop(['Method_Code','CBSA_Name','Parameter_Code','Datum','Parameter_Name','Units_of_Measure','Sample_Duration','Pollutant_Standard','Event_Type','Local_Site_Name','Address','State_Name','County_Name','City_Name','CBSA_Name','Date_of_Last_Change','Method_Code','Method_Name'],axis=1,inplace =True)
		df = merge_3_col(df)
		rewrite_file(df, 'Summarized_daily_ozone.csv')
		print("Cleaned AQI", df.isnull().any().any())
		self.did_run = True

	def complete(self):
		return self.did_run 
		
class cleanSO2(luigi.Task):
	did_run = False
	def requires(self):
		return MergeData.MergeDataDownloaded1()
	def run(self):
		print("Cleaning SO2")
		df = pd.read_csv('Summarized_daily_SO2.csv',low_memory=False)
		df.drop(['Method_Code','CBSA_Name','Parameter_Code','Datum','Parameter_Name','Units_of_Measure','Sample_Duration','Pollutant_Standard','Event_Type','Local_Site_Name','Address','State_Name','County_Name','City_Name','CBSA_Name','Date_of_Last_Change','Method_Code','Method_Name'],axis=1,inplace =True)
		df.drop(['Sample_Duration'] =='3-HR BLK AVG',inplace =True)
		df = merge_3_col(df)
		df = cleanSO2AQI(df)
		rewrite_file(df, 'Summarized_daily_SO2.csv')
		print("Cleaned AQI", df.isnull().any().any())
		self.did_run = True

	def complete(self):
		return self.did_run 
			
class cleanCO(luigi.Task):
	did_run = False
	def requires(self):
		return MergeData.MergeDataDownloaded2()
			
	def run(self):
		df = pd.read_csv('Summarized_daily_CO.csv',low_memory=False)
		#print(df.isnull().sum())
		print("Cleaning CO")
		df.drop(['Method_Code','CBSA_Name','Parameter_Code','Datum','Parameter_Name','Units_of_Measure','Sample_Duration','Pollutant_Standard','Event_Type','Local_Site_Name','Address','State_Name','County_Name','City_Name','CBSA_Name','Date_of_Last_Change','Method_Code','Method_Name'],axis=1,inplace =True)
		df = merge_3_col(df)
		df = cleanCOAQI(df)
		rewrite_file(df, 'Summarized_daily_CO.csv')
		print("Cleaned AQI", df.isnull().any().any())
		self.did_run = True

	def complete(self):
		return self.did_run 
			
class cleanPM25(luigi.Task):
	did_run = False
	def requires(self):
		return MergeData.MergeDataDownloaded2()
	def run(self):		
		df = pd.read_csv('Summarized_daily_PM2.5.csv',low_memory=False)
		#print(df.isnull().sum())
		print("Cleaning PM2.5")
		df.drop(['Method_Code','CBSA_Name','Parameter_Code','Datum','Parameter_Name','Units_of_Measure','Pollutant_Standard','Sample_Duration','Event_Type','Local_Site_Name','Address','State_Name','County_Name','City_Name','CBSA_Name','Date_of_Last_Change','Method_Code','Method_Name'],axis=1,inplace =True)
		df = merge_3_col(df)
		df = cleanPM25AQI(df)
		rewrite_file(df, 'Summarized_daily_PM2.5.csv')
		print("Cleaned AQI", df.isnull().any().any())
		self.did_run = True

	def complete(self):
		return self.did_run 
		
			
class cleanPM10(luigi.Task):
	did_run = False
	def requires(self):
		return MergeData.MergeDataDownloaded2()
			
	def run(self):
		df = pd.read_csv('Summarized_daily_PM10.csv',low_memory=False)
		print("Cleaning PM10")
		df.drop(['Method_Code','CBSA_Name','Parameter_Code','Datum','Parameter_Name','Units_of_Measure','Pollutant_Standard','Sample_Duration','Event_Type','Local_Site_Name','Address','State_Name','County_Name','City_Name','CBSA_Name','Date_of_Last_Change','Method_Code','Method_Name'],axis=1,inplace =True)
		df = merge_3_col(df)
		df = cleanPM10AQI(df)
		rewrite_file(df, 'Summarized_daily_PM10.csv')
		print(df.isnull().any().any())
		print("Cleaned AQI", df.isnull().any().any())
		self.did_run = True

	def complete(self):
		return self.did_run 
		
		
			
class cleanNO2(luigi.Task):
	did_run = False
	def requires(self):
		return MergeData.MergeDataDownloaded1()

	def run(self):
		df = pd.read_csv('Summarized_daily_NO2.csv',low_memory=False)
		#print(df.isnull().sum())
		df.drop(['Method_Code','CBSA_Name','Parameter_Code','Datum','Parameter_Name','Units_of_Measure','Sample_Duration','Pollutant_Standard','Event_Type','Local_Site_Name','Address','State_Name','County_Name','City_Name','CBSA_Name','Date_of_Last_Change','Method_Code','Method_Name'],axis=1,inplace =True)
		df = merge_3_col(df)
		rewrite_file(df, 'Summarized_daily_NO2.csv')
		print(df.isnull().any().any())
		print("Cleaning done!!")
		self.did_run = True

	def complete(self):
		return self.did_run 
		


class cleanAll(luigi.Task):
	did_run = False
	def requires(self): 
		return extractzip()
		
	def run(self):
		yield cleanozone()
		yield cleanSO2()
		yield cleanCO()
		yield cleanPM25()
		yield cleanPM10()
		yield cleanNO2()
		self.did_run = True
	
	def complete(self):
		return self.did_run
		
		
		
class extractzip(luigi.Task):
	did_run = False
	
	def run(self):
		print(os.getcwd())	
		my_zip = zipfile.ZipFile('RawData.zip')
		my_zip.extractall(os.getcwd())
		my_zip.close()
		self.did_run = True
			
	def complete(self):
		return self.did_run
		