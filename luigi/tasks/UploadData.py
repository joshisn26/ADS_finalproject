import luigi
import boto3
from botocore.client import Config
from luigi import configuration, s3
import CleanAndWrite
import os

class UploadAll(luigi.Task):
	did_run = False
	id = luigi.Parameter()
	key = luigi.Parameter()
	files= ['Summarized_daily_ozone.csv','Summarized_daily_SO2.csv', 'Summarized_daily_NO2.csv','Summarized_daily_CO.csv','Summarized_daily_PM2.5.csv','Summarized_daily_PM10.csv']
	
	def requires(self):
		return CleanAndWrite.cleanAll()	
		
		
	def input(self):
		return luigi.LocalTarget(os.getcwd())
		
		
	def run(self):	
		s3 = boto3.resource('s3',
				aws_access_key_id = self.id , 
				aws_secret_access_key =  self.key )
				
		for l in self.files:
			k = l
			data1 = open(self.input().path + '/'+ l, 'rb')
			s3.Bucket('team7finalproject').put_object(Key=k, Body=data1)
			print('uploading to S3')
		self.did_run = True
		
		
	def complete(self):
		print("All tasks comlepted!")
		return self.did_run

