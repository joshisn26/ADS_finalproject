from flask import Flask, render_template, request, session, redirect, url_for
from forms import predictionForm
import json
import urllib.request


app = Flask(__name__)
app.secret_key = "mysec-key"


def AQIPM25(body, output):
	#PM2.5
	url = 'https://ussouthcentral.services.azureml.net/workspaces/7a01f9d6cb9b4df09ebc6b306c3a06f0/services/92401bef86f24342adb1529fcbb6705b/execute?api-version=2.0&format=swagger'
	api_key = 'sQHMJDu4MV8ARaxzAd0c5B+IlkevPiQzmD6jY1HPFNwxYttUkhvxuiYaE4BR6Znlk5HogVjDzEldLuYmfakXpw==' # Replace this with the API key for the web service
	headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}
	req = urllib.request.Request(url, body, headers)
	est_pm25_aqi = 0
	try:
		response = urllib.request.urlopen(req)
		result = response.read()
		response_dict = json.loads(result)
		output.append(response_dict['Results']['output1'][0]["Scored Label Mean"])
		est_pm25_aqi = float(response_dict['Results']['output1'][0]["Scored Label Mean"])
		print("PM2.5 AQI: ", est_pm25_aqi)
		
	except urllib.error.HTTPError as error:
		print("The request failed with status code: " + str(error.code))

		# Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
		print(error.info())
		print(json.loads(error.read().decode("utf8", 'ignore')))
	return est_pm25_aqi
	
def AQIozone(body, output):
	#Ozone
	url = 'https://ussouthcentral.services.azureml.net/workspaces/7a01f9d6cb9b4df09ebc6b306c3a06f0/services/35ee74a1e478428b9c6e647dbee3d8d0/execute?api-version=2.0&format=swagger'
	api_key = 'fjXgrtdw/oGItGW8vAfwKiAdZob4KidBNxPljO9cMbLpM+P9F3xIhfVa3u9R5suZviK60V97hQFxanz/4FNcBQ==' # Replace this with the API key for the web service
	headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}
	req = urllib.request.Request(url, body, headers)
	est_ozone_aqi = 0
	try:
		response = urllib.request.urlopen(req)
		result = response.read()
		response_dict = json.loads(result)
		output.append(response_dict['Results']['output1'][0]["Scored Label Mean"])
		est_ozone_aqi = float(response_dict['Results']['output1'][0]["Scored Label Mean"])
		print("Ozone AQI: ", est_ozone_aqi)
		
	except urllib.error.HTTPError as error:
		print("The request failed with status code: " + str(error.code))

		# Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
		print(error.info())
		print(json.loads(error.read().decode("utf8", 'ignore')))
	return est_ozone_aqi
		
		
@app.route("/prediction", methods =["GET" , "POST"])
def prediction():
	form = predictionForm()
	if request.method == "POST":
		state = form.state.data
		county = form.county.data
		am = 0
		mv= 0
		hr = 0
		print("state before", state, county)
		if (state == '36'):
			if str(county) == '5':
				state = str(state) + str(county).zfill(3)  + '0133'
				print("inside 5")
				pam = 15.46
				pmv= 13.26
				phr = 12
				oam = 0.03
				omv = 0.06
				ohr = 10
			elif str(county) == '55':
				state = str(state) + str(county).zfill(3)  + '1007'
				pam = 12.68
				pmv= 11.25
				phr = 5
				oam = 0.035
				omv = 0.045
				ohr = 11
				
		date = form.date.data	
		print("date",str(date))
		date = str(date).replace('-', '')
		print(state, "-----------", date)
		
		data1 = {	
			"Inputs": {
			"input1":
				[
					{       
					'State_Code': state,   
					'Date_Local': str(date),   
					'Arithmetic_Mean': str(pam),   
					'Max_Value': str(pmv),   
					'1st_Max_Hour': str(phr),

					}
				],
			},
			"GlobalParameters":  {
			}
		}
		data2 = {	
			"Inputs": {
			"input1":
				[
					{       
					'State_Code': state,   
					'Date_Local': str(date),   
					'Arithmetic_Mean': str(oam),   
					'Max_Value': str(omv),   
					'1st_Max_Hour': str(ohr),

					}
				],
			},
			"GlobalParameters":  {
			}
		}
		body1 = str.encode(json.dumps(data1))
		body2 = str.encode(json.dumps(data2))
		output = []
		est_pm25_aqi = AQIPM25(body1,output)
		est_ozone_aqi = AQIozone(body2,output)
		print("----------", output)
		
		
		if (est_pm25_aqi <= 50 )&( est_pm25_aqi >= 0):
			msgpm25 = "Good: It’s a great day to be active outside."
		elif (est_pm25_aqi <= 100 )&( est_pm25_aqi >= 51):
			msgpm25 = "Unusually sensitive people: Consider reducing prolonged or heavy outdoor exertion. Watch for symptoms such as coughing or shortness of breath. These are signs to take it a little easier.Others: It’s a good day to be active outside"
		if (est_ozone_aqi <= 50 )&( est_ozone_aqi >= 0):
			msgozone = "Good: It’s a great day to be active outside."
		elif (est_ozone_aqi <= 100 )&( est_ozone_aqi >= 51):
			msgozone = "Unusually sensitive people: Consider reducing prolonged or heavy outdoor exertion. Watch for symptoms such as coughing or shortness of breath. These are signs to take it a little easier.Others: It’s a good day to be active outside"
		return render_template("Forecast.html", AQI = output,form=form, flag = True , msgpm25= msgpm25, msgozone=msgozone)
	elif request.method == 'GET':
		print("flag false")
		return render_template('Forecast.html',form=form, flag = False )
		
		
@app.route("/")
def home():
    return render_template('main.html', context={})


if __name__ == "__main__":
  app.run(debug=True)