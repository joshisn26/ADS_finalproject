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

	try:
		response = urllib.request.urlopen(req)
		result = response.read()
		response_dict = json.loads(result)
		output.append(response_dict['Results']['output1'][0]["Scored Label Mean"])
		est_pm25_aqi = response_dict['Results']['output1'][0]["Scored Label Mean"]
		print("PM2.5 AQI: ", est_pm25_aqi)
	except urllib.error.HTTPError as error:
		print("The request failed with status code: " + str(error.code))

		# Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
		print(error.info())
		print(json.loads(error.read().decode("utf8", 'ignore')))
		
		
		
@app.route("/prediction", methods =["GET" , "POST"])
def prediction():
	form = predictionForm()
	if request.method == "POST":
		state = form.state.data
		county = form.county.data
		state = str(state) + str(county).zfill(3)  + '0001'
		date = form.date.data	
		print("date",str(date))
		date = str(date).replace('-', '')
		print(state, "-----------", date)
		
		data = {	
			"Inputs": {
			"input1":
				[
					{       
					'State_Code': state,   
					'Date_Local': date,   
					'Arithmetic_Mean': '7.3',   
					'Max_Value': "12.11",   
					'1st_Max_Hour': "4",

					}
				],
			},
			"GlobalParameters":  {
			}
		}
		body = str.encode(json.dumps(data))
		output = []
		AQIPM25(body,output)
		print(output)
		# random_forest(body,output)
		# Neural_net(body,output)
		return render_template("Forecast.html", pmAQI = output,form=form)
	elif request.method == 'GET':
		return render_template('Forecast.html',form=form)
		
		
@app.route("/")
def home():
    return render_template('main.html', context={})


if __name__ == "__main__":
  app.run(debug=True)