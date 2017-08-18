from flask_wtf import Form
from wtforms import StringField,SubmitField,SelectField,IntegerField
from wtforms import validators
from wtforms.fields.html5 import DateField

class predictionForm(Form):
	state = SelectField('state', choices=[(25, 'Massachuastte'), (36, 'New York'), (6, 'California')])
	county = IntegerField('county', validators = [validators.required()])
	date = DateField('date' ,format='%Y-%m-%d',validators= [validators.required()])
	submit = SubmitField("Get AQI")
	


	
