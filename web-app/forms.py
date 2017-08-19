from flask_wtf import Form
from wtforms import StringField,SubmitField,SelectField,IntegerField
from wtforms import validators
from wtforms.fields.html5 import DateField

class predictionForm(Form):
	state = SelectField('state', choices=[(25, 'Massachuastte'), (36, 'New York')])
	county = SelectField('state', choices=[(5, 'NY-Bronx'), (55, 'NY-Monroe'), (27, 'MA-Worcester'), (25,'MA-Boston')])
	date = DateField('date' ,format='%Y-%m-%d',validators= [validators.required()])
	submit = SubmitField("Get AQI")
	
class classificationForm(Form):
	state = StringField('state',validators= [validators.required()])
	county = StringField('county',validators= [validators.required()] )
	year = IntegerField('year' ,validators= [validators.required()])
	maxaqi =  IntegerField('aqi', validators=[validators.required()])
	percentile = IntegerField('per', validators=[validators.required()])
	medianaqi = IntegerField('med', validators=[validators.required()])
	submit = SubmitField("Get State Air Quality Condition")

	
