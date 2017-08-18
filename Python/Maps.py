
# coding: utf-8

# # Maximum AQI for every state over 37 years
# 
# <p class="lead"><i>This Jupyter notebook shows state and county details where maximum AQI recorded from year 1980 to 2017 </i></p>
# 

# In[49]:

import plotly.plotly as py
import pandas as pd
import plotly, os
plotly.offline.init_notebook_mode()

filepath = os.getcwd() + "/Statewise_AQI.csv"
df = pd.read_csv(filepath)

for col in df.columns:
    df[col] = df[col].astype(str)

scl = [[0.0, 'rgb(242,240,247)'],[0.2, 'rgb(218,218,235)'],[0.4, 'rgb(188,189,220)']]

df['text'] = 'State: ' + df['State'] +'<br>' + 'County: ' + df['County'] + '<br>' +'Maxiumn AQI: '+df['Max AQI'] 

data = [ dict(
        type='choropleth',
        colorscale = scl,
        autocolorscale = False,
        locations = df['Code'],
        z = df['Year'].astype(float),
        locationmode = 'USA-states',
        text = df['text'],
        marker = dict(
            line = dict (
                color = 'rgb(255,255,255)',
                width = 2
            ) ),
        colorbar = dict(
            title = "Year")
        ) ]

layout = dict(
        title = 'AQI trends 1980-2017',
        geo = dict(
            scope='usa',
            projection=dict( type='albers usa' ),
            showlakes = True,
            lakecolor = 'rgb(255, 255, 255)'),
             )
    
fig = dict( data=data, layout=layout )
plotly.offline.iplot(fig, filename='jupyter/test')

