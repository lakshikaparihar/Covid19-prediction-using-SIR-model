# Visualization and Prediction of Covid19 cases in India

![](giphy.gif)


We all are well aware of the fact that COVID-19 is affecting us to a great extent, so we have done this project, where we use COVID-19’s dataset for visualizing the impact of COVID in India and also predicting the cases for next 10 days.

So we are running a scheduled python script in zepl which receives the semi-structured data from a website then loads data to SNOWFLAKE and executes DDL and DML commands on the data for retrieving the data we need for prediction. We have sent this predicted data to Snowflake, but snowflake data is in tabular form, so we have used Power BI for visualization.

![Workflow](/static/assets/img/snow.png)

#### Data we have used
* https://github.com/owid/covid-19-data/tree/master/public/data
* https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series

### Link of our work

**Website** : https://lakshika1064.github.io/Covid19-prediction-using-SIR-model/
#### Zepl Notebook : https://app.zepl.com/viewer/notebooks/bm90ZTovL3BhcmloYXJsYWtzaGlrYUBnbWFpbC5jb20vNTYwZjkxYTg2ZTk4NGFkZjhiY2Y3YzIwYTViZmIxYzQvbm90ZS5qc29u
#### Power Bi Dashboard : https://app.powerbi.com/view?r=eyJrIjoiMjcwOGYwMzUtYjI5Yi00OTc0LTlhNGQtYjdmYWMyMTAyYmM4IiwidCI6IjY1ZDU1ZWMwLWZiOTktNGQ4Mi1iODlmLWYwZDI4NjQ2YjM5YiJ9

# License 

MIT
