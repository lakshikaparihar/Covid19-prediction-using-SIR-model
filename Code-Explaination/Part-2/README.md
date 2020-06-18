# Creating a COVID-19 PREDICTION MODEL

Let’s move forward . . .

In part-1 of this project we have an established connection between snowflake and zepl notebook. In this part we will fetch the covid-19 data from different sources and put it in our snowflake database.

![](/Part-2/image.png)

Data i am working on :
* https://github.com/owid/covid-19-data/tree/master/public/data
* https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series

I am using python interface to work on snowflake. Snowflake also provides other interfaces such as SnowSQL (CLI), Node. js, JDBC, and ODBC (drivers).We have already connected our python script with snowflake in the previous part using this command.

>> `conn=z.getDatasource("snowflake_HG36605apsouth1aws_bf684a")`

So , let’s import all the required libraries we need in our project

>> `import pandas as pdimport numpy as np` <br/>
>> `from datetime import datetime, timedelta` <br/>
`import requests` <br/>
`import io` <br/>
`import scipy` <br/>
`import operator` <br/>
`from scipy.integrate import solve_ivp` <br/>
`from scipy.integrate import odeint` <br/>

Now Store the details of snowflake in variables. As we have to use the details recursively so it will be easy.

>> `warehouse = "PC_ZEPL_WH"` <br/>
`database = "PC_ZEPL_DB"` <br/>
`stage_table = "data_stage"` <br/>

As, we need the updated data to get the prediction so we will directly access our CSV file from the web. Using requests.get() we will get the content from the web. Here I am using time series recovered global data and owid-covid world data that we will decode because .content return the data in raw bytes so will convert it into CSV format. 

>> ` # recovered people ` <br/>
`dataread_csv = requests.get("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv").content ` <br/>
`recovered_df = pd.read_csv(io.StringIO(read_csv.decode('utf-8'))) ` <br/>
`recovered_df = recovered_df[recovered_df["Country/Region"] == "India"] ` <br/>
`recovered_df = recovered_df.drop(["Province/State", "Country/Region", "Lat", "Long"], axis=1) ` <br/>
`recovered_df = recovered_df.T ` <br/>
`recovered_df.to_csv( "recovered_timeseries.csv") ` <br/>
`recovered_df = pd.read_csv("recovered_timeseries.csv") ` <br/>
`recovered_df.columns = ["Date", "Recover"] ` <br/>
`recovered_df["Date"] = (pd.to_datetime(recovered_df["Date"])).dt.strftime("%Y-%m-%d") ` <br/>
`recovered_df.to_csv("recovered_timeseries.csv" , index=False) ` <br/>
`recovered_df = pd.read_csv("recovered_timeseries.csv") ` <br/>

After that, we are filtering the data of India and transposing the dataframe so it will be easy for our computation. We are updating our CSV file because for snowflake we will need the CSV file only.

>> `#confirmed and deaths` <br/>
`url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv" ` <br/>
`read_csv = requests.get(url).content ` <br/>
`address = pd.read_csv(io.StringIO(read_csv.decode('utf-8'))) ` <br/>
`address.to_csv("covid_data.csv", index=False) ` <br/>
`address = pd.read_csv("covid_data.csv") ` <br/>

Define a function to fetch the result of the queries from snowflake. As we need to fetch the data many times for the computation of the prediction model .

>> <pre> def execute_result(connection, query):         
>>                final_result = connection.execute(query).fetchall() 
>>                return final_result </pre>

Setup the warehouse , schema ,database , role of snowflake .To execute queries in snowflake we are using execute(), it requires a parameter, which is the SQL statement to be executed. So the query which we will pass to execute function as a parameter will be executed in our snowflake account.

>> <pre>try: 
>>       sql = 'use role {}'.format('PC_ZEPL_ROLE') 
>>        conn.execute(sql)
>>        sql = 'use database {}'.format(database)
>>        conn.execute(sql)
>>        sql = 'use warehouse {}'.format(warehouse)
>>        conn.execute(sql)
>>        sql = 'use schema {}'.format('PUBLIC')
>>        conn.execute(sql)
>>        try:
>>                sql = 'alter warehouse {} resume'.format(warehouse)
>>                conn.execute(sql) 
>>        except: 
>>                pass</pre>

Before creating a table we will always check if it exist or not to avoid any error. In snowflake we need to specify the structure before loading the data.

>><pre>try: 
>>    sql = 'drop table covid_data' 
>>    conn.execute(sql)  
>>except: 
>>    pass 
>>sql = 'create table covid_data(iso_code VARCHAR, ' \ 'continent VARCHAR ,location VARCHAR ,date DATE, total_cases INT ,new_cases Double, total_deaths INT,new_deaths INT,' \ 'total_cases_per_million DOUBLE, new_cases_per_million DOUBLE , total_deaths_per_million DOUBLE,' \ ' new_deaths_per_million DOUBLE,total_tests INT,new_tests INT,new_test_smoothed DOUBLE, total_tests_per_thousand DOUBLE,' \ 'new_tests_per_thousand DOUBLE ,new_test_smoothed_per_thousand DOUBLE, tests_units VARCHAR,stringency_index BIGINT,population DOUBLE ,population_density DOUBLE ,' \ 'median_age DOUBLE ,aged_65_older DOUBLE ,aged_70_older DOUBLE ,gdp_per_capita DOUBLE ,' \ 'extreme_poverty DOUBLE,cvd_death_rate DOUBLE ,diabetes_prevalence DOUBLE , female_smokers DOUBLE ' \ ',male_smokers DOUBLE ,handwashing_facilities DOUBLE , hospital_beds_per_100k DOUBLE)' 
>>conn.execute(sql) </pre>

As you can see in the above code we are executing queries in the snowflake through our python script. If there will be any error in our snowflake query then the script will terminate and show the error.As we can access our snowflake database through the script let’s put our data which we have fetched into the database. 

From many features of snowflake we have a feature called staging.So whenever we need to load data from a file into snowflake then the file is  stored or you can say staged so that the data in the files can be loaded to the tables.There are two types of staging: Internal and external. Internal staging is when we store data files within snowflake and if we are storing data files in a location outside of snowflake then we need to use URL to create a reference. It is known as external staging. Here we are using internal staging. We will create a stage, push our CSV in it, and then copy the data into a table.

>><pre>try:
>>     sql = 'drop stage if exists data_stage' 
>>     conn.execute(sql) 
>>except: 
>>      pass
>>sql = """create stage data_stage file_format = (type = "csv" field_delimiter = "," skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')""" conn.execute(sql)
>>csv_file = 'covid_data.csv' sql = "PUT file://" + csv_file + " @DATA_STAGE auto_compress=true" 
>>conn.execute(sql)
>>sql = """copy into covid_data from @DATA_STAGE/covid_data.csv.gz file_format = (type = "csv" field_delimiter = "," skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)""" \ """ON_ERROR = "ABORT_STATEMENT" """
>>conn.execute(sql)</pre>

We are mainly interested in India so we will filter the india’s data from covid_data table and store it in a separate table named INDIA_DATA .

>><pre>try:
>>      sql = 'DROP TABLE INDIA_DATA' 
>>      conn.execute(sql) 
>>except: 
>>      pass
>>sql = "CREATE TABLE INDIA_DATA AS SELECT * FROM COVID_DATA WHERE LOCATION = 'India' 
>>conn.execute(sql)</pre>

Create a table for recovered data and push your recovered.csv file into stage and then copy into table

>><pre>try: 
>>      sql = 'DROP TABLE RECOVERED_DATA'
>>      conn.execute(sql) 
>>except: 
>>      pass
>>sql = "CREATE TABLE RECOVERED_DATA(date DATE,recover INT)" 
>>conn.execute(sql)
>>csv_file = 'recovered_timeseries.csv' sql = "PUT file://" + csv_file + " @DATA_STAGE auto_compress=true" 
>>conn.execute(sql)
>>sql = """copy into RECOVERED_DATA from @DATA_STAGE/recovered_timeseries.csv.gz file_format = (type = "csv" field_delimiter = "," skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)""" \ """ON_ERROR = "ABORT_STATEMENT" """
>>conn.execute(sql)</pre>

Now we have the data we will be needed in our prediction as well as visualization .In the next part we will train our model for the prediction of covid cases.

Snowflake Features included in this part:

* Interface support
* Working on semi-structured data
* Staging
