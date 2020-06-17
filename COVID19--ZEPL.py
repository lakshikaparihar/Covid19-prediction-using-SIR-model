conn = z.getDatasource("snowflake_CP99894apsouth1aws_faa2e8")

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import io
from datetime import datetime
import scipy
import operator
from scipy.integrate import solve_ivp
from scipy.integrate import odeint

warehouse = "PC_ZEPL_WH"
database = "PC_ZEPL_DB"
stage_table = "data_stage"


def transp(data, file_name):
    data = data[data["Country/Region"] == "India"]
    data = data.drop(["Province/State", "Country/Region", "Lat", "Long"], axis=1)
    data = data.T
    data.to_csv(file_name)


# recovered people data
read_csv = requests.get(
    "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv").content
recovered_df = pd.read_csv(io.StringIO(read_csv.decode('utf-8')))
transp(recovered_df, "recovered_timeseries.csv")
recovered_df = pd.read_csv("recovered_timeseries.csv")
recovered_df.columns = ["Date", "Recover"]
recovered_df["Date"] = (pd.to_datetime(recovered_df["Date"])).dt.strftime("%Y-%m-%d")
recovered_df.to_csv("recovered_timeseries.csv", index=False)
recovered_df = pd.read_csv("recovered_timeseries.csv")

# confirmed and deaths
url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
read_csv = requests.get(url).content
address = pd.read_csv(io.StringIO(read_csv.decode('utf-8')))
address.to_csv("covid_data.csv", index=False)
address = pd.read_csv("covid_data.csv")


# print(address.head())


def execute_result(connection, query):
    final_result = connection.execute(query).fetchall()
    return final_result


try:
    sql = 'use role {}'.format('PC_ZEPL_ROLE')
    conn.execute(sql)

    sql = 'use database {}'.format(database)
    conn.execute(sql)

    sql = 'use warehouse {}'.format(warehouse)
    conn.execute(sql)

    sql = 'use schema {}'.format('PUBLIC')
    conn.execute(sql)

    try:
        sql = 'alter warehouse {} resume'.format(warehouse)
        conn.execute(sql)
    except:
        pass

    try:
        sql = 'drop table covid_data'
        conn.execute(sql)
        print("6")
    except:
        pass

    sql = 'create table covid_data(iso_code VARCHAR, ' \
          'continent VARCHAR ,location VARCHAR ,date DATE, total_cases INT ,new_cases Double, total_deaths INT,new_deaths INT,' \
          'total_cases_per_million DOUBLE, new_cases_per_million DOUBLE , total_deaths_per_million DOUBLE,' \
          ' new_deaths_per_million DOUBLE,total_tests INT,new_tests INT,new_test_smoothed DOUBLE, total_tests_per_thousand DOUBLE,' \
          'new_tests_per_thousand DOUBLE ,new_test_smoothed_per_thousand DOUBLE, tests_units VARCHAR,stringency_index BIGINT,population DOUBLE ,population_density DOUBLE ,' \
          'median_age DOUBLE ,aged_65_older DOUBLE ,aged_70_older DOUBLE ,gdp_per_capita DOUBLE ,' \
          'extreme_poverty DOUBLE,cvd_death_rate DOUBLE ,diabetes_prevalence DOUBLE , female_smokers DOUBLE ' \
          ',male_smokers DOUBLE ,handwashing_facilities DOUBLE , hospital_beds_per_100k DOUBLE)'
    conn.execute(sql)
    # print("7")

    try:
        sql = 'drop stage if exists data_stage'
        conn.execute(sql)
    except:
        pass

    sql = """create stage data_stage file_format = (type = "csv" field_delimiter = "," skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')"""
    conn.execute(sql)

    csv_file = 'covid_data.csv'
    sql = "PUT file://" + csv_file + " @DATA_STAGE auto_compress=true"
    conn.execute(sql)

    sql = """copy into covid_data from @DATA_STAGE/covid_data.csv.gz file_format = (type = "csv" field_delimiter = "," skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)""" \
          """ON_ERROR = "ABORT_STATEMENT" """
    conn.execute(sql)
    # print("11")

    try:
        sql = 'DROP TABLE INDIA_DATA'
        conn.execute(sql)
        # print(12)
    except:
        pass

    sql = "CREATE TABLE INDIA_DATA AS SELECT * FROM COVID_DATA WHERE LOCATION = 'India' AND Date > '2020-01-30'"
    conn.execute(sql)
    # print(13)

    try:
        sql = 'DROP TABLE RECOVERED_DATA'
        conn.execute(sql)
        # print(12)
    except:
        pass

    sql = "CREATE TABLE RECOVERED_DATA(date DATE,recover INT)"
    conn.execute(sql)
    print("8")

    csv_file = 'recovered_timeseries.csv'
    sql = "PUT file://" + csv_file + " @DATA_STAGE auto_compress=true"
    conn.execute(sql)
    print("10")

    sql = """copy into RECOVERED_DATA from @DATA_STAGE/recovered_timeseries.csv.gz file_format = (type = "csv" field_delimiter = "," skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)""" \
          """ON_ERROR = "ABORT_STATEMENT" """
    conn.execute(sql)
    print("11")

    sql = "SELECT TOTAL_CASES+NEW_CASES FROM INDIA_DATA ORDER BY DATE DESC LIMIT 12"
    res = execute_result(conn, sql)
    confirmed = [item for t in res for item in t]
    confirmed.sort()

    sql = "SELECT RECOVER FROM RECOVERED_DATA ORDER BY DATE DESC LIMIT 1"
    res = execute_result(conn, sql)
    recovered = [item for t in res for item in t]

    sql = "SELECT DATE FROM RECOVERED_DATA ORDER BY DATE DESC LIMIT 1"
    res = execute_result(conn, sql)
    start_date = [item for t in res for item in t]
    start_date = start_date[0]

    sql = "SELECT TOTAL_DEATHS FROM INDIA_DATA ORDER BY DATE DESC LIMIT 1"
    res = execute_result(conn, sql)
    deaths = [item for t in res for item in t]

    sql = "select DISTINCT(POPULATION) from INDIA_DATA"
    res = execute_result(conn, sql)
    population = [item for t in res for item in t]
    population = population[0]

    # model creation
    sigma = 4.4
    beta_sum = 0
    recovery_rate = 1 / 14
    for i in range(10):
        cases_day_1 = confirmed[i]
        cases_day_2 = confirmed[i + 1]
        cases_day_3 = confirmed[i + 2]
        infected_ratio_day_1 = cases_day_1 / population
        infected_ratio_day_2 = cases_day_2 / population
        infected_ratio_day_3 = cases_day_3 / population
        ej1 = (infected_ratio_day_2 - infected_ratio_day_1 + (recovery_rate * infected_ratio_day_1)) / sigma
        ej2 = (infected_ratio_day_3 - infected_ratio_day_2 + (recovery_rate * infected_ratio_day_2)) / sigma
        r1 = recovery_rate * infected_ratio_day_1
        s1 = 1 - (infected_ratio_day_1 + ej1 + r1)
        beta = (ej2 - ej1 + (sigma * ej1)) / (s1 * infected_ratio_day_1)
        beta_sum += beta

    contact_rate = beta_sum / 12

    Last_infected, Last_recovered = confirmed[-1], recovered[0]  # Initial conditions for infected and recovered people
    Last_deaths = deaths[0]

    everyone_else = population - Last_infected - Last_recovered - Last_deaths  # Initial conditions for everyone else.

    Current_condition = everyone_else, Last_infected, Last_recovered

    time = np.linspace(0, 8, num=8)


    def SIR(Current_condition, t, population, contact_rate, recovery_rate):
        S, I, R = Current_condition
        dS = -contact_rate * S * I / population
        dI = contact_rate * S * I / population - recovery_rate * I
        dR = recovery_rate * I
        return dS, dI, dR


    future_forecast_dates = []
    for i in range(9):
        future_forecast_dates.append((start_date + timedelta(days=i)).strftime("%m/%d/%Y"))
    print(future_forecast_dates)
    result = odeint(SIR, Current_condition, time, args=(population, contact_rate, recovery_rate))
    S, I, R = result.T
    prediction = set(zip(future_forecast_dates[-10:], I, R))
    prediction = pd.DataFrame(prediction)
    prediction.columns = ['date', 'Infected', 'recovered']
    # print(prediction.head())
    minValue = prediction['date'].min()
    # prediction.sort_values(by=['date'],axis=1)
    # prediction.drop(prediction.index[[0]])
    # prediction= prediction.drop(prediction['date'].idxmin())
    prediction.drop(prediction[prediction['date'] == minValue].index, inplace=True)
    # print(prediction)

    sql = 'DELETE FROM TEMP_TABLE'
    conn.execute(sql)

    # sql = 'CREATE TABLE TEMP_TABLE(date DATE,infected DOUBLE , recovered DOUBLE)'
    # conn.execute(sql)

    prediction.to_csv("prediction_data.csv", index=False)
    prediction = pd.read_csv("prediction_data.csv")

    csv_file = 'prediction_data.csv'
    sql = "PUT file://" + csv_file + " @DATA_STAGE auto_compress=true"
    conn.execute(sql)

    sql = 'copy into TEMP_TABLE from @DATA_STAGE/prediction_data.csv.gz ' \
          'file_format = (type = "csv" field_delimiter = "," skip_header = 1)' \
          'ON_ERROR = "ABORT_STATEMENT" '
    conn.execute(sql)

    # UPDATE THE PREDICTION TABLE FOR INFECTED PERSON
    # sql = "MERGE INTO PREDICTION P USING INDIA_DATA I  ON (I.DATE = P.DATE) WHEN MATCHED THEN UPDATE SET P.INFECTED = I.TOTAL_CASES+I.NEW_CASES"
    # conn.execute(sql)

    # UPDATE THE PREDICTION TABLE FOR PREDICTED DATA
    # sql = "MERGE INTO PREDICTION P USING TEMP_TABLE T ON (P.DATE = T.DATE) WHEN MATCHED THEN UPDATE SET P.PREDICTION_INFECTED = T.INFECTED WHEN NOT MATCHED THEN INSERT(DATE,PREDICTION_INFECTED) VALUES (T.DATE,T.INFECTED)"
    # conn.execute(sql)



except Exception as e:
    print(e)

