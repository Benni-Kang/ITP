import datetime
import io
import json
import time
import pandas as pd
import requests
from flask import Flask
from bs4 import BeautifulSoup

app = Flask(__name__)


def getDailyConfirmed():
    # URL of the data file (time_series_covid19_confirmed_global.csv) from CSSE at Johns Hopkins University GitHub
    url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/" \
          "csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"

    # Fetch data from the GitHub
    global_confirmed_data = requests.get(url).content

    # Read the file and turn it in the dataframe type and set the first row as the column name
    df = pd.read_csv(io.StringIO(global_confirmed_data.decode('utf-8')), header=0, names=None)

    # Get the Hong Kong time series data according to the Province/State value and
    # Get all the columns except the first 4 columns and
    # Rename its row name as confirmed
    totalConfirmed_data = ((df.loc[df['Province/State'] == 'Hong Kong']).iloc[:, 4:]).rename(index={71: 'confirmed'})

    # Calculate the daily confirmed cases
    dailyConfirmed_data = totalConfirmed_data.copy(deep=True)
    for i in range(len(dailyConfirmed_data.columns) - 1, 0, -1):
        dailyConfirmed_data.iloc[0, i] -= dailyConfirmed_data.iloc[0, i - 1]

    # Print the total confirmed cases on each day and daily confirmed cases
    print(totalConfirmed_data)
    print(dailyConfirmed_data)
    return dailyConfirmed_data


def getVaccinations():
    # URL of the data file (vaccinations-by-age-group.csv) from Our World in Data on GitHub
    url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/" \
          "vaccinations/vaccinations-by-age-group.csv"

    # Fetch data from the GitHub
    vaccinations_data = requests.get(url).content

    # Read the file and turn it in the dataframe type and set the first row as the column name
    df = pd.read_csv(io.StringIO(vaccinations_data.decode('utf-8')), header=0, names=None)

    # Get the data about Hong Kong and get the transpose of the dataset
    vaccinations_data = ((df.loc[df['location'] == 'Hong Kong']).iloc[:, 1:6]).T.iloc[:, 5:]

    # Reset the columns' names
    vaccinations_data.columns = list(vaccinations_data.iloc[0])

    # Drop the original column name "date"
    vaccinations_data = vaccinations_data.drop("date", axis=0)

    # Get the first day of vaccination which has 8 columns (8 age groups)
    each_day = vaccinations_data.iloc[:, :8]

    # Get and set the date to the same format as the confirmed cases
    date = list(map(int, each_day.columns[0].split("-")))
    date = "/".join(list(map(str, [date[1] % 100, date[2] % 100, date[0] % 100])))

    # Reset the rows' name
    each_day.columns = list(each_day.iloc[0])

    # Drop the age_group row and reset the index as default value
    each_day = each_day.drop("age_group", axis=0).reset_index()

    # Conduct the dataframe from 3*8 to 24*2
    each_day = each_day.set_index(['index']).stack().reset_index()
    each_day.columns = ['type', 'age', date]

    # Merge the type and age group together
    each_day.insert(0, "type_age", each_day['type'] + "_" + each_day['age'])

    # Drop the two rows and initialize the dataframe named "vaccinations_set" to store the processed vaccinations data
    vaccinations_set = each_day.drop(["type", "age"], axis=1)

    # As same as the previous steps and process all the data
    for i in range(16, vaccinations_data.shape[1], 8):
        each_day = vaccinations_data.iloc[:, (i - 8):i]
        date = list(map(int, each_day.columns[0].split("-")))
        date = "/".join(list(map(str, [date[1] % 100, date[2] % 100, date[0] % 100])))
        each_day.columns = list(each_day.iloc[0])
        each_day = each_day.drop("age_group", axis=0).reset_index()
        each_day = each_day.set_index(['index']).stack().reset_index()
        each_day.columns = ['type', 'age', date]
        vaccinations_set.insert(vaccinations_set.shape[1], date, each_day[date])

    # Print the processed data, the format of data is [row index: different age groups and types][column names: date]
    print(vaccinations_set)
    return vaccinations_set


def getLeavingArrivals():
    # Create an empty dataframe
    leavingArrivals_data = pd.DataFrame({"1/23/20": {"residentsArrival": 0, "mainlandArrival": 0, "otherArrival": 0,
                                                     "totalArrival": 0, "residentsDeparture": 0, "mainlandDeparture": 0,
                                                     "otherDeparture": 0, "totalDeparture": 0}})
    # Set the beginning and end time date
    begin = datetime.date(2020, 1, 24)
    today = time.localtime()
    end = datetime.date(today.tm_year, today.tm_mon, today.tm_mday)

    # Get all the data from government website
    for i in range((end - begin).days):
        # Set the date time
        date = begin + datetime.timedelta(days=i)

        # Get the web info from government web with request
        source = requests.get("https://www.immd.gov.hk/hkt/stat_" + str(date).replace("-", "") + ".html")

        # Decoding the info so that the chinese shows normally
        source.encoding = source.apparent_encoding

        # Create a parser with BeautifulSoup
        parser = BeautifulSoup(source.text, 'html.parser')

        # Find all the "<tr>" stored the needed data with class name "p tr-boldText"
        parser_tr = parser.findAll(name="tr", attrs={"class": "p tr-boldText"})

        # Some of them named "q tr-boldText"
        if len(parser_tr) == 0:
            parser_tr = parser.findAll(name="tr", attrs={"class": "q tr-boldText"})

        # Find the needed data one by one with parser get_value() method and also delete the "," inside the number
        residentsArrival = parser_tr[0].find(name='td', attrs={
            "headers": "Hong_Kong_Residents_Arrival"}).get_text().replace(",", "")
        mainlandArrival = parser_tr[0].find(name='td', attrs={
            "headers": "Mainland_Visitors_Arrival"}).get_text().replace(",", "")
        otherArrival = parser_tr[0].find(name='td', attrs={
            "headers": "Other_Visitors_Arrival"}).get_text().replace(",", "")
        totalArrival = parser_tr[0].find(name='td', attrs={
            "headers": "Total_Arrival"}).get_text().replace(",", "")
        residentsDeparture = parser_tr[0].find(name='td', attrs={
            "headers": "Hong_Kong_Residents_Departure"}).get_text().replace(",", "")
        mainlandDeparture = parser_tr[0].find(name='td', attrs={
            "headers": "Mainland_Visitors_Departure"}).get_text().replace(",", "")
        otherDeparture = parser_tr[0].find(name='td', attrs={
            "headers": "Other_Visitors_Departure"}).get_text().replace(",", "")
        totalDeparture = parser_tr[0].find(name='td', attrs={
            "headers": "Total_Departure"}).get_text().replace(",", "")

        # Store them into the dataframe with the same column name as dailyConfirmed_data
        # and convert the type of data to int
        leavingArrivals_data.insert(leavingArrivals_data.shape[1],
                                    str(date.month) + "/" + str(date.day) + "/" + str(date.year),
                                    [int(residentsArrival), int(mainlandArrival), int(otherArrival), int(totalArrival),
                                     int(residentsDeparture),
                                     int(mainlandDeparture), int(otherDeparture), int(totalDeparture)])

    # Delete the first column which is meaningless
    leavingArrivals_data = leavingArrivals_data.drop(columns="1/23/20")

    # Print the leaving and arrivals on each day
    print(leavingArrivals_data)
    return leavingArrivals_data


# Return the data in json format
@app.route("/")
def dailyConfirmed():
    return json.loads(getDailyConfirmed().to_json(orient='columns'))


app.run('127.0.0.1', port=5050)
