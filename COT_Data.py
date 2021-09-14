#Program reads most recent CFTC COT Commitments of Traders Report
#Historical year (2020) is previously downloaded and appended
#Selected market data is filtered out
import pandas as pd
import requests
import zipfile
from datetime import datetime
import json
import os

api_token = os.environ.get('PA_API_TOKEN')
cot_ng = os.environ.get('LINK_COT_NG')
cot_wti = os.environ.get('LINK_COT_WTI')
path_to_data = os.environ.get('PATH_TO_COT_DATA_FILES')

print(path_to_data)

#Download most recent data
url = 'http://www.cftc.gov/files/dea/history/fut_disagg_txt_2021.zip'
resp = requests.get(url)

with open(path_to_data + '/fut_disagg_txt_2021.zip', 'wb') as foutput:
   foutput.write(resp.content)

#Unzip the file
zip_file = path_to_data + '/fut_disagg_txt_2021.zip'

try:
    with zipfile.ZipFile(zip_file) as z:
        z.extractall(path = path_to_data + '/')
        print("Extracted all")
except:
    print("Invalid file")

#Historical CFTC COT Data found at:
# https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm
data2021 = pd.read_csv(path_to_data + '/f_year.txt', usecols=[0,2,7,8,9,13,14,16,17,21,22])
data2020 = pd.read_csv(path_to_data + '/f_year2020.txt', usecols=[0,2,7,8,9,13,14,16,17,21,22])

#Clean up column names into something shorter
data2021.columns = ["Market", "Date", "OI", "Commercial Long", "Commercial Short", "Managed Long", "Managed Short", "Other Long", "Other Short", "NonRept Long", "NonRept Short"]
data2020.columns = ["Market", "Date", "OI", "Commercial Long", "Commercial Short", "Managed Long", "Managed Short", "Other Long", "Other Short", "NonRept Long", "NonRept Short"]

#Dates read from csv as strings. Convert to datetime
data2021['Date'] = pd.to_datetime(data2021['Date'])
data2020['Date'] = pd.to_datetime(data2020['Date'])

#Append the dataframes to each other
dataCombined = data2021.append(data2020)

#Filter out markets of interest
crude = dataCombined[dataCombined.Market == 'CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE']
NG = dataCombined[dataCombined.Market == 'NATURAL GAS - NEW YORK MERCANTILE EXCHANGE']


#Update the database if needed - WTI
#convert dataframe colums to lists
commercial = (crude['Commercial Long']-crude['Commercial Short']).tolist()
managed = (crude['Managed Long']-crude['Managed Short']).tolist()
other = (crude['Other Long']-crude['Other Short']).tolist()
datetime = crude['Date'].tolist()
OI = crude['OI'].tolist()

#Convert date list from timestamp objects to strings and get latest date from COT data
date = []
for item in datetime:
    date.append(str(item.date()))

current_date = date[0]

#Get the last date from database
url = cot_wti
data = requests.get(url)
cot_oil_db = json.loads(data.text)
last_date = cot_oil_db[-1]['date']

#if current_date = last_date, do nothing, else update the python anywhere database
if current_date == last_date:
    print('current_date:', current_date, 'is equal to last_date:', last_date, '- Database not updated')

else:

    headers = {'Authorization': api_token}

    payload = {
    'date': current_date,
    'commercial': commercial[0],
    'managed': managed[0],
    'other': other[0],
    'OI': OI[0],
    }

    resp = requests.post(url, headers=headers, data=payload)
    print(resp)

#Update the database if needed - NG
commercial = (NG['Commercial Long']-NG['Commercial Short']).tolist()
managed = (NG['Managed Long']-NG['Managed Short']).tolist()
other = (NG['Other Long']-NG['Other Short']).tolist()
datetime = NG['Date'].tolist()
OI = NG['OI'].tolist()

date = []
for item in datetime:
    date.append(str(item.date()))

current_date = date[0]

#Get the last date from database
url = cot_ng
data = requests.get(url)
cot_ng_db = json.loads(data.text)
last_date = cot_ng_db[-1]['date']

if current_date == last_date:
    print('current_date:', current_date, 'is equal to last_date:', last_date, '- Database not updated')

else:

    headers = {'Authorization': api_token}

    payload = {
    'date': current_date,
    'commercial': commercial[0],
    'managed': managed[0],
    'other': other[0],
    'OI': OI[0],
    }

    resp = requests.post(url, headers=headers, data=payload)
    print(resp)

