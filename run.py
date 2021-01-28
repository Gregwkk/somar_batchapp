# -*- coding: utf-8 -*-
from ftplib import FTP
import zipfile
import pandas as pd
from io import StringIO
from pycarol import Carol, Staging, ApiKeyAuth
import locale
from datetime import datetime
import re
import requests
from dotenv import load_dotenv
from cidade import cidades
from fuzzywuzzy import process
from fuzzywuzzy import fuzz

load_dotenv(".env") #this will import these env variables to your execution.


print(datetime.now())

def getLatLong(location):

    def isCity(object):
        allowed = ['city', 'village','neighbourhood']
        return object['components']['_type'] in allowed
       
    
    URL = 'https://api.opencagedata.com/geocode/v1/json'
    APIkey = '2768333f0136406c95c3f0de385370c7'
    PARAMS = {'q':location, 'key':APIkey}
    data = requests.get(url = URL, params = PARAMS).json()['results']
    data = list(filter(isCity, data))
    lat = data[0]['geometry']['lat']
    lng = data[0]['geometry']['lng']
    geopoint = str(lat)+','+str(lng)
    return geopoint

locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
#locale.setlocale(locale.LC_ALL, "Portuguese_Brazil.1252")

ftp = FTP('ftp.somarmeteorologia.com.br')
ftp.login(user='noble', passwd='n0bl3@2015')

# ftp.retrlines('LIST')  # list directory contents

with open('noble_prev_csv.zip', 'wb') as fp:
    ftp.retrbinary('RETR noble_prev_csv.zip', fp.write)

ftp.quit()

df = pd.DataFrame(
    {"cidade_estado": [],
     "data": [],
     "temp_min": [],
     "temp_max": [],
     "precipitacao": []
     },
    index=[])

with zipfile.ZipFile('noble_prev_csv.zip', 'r') as f:
    for name in f.namelist():
        data = f.read(name)
        dataframed = pd.read_csv(StringIO(data.decode(
            'utf-8')), names=["cidade_estado", "data", "temp_min", "temp_max", "precipitacao"])
        # print(name, len(data))
        df = pd.concat([df, dataframed])

df['data'] = df['data'].apply(lambda data: datetime.strptime(data+'/'+str(datetime.today().year),'%d/%b/%Y'))

# Tira Argentina e Paraguai
df = df[~df["cidade_estado"].str.contains('AR', na=False)]
df = df[~df["cidade_estado"].str.contains('PY', na=False)]

# df = df.head(20)
# # Coloca espaço entre os nomes das cidades
df['cidade_estado'] = df['cidade_estado'].apply(lambda cidade_estado: re.sub(r'([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))', r'\1 ',cidade_estado))

# #Fuzzy Matching (Levenshtein) no nome da cidade
listaDeCidades = cidades()

cities = pd.DataFrame(df.cidade_estado.dropna().unique(), columns=['cidade_estado'])
# cities = pd.read_csv("cities.csv")

cities['cidade_estado_corrigido'] = cities['cidade_estado'].apply(lambda cidade_estado: process.extractOne(cidade_estado, listaDeCidades,scorer=fuzz.ratio)[0])



cities['geopoint'] = cities['cidade_estado_corrigido'].apply(lambda cidade_estado_corrigido: getLatLong(cidade_estado_corrigido))
# cities.to_csv('cities.csv',encoding='utf-8', index=False)


df = pd.merge(df,cities,how='left',on='cidade_estado')
# df = pd.read_csv("df.csv")
# df.to_csv('df.csv',encoding='utf-8', index=False)


# print(df)

login = Carol()
staging = Staging(login)
staging.send_data(staging_name='meteorologia', data=df, step_size=500,
                connector_id='16d21d5cb6034dd4b846dc43d1c543e5', print_stats=True)
print(datetime.now())




