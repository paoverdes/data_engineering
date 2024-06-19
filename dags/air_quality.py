from airflow.models import Variable

import click
import requests
import json
import sqlalchemy as db
import pandas as pd
import smtplib
from datetime import date

endpoint = 'https://airquality.googleapis.com/v1/currentConditions:lookup?key=' + Variable.get("secret_key_api")
header = {
    'content-type': 'application/json',
    'Accept-Charset': 'UTF-8',
}

sites_api = ['br', 'ar', 'cl']

# Alert config
min_aqi_to_alert = 50
sites_for_alert = ['br', 'ar', 'cl']

def create_connection_and_structure():
    conn = new_db_connection()
    create_table(conn)
    conn.close()

def new_db_connection():
    try:
        host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
        port = 5439
        database = 'data-engineer-database'
        route = 'postgresql://' + Variable.get("db_user") + ':' + Variable.get(
            "secret_db_password") + '@' + host + ':' + str(port) + '/' + database
        engine = db.create_engine(route)
        conn = engine.connect()
        return conn
    except Exception as e:
        print('Finished process with error: An error occurred when connecting to database: ')
        print(e)
        exit()

def process_air_quality_data():
    sites = sites_api
    data_all_sites = []
    for site in sites:
        data = get_site_data(site)
        r = requests.post(endpoint, data=data, headers=header)
        click.echo(f"Status code: {r.status_code}, reason: {r.reason}")
        if r.status_code == 200:
            data_json = r.json()
            data_all_sites.append(data_json)
        else:
            print('An error occurred getting information for region: ' + site)
            print(r.reason)
    insert_information(data_all_sites)
    print('Finished process')

def get_site_data(site):
    latitude = 0
    longitude = 0
    if site == 'ar':
        latitude = -34.61315
        longitude = -58.37723
    if site == 'cl':
        latitude = -33.447487
        longitude = -70.673676
    if site == 'br':
        latitude = -15.793889
        longitude = -47.882778

    return json.dumps({
        "location": {
            "latitude": latitude,
            "longitude": longitude
        },
        "extraComputations": [
            "HEALTH_RECOMMENDATIONS",
            "DOMINANT_POLLUTANT_CONCENTRATION",
            "POLLUTANT_CONCENTRATION",
            "LOCAL_AQI",
            "POLLUTANT_ADDITIONAL_INFO"
        ],
        "languageCode": "es"
    })

def create_table(conn):
    conn.execute('CREATE TABLE IF NOT EXISTS paolaverdes_coderhouse.airquality_data ( '
                   'key VARCHAR(256) NOT NULL'
                   ',region VARCHAR(256)'
                   ',aqi INTEGER'
                   ',co NUMERIC(18,0)'
                   ',no2 NUMERIC(18,0)'
                   ',o3 NUMERIC(18,0)'
                   ', status VARCHAR(256)'
                   ',date DATE,'
                   'PRIMARY KEY (key));')
    conn.execute('CREATE TABLE IF NOT EXISTS paolaverdes_coderhouse.airquality_data_temp ( '
                   'key VARCHAR(256) NOT NULL'
                   ',region VARCHAR(256)'
                   ',aqi INTEGER'
                   ',co NUMERIC(18,0)'
                   ',no2 NUMERIC(18,0)'
                   ',o3 NUMERIC(18,0)'
                   ', status VARCHAR(256)'
                   ',date DATE'
                   ');')

def insert_information(data_all_sites):
    conn = new_db_connection()
    df = pd.json_normalize(data_all_sites)
    print(df)
    print(df.columns)
    df.to_csv('data_entregable_1.csv', index=False)

    for i in range(len(df)):
        region = df.iloc[i]['regionCode']
        aqi = df.iloc[i]['indexes'][0]['aqi']
        co = df.iloc[i]['pollutants'][0]['concentration']['value']
        no2 = df.iloc[i]['pollutants'][1]['concentration']['value']
        o3 = df.iloc[i]['pollutants'][2]['concentration']['value']
        key = region + '-' + str(date.today())
        conn.execute("insert into airquality_data_temp (key, region, aqi, co, no2, o3, status, date) "
                       "values (%s, %s, %s, %s, %s, %s, %s, current_date)",
                       (key, region, aqi, co, no2, o3, 'SUCCESS'))
        conn.execute("merge into airquality_data "
                       "USING airquality_data_temp as temp "
                       "ON airquality_data.key = temp.key "
                       "WHEN MATCHED THEN UPDATE SET "
                       "aqi = temp.aqi, co = temp.co, no2 = temp.no2, o3 = temp.o3, status = temp.status "
                       "WHEN NOT MATCHED THEN INSERT (key, region, aqi, co, no2, o3, status, date) "
                       "values (%s, %s, %s, %s, %s, %s, %s, current_date) ",
                       (key, region, aqi, co, no2, o3, 'SUCCESS'))
        conn.execute("delete from airquality_data_temp;")
    conn.close()
    print('Data inserted')

def verify_information():
    conn = new_db_connection()
    query = 'select distinct region , aqi, date, level, min_val, max_val ' \
            'from airquality_data ad join airquality_index ai ' \
            'on ad.aqi >= ai.min_val ' \
            'and ad.aqi <= ai.max_val where ad.date = current_date  '

    df = pd.read_sql_query(query, conn)
    print(df)

    for i in range(len(df)):
        region = df.iloc[i]['region']
        level = df.iloc[i]['level']
        aqi = df.iloc[i]['aqi']
        min_val = df.iloc[i]['min_val']
        if min_val > min_aqi_to_alert and region in sites_for_alert:
            send_email(region, level, aqi)
    conn.close()

def send_email(region, level, aqi):
    x = smtplib.SMTP('smtp.gmail.com', 587)
    x.starttls()
    x.login(Variable.get("email"), Variable.get("secret_email_password"))
    subject = 'Anomaly Detected'
    body_text = 'Anomaly Detected in region: ' + region + ', with level: ' + level + ', and aqi: ' + str(aqi)
    message = 'Subject: {}\n\n{}'.format(subject, body_text)
    x.sendmail(Variable.get("email"), Variable.get("email"), message)
