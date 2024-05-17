import click
import requests
import json
import psycopg2
import uuid
import pandas as pd
from config import config as cred

endpoint = 'https://airquality.googleapis.com/v1/currentConditions:lookup?key=' + cred.key
header = {
    'content-type': 'application/json',
    'Accept-Charset': 'UTF-8',
}


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


def main():
    try:
        conn = new_db_connection()
    except Exception as e:
        print('An error occurred when connecting to database: ')
        print(e)
        print('Finished process with error')
        exit()
    create_table(conn)
    sites = ['br', 'ar', 'cl']
    data_all_sites = []
    for site in sites:
        data = get_site_data(site)
        r = requests.post(endpoint, data=data, headers=header)
        click.echo(f"Status code: {r.status_code}, reason: {r.reason}")
        if r.status_code == 200:
            data_json = r.json()
            data_all_sites.append(data_json)
        # else:
        ## TODO:handle error
    insert_information(conn, data_all_sites)
    conn.close()
    print('Finished process')


def new_db_connection():
    conn = psycopg2.connect(
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        database='data-engineer-database',
        port=5439,
        user=cred.db_user,
        password=cred.db_password
    )
    return conn


def create_table(conn):
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS paolaverdes_coderhouse.airquality_data ( '
                   'id VARCHAR(256)   ENCODE lzo'
                   ',region VARCHAR(256)   ENCODE lzo'
                   ',aqi INTEGER   ENCODE az64'
                   ',co NUMERIC(18,0)   ENCODE az64'
                   ',no2 NUMERIC(18,0)   ENCODE az64'
                   ',o3 NUMERIC(18,0)   ENCODE az64'
                   ', status VARCHAR(256)   ENCODE lzo'
                   ',date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64);')
    conn.commit()


def insert_information(conn, data_all_sites):
    cursor = conn.cursor()
    df = pd.json_normalize(data_all_sites)
    print(df)
    print(df.columns)
    df.to_csv('data_entregable_1.csv', index=False)

    for i in range(len(df)):
        id = str(uuid.uuid4())
        region = df.iloc[i]['regionCode']
        aqi = df.iloc[i]['indexes'][0]['aqi']
        co = df.iloc[i]['pollutants'][0]['concentration']['value']
        no2 = df.iloc[i]['pollutants'][1]['concentration']['value']
        o3 = df.iloc[i]['pollutants'][2]['concentration']['value']
        cursor.execute("insert into airquality_data (id, region, aqi, co, no2, o3, status, date) "
                       "values (%s, %s, %s, %s, %s, %s, %s, current_date)",
                       (id, region, aqi, co, no2, o3, 'SUCCESS'))
    conn.commit()
    print('Data inserted')

if __name__ == '__main__':
    main()
