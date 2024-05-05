import click
import requests
import json
import redshift_connector
import uuid
import config.credentials as cred

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
    except:
        print('An error occurred when connecting to database')
        print('Finished process with error')
        exit()
    create_table(conn)
    sites = ['br', 'ar', 'cl']
    for site in sites:
        data = get_site_data(site)
        r = requests.post(endpoint, data=data, headers=header)
        click.echo(f"Status code: {r.status_code}, reason: {r.reason}")
        insert_information(conn, r.status_code, r.json())
    print('Finished process')

def new_db_connection():
    conn = redshift_connector.connect(
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


def insert_information(conn, status_code, data):
    cursor = conn.cursor()
    id = str(uuid.uuid4())

    try:
        if status_code == 200:
            region = data['regionCode']
            aqi = data['indexes'][0]['aqi']
            co = data['pollutants'][0]['concentration']['value']
            no2 = data['pollutants'][1]['concentration']['value']
            o3 = data['pollutants'][2]['concentration']['value']
            cursor.execute("insert into airquality_data (id, region, aqi, co, no2, o3, status, date) "
                           "values (%s, %s, %s, %s, %s, %s, %s, current_date)",
                           (id, region, aqi, co, no2, o3, 'SUCCESS'))
            print('Data inserted')
        else:
            cursor.execute("insert into airquality_data (id, status, date) "
                           "values (%s, 'ERROR', current_date)",
                           id)
            print('An error ocurred when getting information')
        conn.commit()
    except:
        print('An error ocurred when connect to database')



if __name__ == '__main__':
    main()
