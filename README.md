# data_engineering

Install the following modules
- pip install click
- pip install requests
- pip install json
- pip install redshift_connector
- pip install uuid

Set up connection to redshift
- Configure _user_ and _password_ in method new_db_connection()

Enable service: airquality.googleapis.com
- https://console.cloud.google.com/apis/library/airquality.googleapis.com
- Configure _key_ in variable _endpoint_
