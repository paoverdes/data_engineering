# data_engineering

Install the following modules
- pip install click
- pip install requests
- pip install json
- pip install psycopg2
- pip install pandas
- pip install SQLAlchemy==1.4.38

Enable service: airquality.googleapis.com
- https://console.cloud.google.com/apis/library/airquality.googleapis.com

Set up connections
- Create the file config.py in config file
  - Configure _key_ value from airquality
  - Configure _db_user_ and _db_password_ to use redshift

To run the project
- _python3 main.py_
