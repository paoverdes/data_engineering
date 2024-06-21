# data_engineering

Enable service: airquality.googleapis.com
- https://console.cloud.google.com/apis/library/airquality.googleapis.com

Docker commands
- Docker version: _docker --version_
- Docker Compose version: _docker-compose --version_
- Generate the image: _docker build -t docker_image ._
- List images:  _docker images -a_
- Run image: _docker run docker_image_
- List containers: _docker ps -a_
- Containers in execution: _docker ps_
- Remove containers: _docker rm {cointainer_id}_

Apache Airflow commands
- Initialize airflow _docker-compose up airflow-init_
-  _docker-compose up_
- Config the following variables:
  - db_user
  - secret_db_password
  - email
  - secret_email_password
  - secret_key_api (from airquality.googleapis.com)

Doing the backfilling
- bash: _docker exec -it data_engineering-airflow-scheduler-1 bash_
- execute the following command: _airflow dags backfill -s {start_date} -e {finish_date} DAG_LOAD_INFORMATION_WORKFLOW_
