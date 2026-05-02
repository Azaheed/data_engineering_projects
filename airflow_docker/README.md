# Apache Airflow Starter Guide

## Setup Airflow
The docker compose file is pretty much complete and only needs to be initialized. I am attaching some reference videos for a better understanding of what the file entails. However if you want to skip it and get to the good stuff, follow the following steps:
1. docker-compose up airflow-init
2. docker-compose up

### Updated Tutorial Episode
1. [Introduction and Local Installation](https://www.youtube.com/watch?v=z7xyNOF8tak&list=PLwFJcsJ61oujAqYpMp1kdUBcPG0sE0QMT&index=1)
2. [Get Airflow running in Docker](https://www.youtube.com/watch?v=J6azvFhndLg&list=PLwFJcsJ61oujAqYpMp1kdUBcPG0sE0QMT&index=2)

## Running apache airflow 2.0 in docker with local executor.
Here are the steps to take to get airflow 2.0 running with docker on your machine. 
1. Clone this repo
2. It is important to note that we are running on a local executor, for those who want a production grade executor, look up the Redist service and the Celery Executor. These configs have been commented out of the yaml file.
3. Open browser and type http://0.0.0.0:8080 to launch the airflow webserver
