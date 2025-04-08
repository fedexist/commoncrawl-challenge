# CommonCrawl challenge

## How to run this project

### Pre-requisites

* Make sure you have at least Python 3.12 installed
* Install [uv](https://docs.astral.sh/uv/getting-started/installation/), if you don't have it on your system, since it's used as package manager for this project.

### Setup

* Create a free account on [WhoisXMLAPI](https://whois.whoisxmlapi.com/), get an API key and paste it in the appropriate environment variable in your .env

### Run locally

* Create a .env file using example.env as base
* make install will create a virtual environment and install the dependencies
* docker-compose up postgres -d, will set up a local instance of PostgreSQL running on docker

### Run on local hosted airflow

* Create an airflow.env file using example.env as base
* docker-compose up -d, will set up an airflow instance, together with the db
* Wait a couple of minutes for the initialization to suceed
* Access localhost:8080 with credentials admin:admin
* Enable the cc_pipeline_dag
* Run it manually

chmod -R 777 output
chmod -R 777 commoncrawl

## TODO for production readiness

1. Proper secret management via environment variables injection: for simplicity purpose I am hardcoding all passwords
2. Regularly update data/hosts for ad based domain detection
3. Have a sensor triggering the airflow pipeline whenever a new partition is detected
