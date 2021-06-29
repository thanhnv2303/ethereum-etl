# Ethereum ETL

## Running in Docker

1. Install Docker https://docs.docker.com/install/
2. Install Docker compose https://docs.docker.com/compose/install/
3. Build a docker image

        > docker-compose build 
4. Create file .env look for example at file example.env

5. Run a docker compose file

        > docker-compose up

## Projects using Ethereum ETL

* [Google](https://goo.gl/oY5BCQ) - Public BigQuery Ethereum datasets
* [Nansen](https://www.nansen.ai/?ref=ethereumetl) - Analytics platform for Ethereum

## Run for dev

1. Install dependencies
   > pip3 install -r requirements.txt
2. Create mongodb
   > cp example.env .env

   > docker-compose up mongo-export

3. Quick run for dev

   > python3 ./quick_run/build_knowledge_graph.py

### connect mongodb for example

1. Connect hamachi network
    
   > Network id: connect_db
   > Password : bkc@123
2. Connect mongodb (Read Only)
   

   Connection string

> main net: 
> > mongodb://readUser:bkc_123@25.39.155.190:27047/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false

> test net: 
> > mongodb://readUser:bkc_123@25.39.155.190:27037/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false
   